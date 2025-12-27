import 'dotenv/config'; // critical
import { Worker, Queue, Job } from 'bullmq';
import { redis } from '../lib/redis';
import clientPromise from '../lib/mongo';
import nodemailer from 'nodemailer';
import { ObjectId } from 'mongodb';
import fs from 'fs/promises';
import path from 'path';
import { URL } from 'url';

const queue = new Queue('campaigns', { connection: redis });
const MAX_ATTEMPTS = Number(process.env.MAX_ATTEMPTS || 3);

// --- Per-account / per-domain rate limiting config ---
const RATE_LIMIT_MAX = Number(process.env.EMAIL_RATE_MAX || 20);
const RATE_LIMIT_DURATION = Number(process.env.EMAIL_RATE_DURATION || 1000 * 60);
const GLOBAL_RATE_LIMIT_MAX = Number(process.env.EMAIL_GLOBAL_RATE_MAX || 200);
const GLOBAL_RATE_LIMIT_DURATION = Number(process.env.EMAIL_GLOBAL_RATE_DURATION || 1000 * 60);

const WARMUP_FACTOR = Number(process.env.EMAIL_WARMUP_FACTOR || 1.0);

const FAILURE_RATE_WARN = Number(process.env.EMAIL_FAILURE_WARN_RATE || 0.05);
const FAILURE_RATE_STRICT = Number(process.env.EMAIL_FAILURE_STRICT_RATE || 0.15);

const DOMAIN_BLOCK_TTL_SEC = Number(process.env.EMAIL_DOMAIN_BLOCK_TTL || 60 * 5);
const GLOBAL_BLOCK_TTL_SEC = Number(process.env.EMAIL_GLOBAL_BLOCK_TTL || 60 * 5);

// PUBLIC_BASE_URL must be set in .env (e.g. https://mail.example.com)
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || '').replace(/\/$/, '');
if (!PUBLIC_BASE_URL) {
  console.warn('Warning: PUBLIC_BASE_URL is not set. Tracking links will not be generated properly. Set PUBLIC_BASE_URL to your public domain or ngrok HTTPS URL and restart the worker.');
}

// --- Nodemailer transporter ---
const transporter = nodemailer.createTransport({
  host: process.env.ZOHO_HOST,
  port: Number(process.env.ZOHO_PORT || 587),
  secure: false,
  auth: {
    user: process.env.ZOHO_USER,
    pass: process.env.ZOHO_PASS,
  },
});

// exponential backoff utility
function computeRetryDelay(attempts: number) {
  const base = 60 * 1000;
  return Math.pow(2, attempts - 1) * base;
}

function jitter(ms: number) {
  const jitterPct = Math.random() * 0.4 - 0.2;
  return Math.max(1000, Math.round(ms + ms * jitterPct));
}

function randomSmallDelay() {
  return Math.round(Math.random() * 1000) + 250;
}

// Try to produce an ObjectId for 24-hex strings
function tryParseObjectId(maybeId: any) {
  if (maybeId == null) return maybeId;
  if (typeof maybeId === 'object' && maybeId instanceof ObjectId) return maybeId;
  if (typeof maybeId === 'string' && /^[0-9a-fA-F]{24}$/.test(maybeId)) {
    try {
      return new ObjectId(maybeId);
    } catch {
      return maybeId;
    }
  }
  return maybeId;
}

// Redis helpers (zset based rate limiting)
async function getWindowCount(zkey: string, windowMs: number) {
  const now = Date.now();
  try {
    await (redis as any).zremrangebyscore(zkey, 0, now - windowMs);
    const count = await (redis as any).zcard(zkey);
    return Number(count || 0);
  } catch (e) {
    console.warn('Redis zset ops failed, allowing send by default', e);
    return 0;
  }
}

async function reserveWindowSlot(zkey: string, windowMs: number, capacity: number) {
  const now = Date.now();
  await (redis as any).zremrangebyscore(zkey, 0, now - windowMs);
  const count = Number(await (redis as any).zcard(zkey) || 0);
  if (count >= capacity) return false;
  const member = `${now}:${Math.floor(Math.random() * 1000000)}`;
  await (redis as any).zadd(zkey, now, member);
  await (redis as any).pexpire(zkey, Math.max(windowMs, 1000));
  return true;
}

/**
 * Increment per-domain stats and optionally persist/publish a campaign-level deliverability snapshot.
 * If campaignId is provided, writes `campaign:{campaignId}:health` fields and publishes campaign:new with `health`.
 */
async function incrementStats(domain: string, success: boolean, campaignId?: string) {
  const statsKey = `stats:domain:${domain}`;
  if (success) {
    await redis.hincrby(statsKey, 'sent', 1);
  } else {
    await redis.hincrby(statsKey, 'failed', 1);
  }
  await (redis as any).expire(statsKey, 60 * 60 * 24);

  // If a campaignId is provided, snapshot this domain's stats onto the campaign-level health hash and publish it
  if (campaignId) {
    try {
      const failed = Number(await redis.hget(statsKey, 'failed') || 0);
      const sent = Number(await redis.hget(statsKey, 'sent') || 0);
      const total = failed + sent;
      const failRate = total === 0 ? 0 : (failed / total);
      const healthKey = `campaign:${campaignId}:health`;
      // Field names use domain-prefixed keys to be queryable; keep them compact
      await redis.hset(healthKey, `domain:${domain}:sent`, String(sent), `domain:${domain}:failed`, String(failed), `domain:${domain}:lastUpdated`, String(Date.now()));
      // Keep a short TTL to avoid unbounded bloat; health persists reasonably but will be refreshed by further sends
      await (redis as any).expire(healthKey, 60 * 60 * 24 * 7); // keep for a week

      // publish a lightweight health update so UI can render immediately
      try {
        await redis.publish('campaign:new', JSON.stringify({
          id: campaignId,
          health: { domain, sent, failed, failRate },
        }));
      } catch (e) {
        // non-fatal
      }
    } catch (e) {
      console.warn('Failed to persist/publish campaign-level health', e);
    }
  }
}

async function getDomainFailureRate(domain: string) {
  const statsKey = `stats:domain:${domain}`;
  const failed = Number(await redis.hget(statsKey, 'failed') || 0);
  const sent = Number(await redis.hget(statsKey, 'sent') || 0);
  const total = failed + sent;
  return total === 0 ? 0 : failed / total;
}

async function setDomainBlock(domain: string, ttlSeconds: number, reason?: string) {
  const key = `throttle:domain:${domain}`;
  await redis.set(key, reason || 'blocked', 'EX', ttlSeconds);
  console.warn(`Domain ${domain} blocked for ${ttlSeconds}s: ${reason || 'throttling triggered'}`);
}

async function isDomainBlocked(domain: string) {
  const key = `throttle:domain:${domain}`;
  const val = await redis.get(key);
  return !!val;
}

async function setGlobalBlock(ttlSeconds: number, reason?: string) {
  const key = `throttle:global`;
  await redis.set(key, reason || 'global_block', 'EX', ttlSeconds);
  console.warn(`Global email sending blocked for ${ttlSeconds}s: ${reason || 'throttling triggered'}`);
}

async function isGlobalBlocked() {
  const key = `throttle:global`;
  const val = await redis.get(key);
  return !!val;
}

async function tryAcquireSendPermit(domain: string) {
  if (await isGlobalBlocked()) return { ok: false, reason: 'global-blocked' };
  if (await isDomainBlocked(domain)) return { ok: false, reason: 'domain-blocked' };

  const failRate = await getDomainFailureRate(domain);
  let allowed = Math.max(1, Math.floor(RATE_LIMIT_MAX * WARMUP_FACTOR));
  if (failRate >= FAILURE_RATE_STRICT) {
    allowed = Math.max(1, Math.floor(allowed * 0.2));
  } else if (failRate >= FAILURE_RATE_WARN) {
    allowed = Math.max(1, Math.floor(allowed * 0.5));
  }

  const domainKey = `rate:domain:${domain}`;
  const reserved = await reserveWindowSlot(domainKey, RATE_LIMIT_DURATION, allowed);
  if (!reserved) {
    return { ok: false, reason: 'domain-capacity' };
  }

  try {
    const globalKey = `rate:global`;
    await (redis as any).zremrangebyscore(globalKey, 0, Date.now() - GLOBAL_RATE_LIMIT_DURATION);
    const gcount = Number(await (redis as any).zcard(globalKey) || 0);
    if (gcount >= GLOBAL_RATE_LIMIT_MAX) {
      return { ok: false, reason: 'global-capacity' };
    }
    const gmember = `${Date.now()}:${Math.floor(Math.random() * 1000000)}`;
    await (redis as any).zadd(globalKey, Date.now(), gmember);
    await (redis as any).pexpire(globalKey, Math.max(GLOBAL_RATE_LIMIT_DURATION, 1000));
  } catch (e) {
    console.warn('Global zset ops failed, allowing send by default', e);
  }

  return { ok: true };
}

/**
 * Publish a contact-level update for UI SSE
 */
async function publishContactUpdate(campaignId: string, contactId: string, payload: Record<string, any>) {
  try {
    await redis.publish(`campaign:${campaignId}:contact_update`, JSON.stringify({ campaignId, contactId, ...payload }));
  } catch (e) {
    console.warn('Failed to publish contact update', e);
  }
}

/**
 * Compute authoritative totals for a campaign.
 * Try Redis meta first (fast), fallback to DB aggregation if missing/partial.
 */
async function computeCampaignTotals(db: any, campaignIdStr: string, campaignObjId: any) {
  const redisKey = `campaign:${campaignIdStr}:meta`;
  try {
    const meta = await redis.hgetall(redisKey);
    if (meta && Object.keys(meta).length > 0) {
      const total = Number(meta.total ?? 0);
      const processed = Number(meta.processed ?? 0);
      const sent = Number(meta.sent ?? 0);
      const failed = Number(meta.failed ?? 0);
      return { total, processed, sent, failed, from: 'redis' };
    }
  } catch (e) {
    // ignore redis errors
  }

  // Fallback to DB aggregation
  try {
    const agg = await db.collection('campaign_contacts').aggregate([
      { $match: { campaignId: campaignObjId } },
      { $group: {
        _id: '$status',
        count: { $sum: 1 },
      } },
    ]).toArray();

    let sent = 0, failed = 0, pending = 0;
    for (const r of agg) {
      if (r._id === 'sent') sent = r.count;
      else if (r._id === 'failed') failed = r.count;
      else if (r._id === 'pending') pending = r.count;
    }
    const camp = await db.collection('campaigns').findOne({ _id: campaignObjId });
    const total = Number(camp?.totals?.intended ?? (sent + failed + pending));
    const processed = sent + failed;
    return { total, processed, sent, failed, from: 'db' };
  } catch (e) {
    console.warn('computeCampaignTotals fallback failed', e);
    return { total: 0, processed: 0, sent: 0, failed: 0, from: 'error' };
  }
}

/**
 * Finalize campaign status based on totals.
 * If processed >= total:
 *   - if failed > 0 => 'completed_with_failures'
 *   - else => 'completed'
 * Update DB totals, redis meta, publish event.
 */
async function finalizeCampaign(campaignIdStr: string) {
  const client = await clientPromise;
  const db = client.db('PlatformData');
  const campaignObjId = new ObjectId(campaignIdStr);

  try {
    const campaign = await db.collection('campaigns').findOne({ _id: campaignObjId });
    if (!campaign) return;

    const totals = await computeCampaignTotals(db, campaignIdStr, campaignObjId);
    const { total, processed, sent, failed } = totals;

    if (total > 0 && processed >= total) {
      const nextStatus = failed > 0 ? 'completed_with_failures' : 'completed';
      const now = new Date();

      await db.collection('campaigns').updateOne(
        { _id: campaignObjId },
        {
          $set: {
            status: nextStatus,
            completedAt: now,
            'totals.processed': processed,
            'totals.sent': sent,
            'totals.failed': failed,
          },
        }
      );

      try {
        await redis.hset(`campaign:${campaignIdStr}:meta`, {
          status: nextStatus,
          processed: String(processed),
          sent: String(sent),
          failed: String(failed),
          total: String(total),
        });
      } catch (e) {
        // ignore
      }

      await redis.publish('campaign:new', JSON.stringify({ id: campaignIdStr, status: nextStatus, totals: { processed, sent, failed, total } }));
    } else {
      try {
        await redis.hset(`campaign:${campaignIdStr}:meta`, {
          processed: String(processed),
          sent: String(sent),
          failed: String(failed),
          total: String(total),
        });
      } catch (e) {}
    }
  } catch (e) {
    console.warn('finalizeCampaign error', e);
  }
}

// Small reconciler to transition 'completed_with_failures' -> 'completed' when fails are cleared,
// and to re-run finalization for campaigns that appear finished in counters but have incorrect status.
let reconcilerHandle: NodeJS.Timeout | null = null;
async function startReconciler() {
  try {
    const client = await clientPromise;
    const db = client.db('PlatformData');

    const runOnce = async () => {
      try {
        const cursor = db.collection('campaigns').find(
          { status: { $in: ['completed_with_failures', 'completed'] } },
          { projection: { _id: 1 } }
        ).limit(200);

        const toCheck = await cursor.toArray();
        for (const c of toCheck) {
          try {
            await finalizeCampaign(c._id.toString());
          } catch (e) {
            console.warn('Reconciler finalizeCampaign failed for', c._id?.toString(), e);
          }
        }
      } catch (e) {
        console.warn('Reconciler run failed', e);
      }
    };

    await runOnce();
    reconcilerHandle = setInterval(() => {
      runOnce().catch((e) => console.warn('Reconciler interval error', e));
    }, Number(process.env.RECONCILER_INTERVAL_MS || 60_000));
  } catch (e) {
    console.warn('Failed to start reconciler', e);
  }
}

// Start reconciler (best-effort)
startReconciler().catch((e) => console.warn('startReconciler error', e));

// ---------------------
// Helper: HTML rewriting for tracking (PATH-based endpoints)
// ---------------------

/**
 * Converts plain-text URLs (including www.x and naked domains like example.com) into anchors.
 * - Does NOT touch existing <a ...> tags.
 * - Generated anchors include target="_blank" rel="noopener noreferrer".
 *
 * Examples:
 *   https://example.com -> <a href="https://example.com" target="_blank" rel="noopener noreferrer">https://example.com</a>
 *   www.example.com -> <a href="http://www.example.com" ...>www.example.com</a>
 *   example.com/path -> <a href="http://example.com/path" ...>example.com/path</a>
 */
function autoLinkPlainTextUrls(html: string) {
  if (!html || typeof html !== 'string') return html;

  // Don't try to auto-link inside existing anchor tags. We'll do a safe replace approach:
  // Split by <a ...>...</a> boundaries to avoid double-wrapping.
  const parts: string[] = [];
  let lastIndex = 0;
  const anchorRegex = /<a\b[^>]*>[\s\S]*?<\/a>/gi;
  let match: RegExpExecArray | null;

  let cursor = 0;
  while ((match = anchorRegex.exec(html)) !== null) {
    const idx = match.index;
    const before = html.slice(cursor, idx);
    parts.push(linkifySegment(before));
    parts.push(match[0]); // keep anchor as-is
    cursor = idx + match[0].length;
  }
  if (cursor < html.length) {
    parts.push(linkifySegment(html.slice(cursor)));
  }

  return parts.join('');

  function linkifySegment(seg: string) {
    if (!seg) return seg;

    // Regex to match URLs and naked domains, with optional trailing path/query.
    // This is intentionally permissive to catch many common patterns.
    const urlLike = /(^|[\s>])((?:(?:https?:\/\/)|(?:\/\/))[\w\-.~:\/?#[\]@!$&'()*+,;=%]+|(?:(?:www\.)[a-z0-9\-.]+\.[a-z]{2,})(?:\/[^\s<]*)?|(?:[a-z0-9\-.]+\.[a-z]{2,})(?:\/[^\s<]*)?)/gi;

    return seg.replace(urlLike, (m: string, prefix: string, captured: string) => {
      let url = captured;
      // If starts with '//' (protocol-relative) leave as-is (prefix with https:)
      if (/^\/\//.test(url)) {
        url = 'https:' + url;
      } else if (!/^https?:\/\//i.test(url)) {
        // If looks like www. or domain.tld, prepend http:// to href but keep display text as original
        url = 'http://' + url;
      }
      // sanitize display text (original captured)
      const display = captured;
      // add target and rel for safety and to reduce certain client heuristics
      return `${prefix}<a href="${url}" target="_blank" rel="noopener noreferrer">${display}</a>`;
    });
  }
}

/**
 * Safely rewrites all http(s) href attributes to point to our click-tracking endpoint.
 * Leaves mailto:, tel:, and fragment links untouched.
 *
 * New path format:
 *   `${PUBLIC_BASE_URL}/api/track/click/{campaignId}/{contactId}?u={base64_destination}&o=1`
 *
 * NOTE: this function preserves the quote style of the original attribute.
 */
function rewriteLinksForTracking(html: string, campaignId: string, contactId: string) {
  if (!html || !PUBLIC_BASE_URL) return html;

  // Regex to capture href value including surrounding quotes
  const hrefRegex = /href\s*=\s*(?:'([^']*)'|"([^"]*)"|([^>\s]+))/gi;

  const replaced = html.replace(hrefRegex, (match, singleQuoted, doubleQuoted, noQuote) => {
    const original = singleQuoted ?? doubleQuoted ?? noQuote ?? '';
    const trimmed = String(original).trim();

    // Skip empty or non-http links
    if (!trimmed) return match;
    if (/^mailto:/i.test(trimmed)) return match;
    if (/^tel:/i.test(trimmed)) return match;
    if (/^#/i.test(trimmed)) return match;

    // If it's protocol-relative (//example.com) convert to https:
    let normalized = trimmed;
    if (/^\/\//.test(trimmed)) {
      normalized = 'https:' + trimmed;
    }

    // If it lacks a scheme but looks like domain (www. or contains a dot + tld),
    // prepend http:// for encoding (we keep display text intact earlier).
    if (!/^https?:\/\//i.test(normalized) && /^[\w.-]+\.[a-z]{2,}/i.test(normalized)) {
      normalized = 'http://' + normalized;
    }

    // Preserve the original quote style when replacing
    try {
      const b64 = Buffer.from(normalized, 'utf8').toString('base64');
      // include an 'o=1' param as a hint to click endpoint to mark open on click (harmless if ignored)
      const clickUrl = `${PUBLIC_BASE_URL}/api/track/click/${encodeURIComponent(campaignId)}/${encodeURIComponent(contactId)}?u=${encodeURIComponent(b64)}&o=1`;
      if (singleQuoted !== undefined) {
        return `href='${clickUrl}'`;
      } else if (doubleQuoted !== undefined) {
        return `href="${clickUrl}"`;
      } else {
        return `href=${clickUrl}`;
      }
    } catch (e) {
      return match;
    }
  });

  return replaced;
}

/**
 * Injects a 1x1 tracking pixel (img) before </body> if present, otherwise appends to end of HTML.
 *
 * New path format:
 *   `${PUBLIC_BASE_URL}/api/track/open/{campaignId}/{contactId}?t={ts}`
 */
function injectOpenPixel(html: string, campaignId: string, contactId: string) {
  if (!html || !PUBLIC_BASE_URL) return html;
  const ts = Date.now();
  const pixelUrl = `${PUBLIC_BASE_URL}/api/track/open/${encodeURIComponent(campaignId)}/${encodeURIComponent(contactId)}?t=${ts}`;
  const imgTag = `<img src="${pixelUrl}" alt="" style="width:1px;height:1px;display:block;max-height:1px;max-width:1px;border:0;margin:0;padding:0;" />`;

  const lower = html.toLowerCase();
  const bodyClose = lower.lastIndexOf('</body>');
  if (bodyClose !== -1) {
    return html.slice(0, bodyClose) + imgTag + html.slice(bodyClose);
  } else {
    return html + imgTag;
  }
}

// ---------------------
// Attachment helper: fetch remote URL or read local path
// ---------------------
async function buildAttachments(definitionPart: any) {
  const attachmentsOut: any[] = [];
  if (!definitionPart || !Array.isArray(definitionPart.attachments)) return attachmentsOut;

  for (const att of definitionPart.attachments) {
    try {
      if (!att) continue;
      const source = att.source || att.type || 'url';
      if (source === 'url' && att.url) {
        // fetch remote resource
        try {
          const res = await fetch(att.url);
          if (!res.ok) {
            console.warn('Failed to fetch attachment URL', att.url, res.status);
            continue;
          }
          const buf = Buffer.from(await res.arrayBuffer());
          const contentType = res.headers.get('content-type') || att.contentType || 'application/octet-stream';
          let filename = att.name || path.basename(new URL(att.url).pathname || 'attachment');
          if (!filename) filename = `attachment-${Date.now()}`;
          attachmentsOut.push({
            filename,
            content: buf,
            contentType,
          });
        } catch (e) {
          console.warn('Attachment fetch failed', att.url, e);
          continue;
        }
      } else if ((source === 'path' || source === 'upload' || source === 'local') && att.path) {
        // local file path - read from disk
        try {
          const resolved = path.isAbsolute(att.path) ? att.path : path.join(process.cwd(), att.path);
          const buf = await fs.readFile(resolved);
          const filename = att.name || path.basename(resolved);
          attachmentsOut.push({
            filename,
            content: buf,
            contentType: att.contentType || 'application/octet-stream',
          });
        } catch (e) {
          console.warn('Failed to read local attachment', att.path, e);
          continue;
        }
      } else if (att.content && att.name) {
        // inline base64 content or Buffer-like
        try {
          let content = att.content;
          if (typeof content === 'string') {
            // base64 encoded check
            if (/^[A-Za-z0-9+/=]+\s*$/.test(content) && (content.length % 4 === 0)) {
              // assume base64
              const buf = Buffer.from(content, 'base64');
              attachmentsOut.push({
                filename: att.name,
                content: buf,
                contentType: att.contentType || 'application/octet-stream',
              });
            } else {
              attachmentsOut.push({
                filename: att.name,
                content,
                contentType: att.contentType || 'text/plain',
              });
            }
          } else {
            attachmentsOut.push({
              filename: att.name,
              content,
              contentType: att.contentType || 'application/octet-stream',
            });
          }
        } catch (e) {
          console.warn('Failed to handle inline attachment', e);
          continue;
        }
      } else {
        console.warn('Unknown attachment entry, skipping', att);
      }
    } catch (e) {
      console.warn('Unhandled attachment processing error', e);
    }
  }

  return attachmentsOut;
}

// ---------------------
// Utility: create plain-text fallback from HTML
// ---------------------
function htmlToPlainText(html: string) {
  if (!html) return '';
  // Replace anchor tags with "text (url)" form, keep other text.
  // First handle <a ...>text</a>
  let text = html.replace(/<a\b[^>]*href=(?:'([^']*)'|"([^"]*)"|([^>\s]+))[^>]*>([\s\S]*?)<\/a>/gi, (_m, s1, s2, s3, inner) => {
    const href = (s1 || s2 || s3 || '').trim();
    const cleanedHref = href.replace(/^['"]|['"]$/g, '');
    const display = inner ? inner.replace(/<[^>]+>/g, '').trim() : cleanedHref;
    // Put dest in parentheses to make it clear in text mode
    return `${display} (${cleanedHref})`;
  });

  // Strip remaining tags
  text = text.replace(/<\/?[^>]+(>|$)/g, '');
  // Unescape common HTML entities (basic)
  text = text.replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'");

  // Collapse whitespace
  text = text.replace(/\s{2,}/g, ' ').trim();
  return text;
}

// ---------------------
// Worker logic
// ---------------------

const worker = new Worker(
  'campaigns',
  async (job: Job) => {
    const { campaignId, contactId, step } = job.data as { campaignId: string; contactId?: string; step?: any };
    if (!campaignId || !contactId) {
      console.warn('Job missing campaignId or contactId', job.id);
      return;
    }

    const jobAttemptsMade = typeof (job.attemptsMade) === 'number' ? job.attemptsMade : 0;
    const bgAttemptNumber = jobAttemptsMade + 1;

    // --- Check campaign status quickly via redis if possible ---
    try {
      const campaignStatus = await redis.hget(`campaign:${campaignId}:meta`, 'status');
      if (campaignStatus === 'paused' || campaignStatus === 'cancelled') {
        console.log(`Skipping job ${job.id} for campaign ${campaignId} due to status ${campaignStatus}`);
        return;
      }
    } catch (e) {
      // ignore redis errors and continue
    }

    const client = await clientPromise;
    const db = client.db('PlatformData');

    const campaignObjId = tryParseObjectId(campaignId);
    const contactObjId = tryParseObjectId(contactId);

    const ledgerFilter = { campaignId: campaignObjId, contactId: contactObjId };
    const contact = await db.collection('contacts').findOne({ _id: contactObjId });
    const ledgerRow = await db.collection('campaign_contacts').findOne(ledgerFilter);

    if (!ledgerRow) {
      try {
        await db.collection('campaign_contacts').insertOne({
          campaignId: campaignObjId,
          contactId: contactObjId,
          status: 'failed',
          attempts: 0,
          bgAttempts: 0,
          lastAttemptAt: new Date(),
          lastError: 'missing ledger row',
        });
      } catch (e) {}
      try { await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1); await redis.hincrby(`campaign:${campaignId}:meta`, 'failed', 1); } catch (_) {}
      await publishContactUpdate(campaignId, contactId, { status: 'failed', attempts: 0, bgAttempts: 0, lastAttemptAt: new Date().toISOString(), lastError: 'missing ledger row' });
      await finalizeCampaign(campaignId);
      return;
    }

    if (!contact || !contact.email) {
      try {
        await db.collection('campaign_contacts').updateOne(
          ledgerFilter,
          {
            $set: { status: 'failed', lastError: 'missing contact or email', lastAttemptAt: new Date(), bgAttempts: bgAttemptNumber },
          }
        );
      } catch (e) {
        console.warn('Failed to mark missing contact/email in ledger', e);
      }
      try { await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1); await redis.hincrby(`campaign:${campaignId}:meta`, 'failed', 1); } catch (_) {}
      await publishContactUpdate(campaignId, contactId, { status: 'failed', attempts: ledgerRow.attempts ?? 0, bgAttempts: bgAttemptNumber, lastAttemptAt: new Date().toISOString(), lastError: 'missing contact or email' });
      await finalizeCampaign(campaignId);
      return;
    }

    const domain = (contact.email.split('@')[1] || '').toLowerCase();

    // mark initial core attempts if needed
    if (bgAttemptNumber === 1 && !(ledgerRow.attempts && ledgerRow.attempts > 0)) {
      try {
        await db.collection('campaign_contacts').updateOne(ledgerFilter, { $set: { attempts: 1 } });
      } catch (e) {
        console.warn('Failed to set initial attempts on ledger row', e);
      }
    }

    if (bgAttemptNumber === 1 && ledgerRow.bgAttempts && ledgerRow.bgAttempts > 0) {
      console.warn(`Job ${job.id} bgAttemptNumber reset to 1 while ledgerRow.bgAttempts=${ledgerRow.bgAttempts}. This usually means job was re-enqueued as a NEW job instead of retried by BullMQ.`);
    }

    // Try to acquire send permit (throttling)
    const permit = await tryAcquireSendPermit(domain);
    if (!permit.ok) {
      const requeueDelay = jitter((permit.reason === 'global-blocked' || permit.reason === 'global-capacity') ? GLOBAL_BLOCK_TTL_SEC * 1000 : 500 + Math.random() * 2000);
      console.log(`Throttling send to domain ${domain} (reason=${permit.reason}), asking BullMQ to retry job ${job.id} via backoff`);

      try {
        await db.collection('campaign_contacts').updateOne(
          ledgerFilter,
          {
            $set: {
              lastError: `throttled:${permit.reason}`,
              lastAttemptAt: new Date(),
            },
          }
        );

        await publishContactUpdate(campaignId, contactId, {
          status: ledgerRow.status ?? 'pending',
          attempts: ledgerRow.attempts ?? 0,
          bgAttempts: ledgerRow.bgAttempts ?? 0,
          lastAttemptAt: new Date().toISOString(),
          lastError: `throttled:${permit.reason}`,
          throttleDelayMs: requeueDelay,
        });
      } catch (e) {
        console.warn('Failed to persist throttle hint or publish contact update', e);
      }

      const retryError: any = new Error(`throttled:${permit.reason}`);
      retryError.isThrottle = true;
      throw retryError;
    }

    // Ensure campaign definition exists
    const defRaw = await redis.get(`campaign:${campaignId}:definition`);
    if (!defRaw) {
      console.warn('Missing campaign definition for', campaignId);
      throw new Error('Missing campaign definition');
    }
    const definition = JSON.parse(defRaw);
    const isInitial = job.name === 'initial';
    const subject = isInitial ? definition.initial?.subject : (step?.subject || definition.initial?.subject);
    const body = isInitial ? definition.initial?.body : (step?.body || definition.initial?.body);

    // Prepare tracked HTML (auto-link plain URLs -> rewrite links -> inject pixel) and attachments
    let trackedHtml = body;
    try {
      if (typeof trackedHtml === 'string' && PUBLIC_BASE_URL) {
        // 1) convert bare URLs to anchors (handles www. and naked domains)
        trackedHtml = autoLinkPlainTextUrls(trackedHtml);

        // 2) rewrite hrefs to tracking endpoint (normalizes missing scheme)
        trackedHtml = rewriteLinksForTracking(trackedHtml, campaignId, contactId);

        // 3) inject open pixel
        trackedHtml = injectOpenPixel(trackedHtml, campaignId, contactId);
      }
    } catch (e) {
      console.warn('Failed to rewrite or inject tracking into HTML, falling back to original body', e);
      trackedHtml = body;
    }

    // Build attachments from definition (best-effort)
    let attachments: any[] = [];
    try {
      const defPart = isInitial ? definition.initial : step ?? definition.initial;
      attachments = await buildAttachments(defPart);
    } catch (e) {
      console.warn('Failed to build attachments', e);
      attachments = [];
    }

    // mark sending & bgAttempts
    try {
      await db.collection('campaign_contacts').updateOne(
        ledgerFilter,
        {
          $set: {
            bgAttempts: bgAttemptNumber,
            status: 'sending',
            lastAttemptAt: new Date(),
            lastError: null,
          },
        }
      );

      await publishContactUpdate(campaignId, contactId, {
        status: 'sending',
        attempts: ledgerRow.attempts ?? 0,
        bgAttempts: bgAttemptNumber,
        lastAttemptAt: new Date().toISOString(),
        lastError: null,
      });
    } catch (e) {
      console.warn('Failed to mark sending/publish update', e);
    }

    // Attempt to send
    try {
      await new Promise((r) => setTimeout(r, randomSmallDelay()));

      const mailOptions: any = {
        from: process.env.SMTP_FROM,
        to: contact.email,
        subject,
        html: trackedHtml,
      };

      // Plain text fallback for deliverability
      try {
        mailOptions.text = htmlToPlainText(trackedHtml || body || '');
      } catch (e) {
        // ignore
      }

      // // Add List-Unsubscribe header when public base url is set (helps deliverability)
      // if (PUBLIC_BASE_URL) {
      //   try {
      //     const unsubscribeUrl = `${PUBLIC_BASE_URL}/unsubscribe?c=${encodeURIComponent(campaignId)}&k=${encodeURIComponent(contactId)}`;
      //     mailOptions.headers = {
      //       ...(mailOptions.headers || {}),
      //       'List-Unsubscribe': `<${unsubscribeUrl}>`,
      //     };
      //   } catch (e) {
      //     // ignore header errors
      //   }
      // }

      if (attachments && attachments.length) {
        mailOptions.attachments = attachments.map(a => {
          // nodemailer understands either path or content buffer
          if (a.path && !a.content) return { filename: a.filename, path: a.path, contentType: a.contentType };
          if (a.content) return { filename: a.filename, content: a.content, contentType: a.contentType };
          return { filename: a.filename, content: a.content, contentType: a.contentType };
        });
      }

      await transporter.sendMail(mailOptions);

      try {
        await db.collection('campaign_contacts').updateOne(
          ledgerFilter,
          {
            $set: { status: 'sent', lastAttemptAt: new Date(), bgAttempts: bgAttemptNumber },
          }
        );
      } catch (e) {
        console.warn('Failed to update ledger on success', e);
      }

      await incrementStats(domain, true, campaignId);

      try {
        await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1);
        await redis.hincrby(`campaign:${campaignId}:meta`, 'sent', 1);
      } catch (e) {}

      await publishContactUpdate(campaignId, contactId, {
        status: 'sent',
        attempts: (ledgerRow.attempts ?? 0),
        bgAttempts: bgAttemptNumber,
        lastAttemptAt: new Date().toISOString(),
        lastError: null,
      });

      // schedule follow-ups if initial
      if (isInitial) {
        for (const followUp of (definition.followUps || []) as any[]) {
          if (!followUp || !followUp.delayMinutes) continue;
          try {
            await queue.add(
              'followup',
              { campaignId, contactId, step: followUp },
              { delay: followUp.delayMinutes * 60 * 1000, removeOnComplete: true, removeOnFail: true, attempts: MAX_ATTEMPTS, backoff: { type: 'exponential', delay: 60_000 } }
            );
          } catch (e) {
            console.warn('Failed to enqueue followup', e);
          }
        }
      }

      // After a success, try to finalize if this completes the campaign
      await finalizeCampaign(campaignId);

      return;
    } catch (err: any) {
      const smtpCode = err && (err.responseCode || err.code || err.statusCode);
      const message = (err && (err.response || err.message || String(err))) || 'unknown error';

      try {
        await incrementStats(domain, false, campaignId);
      } catch (_) {}

      const lowerMsg = String(message).toLowerCase();
      const throttlingIndicators = ['rate limit', 'throttl', 'too many', 'blocked', 'limit exceeded', 'try again later'];
      const isThrottleSignal =
        (typeof smtpCode === 'number' && [421, 450, 451, 452, 429].includes(Number(smtpCode))) ||
        throttlingIndicators.some((s) => lowerMsg.includes(s));

      if (isThrottleSignal) {
        const beforeRow = ledgerRow;
        const nextAttemptsEstimate = (beforeRow?.bgAttempts || 0) + 1;
        const failRate = await getDomainFailureRate(domain);
        const ttl = Math.min(60 * 60, Math.max(30, Math.round(DOMAIN_BLOCK_TTL_SEC * (1 + nextAttemptsEstimate * 0.5 + failRate * 4))));
        try { await setDomainBlock(domain, ttl, `smtp:${smtpCode} msg:${message}`); } catch (e) {}
        if (lowerMsg.includes('rate limit') || Number(smtpCode) === 421) {
          try { await setGlobalBlock(Math.min(60 * 60, GLOBAL_BLOCK_TTL_SEC), `smtp:${smtpCode}`); } catch (e) {}
        }
      }

      const currentAttempt = bgAttemptNumber;

      const updateFields: any = {
        lastError: (err && err.message) ? err.message : String(err),
        lastAttemptAt: new Date(),
        bgAttempts: currentAttempt,
      };

      if (currentAttempt < MAX_ATTEMPTS) {
        updateFields.status = 'sending';
        try {
          await db.collection('campaign_contacts').updateOne(ledgerFilter, { $set: updateFields });
        } catch (e) {
          console.warn('Failed to persist intermediate failure state', e);
        }

        await publishContactUpdate(campaignId, contactId, {
          status: updateFields.status,
          attempts: ledgerRow.attempts ?? 0,
          bgAttempts: currentAttempt,
          lastAttemptAt: updateFields.lastAttemptAt.toISOString(),
          lastError: updateFields.lastError,
        });

        throw err;
      } else {
        updateFields.status = 'failed';
        try {
          await db.collection('campaign_contacts').updateOne(ledgerFilter, { $set: updateFields });
        } catch (e) {
          console.warn('Failed to persist final failed state', e);
        }

        try {
          await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1);
          await redis.hincrby(`campaign:${campaignId}:meta`, 'failed', 1);
        } catch (_) {}

        await publishContactUpdate(campaignId, contactId, {
          status: 'failed',
          attempts: ledgerRow.attempts ?? 0,
          bgAttempts: currentAttempt,
          lastAttemptAt: updateFields.lastAttemptAt.toISOString(),
          lastError: updateFields.lastError,
        });

        // Try to finalize (this failure might make processed==total)
        await finalizeCampaign(campaignId);

        throw err;
      }
    }
  },
  {
    connection: redis,
    concurrency: Number(process.env.WORKER_CONCURRENCY || 5),
    limiter: {
      max: Number(process.env.WORKER_LIMITER_MAX || RATE_LIMIT_MAX),
      duration: Number(process.env.WORKER_LIMITER_DURATION || RATE_LIMIT_DURATION),
    },
  }
);

console.log('Campaign worker running with Redis-backed dynamic throttling and domain rate control (html tracking + attachments enabled)');

worker.on('completed', async (job) => {
  // light listener
});

worker.on('failed', async (job, err) => {
  try {
    console.warn(`Job ${job.id} failed after ${job.attemptsMade} attempts:`, err?.message ?? err);
  } catch (e) {
    console.warn('Job failed handler error', e);
  }
});