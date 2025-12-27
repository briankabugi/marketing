// pages/api/track/click/[campaign]/[contact].ts
import type { NextApiRequest, NextApiResponse } from 'next';
import clientPromise from '../../../../../lib/mongo';
import { redis } from '../../../../../lib/redis';
import { ObjectId } from 'mongodb';

function tryParseObjectId(maybeId: any) {
  if (!maybeId) return null;
  if (typeof maybeId === 'object' && maybeId instanceof ObjectId) return maybeId;
  if (typeof maybeId === 'string' && /^[0-9a-fA-F]{24}$/.test(maybeId)) {
    try { return new ObjectId(maybeId); } catch { return null; }
  }
  return null;
}

function safeLog(...args: any[]) {
  try { console.log(...args); } catch {}
}

/**
 * Try multiple ways to decode the `u` parameter to the real destination URL.
 * We encoded with: encodeURIComponent(Buffer.from(url).toString('base64'))
 * But email clients / proxies may double-encode, rewrite, or pass the raw URL.
 *
 * Improvements:
 *  - accept URL-safe base64 (- and _), convert to standard base64
 *  - tolerate double-encoding and stray spaces/+
 *  - accept bare "www.example.com" by prepending "http://"
 */
function decodeDestination(uRaw?: string | string[] | undefined): string | null {
  if (!uRaw) return null;
  const u = Array.isArray(uRaw) ? uRaw[0] : uRaw;

  // heuristic: if it already looks like an http(s) URL, return raw
  if (/^https?:\/\//i.test(u)) return u;

  // Try decodeURIComponent then base64
  const attempts: string[] = [];

  try {
    attempts.push(decodeURIComponent(u));
  } catch (e) {
    // skip
  }
  attempts.push(u);

  // Also unescape plus/space issues
  attempts.push(u.replace(/\s/g, ''));
  attempts.push(u.replace(/\+/g, ' '));
  attempts.push(u.replace(/\s/g, '+'));

  for (const a of attempts) {
    if (!a) continue;
    try {
      // Clean possible double-encoding (strip surrounding quotes)
      const cleaned = String(a).trim().replace(/^"+|"+$/g, '').replace(/^'+|'+$/g, '');
      // Normalize URL-safe base64 to standard
      const normalized = cleaned.replace(/-/g, '+').replace(/_/g, '/');
      // Padding (base64 requires padding)
      const pad = normalized.length % 4;
      const candidate = normalized + (pad === 0 ? '' : '='.repeat(4 - pad));

      const buf = Buffer.from(candidate, 'base64');
      const str = buf.toString('utf8').trim();
      if (str && /^https?:\/\//i.test(str)) return str;

      // also accept bare www (no scheme) from decoded string
      if (str && /^www\./i.test(str)) return `http://${str}`;
    } catch (e) {
      // try next
    }
  }

  // Last-chance: maybe it's plain URL but percent-encoded differently or missing scheme
  try {
    const asDecoded = decodeURIComponent(String(u)).trim();
    if (/^https?:\/\//i.test(asDecoded)) return asDecoded;
    if (/^www\./i.test(asDecoded)) return `http://${asDecoded}`;
  } catch (e) {}

  // Also if the raw value is a bare www host, accept it
  try {
    const rawClean = String(u).trim();
    if (/^www\./i.test(rawClean)) return `http://${rawClean}`;
  } catch (e) {}

  return null;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { campaign, contact } = req.query;
  const uRaw = req.query.u as string | undefined;
  const trace = (req.query.t as string) || String(Date.now());

  safeLog('[CLICK] incoming request', { url: req.url, query: req.query, headers: {
    'user-agent': req.headers['user-agent'],
    referer: req.headers.referer || req.headers.referrer,
    host: req.headers.host,
    'x-forwarded-for': req.headers['x-forwarded-for'],
  } });

  try {
    if (!campaign || !contact) {
      safeLog('[CLICK] missing campaign/contact path params');
      return res.status(400).send('Missing params');
    }

    const campaignIdStr = String(campaign);
    const contactIdStr = String(contact);
    const campaignIdObj = tryParseObjectId(campaignIdStr);
    const contactIdObj = tryParseObjectId(contactIdStr);

    const dest = decodeDestination(uRaw) ?? (() => {
      // fallback: maybe u contains a raw url despite no base64
      const fallback = Array.isArray(uRaw) ? uRaw[0] : uRaw;
      if (typeof fallback === 'string') {
        const f = fallback.trim();
        if (/^https?:\/\//i.test(f)) return f;
        if (/^www\./i.test(f)) return `http://${f}`;
      }
      return null;
    })();

    if (!dest) {
      safeLog('[CLICK] failed to decode destination', { uRaw });
      // Log an event so we can inspect
      try {
        const client = await clientPromise;
        const db = client.db('PlatformData');
        await db.collection('campaign_events').insertOne({
          campaignId: campaignIdObj ?? campaignIdStr,
          contactId: contactIdObj ?? contactIdStr,
          type: 'click',
          url: null,
          note: 'decode_failed',
          raw_u: uRaw,
          trace,
          createdAt: new Date(),
        });
      } catch (e) {
        safeLog('[CLICK] failed recording decode-failed event', e);
      }
      return res.status(400).send('Invalid destination');
    }

    // Persist event + update ledger + redis + publish
    const client = await clientPromise;
    const db = client.db('PlatformData');

    const ua = (req.headers['user-agent'] as string) || '';
    const ip = (req.headers['x-forwarded-for'] as string)?.split(',')[0] || req.socket.remoteAddress || null;
    const now = new Date();

    // --- BACKFILL OPEN if missing (click implies open) ---
    try {
      // Check ledger for openedAt directly (more efficient than scanning events)
      let ledgerRowForOpen: any = null;

      if (campaignIdObj && contactIdObj) {
        ledgerRowForOpen = await db.collection('campaign_contacts').findOne(
          { campaignId: campaignIdObj, contactId: contactIdObj },
          { projection: { openedAt: 1 } }
        );
      }
      if ((!ledgerRowForOpen) && campaignIdObj) {
        ledgerRowForOpen = await db.collection('campaign_contacts').findOne(
          { campaignId: campaignIdObj, contactId: contactIdStr },
          { projection: { openedAt: 1 } }
        );
      }
      if ((!ledgerRowForOpen)) {
        ledgerRowForOpen = await db.collection('campaign_contacts').findOne(
          { campaignId: campaignIdStr, contactId: contactIdStr },
          { projection: { openedAt: 1 } }
        );
      }

      const alreadyOpened = !!(ledgerRowForOpen && ledgerRowForOpen.openedAt);

      if (!alreadyOpened) {
        // Insert "open" event (best-effort)
        try {
          const evOpen: any = {
            campaignId: campaignIdObj ?? campaignIdStr,
            contactId: contactIdObj ?? contactIdStr,
            type: 'open',
            ua,
            ip,
            createdAt: now,
            trace,
            via: 'click-backfill',
          };
          const rOpen = await db.collection('campaign_events').insertOne(evOpen);
          safeLog('[CLICK] backfilled open ->', rOpen.insertedId?.toString?.());

          // Update campaign_contacts: set openedAt and bump lastActivityAt (use $max)
          try {
            const updateDoc: any = {
              $set: { openedAt: now },
              $max: { lastActivityAt: now },
            };

            let resUpdate = null;
            if (campaignIdObj && contactIdObj) {
              resUpdate = await db.collection('campaign_contacts').updateOne({ campaignId: campaignIdObj, contactId: contactIdObj }, updateDoc);
            }
            if ((!resUpdate || (resUpdate.matchedCount ?? 0) === 0) && campaignIdObj) {
              resUpdate = await db.collection('campaign_contacts').updateOne({ campaignId: campaignIdObj, contactId: contactIdStr }, updateDoc);
            }
            if ((!resUpdate || (resUpdate.matchedCount ?? 0) === 0)) {
              resUpdate = await db.collection('campaign_contacts').updateOne({ campaignId: campaignIdStr, contactId: contactIdStr }, updateDoc);
            }
            if (!resUpdate || (resUpdate.matchedCount ?? 0) === 0) {
              safeLog('[CLICK] open backfill: campaign_contacts no match for', { campaignIdStr, contactIdStr });
            }
          } catch (e) {
            safeLog('[CLICK] open backfill: campaign_contacts update failed', e);
          }

          // Redis counters & publish open event for UI
          try {
            await redis.hincrby(`campaign:${campaignIdStr}:metrics`, 'opens', 1);
            const payloadOpen = { campaignId: campaignIdStr, contactId: contactIdStr, event: 'open', openedAt: now.toISOString(), trace, ts: now.toISOString(), via: 'click-backfill' };
            await redis.publish(`campaign:${campaignIdStr}:contact_update`, JSON.stringify(payloadOpen));
            safeLog('[CLICK] published backfill open contact_update', payloadOpen);
          } catch (e) {
            safeLog('[CLICK] open backfill: redis publish failed', e);
          }
        } catch (e) {
          safeLog('[CLICK] failed to insert backfill open event', e);
        }
      }
    } catch (e) {
      safeLog('[CLICK] open backfill check failed', e);
      // continue anyway
    }

    // --- Insert click event ---
    try {
      const ev: any = {
        campaignId: campaignIdObj ?? campaignIdStr,
        contactId: contactIdObj ?? contactIdStr,
        type: 'click',
        url: dest,
        ua,
        ip,
        createdAt: now,
        trace,
      };
      const r = await db.collection('campaign_events').insertOne(ev);
      safeLog('[CLICK] campaign_events.insertOne ->', r.insertedId?.toString?.());
    } catch (e) {
      safeLog('[CLICK] failed inserting campaign_events', e);
    }

    // Update campaign_contacts: lastClickAt and lastActivityAt (use $max)
    try {
      const updateDoc: any = { $set: { lastClickAt: now }, $max: { lastActivityAt: now } };

      let resUpdate = null;
      if (campaignIdObj && contactIdObj) {
        resUpdate = await db.collection('campaign_contacts').updateOne({ campaignId: campaignIdObj, contactId: contactIdObj }, updateDoc);
      }
      if ((!resUpdate || (resUpdate.matchedCount ?? 0) === 0) && campaignIdObj) {
        resUpdate = await db.collection('campaign_contacts').updateOne({ campaignId: campaignIdObj, contactId: contactIdStr }, updateDoc);
      }
      if ((!resUpdate || (resUpdate.matchedCount ?? 0) === 0)) {
        resUpdate = await db.collection('campaign_contacts').updateOne({ campaignId: campaignIdStr, contactId: contactIdStr }, updateDoc);
      }

      if (!resUpdate || (resUpdate.matchedCount ?? 0) === 0) {
        safeLog('[CLICK] campaign_contacts no match for', { campaignIdStr, contactIdStr });
      }
    } catch (e) {
      safeLog('[CLICK] campaign_contacts update failed', e);
    }

    // Redis counters and publish click update
    try {
      await redis.hincrby(`campaign:${campaignIdStr}:metrics`, 'clicks', 1);
      const payload = { campaignId: campaignIdStr, contactId: contactIdStr, event: 'click', url: dest, trace, ts: now.toISOString() };
      await redis.publish(`campaign:${campaignIdStr}:contact_update`, JSON.stringify(payload));
      safeLog('[CLICK] published contact_update', payload);
    } catch (e) {
      safeLog('[CLICK] redis publish failed', e);
    }

    // Redirect to dest
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate, max-age=0, s-maxage=0');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    return res.redirect(302, dest);
  } catch (err) {
    safeLog('[CLICK] unexpected error', err);
    return res.status(500).send('error');
  }
}
