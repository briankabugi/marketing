// workers/campaignWorker.ts
import 'dotenv/config'; // critical
import { Worker, Queue, Job } from 'bullmq';
import { redis } from '../lib/redis';
import clientPromise from '../lib/mongo';
import nodemailer from 'nodemailer';
import { ObjectId } from 'mongodb';

const queue = new Queue('campaigns', { connection: redis });
const MAX_ATTEMPTS = 3;

// --- Per-account / per-domain rate limiting config ---
// these control baseline limits; they can be reduced dynamically
const RATE_LIMIT_MAX = Number(process.env.EMAIL_RATE_MAX || 20); // max emails per domain window
const RATE_LIMIT_DURATION = Number(process.env.EMAIL_RATE_DURATION || 1000 * 60); // ms window (default: 1 minute)
const GLOBAL_RATE_LIMIT_MAX = Number(process.env.EMAIL_GLOBAL_RATE_MAX || 200); // global (all domains) default
const GLOBAL_RATE_LIMIT_DURATION = Number(process.env.EMAIL_GLOBAL_RATE_DURATION || 1000 * 60); // ms window

// Warm-up factor (0 < f <= 1). Use smaller values for new accounts to ramp slowly.
const WARMUP_FACTOR = Number(process.env.EMAIL_WARMUP_FACTOR || 1.0);

// Failure thresholds for dynamic throttling
const FAILURE_RATE_WARN = Number(process.env.EMAIL_FAILURE_WARN_RATE || 0.05); // 5%
const FAILURE_RATE_STRICT = Number(process.env.EMAIL_FAILURE_STRICT_RATE || 0.15); // 15%

// throttle TTL caps (seconds)
const DOMAIN_BLOCK_TTL_SEC = Number(process.env.EMAIL_DOMAIN_BLOCK_TTL || 60 * 5); // 5 minutes default
const GLOBAL_BLOCK_TTL_SEC = Number(process.env.EMAIL_GLOBAL_BLOCK_TTL || 60 * 5);

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

// exponential backoff for retries (used for manual delay requeues in code if needed)
function computeRetryDelay(attempts: number) {
  const base = 60 * 1000; // 1 minute base
  return Math.pow(2, attempts - 1) * base;
}

function jitter(ms: number) {
  // small random jitter up to +- 20%
  const jitterPct = Math.random() * 0.4 - 0.2;
  return Math.max(1000, Math.round(ms + ms * jitterPct));
}

function randomSmallDelay() {
  return Math.round(Math.random() * 1000) + 250; // 250-1250ms
}

// ---------------------
// Helpers
// ---------------------

/**
 * Try to produce a ObjectId for strings that look like 24-hex; otherwise return original value.
 * This avoids throwing when non-ObjectId strings are passed as contactId.
 */
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

function toObjectIdOrNew(val: string | ObjectId) {
  if (val instanceof ObjectId) return val;
  try {
    return new ObjectId(String(val));
  } catch {
    // Last resort: return as-is (some systems use string ids)
    return val;
  }
}

// ---------------------
// Redis-backed helpers
// ---------------------

/**
 * Remove old timestamps and return current count for a sorted set key.
 * Works with Redis sorted-set where members are unique strings and score==timestamp.
 */
async function getWindowCount(zkey: string, windowMs: number) {
  const now = Date.now();
  // Remove older than window
  try {
    await (redis as any).zremrangebyscore(zkey, 0, now - windowMs);
    const count = await (redis as any).zcard(zkey);
    return Number(count || 0);
  } catch (e) {
    console.warn('Redis zset ops failed, allowing send by default', e);
    return 0;
  }
}

/**
 * Reserve a slot in a sorted set for the current send.
 * Returns true if reserved, false if couldn't reserve due to reaching capacity.
 */
async function reserveWindowSlot(zkey: string, windowMs: number, capacity: number) {
  const now = Date.now();
  // Clean old
  await (redis as any).zremrangebyscore(zkey, 0, now - windowMs);
  const count = Number(await (redis as any).zcard(zkey) || 0);
  if (count >= capacity) return false;
  // Member must be unique; use timestamp + random
  const member = `${now}:${Math.floor(Math.random() * 1000000)}`;
  await (redis as any).zadd(zkey, now, member);
  // set an expiry on the key equal to windowMs to avoid redis bloat
  await (redis as any).pexpire(zkey, Math.max(windowMs, 1000));
  return true;
}

async function incrementStats(domain: string, success: boolean) {
  const statsKey = `stats:domain:${domain}`;
  if (success) {
    await redis.hincrby(statsKey, 'sent', 1);
  } else {
    await redis.hincrby(statsKey, 'failed', 1);
  }
  // keep stats TTL to a day for trend calculations
  await (redis as any).expire(statsKey, 60 * 60 * 24);
}

/**
 * Get recent failure rate (failed / (sent+failed)) for domain
 */
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

// Decide whether we can send now (checks domain, global, and dynamic reductions based on fail rate).
async function tryAcquireSendPermit(domain: string) {
  // If global is blocked, short-circuit
  if (await isGlobalBlocked()) return { ok: false, reason: 'global-blocked' };

  // If domain is blocked, short-circuit
  if (await isDomainBlocked(domain)) return { ok: false, reason: 'domain-blocked' };

  // dynamic capacity reduction based on failure rate
  const failRate = await getDomainFailureRate(domain);
  let allowed = Math.max(1, Math.floor(RATE_LIMIT_MAX * WARMUP_FACTOR));
  if (failRate >= FAILURE_RATE_STRICT) {
    allowed = Math.max(1, Math.floor(allowed * 0.2)); // aggressive reduction
  } else if (failRate >= FAILURE_RATE_WARN) {
    allowed = Math.max(1, Math.floor(allowed * 0.5)); // moderate reduction
  }

  // Reserve a slot on the domain zset
  const domainKey = `rate:domain:${domain}`;
  const reserved = await reserveWindowSlot(domainKey, RATE_LIMIT_DURATION, allowed);
  if (!reserved) {
    return { ok: false, reason: 'domain-capacity' };
  }

  // Reserve a slot on global zset
  try {
    const globalKey = `rate:global`;
    // Clean old global counts
    await (redis as any).zremrangebyscore(globalKey, 0, Date.now() - GLOBAL_RATE_LIMIT_DURATION);
    const gcount = Number(await (redis as any).zcard(globalKey) || 0);
    if (gcount >= GLOBAL_RATE_LIMIT_MAX) {
      // rollback domain reservation by removing the last member we added
      // best-effort: remove entries older than window (already cleaned); nothing to remove specifically
      return { ok: false, reason: 'global-capacity' };
    }
    const gmember = `${Date.now()}:${Math.floor(Math.random() * 1000000)}`;
    await (redis as any).zadd(globalKey, Date.now(), gmember);
    await (redis as any).pexpire(globalKey, Math.max(GLOBAL_RATE_LIMIT_DURATION, 1000));
  } catch (e) {
    // if global bucket can't be used, allow send (defensive)
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
 * Robust completion/finalization helper.
 * Distinguishes between fully successful completion and completion with failures.
 * - If processed >= total and failed === 0 => mark 'completed' and cleanup definition
 * - If processed >= total and failed > 0 => mark 'completed_with_failures' and keep definition so retries can be performed
 */
async function finalizeCampaignIfComplete(campaignIdArg: string | ObjectId, db: any) {
  try {
    const campaignIdStr = typeof campaignIdArg === 'string' ? campaignIdArg : (campaignIdArg instanceof ObjectId ? campaignIdArg.toString() : String(campaignIdArg));
    const campaignObjId = toObjectIdOrNew(campaignIdArg as any);

    // Try to read redis meta (best-effort)
    let meta: Record<string, string> = {};
    try {
      meta = (await redis.hgetall(`campaign:${campaignIdStr}:meta`)) || {};
    } catch (e) {
      meta = {};
    }

    let processed = Number(meta['processed'] ?? 0);
    let total = Number(meta['total'] ?? 0);
    let sent = Number(meta['sent'] ?? 0);
    let failed = Number(meta['failed'] ?? 0);

    // If redis meta looks missing or inconsistent, compute from Mongo
    if (!total || total <= 0) {
      try {
        total = await db.collection('campaign_contacts').countDocuments({ campaignId: campaignObjId });
      } catch (e) {
        total = Number(meta['total'] ?? 0);
      }
    }

    try {
      const processedFromDb = await db.collection('campaign_contacts').countDocuments({ campaignId: campaignObjId, status: { $in: ['sent', 'failed'] } });
      if (processedFromDb > processed) processed = processedFromDb;
    } catch (e) {
      // ignore
    }

    // If still no total or processed doesn't meet threshold, nothing to do
    if (total > 0 && processed >= total) {
      // Compute sent/failed from DB if redis lacks them
      try {
        const agg = await db.collection('campaign_contacts').aggregate([
          { $match: { campaignId: campaignObjId } },
          { $group: { _id: '$status', count: { $sum: 1 } } },
        ]).toArray();
        sent = 0;
        failed = 0;
        for (const row of agg) {
          if (row._id === 'sent') sent = row.count;
          else if (row._id === 'failed') failed = row.count;
        }
      } catch (e) {
        // ignore
      }

      // Idempotent update: if already completed, ensure redis meta is consistent
      try {
        const existing = await db.collection('campaigns').findOne({ _id: campaignObjId }, { projection: { status: 1 } });
        // If already marked completed/with failures, sync redis and return
        if (existing && (existing.status === 'completed' || existing.status === 'completed_with_failures')) {
          const statusToSet = existing.status === 'completed' ? 'completed' : 'completed_with_failures';
          try {
            await redis.hset(`campaign:${campaignIdStr}:meta`, {
              status: statusToSet,
              processed: String(processed),
              sent: String(sent),
              failed: String(failed),
            });
          } catch (_) {}
          return;
        }
      } catch (e) {
        // continue
      }

      const now = new Date();

      if (failed === 0) {
        // All succeeded => mark completed and cleanup definition
        try {
          await db.collection('campaigns').updateOne(
            { _id: campaignObjId },
            {
              $set: {
                status: 'completed',
                completedAt: now,
                'totals.processed': processed,
                'totals.sent': sent,
                'totals.failed': failed,
              },
            }
          );
        } catch (e) {
          console.warn('Failed to persist campaign completion to Mongo', e);
        }

        try {
          await redis.hset(`campaign:${campaignIdStr}:meta`, {
            status: 'completed',
            processed: String(processed),
            sent: String(sent),
            failed: String(failed),
          });
        } catch (_) {}

        try {
          await redis.publish('campaign:new', JSON.stringify({ campaignId: campaignIdStr, status: 'completed' }));
        } catch (_) {}

        try { await redis.del(`campaign:${campaignIdStr}:definition`); } catch (_) {}
        console.log(`Campaign ${campaignIdStr} marked completed (processed=${processed} total=${total})`);
      } else {
        // Some failed => mark 'completed_with_failures' and keep definition so user can retry failed contacts.
        try {
          await db.collection('campaigns').updateOne(
            { _id: campaignObjId },
            {
              $set: {
                status: 'completed_with_failures',
                completedAt: now,
                'totals.processed': processed,
                'totals.sent': sent,
                'totals.failed': failed,
              },
            }
          );
        } catch (e) {
          console.warn('Failed to persist campaign partial completion to Mongo', e);
        }

        try {
          await redis.hset(`campaign:${campaignIdStr}:meta`, {
            status: 'completed_with_failures',
            processed: String(processed),
            sent: String(sent),
            failed: String(failed),
          });
        } catch (_) {}

        try {
          await redis.publish('campaign:new', JSON.stringify({ campaignId: campaignIdStr, status: 'completed_with_failures', failed }));
        } catch (_) {}

        // NOTE: do NOT delete campaign:{id}:definition here so that retry flows can re-use it.
        console.log(`Campaign ${campaignIdStr} finished with failures (processed=${processed} total=${total} failed=${failed})`);
      }
    }
  } catch (e) {
    console.warn('finalizeCampaignIfComplete error', e);
  }
}

// ---------------------
// Worker logic
// ---------------------

// create worker and keep reference to attach listeners if needed later
const worker = new Worker(
  'campaigns',
  async (job: Job) => {
    // Job payload
    const { campaignId, contactId, step } = job.data as { campaignId: string; contactId?: string; step?: any };
    if (!campaignId || !contactId) {
      console.warn('Job missing campaignId or contactId', job.id);
      return;
    }

    // compute current bg attempt number (BullMQ attemptsMade is number of previous attempts)
    const jobAttemptsMade = typeof (job.attemptsMade) === 'number' ? job.attemptsMade : 0;
    const bgAttemptNumber = jobAttemptsMade + 1; // e.g., first run => 1

    // --- Check campaign status ---
    try {
      const campaignStatus = await redis.hget(`campaign:${campaignId}:meta`, 'status');
      if (campaignStatus === 'paused' || campaignStatus === 'cancelled') {
        console.log(`Skipping job ${job.id} for campaign ${campaignId} due to status ${campaignStatus}`);
        return;
      }
    } catch (e) {
      // if redis read fails, proceed and rely on DB state below
    }

    const client = await clientPromise;
    const db = client.db('PlatformData');

    // --- Normalize IDs for DB queries (robust to string/ObjectId differences) ---
    const campaignObjId = tryParseObjectId(campaignId);
    const contactObjId = tryParseObjectId(contactId);

    // --- Load contact --- (ledger row)
    const ledgerFilter = { campaignId: campaignObjId, contactId: contactObjId };
    const contact = await db.collection('contacts').findOne({ _id: contactObjId });
    const ledgerRow = await db.collection('campaign_contacts').findOne(ledgerFilter);

    // If ledger row is missing, be defensive and initialize or mark failed
    if (!ledgerRow) {
      // create a ledger row (best-effort) and mark failed/processed
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
      } catch (e) {
        // ignore insert errors
      }
      try {
        await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1);
        await redis.hincrby(`campaign:${campaignId}:meta`, 'failed', 1);
      } catch (_) {}
      await publishContactUpdate(campaignId, contactId, { status: 'failed', attempts: 0, bgAttempts: 0, lastAttemptAt: new Date().toISOString(), lastError: 'missing ledger row' });
      return;
    }

    if (!contact || !contact.email) {
      // Missing contact or email: final failure, update ledger and counters
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
      try {
        await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1);
        await redis.hincrby(`campaign:${campaignId}:meta`, 'failed', 1);
      } catch (_) {}
      await publishContactUpdate(campaignId, contactId, { status: 'failed', attempts: ledgerRow.attempts ?? 0, bgAttempts: bgAttemptNumber, lastAttemptAt: new Date().toISOString(), lastError: 'missing contact or email' });

      // If this final failure caused completion, try to finalize campaign
      try {
        await finalizeCampaignIfComplete(campaignId, db);
      } catch (_) {}

      return;
    }

    const domain = (contact.email.split('@')[1] || '').toLowerCase();

    // If this is the very first attempt (bgAttemptNumber === 1) AND ledgerRow.attempts is falsy (0 or undefined),
    // treat this as the initial "core attempt" and increment the core attempts counter so initial sends count toward the user's 3 manual/initial attempts.
    // If ledgerRow.attempts > 0, it's either been manually retried already (control API should have incremented attempts) or previously counted — do not increment here.
    if (bgAttemptNumber === 1 && !(ledgerRow.attempts && ledgerRow.attempts > 0)) {
      // Set attempts to 1 (initial core attempt)
      try {
        await db.collection('campaign_contacts').updateOne(ledgerFilter, { $set: { attempts: 1 } });
      } catch (e) {
        console.warn('Failed to set initial attempts on ledger row', e);
      }
    }

    // diagnostic: if bgAttemptNumber keeps resetting to 1 but ledgerRow.bgAttempts already shows >0, log it
    if (bgAttemptNumber === 1 && ledgerRow.bgAttempts && ledgerRow.bgAttempts > 0) {
      console.warn(`Job ${job.id} bgAttemptNumber reset to 1 while ledgerRow.bgAttempts=${ledgerRow.bgAttempts}. This usually means job was re-enqueued as a NEW job instead of retried by BullMQ.`);
    }

    // --- Try to acquire permit to send now ---
    const permit = await tryAcquireSendPermit(domain);
    if (!permit.ok) {
      // ----------------------
      // We must NOT requeue a fresh job here (that resets attemptsMade).
      // Instead, record a throttling hint and throw so BullMQ will retry the same job (attempts/backoff).
      // ----------------------

      const requeueDelay = jitter((permit.reason === 'global-blocked' || permit.reason === 'global-capacity') ? GLOBAL_BLOCK_TTL_SEC * 1000 : 500 + Math.random() * 2000);
      console.log(`Throttling send to domain ${domain} (reason=${permit.reason}), asking BullMQ to retry job ${job.id} via backoff`);

      try {
        // update ledger with a throttling hint so UI can reflect a reason (do not mark processed)
        await db.collection('campaign_contacts').updateOne(
          ledgerFilter,
          {
            $set: {
              // keep status as 'pending' to indicate it hasn't been sent; keep bgAttempts untouched so bgAttempt increments when a real attempt runs
              lastError: `throttled:${permit.reason}`,
              lastAttemptAt: new Date(),
            },
          }
        );

        // Publish contact-level update so UI knows the contact is being delayed/throttled
        await publishContactUpdate(campaignId, contactId, {
          status: ledgerRow.status ?? 'pending',
          attempts: ledgerRow.attempts ?? 0,
          bgAttempts: ledgerRow.bgAttempts ?? 0,
          lastAttemptAt: new Date().toISOString(),
          lastError: `throttled:${permit.reason}`,
          // include a hint how long we estimate to delay (best-effort) — UI may show this if desired
          throttleDelayMs: requeueDelay,
        });
      } catch (e) {
        console.warn('Failed to persist throttle hint or publish contact update', e);
      }

      // Throw an error so BullMQ will perform retry/backoff according to the job options.
      // IMPORTANT: For this to work you MUST enqueue jobs with attempts/backoff (see the start and control APIs).
      const retryError: any = new Error(`throttled:${permit.reason}`);
      // Attach a marker so telemetry can detect throttles more easily if needed
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

    // Before attempting to send, write bgAttempts (attempt number) and mark as sending so UI gets immediate feedback.
    try {
      await db.collection('campaign_contacts').updateOne(
        ledgerFilter,
        {
          $set: {
            // Do not overwrite core attempts here — only touch bgAttempts
            bgAttempts: bgAttemptNumber,
            status: 'sending',
            lastAttemptAt: new Date(),
            lastError: null,
          },
        }
      );
      // Publish contact-level update so UI marks the row as 'sending' and shows bgAttempts increment
      await publishContactUpdate(campaignId, contactId, {
        status: 'sending',
        attempts: ledgerRow.attempts ?? 0,
        bgAttempts: bgAttemptNumber,
        lastAttemptAt: new Date().toISOString(),
        lastError: null,
      });
    } catch (e) {
      // proceed even if publishing fails
      console.warn('Failed to mark sending/publish update', e);
    }

    // Attempt to send
    try {
      // small jitter to avoid thundering herd within the same millisecond
      await new Promise((r) => setTimeout(r, randomSmallDelay()));

      await transporter.sendMail({
        from: process.env.SMTP_FROM,
        to: contact.email,
        subject,
        html: body,
      });

      // success: update DB & stats (finalize this job)
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

      await incrementStats(domain, true);

      // Mark processed & sent (these are final counts — each contact counts once when it completes)
      try {
        await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1);
        await redis.hincrby(`campaign:${campaignId}:meta`, 'sent', 1);
      } catch (e) {
        // Non-fatal
      }

      // Publish contact update
      await publishContactUpdate(campaignId, contactId, {
        status: 'sent',
        attempts: (ledgerRow.attempts ?? 0),
        bgAttempts: bgAttemptNumber,
        lastAttemptAt: new Date().toISOString(),
        lastError: null,
      });

      // If initial, schedule follow-ups as before (ensure queued jobs carry MAX_ATTEMPTS/backoff so they each have their own MQ cycle)
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

        // check totals & possibly complete campaign (robustly)
        try {
          await finalizeCampaignIfComplete(campaignId, db);
        } catch (e) {
          console.warn('Post-send completion check failed', e);
        }
      }

      return;
    } catch (err: any) {
      // --- Failure handling with dynamic throttling decisions ---
      const smtpCode = err && (err.responseCode || err.code || err.statusCode);
      const message = (err && (err.response || err.message || String(err))) || 'unknown error';

      // Record failure stats (domain-level)
      try {
        await incrementStats(domain, false);
      } catch (_) {}

      // Throttling detection
      const lowerMsg = String(message).toLowerCase();
      const throttlingIndicators = ['rate limit', 'throttl', 'too many', 'blocked', 'limit exceeded', 'try again later'];
      const isThrottleSignal =
        (typeof smtpCode === 'number' && [421, 450, 451, 452, 429].includes(Number(smtpCode))) ||
        throttlingIndicators.some((s) => lowerMsg.includes(s));

      if (isThrottleSignal) {
        // escalate blocking time proportional to attempts or recent fail rate
        const beforeRow = ledgerRow;
        const nextAttemptsEstimate = (beforeRow?.bgAttempts || 0) + 1;
        const failRate = await getDomainFailureRate(domain);
        const ttl = Math.min(60 * 60, Math.max(30, Math.round(DOMAIN_BLOCK_TTL_SEC * (1 + nextAttemptsEstimate * 0.5 + failRate * 4))));
        try {
          await setDomainBlock(domain, ttl, `smtp:${smtpCode} msg:${message}`);
        } catch (e) {}

        if (lowerMsg.includes('rate limit') || Number(smtpCode) === 421) {
          try {
            await setGlobalBlock(Math.min(60 * 60, GLOBAL_BLOCK_TTL_SEC), `smtp:${smtpCode}`);
          } catch (e) {}
        }
      }

      // Determine if this attempt was the final BullMQ attempt
      const currentAttempt = bgAttemptNumber; // job.attemptsMade + 1

      // Update ledger row with bgAttempt and lastError; DO NOT increment processed or mark final failed until we've exhausted MQ attempts
      const updateFields: any = {
        lastError: (err && err.message) ? err.message : String(err),
        lastAttemptAt: new Date(),
        bgAttempts: currentAttempt,
      };

      if (currentAttempt < MAX_ATTEMPTS) {
        // Intermediate failure: keep core attempt untouched, keep status as 'sending' or 'pending' so UI cannot manual-retry yet
        updateFields.status = 'sending';
        try {
          await db.collection('campaign_contacts').updateOne(ledgerFilter, { $set: updateFields });
        } catch (e) {
          console.warn('Failed to persist intermediate failure state', e);
        }

        // Publish contact update so UI shows increased bgAttempts and error
        await publishContactUpdate(campaignId, contactId, {
          status: updateFields.status,
          attempts: ledgerRow.attempts ?? 0,
          bgAttempts: currentAttempt,
          lastAttemptAt: updateFields.lastAttemptAt.toISOString(),
          lastError: updateFields.lastError,
        });

        // Ask BullMQ to retry by throwing the error — rely on BullMQ attempts/backoff configured on the job
        // Throwing preserves attemptsMade increment on retry; do not re-add a fresh job.
        throw err;
      } else {
        // Final background attempt exhausted: mark as failed, increment processed & failed counters (this is final)
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

        // publish final failed contact event
        await publishContactUpdate(campaignId, contactId, {
          status: 'failed',
          attempts: ledgerRow.attempts ?? 0,
          bgAttempts: currentAttempt,
          lastAttemptAt: updateFields.lastAttemptAt.toISOString(),
          lastError: updateFields.lastError,
        });

        // After final failure, attempt to finalize campaign as well
        try {
          await finalizeCampaignIfComplete(campaignId, db);
        } catch (_) {}

        // Throw the error so BullMQ marks job failed (keeps consistent queue state)
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

// Logging to show the worker is running
console.log('Campaign worker running with Redis-backed dynamic throttling and domain rate control');

// attach optional process-level handlers if you want to log job events (kept non-intrusive)
worker.on('completed', async (job) => {
  // We already handled success inside the job processor (DB updates & publishes).
  // This listener is left intentionally light to avoid duplicating logic.
  // Could be used for additional telemetry if desired.
});

worker.on('failed', async (job, err) => {
  // We already handle failed-case DB updates inside job processor (we throw on failure).
  // This listener is available for logging or external telemetry.
  try {
    console.warn(`Job ${job.id} failed after ${job.attemptsMade} attempts:`, err?.message ?? err);
  } catch (e) {
    console.warn('Job failed handler error', e);
  }
});
