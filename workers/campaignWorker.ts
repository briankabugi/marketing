// workers/campaignWorker.ts
import 'dotenv/config'; // critical
import { Worker, Queue } from 'bullmq';
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

// exponential backoff for retries
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
// Redis-backed helpers
// ---------------------

/**
 * Remove old timestamps and return current count for a sorted set key.
 * Works with Redis sorted-set where members are unique strings and score==timestamp.
 */
async function getWindowCount(zkey: string, windowMs: number) {
  const now = Date.now();
  const min = 0;
  const max = now - windowMs;
  // Remove older than window
  // Note: method names depend on your redis client; assuming ioredis or node-redis v4 compatibility
  try {
    await (redis as any).zremrangebyscore(zkey, 0, now - windowMs);
    const count = await (redis as any).zcard(zkey);
    return Number(count || 0);
  } catch (e) {
    // fallback: if redis doesn't support zset, degrade gracefully by allowing send
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

// ---------------------
// Worker logic
// ---------------------

new Worker(
  'campaigns',
  async (job) => {
    const { campaignId, contactId, step } = job.data as { campaignId: string; contactId?: string; step?: any };
    if (!campaignId || !contactId) {
      console.warn('Job missing campaignId or contactId', job.id);
      return;
    }

    // --- Check campaign status ---
    const campaignStatus = await redis.hget(`campaign:${campaignId}:meta`, 'status');
    if (campaignStatus === 'paused' || campaignStatus === 'cancelled') {
      console.log(`Skipping job ${job.id} for campaign ${campaignId} due to status ${campaignStatus}`);
      return;
    }

    const client = await clientPromise;
    const db = client.db('PlatformData');

    // --- Load contact ---
    const contact = await db.collection('contacts').findOne({ _id: new ObjectId(contactId) });
    if (!contact || !contact.email) {
      await db.collection('campaign_contacts').updateOne(
        { campaignId: new ObjectId(campaignId), contactId: new ObjectId(contactId) },
        { $set: { status: 'failed', lastError: 'missing contact or email', lastAttemptAt: new Date() }, $inc: { attempts: 1 } }
      );
      await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1);
      await redis.hincrby(`campaign:${campaignId}:meta`, 'failed', 1);
      return;
    }

    const domain = contact.email.split('@')[1] || '';

    // --- Try to acquire permit to send now ---
    const permit = await tryAcquireSendPermit(domain);
    if (!permit.ok) {
      // dynamic backoff + jitter
      const requeueDelay = jitter( (permit.reason === 'global-blocked' || permit.reason === 'global-capacity') ? GLOBAL_BLOCK_TTL_SEC * 1000 : 500 + Math.random() * 2000 );
      console.log(`Throttling send to domain ${domain} (reason=${permit.reason}), requeueing job ${job.id} with ${requeueDelay}ms delay`);
      await queue.add(
        job.name,
        { campaignId, contactId, step },
        { delay: requeueDelay, removeOnComplete: true, removeOnFail: true }
      );
      return;
    }

    const defRaw = await redis.get(`campaign:${campaignId}:definition`);
    if (!defRaw) {
      console.warn('Missing campaign definition for', campaignId);
      throw new Error('Missing campaign definition');
    }

    const definition = JSON.parse(defRaw);
    const isInitial = job.name === 'initial';
    const subject = isInitial ? definition.initial.subject : (step?.subject || definition.initial.subject);
    const body = isInitial ? definition.initial.body : (step?.body || definition.initial.body);

    try {
      await transporter.sendMail({
        from: process.env.SMTP_FROM,
        to: contact.email,
        subject,
        html: body,
      });

      // success: update DB & stats
      await db.collection('campaign_contacts').updateOne(
        { campaignId: new ObjectId(campaignId), contactId: new ObjectId(contactId) },
        { $set: { status: 'sent', lastAttemptAt: new Date() }, $inc: { attempts: 1 } }
      );
      await incrementStats(domain, true);

      if (isInitial) {
        await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1);
        await redis.hincrby(`campaign:${campaignId}:meta`, 'sent', 1);

        for (const followUp of (definition.followUps || []) as any[]) {
          if (!followUp || !followUp.delayMinutes) continue;
          await queue.add(
            'followup',
            { campaignId, contactId, step: followUp },
            { delay: followUp.delayMinutes * 60 * 1000, removeOnComplete: true, removeOnFail: true }
          );
        }

        const meta = await redis.hgetall(`campaign:${campaignId}:meta`);
        const processed = Number(meta.processed || 0);
        const total = Number(meta.total || 0);

        if (total > 0 && processed >= total) {
          await db.collection('campaigns').updateOne(
            { _id: new ObjectId(campaignId) },
            {
              $set: {
                status: 'completed',
                completedAt: new Date(),
                'totals.processed': processed,
                'totals.sent': Number(meta.sent || 0),
                'totals.failed': Number(meta.failed || 0),
              },
            }
          );

          await redis.hset(`campaign:${campaignId}:meta`, 'status', 'completed');
          await redis.publish('campaign:new', JSON.stringify({ campaignId, status: 'completed' }));
          await redis.del(`campaign:${campaignId}:definition`);
        }
      }
      return;
    } catch (err: any) {
      // --- Failure handling with dynamic throttling decisions ---
      // Inspect SMTP response code/message for throttling signals (420/421/450/451/452 etc) and keywords.
      const smtpCode = err && (err.responseCode || err.code || err.statusCode);
      const message = (err && (err.response || err.message || String(err))) || 'unknown error';

      // Record failure stats
      await incrementStats(domain, false);

      // If we see explicit "rate limit", "throttle", or certain SMTP codes, set a domain block
      const lowerMsg = String(message).toLowerCase();
      const throttlingIndicators = ['rate limit', 'throttl', 'too many', 'blocked', 'limit exceeded', 'try again later'];
      const isThrottleSignal =
        (typeof smtpCode === 'number' && [421, 450, 451, 452, 429].includes(Number(smtpCode))) ||
        throttlingIndicators.some((s) => lowerMsg.includes(s));

      if (isThrottleSignal) {
        // escalate blocking time proportional to attempts or recent fail rate
        const before = await db.collection('campaign_contacts').findOne({
          campaignId: new ObjectId(campaignId),
          contactId: new ObjectId(contactId),
        });
        const nextAttempts = (before?.attempts || 0) + 1;
        const failRate = await getDomainFailureRate(domain);
        // TTL increases with attempts and with high failRate
        const ttl = Math.min(60 * 60, Math.max(30, Math.round(DOMAIN_BLOCK_TTL_SEC * (1 + nextAttempts * 0.5 + failRate * 4))));
        await setDomainBlock(domain, ttl, `smtp:${smtpCode} msg:${message}`);

        // If the error looks like a global problem (e.g., many domains failing), consider global block
        if (lowerMsg.includes('rate limit') || Number(smtpCode) === 421) {
          await setGlobalBlock(Math.min(60 * 60, GLOBAL_BLOCK_TTL_SEC), `smtp:${smtpCode}`);
        }
      }

      // update DB record for contact
      const before = await db.collection('campaign_contacts').findOne({
        campaignId: new ObjectId(campaignId),
        contactId: new ObjectId(contactId),
      });
      const nextAttempts = (before?.attempts || 0) + 1;

      await db.collection('campaign_contacts').updateOne(
        { campaignId: new ObjectId(campaignId), contactId: new ObjectId(contactId) },
        {
          $set: {
            status: nextAttempts >= MAX_ATTEMPTS ? 'failed' : 'failed',
            lastError: (err && err.message) ? err.message : String(err),
            lastAttemptAt: new Date(),
          },
          $inc: { attempts: 1 },
        }
      );

      await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1);
      await redis.hincrby(`campaign:${campaignId}:meta`, 'failed', 1);

      // If we can retry, schedule with exponential backoff + jitter
      if (nextAttempts < MAX_ATTEMPTS) {
        const delay = jitter(computeRetryDelay(nextAttempts));
        console.log(`Retrying job ${job.id} for ${contact.email} in ${delay}ms (attempt ${nextAttempts}) due to error:`, message);
        await queue.add(
          job.name,
          { campaignId, contactId, step },
          { delay, removeOnComplete: true, removeOnFail: true }
        );
      }
      return;
    }
  },
  {
    connection: redis,
    // Optional global concurrency limiter
    concurrency: Number(process.env.WORKER_CONCURRENCY || 5),
    // keep the bullmq limiter as a global safety net; primary throttling handled above via redis
    limiter: {
      max: Number(process.env.WORKER_LIMITER_MAX || RATE_LIMIT_MAX),
      duration: Number(process.env.WORKER_LIMITER_DURATION || RATE_LIMIT_DURATION),
    },
  }
);

console.log('Campaign worker running with Redis-backed dynamic throttling and domain rate control');