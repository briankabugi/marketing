// workers/campaignWorker.ts
import 'dotenv/config'; // critical
import { Worker, Queue } from 'bullmq';
import { redis } from '../lib/redis';
import clientPromise from '../lib/mongo';
import nodemailer from 'nodemailer';
import { ObjectId } from 'mongodb';

const queue = new Queue('campaigns', { connection: redis });
const MAX_ATTEMPTS = 3;

// --- Per-account rate limiting config ---
const RATE_LIMIT_MAX = Number(process.env.EMAIL_RATE_MAX || 20); // max emails
const RATE_LIMIT_DURATION = Number(process.env.EMAIL_RATE_DURATION || 1000); // ms

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

// simple domain throttling map
const domainTimestamps: Record<string, number[]> = {};

function canSendToDomain(domain: string) {
  const now = Date.now();
  domainTimestamps[domain] = domainTimestamps[domain] || [];
  // remove timestamps older than RATE_LIMIT_DURATION
  domainTimestamps[domain] = domainTimestamps[domain].filter(ts => now - ts < RATE_LIMIT_DURATION);
  if (domainTimestamps[domain].length >= RATE_LIMIT_MAX) return false;
  domainTimestamps[domain].push(now);
  return true;
}

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
    if (!canSendToDomain(domain)) {
      console.log(`Throttling send to domain ${domain}, requeueing job ${job.id}`);
      await queue.add(
        job.name,
        { campaignId, contactId, step },
        { delay: 500, removeOnComplete: true, removeOnFail: true } // small delay, retry soon
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
    } catch (err: any) {
      const before = await db.collection('campaign_contacts').findOne({
        campaignId: new ObjectId(campaignId),
        contactId: new ObjectId(contactId),
      });
      const nextAttempts = (before?.attempts || 0) + 1;

      await db.collection('campaign_contacts').updateOne(
        { campaignId: new ObjectId(campaignId), contactId: new ObjectId(contactId) },
        {
          $set: { status: nextAttempts >= MAX_ATTEMPTS ? 'failed' : 'failed', lastError: (err && err.message) ? err.message : String(err), lastAttemptAt: new Date() },
          $inc: { attempts: 1 },
        }
      );

      await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1);
      await redis.hincrby(`campaign:${campaignId}:meta`, 'failed', 1);

      if (nextAttempts < MAX_ATTEMPTS) {
        await queue.add(
          job.name,
          { campaignId, contactId, step },
          { delay: computeRetryDelay(nextAttempts), removeOnComplete: true, removeOnFail: true }
        );
      }
      return;
    }

    // --- Success path ---
    await db.collection('campaign_contacts').updateOne(
      { campaignId: new ObjectId(campaignId), contactId: new ObjectId(contactId) },
      { $set: { status: 'sent', lastAttemptAt: new Date() }, $inc: { attempts: 1 } }
    );

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
  },
  {
    connection: redis,
    // Optional global concurrency limiter
    concurrency: Number(process.env.WORKER_CONCURRENCY || 5),
    limiter: {
      max: RATE_LIMIT_MAX,
      duration: RATE_LIMIT_DURATION,
    },
  }
);

console.log('Campaign worker running with rate limiting and domain throttling');