// workers/campaignWorker.ts
import 'dotenv/config';
import { Worker, Queue } from 'bullmq';
import { redis } from '../lib/redis';
import clientPromise from '../lib/mongo';
import nodemailer from 'nodemailer';
import { ObjectId } from 'mongodb';

const queue = new Queue('campaigns', { connection: redis });
const MAX_ATTEMPTS = 3;

const transporter = nodemailer.createTransport({
  host: process.env.ZOHO_HOST,
  port: Number(process.env.ZOHO_PORT || 587),
  secure: false,
  auth: {
    user: process.env.ZOHO_USER,
    pass: process.env.ZOHO_PASS,
  },
});

function computeRetryDelay(attempts: number) {
  const base = 60 * 1000;
  return Math.pow(2, attempts - 1) * base;
}

new Worker(
  'campaigns',
  async (job) => {
    const { campaignId, contactId, step } = job.data as {
      campaignId: string;
      contactId: string;
      step?: any;
    };

    if (!campaignId || !contactId) return;

    const client = await clientPromise;
    const db = client.db('PlatformData');

    const campaignObjectId = new ObjectId(campaignId);
    const contactObjectId = new ObjectId(contactId);

    // --- Load ledger entry (idempotency gate) ---
    const ledger = await db.collection('campaign_contacts').findOne({
      campaignId: campaignObjectId,
      contactId: contactObjectId,
    });

    if (!ledger) return;

    // Already sent → NOOP
    if (ledger.status === 'sent') return;

    if (ledger.attempts >= MAX_ATTEMPTS) {
      await db.collection('campaign_contacts').updateOne(
        { _id: ledger._id },
        { $set: { status: 'failed', lastAttemptAt: new Date() } }
      );
      return;
    }

    // --- Load contact (send-time unsubscribe check) ---
    const contact = await db.collection('contacts').findOne({
      _id: contactObjectId,
      unsubscribedAt: { $exists: false },
    });

    if (!contact || !contact.email) {
      await db.collection('campaign_contacts').updateOne(
        { _id: ledger._id },
        {
          $set: {
            status: 'failed',
            lastError: 'missing or unsubscribed contact',
            lastAttemptAt: new Date(),
          },
          $inc: { attempts: 1 },
        }
      );
      await redis.hincrby(`campaign:${campaignId}:meta`, 'failed', 1);
      return;
    }

    // --- Load campaign definition ---
    const defRaw = await redis.get(`campaign:${campaignId}:definition`);
    if (!defRaw) throw new Error('Missing campaign definition');

    const definition = JSON.parse(defRaw);
    const isInitial = job.name === 'initial';

    const subject = isInitial
      ? definition.initial.subject
      : step?.subject ?? definition.initial.subject;

    const body = isInitial
      ? definition.initial.body
      : step?.body ?? definition.initial.body;

    // --- Attempt send ---
    try {
      await transporter.sendMail({
        from: process.env.SMTP_FROM,
        to: contact.email,
        subject,
        html: body,
      });
    } catch (err: any) {
      const attempts = ledger.attempts + 1;

      await db.collection('campaign_contacts').updateOne(
        { _id: ledger._id },
        {
          $set: {
            status: attempts >= MAX_ATTEMPTS ? 'failed' : 'pending',
            lastError: err?.message ?? String(err),
            lastAttemptAt: new Date(),
          },
          $inc: { attempts: 1 },
        }
      );

      if (attempts < MAX_ATTEMPTS) {
        await queue.add(
          job.name,
          { campaignId, contactId, step },
          {
            delay: computeRetryDelay(attempts),
            removeOnComplete: true,
            removeOnFail: true,
          }
        );
      } else {
        await redis.hincrby(`campaign:${campaignId}:meta`, 'failed', 1);
        await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1);
      }

      return;
    }

    // --- Success ---
    await db.collection('campaign_contacts').updateOne(
      { _id: ledger._id },
      {
        $set: { status: 'sent', lastAttemptAt: new Date() },
        $inc: { attempts: 1 },
      }
    );

    await redis.hincrby(`campaign:${campaignId}:meta`, 'sent', 1);
    await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1);

    // --- Schedule follow-ups ONCE ---
    if (isInitial && ledger.followUpIndex == null) {
      const followUps = definition.followUps || [];

      for (let i = 0; i < followUps.length; i++) {
        const f = followUps[i];
        if (!f?.delayMinutes) continue;

        await queue.add(
          'followup',
          { campaignId, contactId, step: f },
          { delay: f.delayMinutes * 60 * 1000 }
        );
      }

      await db.collection('campaign_contacts').updateOne(
        { _id: ledger._id },
        { $set: { followUpIndex: followUps.length } }
      );
    }

    // --- Completion check (safe) ---
    const remaining = await db.collection('campaign_contacts').countDocuments({
      campaignId: campaignObjectId,
      status: 'pending',
    });

    if (remaining === 0) {
      const meta = await redis.hgetall(`campaign:${campaignId}:meta`);

      await db.collection('campaigns').updateOne(
        { _id: campaignObjectId },
        {
          $set: {
            status: 'completed',
            completedAt: new Date(),
            'totals.processed': Number(meta.processed || 0),
            'totals.sent': Number(meta.sent || 0),
            'totals.failed': Number(meta.failed || 0),
          },
        }
      );

      await redis.hset(`campaign:${campaignId}:meta`, 'status', 'completed');
      await redis.publish(
        'campaign:new',
        JSON.stringify({ campaignId, status: 'completed' })
      );
    }
  },
  { connection: redis }
);

console.log('Campaign worker running');