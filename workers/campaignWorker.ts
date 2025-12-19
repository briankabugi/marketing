import { Worker, Queue, Job } from 'bullmq';
import { redis } from '../lib/redis';
import nodemailer from 'nodemailer';

type Contact = { email: string };
type Step = { delayMinutes: number; subject: string; body: string };

const queue = new Queue('campaigns', { connection: redis });

const transporter = nodemailer.createTransport({
  host: process.env.ZOHO_HOST,
  port: Number(process.env.ZOHO_PORT || 587),
  secure: false,
  auth: {
    user: process.env.ZOHO_USER,
    pass: process.env.ZOHO_PASS,
  },
});

new Worker(
  'campaigns',
  async (job: Job) => {
    // job.names: 'initial' or 'followup'
    const { campaignId, contact, step } = job.data as { campaignId: string; contact: Contact; step?: Step };

    const defRaw = await redis.get(`campaign:${campaignId}:definition`);
    if (!defRaw) {
      // nothing to do
      console.warn('Missing campaign definition for', campaignId);
      return;
    }

    const definition = JSON.parse(defRaw);
    const isInitial = job.name === 'initial';

    const subject = isInitial ? definition.initial.subject : (step?.subject || definition.initial.subject);
    const body = isInitial ? definition.initial.body : (step?.body || definition.initial.body);

    // send (synchronously)
    try {
      await transporter.sendMail({
        from: process.env.SMTP_FROM,
        to: contact.email,
        subject,
        html: body,
      });
    } catch (err) {
      console.error('Mail send failed', err);
      // mail provider errors are left to queue retry policy
      throw err;
    }

    // For 'initial' jobs: increment processed and schedule followups
    if (isInitial) {
      await redis.hincrby(`campaign:${campaignId}:meta`, 'processed', 1);

      // schedule follow-ups
      for (const followUp of (definition.followUps || []) as Step[]) {
        if (!followUp || !followUp.delayMinutes) continue;
        await queue.add(
          'followup',
          { campaignId, contact, step: followUp },
          { delay: followUp.delayMinutes * 60 * 1000 }
        );
      }
    } else {
      // followup sends do not alter processed count (we count initial sends for campaign completion)
    }

    // check completion (processed equals total)
    const meta = await redis.hgetall(`campaign:${campaignId}:meta`);
    const processed = Number(meta.processed || 0);
    const total = Number(meta.total || 0);
    if (total > 0 && processed >= total) {
      await redis.hset(`campaign:${campaignId}:meta`, 'status', 'completed');
      await redis.publish('campaign:new', JSON.stringify({ campaignId, status: 'completed' }));
    }
  },
  { connection: redis }
);

console.log('Campaign worker running');
