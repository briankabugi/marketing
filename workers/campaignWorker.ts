
import { Worker, Queue, Job } from 'bullmq';
import { redis } from '../lib/redis';
import nodemailer from 'nodemailer';

type Step = { delayMinutes: number; subject: string; body: string };
type Contact = { email: string };

const queue = new Queue('campaigns', { connection: redis });

const transporter = nodemailer.createTransport({
  host: process.env.ZOHO_HOST,
  port: Number(process.env.ZOHO_PORT),
  secure: false,
  auth: {
    user: process.env.ZOHO_USER,
    pass: process.env.ZOHO_PASS
  }
});

new Worker(
  'campaigns',
  async (job: Job) => {
    const { campaignId, contact } = job.data;

    await transporter.sendMail({
      from: process.env.SMTP_FROM,
      to: contact.email,
      subject: 'Test Campaign Email',
      html: '<p>Hello from campaign</p>'
    });

    await redis.hincrby(`campaign:${campaignId}:meta`, 'sent', 1);
  },
  { connection: redis }
);

console.log('Worker running');
