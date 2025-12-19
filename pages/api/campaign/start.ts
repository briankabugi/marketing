import type { NextApiRequest, NextApiResponse } from 'next';
import { Queue } from 'bullmq';
import { redis } from '../../../lib/redis';
import clientPromise from '../../../lib/mongo';

const queue = new Queue('campaigns', { connection: redis });

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  /**
   * Expected payload:
   * {
   *   name: string,
   *   contacts: { type: 'all' } | { type: 'segment', value: string },
   *   initial: { subject: string, body: string },
   *   followUps?: Step[]
   * }
   */
  const { name, contacts, initial, followUps = [] } = req.body;

  // ---- Validation ----
  if (!name || typeof name !== 'string') {
    return res.status(400).json({ error: 'Campaign name required' });
  }

  if (!initial?.subject || !initial?.body) {
    return res.status(400).json({ error: 'Initial email subject and body required' });
  }

  if (!contacts || !['all', 'segment'].includes(contacts.type)) {
    return res.status(400).json({ error: 'Invalid contact selection' });
  }

  // ---- Load contacts from MongoDB ----
  const client = await clientPromise;
  const db = client.db('PlatformData');

  const query =
    contacts.type === 'all'
      ? {}
      : { segments: contacts.value };

  const contactDocs = await db
    .collection('contacts')
    .find(query)
    .project({ email: 1 })
    .toArray();

  if (contactDocs.length === 0) {
    return res.status(400).json({ error: 'No contacts matched selection' });
  }

  // ---- Create campaign ----
  const campaignId = Date.now().toString();
  const createdAt = new Date().toISOString();

  await redis.hset(`campaign:${campaignId}:meta`, {
    name,
    total: contactDocs.length,
    processed: 0,
    status: 'running',
    createdAt,
    initialSubject: initial.subject,
    initialBody: initial.body,
    followUps: JSON.stringify(followUps),
  });

  await redis.sadd('campaign:all', campaignId);

  // ---- Enqueue jobs ----
  for (const c of contactDocs) {
    if (!c.email) continue;

    await queue.add(
      'send',
      {
        campaignId,
        contact: { email: c.email },
        stepIndex: 0, // initial email
      },
      {
        removeOnComplete: true,
        removeOnFail: true,
      }
    );
  }

  // ---- Notify UI (SSE / live updates) ----
  await redis.publish(
    'campaign:new',
    JSON.stringify({
      id: campaignId,
      name,
      total: contactDocs.length,
      createdAt,
    })
  );

  return res.status(201).json({
    campaignId,
    total: contactDocs.length,
  });
}
