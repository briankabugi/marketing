// pages/api/campaign/start.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import { Queue } from 'bullmq';
import { redis } from '../../../lib/redis';
import clientPromise from '../../../lib/mongo';
import { ObjectId } from 'mongodb';

const queue = new Queue('campaigns', { connection: redis });
const MAX_ATTEMPTS = 3;

// utility to chunk arrays
function chunkArray<T>(arr: T[], size = 1000): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { name, contacts, initial, followUps = [] } = req.body;

  if (!name || typeof name !== 'string') {
    return res.status(400).json({ error: 'Campaign name required' });
  }

  if (!initial?.subject || !initial?.body) {
    return res.status(400).json({ error: 'Initial email subject and body required' });
  }

  if (!contacts || !['all', 'segment'].includes(contacts.type)) {
    return res.status(400).json({ error: 'Invalid contact selection' });
  }

  const client = await clientPromise;
  const db = client.db('PlatformData');

  const query =
    contacts.type === 'all'
      ? {}
      : { segments: contacts.value };

  const contactDocs = await db
    .collection('contacts')
    .find(query)
    .project({ _id: 1, email: 1 })
    .toArray();

  if (!contactDocs.length) {
    return res.status(400).json({ error: 'No contacts matched selection' });
  }

  const createdAt = new Date();

  // --- Create campaign document ---
  const campaignDoc = {
    name,
    contacts,
    initial,
    followUps,
    totals: {
      intended: contactDocs.length,
      processed: 0,
      sent: 0,
      failed: 0,
    },
    status: 'running',
    createdAt,
  };

  const campaignResult = await db
    .collection('campaigns')
    .insertOne(campaignDoc);

  const campaignObjectId = campaignResult.insertedId;
  const campaignId = campaignObjectId.toString();

  // --- Insert ledger idempotently ---
  const ledgerDocs = contactDocs.map((c) => ({
    campaignId: campaignObjectId,
    contactId: c._id,
    status: 'pending',
    attempts: 0,
    bgAttempts: 0,
    createdAt: new Date(),
  }));

  const chunks = chunkArray(ledgerDocs, 1000);
  for (const chunk of chunks) {
    try {
      await db
        .collection('campaign_contacts')
        .insertMany(chunk, { ordered: false });
    } catch (e) {
      // Duplicate key errors are safe to ignore (idempotency)
    }
  }

  // --- Redis runtime meta (best-effort) ---
  try {
    await redis.hset(`campaign:${campaignId}:meta`, {
      name,
      total: String(contactDocs.length),
      processed: '0',
      sent: '0',
      failed: '0',
      status: 'running',
      createdAt: createdAt.toISOString(),
    });

    await redis.set(
      `campaign:${campaignId}:definition`,
      JSON.stringify({ initial, followUps, name, contacts })
    );

    await redis.sadd('campaign:all', campaignId);
  } catch (e) {
    console.warn('Redis unavailable during campaign start', e);
  }

  // --- Enqueue initial jobs (MUST include attempts + backoff for BullMQ retry system) ---
  for (const c of contactDocs) {
    if (!c.email) continue;

    await queue.add(
      'initial',
      {
        campaignId,
        contactId: c._id.toString(),
      },
      {
        attempts: MAX_ATTEMPTS,
        backoff: {
          type: 'exponential',
          delay: 60_000, // 1 minute base
        },
        removeOnComplete: true,
        removeOnFail: true,
      }
    );
  }

  // --- Notify UI ---
  try {
    await redis.publish(
      'campaign:new',
      JSON.stringify({
        id: campaignId,
        name,
        total: contactDocs.length,
        createdAt: createdAt.toISOString(),
      })
    );
  } catch {
    // non-fatal
  }

  return res.status(201).json({
    campaignId,
    total: contactDocs.length,
  });
}
