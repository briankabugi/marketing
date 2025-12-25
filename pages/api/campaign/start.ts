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

/**
 * Attachment validation rules:
 * - Accept attachment metadata only (no file upload here).
 * - Each attachment must have: name (string), source ('url'|'path'|'content')
 * - If source === 'url' => url must be valid http(s) url
 * - If source === 'path' => path must not contain '..' (basic traversal protection)
 * - If source === 'content' => content should be base64 string (we don't decode here except minimal length check)
 */
type Attachment = {
  name: string;
  source: 'url' | 'path' | 'content';
  url?: string;
  path?: string;
  content?: string; // base64
  contentType?: string;
};

function isValidUrl(u: any) {
  if (typeof u !== 'string') return false;
  try {
    const parsed = new URL(u);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch {
    return false;
  }
}

function hasDotDotSegments(p: string) {
  // normalize and check for .. segments (basic)
  return p.split('/').some(segment => segment === '..');
}

function validateAttachments(arr: any): Attachment[] {
  if (!arr) return [];
  if (!Array.isArray(arr)) throw new Error('attachments must be an array');
  const out: Attachment[] = [];
  const MAX_ATTACHMENTS = 10;
  const MAX_INLINE_BASE64_BYTES = 10 * 1024 * 1024; // 10 MB cap for inline base64 content

  if (arr.length > MAX_ATTACHMENTS) {
    throw new Error(`Too many attachments; max ${MAX_ATTACHMENTS}`);
  }

  for (const a of arr) {
    if (!a || typeof a !== 'object') throw new Error('Invalid attachment entry');
    const name = typeof a.name === 'string' && a.name.trim() ? a.name.trim() : null;
    const source = (a.source === 'url' || a.source === 'path' || a.source === 'content') ? a.source : null;
    const contentType = typeof a.contentType === 'string' ? a.contentType.trim() : undefined;

    if (!name) throw new Error('Attachment missing name');
    if (!source) throw new Error('Attachment missing valid source');

    if (source === 'url') {
      if (!isValidUrl(a.url)) throw new Error(`Attachment url invalid for ${name}`);
      out.push({ name, source, url: a.url, contentType });
    } else if (source === 'path') {
      if (typeof a.path !== 'string' || !a.path.trim()) throw new Error(`Attachment path missing for ${name}`);
      if (hasDotDotSegments(a.path)) throw new Error(`Attachment path contains .. segments (not allowed): ${name}`);
      // prefer relative paths for safety; do not resolve/validate existence here
      out.push({ name, source, path: a.path.trim(), contentType });
    } else if (source === 'content') {
      if (typeof a.content !== 'string' || !a.content.trim()) throw new Error(`Attachment content missing for ${name}`);
      // crude base64 length estimate: base64 length * 3/4
      const estimatedBytes = Math.floor((a.content.length * 3) / 4);
      if (estimatedBytes > MAX_INLINE_BASE64_BYTES) throw new Error(`Attachment ${name} exceeds inline size limit`);
      // Basic base64 characters validation (not perfect)
      if (!/^[A-Za-z0-9+/=\s]+$/.test(a.content)) {
        throw new Error(`Attachment ${name} content is not valid base64`);
      }
      out.push({ name, source, content: a.content.replace(/\s+/g, ''), contentType });
    }
  }

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

  // Validate and normalize attachments (if provided)
  try {
    if (initial.attachments) {
      initial.attachments = validateAttachments(initial.attachments);
    }
    if (Array.isArray(followUps)) {
      for (let i = 0; i < followUps.length; i++) {
        if (followUps[i]?.attachments) {
          followUps[i].attachments = validateAttachments(followUps[i].attachments);
        }
      }
    }
  } catch (e: any) {
    return res.status(400).json({ error: `Invalid attachments: ${e.message || String(e)}` });
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

  // --- Create campaign document (persist attachments metadata) ---
  const campaignDoc: any = {
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

    // Persist an enriched definition (includes attachments metadata) for the worker to pick up
    const definition = { initial, followUps, name, contacts };
    await redis.set(
      `campaign:${campaignId}:definition`,
      JSON.stringify(definition)
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
