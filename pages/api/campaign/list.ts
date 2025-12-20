// pages/api/campaign/list.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import clientPromise from '../../../lib/mongo';
import { redis } from '../../../lib/redis';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const client = await clientPromise;
  const db = client.db('PlatformData');

  // read campaigns from Mongo (persisted)
  const campaigns = await db.collection('campaigns').find({}).sort({ createdAt: -1 }).toArray();

  // For each campaign, try merging in Redis meta (if present)
  const out = await Promise.all(
    campaigns.map(async (c) => {
      const id = c._id.toString();
      const meta = await redis.hgetall(`campaign:${id}:meta`);
      return {
        id,
        name: c.name,
        total: Number(meta?.total ?? (c.totals?.intended ?? 0)),
        processed: Number(meta?.processed ?? (c.totals?.processed ?? 0)),
        sent: Number(meta?.sent ?? (c.totals?.sent ?? 0)),
        failed: Number(meta?.failed ?? (c.totals?.failed ?? 0)),
        status: meta?.status ?? c.status ?? 'running',
        createdAt: meta?.createdAt ?? (c.createdAt?.toISOString ? c.createdAt.toISOString() : c.createdAt),
      };
    })
  );

  res.status(200).json({ campaigns: out });
}
