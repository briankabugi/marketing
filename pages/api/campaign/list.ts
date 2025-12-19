import type { NextApiRequest, NextApiResponse } from 'next';
import { redis } from '../../../lib/redis';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const ids = await redis.smembers('campaign:all');

  const campaigns = await Promise.all(
    ids.map(async (id) => {
      const meta = await redis.hgetall(`campaign:${id}:meta`);
      return {
        id,
        name: meta.name,
        total: Number(meta.total || 0),
        processed: Number(meta.processed || 0),
        status: meta.status || 'running',
        createdAt: meta.createdAt,
      };
    })
  );

  res.status(200).json({ campaigns });
}
