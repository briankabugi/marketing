import type { NextApiRequest, NextApiResponse } from 'next';
import { redis } from '../../../lib/redis';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const sub = redis.duplicate();
  await sub.connect();

  await sub.subscribe('campaign:new', (msg) => {
    res.write(`data: ${msg}\n\n`);
  });

  req.on('close', () => sub.disconnect());
}