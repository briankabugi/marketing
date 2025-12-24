// pages/api/campaign/[id]/events.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import { redis as mainRedis } from '../../../../lib/redis';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { id } = req.query;
  if (!id || typeof id !== 'string') {
    res.status(400).end('Invalid campaign id');
    return;
  }

  // Set SSE headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');

  // Keep connection open
  res.flushHeaders();

  // Duplicate a Redis connection for pub/sub
  const sub = (mainRedis as any).duplicate();
  await sub.connect();

  const channels = [
    `campaign:${id}:contact_update`,
    'campaign:new', // keep existing channel for compatibility
  ];

  // Message handler: forward as SSE events
  const messageHandler = (channel: string, message: string) => {
    try {
      // Send event named after channel (normalized)
      const eventName = channel === 'campaign:new' ? 'campaign' : 'contact';
      // SSE format: event: <name>\ndata: <json>\n\n
      res.write(`event: ${eventName}\n`);
      // escape newlines in payload by replacing with \\n inside JSON string
      res.write(`data: ${message}\n\n`);
    } catch (e) {
      console.warn('SSE send failed', e);
    }
  };

  // Subscribe to channels
  try {
    await sub.subscribe(channels, (msg: string, ch: string) => {
      // ioredis v5 subscribe signature differs; use message handler above if provided
      // But to be compatible, call messageHandler
      messageHandler(ch, msg);
    });
  } catch (e) {
    console.error('Failed to subscribe to redis channels', e);
    res.write(`event: error\n`);
    res.write(`data: ${JSON.stringify({ error: 'subscribe-failed' })}\n\n`);
  }

  // Cleanup on client disconnect
  req.on('close', async () => {
    try {
      await sub.unsubscribe(channels);
    } catch {}
    try { await sub.quit(); } catch {}
    res.end();
  });
}
