// pages/api/campaign/[id]/events.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import { redis, subscribeToChannels } from '../../../../lib/redis';

/**
 * SSE endpoint that subscribes to Redis channels and forwards events to clients.
 * - Keeps the connection alive with periodic heartbeats.
 * - Supports node-redis v4 subscribe signature and ioredis-style subscribe.
 * - Forwards campaign-level events as "campaign" and contact-level as "contact".
 * - Protects against malformed JSON and newline payload issues by serializing here.
 */

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { id } = req.query;
  if (!id || typeof id !== 'string') {
    res.status(400).end('Invalid campaign id');
    return;
  }

  // SSE headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');

  try {
    res.flushHeaders();
  } catch {}

  // Duplicate Redis client for pub/sub
  const sub = (redis as any).duplicate();
  try {
    await sub.connect();
  } catch (err) {
    console.error('Failed to connect redis subscriber', err);
    res.write(`event: error\n`);
    res.write(`data: ${JSON.stringify({ error: 'redis-connect-failed' })}\n\n`);
    res.end();
    return;
  }

  const channels = [
    `campaign:${id}:contact_update`,
    'campaign:new',
  ];

  // SSE helper
  function writeEvent(eventName: string, payload: unknown) {
    try {
      const data = JSON.stringify(payload);
      res.write(`event: ${eventName}\n`);
      res.write(`data: ${data}\n\n`);
      try { (res as any).flush?.(); } catch {}
    } catch (e) {
      console.warn('Failed to write SSE event', e);
    }
  }

  // Standardized message handler
  const messageHandler = (channel: string, message: string) => {
    try {
      let payload: any = null;
      try {
        payload = typeof message === 'string' ? JSON.parse(message) : message;
      } catch {
        payload = { raw: String(message) };
      }

      if (channel === 'campaign:new') {
        writeEvent('campaign', payload);
      } else {
        writeEvent('contact', payload);
      }
    } catch (e) {
      console.warn('messageHandler error', e);
    }
  };

  // Use helper from lib/redis for robust subscription
  try {
    await subscribeToChannels(sub, channels, messageHandler);
  } catch (e) {
    console.error('Failed to set up Redis subscriptions', e);
    writeEvent('error', { error: 'subscribe-failed' });
  }

  // Heartbeat
  const heartbeatMs = 15000;
  const heartbeat = setInterval(() => {
    try {
      writeEvent('ping', { ts: Date.now() });
    } catch {}
  }, heartbeatMs);

  // Cleanup
  req.on('close', async () => {
    clearInterval(heartbeat);
    try { await sub.unsubscribe(channels); } catch {}
    try { await sub.quit(); } catch {}
    try { res.end(); } catch {}
  });
}
