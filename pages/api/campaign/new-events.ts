// pages/api/campaign/new-events.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import { redis } from '../../../lib/redis';

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  // Standard SSE headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  // Disable buffering for nginx/fastly proxies
  res.setHeader('X-Accel-Buffering', 'no');
  (res as any).flushHeaders && (res as any).flushHeaders();

  // Create a dedicated subscriber so we don't interfere with main client usage
  const sub = redis.duplicate();

  try {
    await sub.connect();
  } catch (err) {
    console.error('Failed to connect redis subscriber for /campaign/new-events', err);
    // Send an SSE error event then close
    try {
      res.write(`event: error\n`);
      res.write(`data: ${JSON.stringify({ error: 'redis-connect-failed' })}\n\n`);
    } catch (_) { }
    res.end();
    return;
  }

  // Heartbeat to keep connection alive for proxies/load balancers
  const heartbeatMs = 15000;
  const heartbeat = setInterval(() => {
    try {
      res.write(`event: ping\n`);
      res.write(`data: ${JSON.stringify({ ts: Date.now() })}\n\n`);
    } catch (e) {
      // ignore writes errors
    }
  }, heartbeatMs);

  // Subscribe to campaign:new events
  try {
    await sub.subscribe("campaign:new");

    sub.on("message", (channel, msg) => {
      if (channel !== "campaign:new") return;
      try {
        res.write(`data: ${msg}\n\n`);
      } catch (e) {
        console.warn("SSE write failed", e);
      }
    });
  } catch (err) {
    console.error('Failed to subscribe to campaign:new', err);
    try {
      res.write(`event: error\n`);
      res.write(`data: ${JSON.stringify({ error: 'subscribe-failed' })}\n\n`);
    } catch (_) { }
  }

  // Cleanup on client disconnect
  req.on('close', async () => {
    clearInterval(heartbeat);
    try {
      // best-effort unsubscribe + quit
      try { await sub.unsubscribe('campaign:new'); } catch (_) { }
      try { await sub.quit(); } catch (_) { }
    } catch (e) {
      console.warn('Error while cleaning up redis subscriber for /campaign/new-events', e);
    }
    try { res.end(); } catch (_) { }
  });
}
