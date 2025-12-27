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
  const sub = (redis as any).duplicate();

  try {
    // connect with same defensive approach used elsewhere
    await sub.connect().catch((err: any) => {
      if (err && String(err).includes('already connecting')) {
        console.warn('new-events: sub.connect already connecting — continuing');
      } else {
        console.warn('new-events: sub.connect failed (continuing)', err);
      }
    });
  } catch (err) {
    console.error('Failed to connect redis subscriber for /campaign/new-events', err);
    // Send an SSE error event then close
    try {
      res.write(`event: error\n`);
      res.write(`data: ${JSON.stringify({ error: 'redis-connect-failed' })}\n\n`);
    } catch (_) {}
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

  // Helper to write SSE with event name 'campaign' for consistency with other endpoints
  function writeCampaignEvent(payloadRaw: string) {
    try {
      // try parse JSON payload, but allow plain string too
      let payload: any;
      try {
        payload = JSON.parse(payloadRaw);
      } catch {
        payload = { raw: String(payloadRaw) };
      }
      const data = JSON.stringify(payload);
      res.write(`event: campaign\n`);
      res.write(`data: ${data}\n\n`);
      try { (res as any).flush?.(); } catch {}
    } catch (e) {
      console.warn('SSE write failed', e);
    }
  }

  // Subscribe to campaign:new events
  try {
    // If the redis client supports the subscribe callback API (node-redis v4),
    // prefer that; otherwise fall back to 'message' event emitter pattern.
    if (typeof (sub as any).subscribe === 'function' && (sub as any).subscribe.length >= 1) {
      // node-redis v4 style: await sub.subscribe(channel, listener)
      try {
        await (sub as any).subscribe('campaign:new', (message: string) => {
          writeCampaignEvent(message);
        });
      } catch (e) {
        // fallback to event emitter approach below
        console.warn('new-events: subscribe(callback) failed, falling back to event emitter', e);
        // attempt emitter style subscribe
        try {
          (sub as any).on('message', (channel: string, msg: string) => {
            if (channel !== 'campaign:new') return;
            writeCampaignEvent(msg);
          });
          try { (sub as any).subscribe('campaign:new'); } catch (err) { /* best-effort */ }
        } catch (_) { throw e; }
      }
    } else {
      // older redis clients: use 'message' event + subscribe
      (sub as any).on('message', (channel: string, msg: string) => {
        if (channel !== 'campaign:new') return;
        writeCampaignEvent(msg);
      });
      await (sub as any).subscribe('campaign:new');
    }
  } catch (err) {
    console.error('Failed to subscribe to campaign:new', err);
    try {
      res.write(`event: error\n`);
      res.write(`data: ${JSON.stringify({ error: 'subscribe-failed' })}\n\n`);
    } catch (_) {}
  }

  // Cleanup on client disconnect
  req.on('close', async () => {
    clearInterval(heartbeat);
    try {
      // best-effort unsubscribe + quit
      try {
        // attempt to use unsubscribe where available
        if (typeof (sub as any).unsubscribe === 'function') {
          await (sub as any).unsubscribe('campaign:new').catch(() => {});
        } else {
          // no-op fallback
        }
      } catch (_) {}
      try { await (sub as any).quit(); } catch (_) {}
    } catch (e) {
      console.warn('Error while cleaning up redis subscriber for /campaign/new-events', e);
    }
    try { res.end(); } catch (_) {}
  });
}
