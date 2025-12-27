// pages/api/campaign/[id]/events.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import { redis, subscribeToChannels } from '../../../../lib/redis';

/**
 * SSE endpoint that subscribes to Redis channels and forwards events to clients.
 * Forwards campaign-level events as "campaign" and contact-level as "contact".
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
    res.flushHeaders?.();
  } catch {}

  // Duplicate Redis client for pub/sub
  const sub = (redis as any).duplicate();

  try {
    // try to connect (subscribeToChannels has its own guard)
    await sub.connect().catch((err: any) => {
      // connect may already be in-progress; ignore
      // but log for debugging
      if (err && String(err).includes('already connecting')) {
        console.warn('subscribe: sub.connect already connecting — continuing');
      } else {
        console.warn('subscribe: sub.connect failed (continuing)', err);
      }
    });
  } catch (err) {
    console.error('Failed to connect redis subscriber', err);
    res.write(`event: error\n`);
    res.write(`data: ${JSON.stringify({ error: 'redis-connect-failed' })}\n\n`);
    res.end();
    return;
  }

  const channels = [
    `campaign:${id}:contact_update`,
    `campaign:${id}:events`,
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
        // message may already be a JS object or be non-JSON; keep raw string in payload.raw
        payload = { raw: String(message) };
      }

      // Debug log
      // console.log('[SSE] redis message', channel, payload);

      // Filter global campaign:new events so only clients interested in this campaign get updates
      if (channel === 'campaign:new') {
        // If payload has an id and it doesn't match, ignore.
        // If payload has no id but includes signals like refreshContacts, allow through as a generic campaign-level notification.
        const payloadId = payload && (payload.id ?? payload.campaignId ?? null);
        if (payloadId && String(payloadId) !== String(id)) {
          // not for this client
          return;
        }
        writeEvent('campaign', payload);
        return;
      }

      // contact updates specifically for this campaign
      if (channel.endsWith(':contact_update')) {
        // payload may be { campaignId, contactId, ... } or just { contactId, ... }
        // Forward as 'contact' event
        writeEvent('contact', payload);
        return;
      }

      // campaign-specific events channel (e.g. campaign:{id}:events)
      if (channel === `campaign:${id}:events`) {
        writeEvent('campaign_event', payload);
        return;
      }

      // Fallback: if some other channel slipped through, forward generically but label it
      writeEvent('campaign_event', { channel, payload });
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

  // Cleanup on close
  req.on('close', async () => {
    clearInterval(heartbeat);
    try {
      // remove our message handler if present
      if ((sub as any).__subscribe_to_channels_on_message) {
        try { sub.off('message', (sub as any).__subscribe_to_channels_on_message); } catch {}
      }
      try { await sub.unsubscribe(channels); } catch {}
      try { await sub.quit(); } catch {}
    } catch (e) {
      console.warn('Error during SSE cleanup', e);
    }
    try { res.end(); } catch {}
  });
}
