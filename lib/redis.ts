// lib/redis.ts
import IORedis from 'ioredis';

export const redis = new IORedis(process.env.REDIS_URL || 'redis://127.0.0.1:6379', {
  maxRetriesPerRequest: null,
  lazyConnect: true,
  reconnectOnError: (err: any) => {
    const targetErrors = ['READONLY', 'ECONNRESET', 'ETIMEDOUT'];
    try {
      if (err && typeof err.message === 'string') {
        return targetErrors.some(e => err.message.includes(e));
      }
    } catch {
      // fall through
    }
    return false;
  },
});

redis.on('connect', () => console.log('Redis connected'));
redis.on('ready', () => console.log('Redis ready'));
redis.on('error', (err) => console.error('Redis error', err));
redis.on('close', () => console.warn('Redis connection closed'));
redis.on('reconnecting', () => console.log('Redis reconnecting'));

/**
 * Subscribe to a single Redis channel (ioredis-style).
 *
 * Returns an async unsubscribe function that will remove listener, unsubscribe and quit the duplicate client.
 *
 * This implementation avoids calling `subscribe(channel, callback)` (which mismatches TS overloads)
 * and instead calls `await sub.subscribe(channel)` then listens to the 'message' event.
 */
export async function subscribe(channel: string, handler: (data: any) => void): Promise<() => Promise<void>> {
  const sub = redis.duplicate();

  // message listener that delegates to handler with parsed JSON when possible
  const onMessage = (msgChannel: string, message: string) => {
    if (msgChannel !== channel) return;
    try {
      handler(JSON.parse(message));
    } catch {
      handler(message);
    }
  };

  // attach error logging early
  sub.on('error', (err: any) => {
    console.error(`Redis subscriber error for channel ${channel}`, err);
  });

  // attach the message handler and subscribe
  sub.on('message', onMessage);

  try {
    // ioredis duplicate may be lazy; connect explicitly
    if (typeof (sub as any).connect === 'function') {
      // connect() is a no-op if already connected
      // ignore errors - the caller will see lack of events
      // but log for debugging
      await (sub as any).connect().catch((err: any) => {
        console.warn('Subscriber connect error (continuing):', err);
      });
    }
    // call subscribe without a callback to satisfy TS overloads
    await (sub as any).subscribe(channel);
  } catch (err) {
    console.error(`Failed to subscribe to Redis channel ${channel}`, err);
  }

  // Return unsubscribe function
  return async () => {
    try {
      sub.off('message', onMessage);
      try {
        await (sub as any).unsubscribe(channel);
      } catch (e) {
        // ignore
      }
      try {
        await (sub as any).quit();
      } catch (e) {
        // ignore
      }
    } catch (err) {
      console.warn(`Failed to cleanly unsubscribe from Redis channel ${channel}`, err);
    }
  };
}

/**
 * Robust multi-channel subscription helper.
 * Supports ioredis style duplicate subscriber.
 *
 * sub: a pre-created subscriber client (e.g. redis.duplicate()) OR any compatible client.
 * channels: list of channel names
 * handler: (channel, message) => void
 *
 * This function will subscribe to each channel and also attach a fallback 'message' handler
 * (useful when some redis clients call back with different signatures).
 *
 * NOTE: This function does NOT call sub.quit()/sub.unsubscribe for you — the caller is responsible for cleanup.
 */
export async function subscribeToChannels(
  sub: any,
  channels: string[],
  handler: (channel: string, message: string) => void
): Promise<void> {
  if (!sub || !channels || channels.length === 0) return;

  // ensure connection if possible
  try {
    if (typeof sub.connect === 'function') {
      await sub.connect().catch((err: any) => {
        console.warn('subscribeToChannels: subscriber connect warning', err);
      });
    }
  } catch (e) {
    // ignore connect errors
  }

  // Per-channel subscribe (ioredis subscribe returns a Promise)
  for (const ch of channels) {
    try {
      if (typeof sub.subscribe === 'function') {
        // call subscribe without callback to keep TS happy
        await sub.subscribe(ch).catch((err: any) => {
          // some clients may reject; try to continue
          console.warn(`subscribeToChannels: subscribe(${ch}) failed`, err);
        });
      } else {
        // fallback: some clients may have different API
        try {
          await (sub as any).subscribe(ch);
        } catch (e) {
          console.warn(`subscribeToChannels: fallback subscribe for ${ch} failed`, e);
        }
      }
    } catch (e) {
      console.warn(`subscribe failed for channel ${ch}, will attempt fallback`, e);
    }
  }

  // Attach 'message' listener for ioredis-style events: (channel, message)
  try {
    if (typeof sub.on === 'function') {
      const onMsg = (a: any, b: any) => {
        // a may be channel or message depending on client; prefer (channel, message)
        if (typeof a === 'string' && typeof b === 'string') {
          handler(a, b);
        } else if (typeof b === 'string' && channels.includes(b)) {
          // unusual ordering, try to map
          handler(b, a);
        } else {
          // best-effort fallback: stringify inputs
          handler(String(a), String(b));
        }
      };
      // store reference so caller may remove later if needed
      (sub as any).__subscribe_to_channels_on_message = onMsg;
      sub.on('message', onMsg);
    }
  } catch (e) {
    console.warn('Fallback Redis message hook failed', e);
  }
}
