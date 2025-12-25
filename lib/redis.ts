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
 * Returns an async unsubscribe function.
 */
export async function subscribe(channel: string, handler: (data: any) => void): Promise<() => Promise<void>> {
  const sub = redis.duplicate();

  const onMessage = (msgChannel: string, message: string) => {
    if (msgChannel !== channel) return;
    try {
      handler(JSON.parse(message));
    } catch {
      handler(message);
    }
  };

  sub.on('error', (err: any) => {
    console.error(`Redis subscriber error for channel ${channel}`, err);
  });

  sub.on('message', onMessage);

  try {
    // connect only if not already connecting/ready
    try {
      if (typeof (sub as any).connect === 'function') {
        const status = (sub as any).status;
        if (status !== 'ready' && status !== 'connecting') {
          await (sub as any).connect().catch((err: any) => {
            console.warn('Subscriber connect error (continuing):', err);
          });
        } else {
          // already connecting/ready — skip connect
        }
      }
    } catch (e) {
      console.warn('Subscriber connect guard error', e);
    }

    await (sub as any).subscribe(channel).catch((err: any) => {
      console.warn(`subscribe(${channel}) failed`, err);
    });
  } catch (err) {
    console.error(`Failed to subscribe to Redis channel ${channel}`, err);
  }

  return async () => {
    try {
      sub.off('message', onMessage);
      try { await (sub as any).unsubscribe(channel); } catch (e) {}
      try { await (sub as any).quit(); } catch (e) {}
    } catch (err) {
      console.warn(`Failed to cleanly unsubscribe from Redis channel ${channel}`, err);
    }
  };
}

/**
 * Robust multi-channel subscription helper.
 */
export async function subscribeToChannels(
  sub: any,
  channels: string[],
  handler: (channel: string, message: string) => void
): Promise<void> {
  if (!sub || !channels || channels.length === 0) return;

  // ensure connection if possible (guard against duplicate connect calls)
  try {
    if (typeof sub.connect === 'function') {
      const status = (sub as any).status;
      if (status !== 'ready' && status !== 'connecting') {
        await sub.connect().catch((err: any) => {
          console.warn('subscribeToChannels: subscriber connect warning', err);
        });
      } else {
        // already connecting/ready, skip connect
      }
    }
  } catch (e) {
    // ignore connect errors
  }

  for (const ch of channels) {
    try {
      if (typeof sub.subscribe === 'function') {
        await sub.subscribe(ch).catch((err: any) => {
          console.warn(`subscribeToChannels: subscribe(${ch}) failed`, err);
        });
      } else {
        await (sub as any).subscribe(ch);
      }
    } catch (e) {
      console.warn(`subscribe failed for channel ${ch}, will attempt fallback`, e);
    }
  }

  // Attach 'message' listener
  try {
    if (typeof sub.on === 'function') {
      const onMsg = (a: any, b: any) => {
        if (typeof a === 'string' && typeof b === 'string') {
          handler(a, b);
        } else if (typeof b === 'string' && channels.includes(b)) {
          handler(b, a);
        } else {
          handler(String(a), String(b));
        }
      };
      (sub as any).__subscribe_to_channels_on_message = onMsg;
      sub.on('message', onMsg);
    }
  } catch (e) {
    console.warn('Fallback Redis message hook failed', e);
  }
}
