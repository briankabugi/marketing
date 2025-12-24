// lib/redis.ts
import IORedis from "ioredis";

export const redis = new IORedis(process.env.REDIS_URL || "redis://127.0.0.1:6379", {
  maxRetriesPerRequest: null,
  lazyConnect: true,
  reconnectOnError: (err) => {
    const targetErrors = ["READONLY", "ECONNRESET", "ETIMEDOUT"];
    return targetErrors.some((e) => err.message.includes(e));
  },
});

redis.on("connect", () => console.log("Redis connected"));
redis.on("ready", () => console.log("Redis ready"));
redis.on("error", (err) => console.error("Redis error", err));
redis.on("close", () => console.warn("Redis connection closed"));
redis.on("reconnecting", () => console.log("Redis reconnecting"));

/**
 * Single-channel subscription helper
 */
export function subscribe(
  channel: string,
  handler: (data: any) => void
): () => Promise<void> {
  const sub = redis.duplicate();

  const onMessage = (ch: string, message: string) => {
    if (ch !== channel) return;
    try {
      handler(JSON.parse(message));
    } catch {
      handler(message);
    }
  };

  sub.on("message", onMessage);

  sub.subscribe(channel).catch((err) => {
    console.error(`Redis subscribe failed for ${channel}`, err);
  });

  return async () => {
    try {
      sub.off("message", onMessage);
      await sub.unsubscribe(channel);
      await sub.quit();
    } catch (err) {
      console.warn(`Failed to unsubscribe from ${channel}`, err);
    }
  };
}

/**
 * Multi-channel subscriber (worker & SSE safe)
 */
export async function subscribeToChannels(
  sub: IORedis,
  channels: string[],
  handler: (channel: string, message: string) => void
) {
  for (const ch of channels) {
    await sub.subscribe(ch);
  }

  sub.on("message", (channel, message) => {
    if (channels.includes(channel)) {
      handler(channel, message);
    }
  });
}
