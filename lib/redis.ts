import IORedis from 'ioredis';

export const redis = new IORedis(process.env.REDIS_URL || 'redis://127.0.0.1:6379', {
  maxRetriesPerRequest: null,
  lazyConnect: true,           // prevents immediate connection on import
  reconnectOnError: (err) => {
    // reconnect on most errors
    const targetErrors = ['READONLY', 'ECONNRESET', 'ETIMEDOUT'];
    if (targetErrors.some(e => err.message.includes(e))) return true;
    return false;
  },
});

// Optional: log Redis connection events for observability
redis.on('connect', () => console.log('Redis connected'));
redis.on('ready', () => console.log('Redis ready'));
redis.on('error', (err) => console.error('Redis error', err));
redis.on('close', () => console.warn('Redis connection closed'));
redis.on('reconnecting', () => console.log('Redis reconnecting'));