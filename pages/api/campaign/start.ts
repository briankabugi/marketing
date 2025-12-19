
import { Queue } from 'bullmq';
import { redis } from '../../../lib/redis';

const queue = new Queue('campaigns', { connection: redis });

export default async function handler(req, res) {
  const campaignId = Date.now().toString();

  await redis.hset(`campaign:${campaignId}:meta`, {
    name: 'Test Campaign',
    sent: 0
  });

  await queue.add('send', {
    campaignId,
    contact: { email: 'test@example.com' }
  });

  await redis.sadd('campaign:all', campaignId);

  res.json({ campaignId });
}
