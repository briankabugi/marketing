// pages/api/campaign/[id]/control.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import clientPromise from '../../../../lib/mongo';
import { redis } from '../../../../lib/redis';
import { Queue } from 'bullmq';
import { ObjectId } from 'mongodb';

const queue = new Queue('campaigns', { connection: redis });

type Action = 'pause' | 'resume' | 'cancel' | 'delete';

async function safeHSet(key: string, obj: Record<string, string>) {
  try {
    await redis.hset(key, obj);
  } catch (err) {
    console.warn('Redis unavailable while setting meta', err);
  }
}

async function safePublish(channel: string, payload: any) {
  try {
    await redis.publish(channel, JSON.stringify(payload));
  } catch (err) {
    console.warn('Redis publish failed', err);
  }
}

// Remove waiting/delayed/active jobs for a campaignId (best-effort)
async function removeQueuedJobsForCampaign(campaignId: string) {
  try {
    // get waiting/delayed/active jobs
    const jobs = await queue.getJobs(
      ['waiting', 'delayed', 'active', 'paused'],
      0,
      -1
    );

    const matched = jobs.filter((j) => {
      try {
        return j.data?.campaignId === campaignId;
      } catch {
        return false;
      }
    });

    for (const j of matched) {
      try {
        // if the job is active, remove may fail - still try
        await j.remove();
      } catch (e) {
        // best-effort — ignore
        console.warn(`Failed to remove job ${j.id}`, e);
      }
    }
    return matched.length;
  } catch (err) {
    console.warn('Failed to enumerate/remove jobs for campaign', err);
    return 0;
  }
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { id } = req.query;
  if (!id || typeof id !== 'string') {
    return res.status(400).json({ error: 'Invalid campaign id' });
  }

  const { action, confirm } = req.body as {
    action?: Action;
    confirm?: boolean;
  };

  if (!action || !['pause', 'resume', 'cancel', 'delete'].includes(action)) {
    return res.status(400).json({ error: 'Invalid action' });
  }

  const client = await clientPromise;
  const db = client.db('PlatformData');
  const campaignObjectId = new ObjectId(id);

  try {
    // Reload campaign from Mongo (authoritative)
    const campaign = await db
      .collection('campaigns')
      .findOne({ _id: campaignObjectId });

    if (!campaign) {
      return res.status(404).json({ error: 'Campaign not found' });
    }

    // Lightweight helpers
    const redisKey = `campaign:${id}:meta`;

    // ACTION: Pause
    if (action === 'pause') {
      if (campaign.status === 'paused') {
        return res.status(200).json({ ok: true, message: 'Already paused' });
      }
      await db.collection('campaigns').updateOne(
        { _id: campaignObjectId },
        { $set: { status: 'paused' } }
      );
      await safeHSet(redisKey, { status: 'paused' });
      await safePublish('campaign:new', { id, status: 'paused' });
      return res.status(200).json({ ok: true, action: 'paused' });
    }

    // ACTION: Resume
    if (action === 'resume') {
      if (campaign.status === 'running') {
        return res.status(200).json({ ok: true, message: 'Already running' });
      }

      await db.collection('campaigns').updateOne(
        { _id: campaignObjectId },
        { $set: { status: 'running' } }
      );
      await safeHSet(redisKey, { status: 'running' });
      await safePublish('campaign:new', { id, status: 'running' });
      return res.status(200).json({ ok: true, action: 'resumed' });
    }

    // ACTION: Cancel
    if (action === 'cancel') {
      if (campaign.status === 'cancelled') {
        return res
          .status(200)
          .json({ ok: true, message: 'Already cancelled' });
      }

      // 1) Mark campaign cancelled and completedAt
      const completedAt = new Date();
      await db.collection('campaigns').updateOne(
        { _id: campaignObjectId },
        { $set: { status: 'cancelled', completedAt } }
      );
      await safeHSet(redisKey, { status: 'cancelled' });

      // 2) Find and atomically mark pending ledger rows as failed (single updateMany)
      const filter = { campaignId: campaignObjectId, status: 'pending' };
      const update = {
        $set: {
          status: 'failed',
          lastError: 'cancelled',
          lastAttemptAt: completedAt,
        },
        $inc: { attempts: 1 },
      };

      const updateResult = await db
        .collection('campaign_contacts')
        .updateMany(filter, update);

      const cancelledCount = updateResult.modifiedCount ?? 0;

      // 3) Update Redis counters (best-effort)
      try {
        if (cancelledCount > 0) {
          await redis.hincrby(redisKey, 'processed', cancelledCount);
          await redis.hincrby(redisKey, 'failed', cancelledCount);
        }
      } catch (e) {
        console.warn('Failed to update redis counters during cancel', e);
      }

      // 4) Persist totals snapshot to campaigns.totals (read redis if available, fallback to db)
      let meta = {};
      try {
        meta = (await redis.hgetall(redisKey)) || {};
      } catch {
        meta = {};
      }

      // Compute totals final values combining persisted totals and our cancelledCount as fallback
      const processedNow = Number(meta['processed'] ?? (campaign.totals?.processed ?? 0) + cancelledCount);
      const sentNow = Number(meta['sent'] ?? (campaign.totals?.sent ?? 0));
      const failedNow = Number(meta['failed'] ?? (campaign.totals?.failed ?? 0) + cancelledCount);

      await db.collection('campaigns').updateOne(
        { _id: campaignObjectId },
        {
          $set: {
            'totals.processed': processedNow,
            'totals.sent': sentNow,
            'totals.failed': failedNow,
            completedAt,
          },
        }
      );

      // 5) Remove queued jobs for this campaign (best-effort)
      const removedJobs = await removeQueuedJobsForCampaign(id);

      // 6) Publish event
      await safePublish('campaign:new', { id, status: 'cancelled', cancelledCount, removedJobs });

      return res.status(200).json({
        ok: true,
        action: 'cancelled',
        cancelledCount,
        removedJobs,
      });
    }

    // ACTION: Delete
    if (action === 'delete') {
      // Require explicit confirmation — safeguards in UI must set confirm=true
      if (confirm !== true) {
        return res
          .status(400)
          .json({ error: 'Deletion requires confirm=true in request body' });
      }

      // Prevent accidental deletion while running
      if (campaign.status === 'running') {
        return res
          .status(400)
          .json({ error: 'Cancel the campaign before deletion' });
      }

      // Remove queued jobs (best-effort)
      const removedJobs = await removeQueuedJobsForCampaign(id);

      // Delete Redis keys (best-effort)
      try {
        await redis.del(`campaign:${id}:meta`);
        await redis.del(`campaign:${id}:definition`);
        await redis.srem('campaign:all', id);
      } catch (e) {
        console.warn('Redis cleanup on delete failed', e);
      }

      // Delete Mongo docs
      const [campaignDel, ledgerDel] = await Promise.all([
        db.collection('campaigns').deleteOne({ _id: campaignObjectId }),
        db.collection('campaign_contacts').deleteMany({ campaignId: campaignObjectId }),
      ]);

      await safePublish('campaign:new', { id, status: 'deleted' });

      return res.status(200).json({
        ok: true,
        action: 'deleted',
        campaignDeleted: campaignDel.deletedCount ?? 0,
        ledgerDeleted: ledgerDel.deletedCount ?? 0,
        removedJobs,
      });
    }

    // Should not reach here
    return res.status(400).json({ error: 'Unsupported action' });
  } catch (err) {
    console.error('Campaign control error', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}
