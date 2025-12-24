import type { NextApiRequest, NextApiResponse } from 'next';
import clientPromise from '../../../../lib/mongo';
import { redis } from '../../../../lib/redis';
import { Queue } from 'bullmq';
import { ObjectId } from 'mongodb';

const queue = new Queue('campaigns', { connection: redis });

// keep parity with worker; allow env override
const MAX_ATTEMPTS = Number(process.env.MAX_ATTEMPTS || 3);
// server-side cap for batch retries per request
const BATCH_RETRY_LIMIT = Number(process.env.BATCH_RETRY_LIMIT || 5000);

type Action = 'pause' | 'resume' | 'cancel' | 'delete' | 'retryFailed' | 'retryContact';

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

// safe read of redis meta integer field
async function safeGetMetaInt(redisKey: string, field: string) {
  try {
    const v = await redis.hget(redisKey, field);
    return Number(v || 0);
  } catch (e) {
    return 0;
  }
}

/**
 * Ensure there is a usable `campaign:{id}:definition` in Redis.
 * If missing, attempt to construct one from the Mongo campaign document.
 * Returns true if definition exists or was written successfully.
 */
async function ensureCampaignDefinition(id: string, campaignDoc: any): Promise<boolean> {
  const key = `campaign:${id}:definition`;
  try {
    const existing = await redis.get(key);
    if (existing) return true;

    // Attempt to build a minimal compatible definition
    // Worker expects { initial: { subject, body }, followUps: [...] }
    let built: any = null;

    // Prefer explicit shapes commonly used
    if (campaignDoc?.definition && typeof campaignDoc.definition === 'object') {
      built = campaignDoc.definition;
    } else if (campaignDoc?.initial && typeof campaignDoc.initial === 'object') {
      built = { initial: campaignDoc.initial, followUps: campaignDoc.followUps || [] };
    } else if (campaignDoc?.template && typeof campaignDoc.template === 'object') {
      built = { initial: { subject: campaignDoc.template.subject || campaignDoc.name, body: campaignDoc.template.body || campaignDoc.content || '' }, followUps: campaignDoc.template.followUps || campaignDoc.followUps || [] };
    } else {
      // fallback: try to glean subject/body from common fields
      const subject = campaignDoc?.subject || campaignDoc?.title || campaignDoc?.name || `Campaign ${id}`;
      const body = campaignDoc?.body || campaignDoc?.content || campaignDoc?.html || '';
      const followUps = campaignDoc?.followUps || campaignDoc?.steps || [];
      built = { initial: { subject, body }, followUps };
    }

    // If built doesn't look right (no initial subject/body), fail safe
    if (!built || !built.initial || (built.initial.subject == null && built.initial.body == null)) {
      console.warn('Unable to construct campaign definition from campaign document', { campaignId: id, sample: campaignDoc ? Object.keys(campaignDoc).slice(0, 8) : null });
      return false;
    }

    // Persist into Redis (no expiry) so worker can read it
    try {
      await redis.set(key, JSON.stringify(built));
      console.log(`Wrote fallback campaign definition into redis for ${id}`);
      return true;
    } catch (e) {
      console.warn('Failed to write campaign definition to redis', e);
      return false;
    }
  } catch (e) {
    console.warn('Error checking/writing campaign definition in redis', e);
    return false;
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

  const { action, confirm, contactId } = req.body as {
    action?: Action;
    confirm?: boolean;
    contactId?: string;
  };

  if (!action || !['pause', 'resume', 'cancel', 'delete', 'retryFailed', 'retryContact'].includes(action)) {
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

    // ACTION: Retry all failed contacts that are below MAX_ATTEMPTS
    if (action === 'retryFailed') {
      // Disallow retrying if campaign is cancelled/deleted
      if (campaign.status === 'cancelled' || campaign.status === 'deleted') {
        return res.status(400).json({ error: 'Cannot retry contacts for cancelled/deleted campaign' });
      }

      // Ensure a Redis campaign definition exists (worker requires it)
      const hasDef = await ensureCampaignDefinition(id, campaign);
      if (!hasDef) {
        return res.status(500).json({ error: 'Missing campaign definition in Redis and unable to construct one from campaign document. Retry cannot proceed.' });
      }

      // find failed contacts with attempts < MAX_ATTEMPTS
      const filter = { campaignId: campaignObjectId, status: 'failed', attempts: { $lt: MAX_ATTEMPTS } };
      const failedDocs = await db
        .collection('campaign_contacts')
        .find(filter, { projection: { _id: 1, contactId: 1, step: 1 } })
        .toArray();

      const toRetryCount = failedDocs.length;
      if (toRetryCount === 0) {
        return res.status(200).json({ ok: true, retried: 0, message: 'No eligible failed contacts to retry' });
      }

      // server-side cap enforcement
      if (toRetryCount > BATCH_RETRY_LIMIT) {
        return res.status(400).json({
          error: 'Batch retry exceeds server limit',
          message: `Trying to retry ${toRetryCount} contacts exceeds server cap of ${BATCH_RETRY_LIMIT}. Use pagination to retry in smaller batches.`,
          toRetryCount,
          limit: BATCH_RETRY_LIMIT,
        });
      }

      // Atomically mark them pending (a single updateMany)
      const updateResult = await db.collection('campaign_contacts').updateMany(filter, {
        $set: { status: 'pending', lastError: null },
      });

      const updated = updateResult.modifiedCount ?? 0;

      // Enqueue jobs (batch)
      const jobs: Promise<any>[] = [];
      const CHUNK = 200; // reasonable chunking
      for (let i = 0; i < failedDocs.length; i += CHUNK) {
        const chunk = failedDocs.slice(i, i + CHUNK);
        for (const doc of chunk) {
          const contactObjId = doc.contactId ? doc.contactId : doc._id;
          // Determine job name and payload: prefer step payload when present
          try {
            if (doc.step) {
              jobs.push(
                queue.add(
                  'followup',
                  { campaignId: id, contactId: contactObjId.toString ? contactObjId.toString() : String(contactObjId), step: doc.step },
                  { removeOnComplete: true, removeOnFail: true }
                )
              );
            } else {
              jobs.push(
                queue.add(
                  'initial',
                  { campaignId: id, contactId: contactObjId.toString ? contactObjId.toString() : String(contactObjId) },
                  { removeOnComplete: true, removeOnFail: true }
                )
              );
            }
          } catch (e) {
            console.warn('Failed to queue job for retryFailed chunk item', e);
          }
        }
      }

      // Wait for enqueues (best-effort)
      try {
        await Promise.all(jobs);
      } catch (e) {
        console.warn('Some queue.add calls failed during retryFailed', e);
      }

      // Update redis counters: decrease failed by updated (best-effort, but avoid negative)
      try {
        const currentFailed = await safeGetMetaInt(redisKey, 'failed');
        const dec = Math.min(updated, currentFailed);
        if (dec > 0) {
          await redis.hincrby(redisKey, 'failed', -dec);
        }
      } catch (e) {
        console.warn('Failed to update redis counters during retryFailed', e);
      }

      await safePublish('campaign:new', { id, action: 'retryFailed', retried: updated });

      return res.status(200).json({ ok: true, retried: updated, attemptedEnqueue: toRetryCount });
    }

    // ACTION: Retry a single failed contact by contactId
    if (action === 'retryContact') {
      if (!contactId || typeof contactId !== 'string') {
        return res.status(400).json({ error: 'Missing contactId for retryContact' });
      }

      // Disallow retrying if campaign is cancelled/deleted
      if (campaign.status === 'cancelled' || campaign.status === 'deleted') {
        return res.status(400).json({ error: 'Cannot retry contacts for cancelled/deleted campaign' });
      }

      // Ensure a Redis campaign definition exists (worker requires it)
      const hasDef = await ensureCampaignDefinition(id, campaign);
      if (!hasDef) {
        return res.status(500).json({ error: 'Missing campaign definition in Redis and unable to construct one from campaign document. Retry cannot proceed.' });
      }

      // Find the ledger row
      let contactObjId;
      try {
        // try parse as ObjectId first
        contactObjId = new ObjectId(contactId);
      } catch {
        // fallback: use raw string
        contactObjId = contactId;
      }

      const doc = await db.collection('campaign_contacts').findOne({
        campaignId: campaignObjectId,
        contactId: contactObjId,
      });

      if (!doc) {
        return res.status(404).json({ error: 'Contact ledger row not found for campaign' });
      }

      if (doc.status !== 'failed') {
        return res.status(400).json({ error: 'Contact is not in failed state' });
      }

      if ((doc.attempts || 0) >= MAX_ATTEMPTS) {
        return res.status(400).json({ error: 'Contact has reached max attempts and cannot be retried' });
      }

      // Update single doc to pending
      await db.collection('campaign_contacts').updateOne(
        { _id: doc._id },
        { $set: { status: 'pending', lastError: null } }
      );

      // Enqueue appropriate job (use doc.step if present)
      try {
        if (doc.step) {
          await queue.add(
            'followup',
            { campaignId: id, contactId: contactObjId.toString ? contactObjId.toString() : String(contactObjId), step: doc.step },
            { removeOnComplete: true, removeOnFail: true }
          );
        } else {
          await queue.add(
            'initial',
            { campaignId: id, contactId: contactObjId.toString ? contactObjId.toString() : String(contactObjId) },
            { removeOnComplete: true, removeOnFail: true }
          );
        }
      } catch (e) {
        console.warn('Failed to enqueue retry job for contact', e);
        // We already set status to pending — if enqueue fails, we can roll back to failed (best-effort)
        try {
          await db.collection('campaign_contacts').updateOne(
            { _id: doc._id },
            { $set: { status: 'failed', lastError: 'enqueue-failed' } }
          );
        } catch (_) {}
        return res.status(500).json({ error: 'Failed to enqueue retry job' });
      }

      // Update redis counters: decrease failed by 1 if possible
      try {
        const currentFailed = await safeGetMetaInt(redisKey, 'failed');
        if (currentFailed > 0) {
          await redis.hincrby(redisKey, 'failed', -1);
        }
      } catch (e) {
        console.warn('Failed to update redis counters during retryContact', e);
      }

      await safePublish('campaign:new', { id, action: 'retryContact', contactId });

      return res.status(200).json({ ok: true, retried: 1, contactId });
    }

    // Should not reach here
    return res.status(400).json({ error: 'Unsupported action' });
  } catch (err) {
    console.error('Campaign control error', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}
