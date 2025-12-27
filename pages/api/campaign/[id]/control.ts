// pages/api/campaign/[id]/control.ts
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
// when resuming, limit how many pending jobs we'll try to (re)enqueue
const RESUME_ENQUEUE_LIMIT = Number(process.env.RESUME_ENQUEUE_LIMIT || 5000);
// how long before a 'sending' row is considered stale and eligible for recovery (ms)
const STALE_SENDING_MS = Number(process.env.STALE_SENDING_MS || 90_000);

type Action = 'pause' | 'resume' | 'cancel' | 'delete' | 'retryFailed' | 'retryContact' | 'reconcile' | 'manualHold' | 'manualUndo';

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

/**
 * Remove any queued jobs that belong to a specific campaign + contact.
 * Returns the number of removed jobs (best-effort).
 */
async function removeQueuedJobsForContact(campaignId: string, contactId: string) {
  try {
    const jobs = await queue.getJobs(['waiting', 'delayed', 'active', 'paused'], 0, -1);
    const matched = jobs.filter((j) => {
      try {
        return String(j.data?.campaignId) === String(campaignId) && String(j.data?.contactId) === String(contactId);
      } catch {
        return false;
      }
    });

    let removed = 0;
    for (const j of matched) {
      try {
        await j.remove();
        removed++;
      } catch (e) {
        console.warn(`Failed to remove job ${j.id} for contact ${contactId}`, e);
      }
    }
    return removed;
  } catch (e) {
    console.warn('Failed to enumerate/remove jobs for contact', e);
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

/**
 * Get a set of contactId strings for jobs currently enqueued for this campaign.
 * Used to avoid enqueuing duplicates when resuming.
 */
async function getQueuedContactIdsForCampaign(campaignId: string): Promise<Set<string>> {
  const set = new Set<string>();
  try {
    const jobs = await queue.getJobs(['waiting', 'delayed', 'active', 'paused'], 0, -1);
    for (const j of jobs) {
      try {
        if (j.data?.campaignId === campaignId && j.data?.contactId) {
          set.add(String(j.data.contactId));
        }
      } catch {
        // ignore malformed jobs
      }
    }
  } catch (e) {
    console.warn('Failed to list queued jobs for campaign when checking duplicates', e);
  }
  return set;
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

  if (!action || !['pause', 'resume', 'cancel', 'delete', 'retryFailed', 'retryContact', 'reconcile', 'manualHold', 'manualUndo'].includes(action)) {
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
      // publish campaign-level event so UI disables retry buttons and updates state
      await safePublish('campaign:new', { id, status: 'paused' });
      return res.status(200).json({ ok: true, action: 'paused' });
    }

    // ACTION: Resume
    if (action === 'resume') {
      if (campaign.status === 'running') {
        return res.status(200).json({ ok: true, message: 'Already running' });
      }

      // set running in DB + redis
      await db.collection('campaigns').updateOne(
        { _id: campaignObjectId },
        { $set: { status: 'running' } }
      );
      await safeHSet(redisKey, { status: 'running' });
      await safePublish('campaign:new', { id, status: 'running' });

      // Re-enqueue pending contacts that currently have no queued job for this campaign.
      // This addresses cases where jobs were removed or not enqueued while the campaign was paused,
      // ensuring pending rows actually get processed again after resume.
      try {
        const queuedSet = await getQueuedContactIdsForCampaign(id);

        // fetch pending docs (limit to a sane cap)
        const pendingCursor = db.collection('campaign_contacts').find(
          { campaignId: campaignObjectId, status: 'pending' },
          { projection: { _id: 1, contactId: 1, step: 1 } }
        ).limit(RESUME_ENQUEUE_LIMIT);

        const pending: any[] = await pendingCursor.toArray();

        const enqueuePromises: Promise<any>[] = [];
        for (const d of pending) {
          const cid = d.contactId ? (d.contactId.toString ? d.contactId.toString() : String(d.contactId)) : (d._id.toString ? d._id.toString() : String(d._id));
          if (queuedSet.has(cid)) continue; // already queued

          try {
            enqueuePromises.push(
              queue.add(
                d.step ? 'followup' : 'initial',
                { campaignId: id, contactId: cid, step: d.step },
                { removeOnComplete: true, removeOnFail: true, attempts: MAX_ATTEMPTS, backoff: { type: 'exponential', delay: 60_000 } }
              )
            );
          } catch (e) {
            console.warn('Failed to enqueue pending contact on resume', e);
          }
        }

        // attempt to enqueue in parallel (best-effort)
        if (enqueuePromises.length > 0) {
          await Promise.allSettled(enqueuePromises);
          // notify UI that contacts have been re-queued (counts unchanged, but UI may want to refresh)
          await safePublish('campaign:new', { id, action: 'resume_requeued', requeued: enqueuePromises.length });
        }
      } catch (e) {
        console.warn('Failed to re-enqueue pending contacts on resume', e);
      }

      // Recover stale 'sending' ledger rows that may have been left in 'sending' by a crashed worker or killed process.
      // Criteria: status === 'sending' AND lastAttemptAt older than STALE_SENDING_MS AND bgAttempts < MAX_ATTEMPTS
      // We'll set them back to 'pending', reset bgAttempts to 0 (so background cycle restarts), and enqueue jobs.
      try {
        const cutoff = new Date(Date.now() - STALE_SENDING_MS);
        const staleCursor = db.collection('campaign_contacts').find(
          { campaignId: campaignObjectId, status: 'sending', lastAttemptAt: { $lt: cutoff } },
          { projection: { _id: 1, contactId: 1, step: 1, bgAttempts: 1 } }
        ).limit(RESUME_ENQUEUE_LIMIT);

        const staleDocs = await staleCursor.toArray();
        if (staleDocs.length > 0) {
          const ids = staleDocs.map(d => d._id);
          // Reset to pending and clear lastError, reset bgAttempts so background retries start fresh
          await db.collection('campaign_contacts').updateMany(
            { _id: { $in: ids } },
            { $set: { status: 'pending', lastError: null, bgAttempts: 0 } }
          );

          // Enqueue each stale doc if not already queued
          const queuedSet2 = await getQueuedContactIdsForCampaign(id);
          const requeuePromises: Promise<any>[] = [];
          for (const d of staleDocs) {
            const cid = d.contactId ? (d.contactId.toString ? d.contactId.toString() : String(d.contactId)) : (d._id.toString ? d._id.toString() : String(d._id));
            if (queuedSet2.has(cid)) continue;
            try {
              requeuePromises.push(
                queue.add(
                  d.step ? 'followup' : 'initial',
                  { campaignId: id, contactId: cid, step: d.step },
                  { removeOnComplete: true, removeOnFail: true, attempts: MAX_ATTEMPTS, backoff: { type: 'exponential', delay: 60_000 } }
                )
              );
            } catch (e) {
              console.warn('Failed to enqueue stale sending contact on resume', e);
            }
            // publish contact-level update hint so UI reflects it's now pending
            try {
              safePublish(`campaign:${id}:contact_update`, { contactId: cid, status: 'pending', bgAttempts: 0, attempts: undefined });
            } catch (_) {}
          }

          if (requeuePromises.length > 0) await Promise.allSettled(requeuePromises);
          await safePublish('campaign:new', { id, action: 'resume_recovered_sending', recovered: staleDocs.length });
        }
      } catch (e) {
        console.warn('Failed to recover stale sending rows on resume', e);
      }

      return res.status(200).json({ ok: true, action: 'resumed' });
    }

    // ACTION: Reconcile (lightweight, single-request reconciliation)
    if (action === 'reconcile') {
      try {
        // 1) Recover stale 'sending' rows -> pending
        const cutoff = new Date(Date.now() - STALE_SENDING_MS);
        const staleSending = await db.collection('campaign_contacts').find({
          campaignId: campaignObjectId,
          status: 'sending',
          lastAttemptAt: { $lt: cutoff },
        }).project({ _id: 1, contactId: 1, step: 1 }).limit(RESUME_ENQUEUE_LIMIT).toArray();

        if (staleSending.length > 0) {
          const ids = staleSending.map(d => d._id);
          await db.collection('campaign_contacts').updateMany(
            { _id: { $in: ids } },
            { $set: { status: 'pending', lastError: null, bgAttempts: 0 } }
          );
        }

        // 2) Enqueue pending docs that do not currently have a job
        const queuedSet = await getQueuedContactIdsForCampaign(id);
        const pendingCursor = db.collection('campaign_contacts').find(
          { campaignId: campaignObjectId, status: 'pending' },
          { projection: { _id: 1, contactId: 1, step: 1 } }
        ).limit(RESUME_ENQUEUE_LIMIT);
        const pendingRows = await pendingCursor.toArray();

        const enqueuePromises: Promise<any>[] = [];
        for (const d of pendingRows) {
          const cid = d.contactId ? (d.contactId.toString ? d.contactId.toString() : String(d.contactId)) : (d._id.toString ? d._id.toString() : String(d._id));
          if (queuedSet.has(cid)) continue;
          try {
            enqueuePromises.push(
              queue.add(
                d.step ? 'followup' : 'initial',
                { campaignId: id, contactId: cid, step: d.step },
                { removeOnComplete: true, removeOnFail: true, attempts: MAX_ATTEMPTS, backoff: { type: 'exponential', delay: 60_000 } }
              )
            );
          } catch (e) {
            console.warn('Reconcile: failed to enqueue', e);
          }
        }
        if (enqueuePromises.length) await Promise.allSettled(enqueuePromises);

        // 3) Recalculate totals from DB (authoritative)
        const agg = await db.collection('campaign_contacts').aggregate([
          { $match: { campaignId: campaignObjectId } },
          { $group: { _id: '$status', count: { $sum: 1 } } }
        ]).toArray();

        let pending = 0, sent = 0, failed = 0;
        for (const r of agg) {
          if (r._id === 'pending') pending = r.count;
          else if (r._id === 'sent') sent = r.count;
          else if (r._id === 'failed') failed = r.count;
        }
        const processed = sent + failed;
        const intended = campaign.totals?.intended ?? (await db.collection('campaign_contacts').countDocuments({ campaignId: campaignObjectId }));

        // Write back authoritative totals to DB
        await db.collection('campaigns').updateOne(
          { _id: campaignObjectId },
          { $set: { 'totals.processed': processed, 'totals.sent': sent, 'totals.failed': failed } }
        );

        // Best-effort write to Redis meta
        try {
          await redis.hset(redisKey, { processed: String(processed), sent: String(sent), failed: String(failed), total: String(intended) });
        } catch (_) {}

        // 4) Decide canonical campaign status
        let newStatus = campaign.status;
        if (processed >= intended) {
          if (failed > 0) newStatus = 'completed_with_failures';
          else newStatus = 'completed';
        } else {
          newStatus = 'running';
        }

        // persist status if changed
        if (newStatus !== campaign.status) {
          await db.collection('campaigns').updateOne({ _id: campaignObjectId }, { $set: { status: newStatus, completedAt: (newStatus.startsWith('completed') ? new Date() : null) } } as any);
          await safeHSet(redisKey, { status: newStatus });
        }

        await safePublish('campaign:new', { id, action: 'reconcile', requeued: enqueuePromises.length, recovered: staleSending.length, status: newStatus });

        return res.status(200).json({ ok: true, requeued: enqueuePromises.length, recovered: staleSending.length, status: newStatus });
      } catch (e) {
        console.error('Reconcile failed', e);
        return res.status(500).json({ error: 'reconcile-failed' });
      }
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
      // Disallow retrying if campaign is cancelled/deleted/paused
      if (campaign.status === 'cancelled' || campaign.status === 'deleted' || campaign.status === 'paused') {
        return res.status(400).json({ error: 'Cannot retry contacts for cancelled/deleted/paused campaign' });
      }

      // Ensure a Redis campaign definition exists (worker requires it)
      const hasDef = await ensureCampaignDefinition(id, campaign);
      if (!hasDef) {
        return res.status(500).json({ error: 'Missing campaign definition in Redis and unable to construct one from campaign document. Retry cannot proceed.' });
      }

      // find failed contacts with attempts < MAX_ATTEMPTS AND where bgAttempts >= MAX_ATTEMPTS (meaning background cycle finished)
      // If bgAttempts is missing (undefined/null), treat it as finished (conservative). But prefer explicit >=.
      const filter: any = {
        campaignId: campaignObjectId,
        status: 'failed',
        attempts: { $lt: MAX_ATTEMPTS },
        $or: [
          { bgAttempts: { $exists: false } },
          { bgAttempts: { $gte: MAX_ATTEMPTS } },
        ],
      };

      const failedDocs = await db
        .collection('campaign_contacts')
        .find(filter, { projection: { _id: 1, contactId: 1, step: 1 } })
        .toArray();

      const toRetryCount = failedDocs.length;
      if (toRetryCount === 0) {
        return res.status(200).json({ ok: true, retried: 0, message: 'No eligible failed contacts to retry (either none failed, reached max attempts, or background retries still in progress).' });
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

      // Atomically mark them pending and increment core `attempts` and reset bgAttempts
      const ids = failedDocs.map(d => d._id);
      const updateResult = await db.collection('campaign_contacts').updateMany(
        { _id: { $in: ids } },
        {
          $set: { status: 'pending', lastError: null, bgAttempts: 0 },
          $inc: { attempts: 1 },
        }
      );

      const updated = updateResult.modifiedCount ?? 0;

      // Enqueue jobs (batch), ensure each job has MQ attempts/backoff set
      const jobs: Promise<any>[] = [];
      const CHUNK = 200; // reasonable chunking
      for (let i = 0; i < failedDocs.length; i += CHUNK) {
        const chunk = failedDocs.slice(i, i + CHUNK);
        for (const doc of chunk) {
          const contactObjId = doc.contactId ? doc.contactId : doc._id;
          try {
            if (doc.step) {
              jobs.push(
                queue.add(
                  'followup',
                  { campaignId: id, contactId: contactObjId.toString ? contactObjId.toString() : String(contactObjId), step: doc.step },
                  { removeOnComplete: true, removeOnFail: true, attempts: MAX_ATTEMPTS, backoff: { type: 'exponential', delay: 60_000 } }
                )
              );
            } else {
              jobs.push(
                queue.add(
                  'initial',
                  { campaignId: id, contactId: contactObjId.toString ? contactObjId.toString() : String(contactObjId) },
                  { removeOnComplete: true, removeOnFail: true, attempts: MAX_ATTEMPTS, backoff: { type: 'exponential', delay: 60_000 } }
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

      // Also correct processed counter because we are moving final failed -> pending (processed should drop)
      try {
        const currentProcessed = await safeGetMetaInt(redisKey, 'processed');
        const decProc = Math.min(updated, currentProcessed);
        if (decProc > 0) {
          await redis.hincrby(redisKey, 'processed', -decProc);
        }
      } catch (e) {
        console.warn('Failed to update processed counter during retryFailed', e);
      }

      // Publish contact-level updates for each retried contact (best-effort)
      try {
        for (const doc of failedDocs) {
          const cid = doc.contactId ? (doc.contactId.toString ? doc.contactId.toString() : String(doc.contactId)) : (doc._id.toString ? doc._id.toString() : String(doc._id));
          await safePublish(`campaign:${id}:contact_update`, { contactId: cid, status: 'pending', bgAttempts: 0, attempts: 1 });
        }
      } catch (_) {
        // ignore publish errors
      }

      await safePublish('campaign:new', { id, action: 'retryFailed', retried: updated });

      return res.status(200).json({ ok: true, retried: updated, attemptedEnqueue: toRetryCount });
    }

    // ACTION: Retry a single failed contact by contactId
    if (action === 'retryContact') {
      if (!contactId || typeof contactId !== 'string') {
        return res.status(400).json({ error: 'Missing contactId for retryContact' });
      }

      // Disallow retrying if campaign is cancelled/deleted/paused
      if (campaign.status === 'cancelled' || campaign.status === 'deleted' || campaign.status === 'paused') {
        return res.status(400).json({ error: 'Cannot retry contacts for cancelled/deleted/paused campaign' });
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
        return res.status(400).json({ error: 'Contact has reached max manual attempts and cannot be retried' });
      }

      // Prevent manual retry while BullMQ background retries are still running for this contact
      // Require bgAttempts >= MAX_ATTEMPTS (or missing) before allowing manual retry
      const bgDone = (typeof doc.bgAttempts === 'number' ? doc.bgAttempts >= MAX_ATTEMPTS : true);
      if (!bgDone) {
        return res.status(400).json({ error: 'Background retries are still in progress for this contact. Please wait until the background retry cycle completes.' });
      }

      // Update single doc to pending, increment core attempts, reset bgAttempts
      await db.collection('campaign_contacts').updateOne(
        { _id: doc._id },
        { $set: { status: 'pending', lastError: null, bgAttempts: 0 }, $inc: { attempts: 1 } }
      );

      // Enqueue appropriate job (use doc.step if present) and ensure MQ attempts/backoff are set
      try {
        if (doc.step) {
          await queue.add(
            'followup',
            { campaignId: id, contactId: contactObjId.toString ? contactObjId.toString() : String(contactObjId), step: doc.step },
            { removeOnComplete: true, removeOnFail: true, attempts: MAX_ATTEMPTS, backoff: { type: 'exponential', delay: 60_000 } }
          );
        } else {
          await queue.add(
            'initial',
            { campaignId: id, contactId: contactObjId.toString ? contactObjId.toString() : String(contactObjId) },
            { removeOnComplete: true, removeOnFail: true, attempts: MAX_ATTEMPTS, backoff: { type: 'exponential', delay: 60_000 } }
          );
        }
      } catch (e) {
        console.warn('Failed to enqueue retry job for contact', e);
        // Rollback: try to revert the ledger row changes (best-effort)
        try {
          await db.collection('campaign_contacts').updateOne(
            { _id: doc._id },
            { $set: { status: 'failed', lastError: 'enqueue-failed' }, $inc: { attempts: -1 }, $set: { bgAttempts: doc.bgAttempts ?? 0 } } as any
          );
        } catch (_) {}
        return res.status(500).json({ error: 'Failed to enqueue retry job' });
      }

      // Update redis counters: decrease failed by 1 if possible (we moved it from failed -> pending)
      try {
        const currentFailed = await safeGetMetaInt(redisKey, 'failed');
        if (currentFailed > 0) {
          await redis.hincrby(redisKey, 'failed', -1);
        }
      } catch (e) {
        console.warn('Failed to update redis counters during retryContact', e);
      }

      // Also decrement processed so totals align (we are un-finalizing a contact)
      try {
        const currentProcessed = await safeGetMetaInt(redisKey, 'processed');
        if (currentProcessed > 0) {
          await redis.hincrby(redisKey, 'processed', -1);
        }
      } catch (e) {
        console.warn('Failed to update processed counter during retryContact', e);
      }

      // Publish contact update
      try {
        const cidStr = contactObjId.toString ? contactObjId.toString() : String(contactObjId);
        await safePublish(`campaign:${id}:contact_update`, { contactId: cidStr, status: 'pending', attempts: (doc.attempts || 0) + 1, bgAttempts: 0 });
      } catch (_) {}

      await safePublish('campaign:new', { id, action: 'retryContact', contactId });

      return res.status(200).json({ ok: true, retried: 1, contactId });
    }

    // ACTION: Manual Hold (Manual Override) - place contact into a human-controlled hold state
    if (action === 'manualHold') {
      if (!contactId || typeof contactId !== 'string') {
        return res.status(400).json({ error: 'Missing contactId for manualHold' });
      }

      // Find ledger row
      let contactObjId;
      try {
        contactObjId = new ObjectId(contactId);
      } catch {
        contactObjId = contactId;
      }

      const doc = await db.collection('campaign_contacts').findOne({
        campaignId: campaignObjectId,
        contactId: contactObjId,
      });

      if (!doc) {
        return res.status(404).json({ error: 'Contact ledger row not found for campaign' });
      }

      // If already in manual_hold, no-op
      if (doc.status === 'manual_hold') {
        return res.status(200).json({ ok: true, message: 'Contact already under manual hold' });
      }

      const prevStatus = doc.status ?? null;
      const now = new Date();

      // Remove any queued jobs for this contact (best-effort)
      const cidStr = contactObjId.toString ? contactObjId.toString() : String(contactObjId);
      const removedJobs = await removeQueuedJobsForContact(id, cidStr);

      // Atomically update ledger: set status manual_hold and push history entry
      try {
        await db.collection('campaign_contacts').updateOne(
          { _id: doc._id },
          {
            $set: { status: 'manual_hold', lastError: null },
            $push: {
              manualHistory: {
                at: now,
                by: 'user',
                action: 'hold',
                prevStatus,
                removedJobs: removedJobs,
              }
            }
          }
        );
      } catch (e) {
        console.warn('Failed to set manual_hold on ledger row', e);
        return res.status(500).json({ error: 'failed-to-apply-manual-hold' });
      }

      // Publish contact-level update for UI
      try {
        await safePublish(`campaign:${id}:contact_update`, {
          contactId: cidStr,
          status: 'manual_hold',
          prevStatus,
          manualActionAt: now.toISOString(),
          removedJobs,
        });
      } catch (_) { }

      await safePublish('campaign:new', { id, action: 'manualHold', contactId: cidStr });

      return res.status(200).json({ ok: true, action: 'manual_hold', contactId: cidStr, removedJobs });
    }

    // ACTION: Manual Undo (release previously held contact)
    if (action === 'manualUndo') {
      if (!contactId || typeof contactId !== 'string') {
        return res.status(400).json({ error: 'Missing contactId for manualUndo' });
      }

      // Find ledger row
      let contactObjId2;
      try {
        contactObjId2 = new ObjectId(contactId);
      } catch {
        contactObjId2 = contactId;
      }

      const doc2 = await db.collection('campaign_contacts').findOne({
        campaignId: campaignObjectId,
        contactId: contactObjId2,
      });

      if (!doc2) {
        return res.status(404).json({ error: 'Contact ledger row not found for campaign' });
      }

      if (doc2.status !== 'manual_hold') {
        return res.status(400).json({ error: 'Contact is not under manual hold' });
      }

      const history = Array.isArray(doc2.manualHistory) ? doc2.manualHistory : [];
      // Find the last hold entry to determine previous status to restore
      let lastHoldEntry: any = null;
      for (let i = history.length - 1; i >= 0; i--) {
        const h = history[i];
        if (h && h.action === 'hold') {
          lastHoldEntry = h;
          break;
        }
      }

      if (!lastHoldEntry) {
        // fallback: if no history, restore to 'pending' conservatively
        lastHoldEntry = { prevStatus: 'pending', at: null };
      }

      const restoreTo = lastHoldEntry.prevStatus ?? 'pending';
      const now2 = new Date();

      // Update ledger: set status back to restoreTo and push a release history entry.
      try {
        await db.collection('campaign_contacts').updateOne(
          { _id: doc2._id },
          {
            $set: { status: restoreTo, lastError: null },
            $push: {
              manualHistory: {
                at: now2,
                by: 'user',
                action: 'release',
                restoredTo: restoreTo,
                refHoldAt: lastHoldEntry.at ?? null,
              }
            }
          }
        );
      } catch (e) {
        console.warn('Failed to release manual_hold on ledger row', e);
        return res.status(500).json({ error: 'failed-to-release-manual-hold' });
      }

      const cidStr2 = contactObjId2.toString ? contactObjId2.toString() : String(contactObjId2);

      // If we restored to pending, attempt to re-enqueue the job (best-effort)
      let enqueued = false;
      try {
        if (restoreTo === 'pending') {
          // decide job type based on doc2.step presence
          const contactObjForJob = doc2.contactId ? (doc2.contactId.toString ? doc2.contactId.toString() : String(doc2.contactId)) : (doc2._id.toString ? doc2._id.toString() : String(doc2._id));
          if (doc2.step) {
            await queue.add(
              'followup',
              { campaignId: id, contactId: contactObjForJob, step: doc2.step },
              { removeOnComplete: true, removeOnFail: true, attempts: MAX_ATTEMPTS, backoff: { type: 'exponential', delay: 60_000 } }
            );
          } else {
            await queue.add(
              'initial',
              { campaignId: id, contactId: contactObjForJob },
              { removeOnComplete: true, removeOnFail: true, attempts: MAX_ATTEMPTS, backoff: { type: 'exponential', delay: 60_000 } }
            );
          }
          enqueued = true;
        }
      } catch (e) {
        console.warn('Failed to enqueue job when releasing manual hold', e);
        // Do not revert status change; surface best-effort result below
      }

      // Publish contact-level update
      try {
        await safePublish(`campaign:${id}:contact_update`, {
          contactId: cidStr2,
          status: restoreTo,
          manualActionAt: now2.toISOString(),
          requeued: enqueued,
        });
      } catch (_) { }

      await safePublish('campaign:new', { id, action: 'manualUndo', contactId: cidStr2, restoredTo: restoreTo, requeued: enqueued });

      return res.status(200).json({ ok: true, action: 'manualUndo', contactId: cidStr2, restoredTo: restoreTo, requeued: enqueued });
    }

    // Should not reach here
    return res.status(400).json({ error: 'Unsupported action' });
  } catch (err) {
    console.error('Campaign control error', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}