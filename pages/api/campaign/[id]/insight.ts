// pages/api/campaign/[id]/insight.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import clientPromise from '../../../../lib/mongo';
import { redis } from '../../../../lib/redis';
import { ObjectId } from 'mongodb';

const MAX_ATTEMPTS = Number(process.env.MAX_ATTEMPTS || 3);

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const { id } = req.query;

  if (!id || typeof id !== 'string') {
    return res.status(400).json({ error: 'Invalid campaign id' });
  }

  try {
    const client = await clientPromise;
    const db = client.db('PlatformData');
    const campaignId = new ObjectId(id);

    // --- MongoDB: authoritative campaign document ---
    const campaign = await db.collection('campaigns').findOne({ _id: campaignId });
    if (!campaign) return res.status(404).json({ error: 'Campaign not found' });

    // --- Redis meta (best-effort, optional) ---
    let redisMeta: Record<string, string> | null = null;
    try {
      if (redis?.status === 'ready' || (redis as any).isOpen) {
        const meta = await redis.hgetall(`campaign:${id}:meta`);
        redisMeta = Object.keys(meta).length > 0 ? meta : null;
      }
    } catch (e) {
      console.warn('Redis unavailable, falling back to Mongo', e);
    }

    // --- Breakdown from MongoDB ledger ---
    const aggregation = await db
      .collection('campaign_contacts')
      .aggregate([
        { $match: { campaignId } },
        { $group: { _id: '$status', count: { $sum: 1 } } },
      ])
      .toArray();

    const breakdown = { pending: 0, sent: 0, failed: 0, manual_hold: 0 };
    for (const row of aggregation) {
      if (row._id === 'pending') breakdown.pending = row.count;
      else if (row._id === 'sent') breakdown.sent = row.count;
      else if (row._id === 'failed') breakdown.failed = row.count;
      else if (row._id === 'manual_hold') breakdown.manual_hold = row.count;
    }

    const totals = {
      intended: Number(redisMeta?.total ?? campaign.totals?.intended ?? 0),
      processed: breakdown.sent + breakdown.failed,
      sent: breakdown.sent,
      failed: breakdown.failed,
    };

    // queuedEstimate (best-effort)
    const queuedEstimate = (() => {
      try {
        const intended = Number(redisMeta?.total ?? campaign.totals?.intended ?? 0);
        const processed = Number(breakdown.sent + breakdown.failed);
        if (Number.isFinite(intended - processed)) return Math.max(0, intended - processed);
        return null;
      } catch {
        return null;
      }
    })();

    // --- Recent failures ---
    const recentFailures = await db
      .collection('campaign_contacts')
      .find({ campaignId, status: 'failed' })
      .sort({ lastAttemptAt: -1 })
      .limit(10)
      .toArray();

    // --- Recent manual holds (for UI to inspect) ---
    let recentManualHolds: Array<any> = [];
    try {
      const holds = await db
        .collection('campaign_contacts')
        .find({ campaignId, status: 'manual_hold' })
        .sort({ 'manualHistory.at': -1 }) // best-effort; if manualHistory.at not indexed this still returns
        .limit(10)
        .toArray();

      recentManualHolds = await Promise.all(holds.map(async (h) => {
        // try to resolve contact email if present in the ledger row, otherwise attempt contacts collection
        let email = h.email ?? null;
        try {
          if (!email && h.contactId) {
            const maybeContactId = h.contactId;
            const contactDoc = await db.collection('contacts').findOne({ _id: maybeContactId });
            if (contactDoc && contactDoc.email) email = contactDoc.email;
          }
        } catch (_) { /* ignore */ }

        // find last hold entry in manualHistory
        let heldAt: string | null = null;
        let prevStatus: string | null = null;
        try {
          const mh = Array.isArray(h.manualHistory) ? h.manualHistory : [];
          for (let i = mh.length - 1; i >= 0; i--) {
            const e = mh[i];
            if (e && e.action === 'hold') {
              heldAt = e.at ? new Date(e.at).toISOString() : null;
              prevStatus = e.prevStatus ?? null;
              break;
            }
          }
        } catch (_) { /* ignore parsing errors */ }

        return {
          contactId: h.contactId?.toString?.() ?? h.contactId,
          email,
          prevStatus,
          heldAt,
        };
      }));
    } catch (e) {
      console.warn('Failed to fetch recent manual holds', e);
      recentManualHolds = [];
    }

    // --------------------------------------------------
    // Engagement analytics (campaign_events)
    // --------------------------------------------------

    const events = db.collection('campaign_events');

    // Total opens / clicks
    const [openCount, clickCount] = await Promise.all([
      events.countDocuments({ campaignId, type: 'open' }),
      events.countDocuments({ campaignId, type: 'click' }),
    ]);

    // Unique openers / clickers
    const [uniqueOpeners, uniqueClickers] = await Promise.all([
      events.distinct('contactId', { campaignId, type: 'open' }),
      events.distinct('contactId', { campaignId, type: 'click' }),
    ]);

    // Top clicked links
    const topLinks = await events
      .aggregate([
        { $match: { campaignId, type: 'click' } },
        { $group: { _id: '$url', clicks: { $sum: 1 } } },
        { $sort: { clicks: -1 } },
        { $limit: 10 },
      ])
      .toArray();

    // Replies summary (use campaign_events.reply as canonical)
    const [replyCountTotal, replyUniqueContacts] = await Promise.all([
      events.countDocuments({ campaignId, type: 'reply' }),
      events.distinct('contactId', { campaignId, type: 'reply' }),
    ]);

    const sent = breakdown.sent || 0;

    const engagement = {
      opens: {
        total: openCount,
        unique: Array.isArray(uniqueOpeners) ? uniqueOpeners.length : 0,
        rate: sent > 0 ? (Array.isArray(uniqueOpeners) ? uniqueOpeners.length : 0) / sent : 0,
      },
      clicks: {
        total: clickCount,
        unique: Array.isArray(uniqueClickers) ? uniqueClickers.length : 0,
        rate: sent > 0 ? (Array.isArray(uniqueClickers) ? uniqueClickers.length : 0) / sent : 0,
      },
      links: topLinks.map(l => ({
        url: l._id,
        clicks: l.clicks,
      })),
    };

    // --- Campaign-level health snapshot (from Redis, best-effort) ---
    let health: Array<{ domain: string; sent: number; failed: number; failRate: number; lastUpdated?: number }> = [];
    try {
      if (redis?.status === 'ready' || (redis as any).isOpen) {
        const h = await redis.hgetall(`campaign:${id}:health`);
        // h is flat map with keys like "domain:example.com:sent" etc. Convert into domain buckets.
        const buckets: Record<string, any> = {};
        for (const key of Object.keys(h || {})) {
          // key pattern: domain:{domain}:sent, domain:{domain}:failed, domain:{domain}:lastUpdated
          const m = key.match(/^domain:(.+?):(sent|failed|lastUpdated)$/);
          if (!m) continue;
          const domain = m[1];
          const field = m[2];
          buckets[domain] = buckets[domain] || { domain, sent: 0, failed: 0, lastUpdated: undefined };
          if (field === 'sent') buckets[domain].sent = Number(h[key] || 0);
          else if (field === 'failed') buckets[domain].failed = Number(h[key] || 0);
          else if (field === 'lastUpdated') buckets[domain].lastUpdated = Number(h[key] || undefined);
        }
        health = Object.values(buckets).map((b: any) => {
          const total = (b.sent || 0) + (b.failed || 0);
          return { domain: b.domain, sent: b.sent || 0, failed: b.failed || 0, failRate: total === 0 ? 0 : (b.failed / total), lastUpdated: b.lastUpdated };
        });
      }
    } catch (e) {
      console.warn('Failed to read campaign health from redis', e);
      health = [];
    }

    // --------------------------------------------------

    res.status(200).json({
      campaign: {
        id: campaign._id.toString(),
        name: campaign.name,
        status: campaign.status,
        createdAt: campaign.createdAt,
        completedAt: campaign.completedAt ?? null,
        // <-- add initial and followUps so UI can render them
        initial: campaign.initial ?? null,
        followUps: Array.isArray(campaign.followUps) ? campaign.followUps : [],
      },
      totals,
      breakdown,
      queuedEstimate,
      engagement,
      // replies summary
      replies: {
        total: replyCountTotal,
        uniqueContacts: Array.isArray(replyUniqueContacts) ? replyUniqueContacts.length : 0,
      },
      recentFailures: recentFailures.map((f) => ({
        contactId: f.contactId?.toString?.() ?? f.contactId,
        email: f.email,
        attempts: f.attempts,
        error: f.lastError ?? null,
        lastAttemptAt: f.lastAttemptAt,
      })),
      manualHolds: recentManualHolds,
      health,
      maxAttempts: MAX_ATTEMPTS,
    });
  } catch (err) {
    console.error('Insight API error', err);
    res.status(500).json({ error: 'Internal server error' });
  }
}
