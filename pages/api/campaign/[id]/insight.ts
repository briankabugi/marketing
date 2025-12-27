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

    const breakdown = { pending: 0, sent: 0, failed: 0 };
    for (const row of aggregation) {
      if (row._id === 'pending') breakdown.pending = row.count;
      else if (row._id === 'sent') breakdown.sent = row.count;
      else if (row._id === 'failed') breakdown.failed = row.count;
    }

    const totals = {
      intended: Number(redisMeta?.total ?? campaign.totals?.intended ?? 0),
      processed: breakdown.sent + breakdown.failed,
      sent: breakdown.sent,
      failed: breakdown.failed,
    };

    // --- Recent failures ---
    const recentFailures = await db
      .collection('campaign_contacts')
      .find({ campaignId, status: 'failed' })
      .sort({ lastAttemptAt: -1 })
      .limit(10)
      .toArray();

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
        unique: uniqueOpeners.length,
        rate: sent > 0 ? uniqueOpeners.length / sent : 0,
      },
      clicks: {
        total: clickCount,
        unique: uniqueClickers.length,
        rate: sent > 0 ? uniqueClickers.length / sent : 0,
      },
      links: topLinks.map(l => ({
        url: l._id,
        clicks: l.clicks,
      })),
    };

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
      maxAttempts: MAX_ATTEMPTS,
    });
  } catch (err) {
    console.error('Insight API error', err);
    res.status(500).json({ error: 'Internal server error' });
  }
}
