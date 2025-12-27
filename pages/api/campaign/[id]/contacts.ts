// pages/api/campaign/[id]/contacts.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import clientPromise from '../../../../lib/mongo';
import { ObjectId } from 'mongodb';

function tryParseObjectId(s: any) {
  if (!s) return null;
  if (typeof s === 'object' && s instanceof ObjectId) return s;
  if (typeof s === 'string' && /^[0-9a-fA-F]{24}$/.test(s)) {
    try { return new ObjectId(s); } catch { return null; }
  }
  return null;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const { id } = req.query;
  if (!id || typeof id !== 'string') return res.status(400).json({ error: 'Invalid campaign id' });

  const status = typeof req.query.status === 'string' ? req.query.status : undefined;
  const page = Math.max(1, Number(req.query.page ? Number(req.query.page) : 1));
  const pageSize = Math.max(1, Number(req.query.pageSize ? Number(req.query.pageSize) : 25));

  try {
    const client = await clientPromise;
    const db = client.db('PlatformData');

    const campaignIdStr = String(id);
    const campaignIdObj = tryParseObjectId(campaignIdStr);

    // Base filter for campaign_contacts
    const baseFilter: any = { campaignId: campaignIdObj ?? campaignIdStr };
    if (status && status !== 'all') baseFilter.status = status;

    // Total count
    let total = await db.collection('campaign_contacts').countDocuments(baseFilter);

    // Pagination
    const skip = (page - 1) * pageSize;
    const cursor = db.collection('campaign_contacts').find(baseFilter).sort({ createdAt: 1 }).skip(skip).limit(pageSize);

    const rows = await cursor.toArray();

    // Collect contactIds present on this page
    const contactIdValues = rows.map(r => r.contactId).filter(Boolean);

    // Build arrays for ObjectId vs string contactId lookups
    const contactObjIds = contactIdValues
      .map((v) => {
        try {
          if (typeof v === 'object' && v instanceof ObjectId) return v;
          if (typeof v === 'string' && /^[0-9a-fA-F]{24}$/.test(v)) return new ObjectId(v);
        } catch {}
        return null;
      })
      .filter(Boolean) as ObjectId[];

    const stringContactIds = contactIdValues
      .map((v) => (typeof v === 'string' ? v : (v && v.toString ? v.toString() : null)))
      .filter(Boolean) as string[];

    // Fetch contact details (email) for these contactIds
    let contactDocsMap: Record<string, any> = {};
    if (contactObjIds.length || stringContactIds.length) {
      const contactQuery: any = { $or: [] as any[] };
      if (contactObjIds.length) contactQuery.$or.push({ _id: { $in: contactObjIds } });
      if (stringContactIds.length) contactQuery.$or.push({ _id: { $in: stringContactIds } });
      if (contactQuery.$or.length > 0) {
        const contactDocs = await db.collection('contacts').find(contactQuery).project({ email: 1 }).toArray();
        for (const c of contactDocs) contactDocsMap[(c._id && c._id.toString ? c._id.toString() : String(c._id))] = c;
      }
    }

    // -------------------------
    // Prefer ledger fields (campaign_contacts) for engagement
    // Only aggregate campaign_events for those contactIds missing ledger fields.
    // -------------------------

    // Initialize lookups from ledger rows when possible
    const openLookup: Record<string, string | null> = {};
    const clickLookup: Record<string, string | null> = {};

    // Keep a list of contact keys that need event-aggregation fallback
    const needOpenAggKeys: string[] = [];
    const needClickAggKeys: string[] = [];

    for (const r of rows) {
      const cidKey = r.contactId && r.contactId.toString ? r.contactId.toString() : String(r.contactId ?? '');
      // ledger might already have openedAt/lastClickAt (worker & tracking endpoints update these)
      openLookup[cidKey] = r.openedAt ? (new Date(r.openedAt)).toISOString() : null;
      clickLookup[cidKey] = r.lastClickAt ? (new Date(r.lastClickAt)).toISOString() : null;

      if (!openLookup[cidKey]) needOpenAggKeys.push(cidKey);
      if (!clickLookup[cidKey]) needClickAggKeys.push(cidKey);
    }

    // If some rows are missing ledger open/click info, aggregate events only for those keys
    if (contactIdValues.length > 0 && (needOpenAggKeys.length > 0 || needClickAggKeys.length > 0)) {
      // Build mapping back to the two forms (ObjectId vs string) for only the missing ones
      const missingObjIdsForOpen: ObjectId[] = [];
      const missingStrIdsForOpen: string[] = [];

      const missingObjIdsForClick: ObjectId[] = [];
      const missingStrIdsForClick: string[] = [];

      // Create reverse map from key -> original value to decide if that key corresponds to an ObjectId or string
      const keyToOriginal: Record<string, any> = {};
      for (const v of contactIdValues) {
        const key = (v && v.toString ? v.toString() : String(v));
        if (!(key in keyToOriginal)) keyToOriginal[key] = v;
      }

      for (const key of needOpenAggKeys) {
        const orig = keyToOriginal[key];
        if (!orig) continue;
        if (typeof orig === 'object' && orig instanceof ObjectId) missingObjIdsForOpen.push(orig);
        else if (typeof orig === 'string' && /^[0-9a-fA-F]{24}$/.test(orig)) {
          try { missingObjIdsForOpen.push(new ObjectId(orig)); } catch { missingStrIdsForOpen.push(orig); }
        } else {
          missingStrIdsForOpen.push(String(orig));
        }
      }

      for (const key of needClickAggKeys) {
        const orig = keyToOriginal[key];
        if (!orig) continue;
        if (typeof orig === 'object' && orig instanceof ObjectId) missingObjIdsForClick.push(orig);
        else if (typeof orig === 'string' && /^[0-9a-fA-F]{24}$/.test(orig)) {
          try { missingObjIdsForClick.push(new ObjectId(orig)); } catch { missingStrIdsForClick.push(orig); }
        } else {
          missingStrIdsForClick.push(String(orig));
        }
      }

      // Build campaignId match that copes with ObjectId or string
      const campaignIdMatch: any = campaignIdObj ? { $or: [{ campaignId: campaignIdObj }, { campaignId: campaignIdStr }] } : { campaignId: campaignIdStr };

      // Aggregate opens for missing keys
      try {
        const contactMatchClauses: any[] = [];
        if (missingObjIdsForOpen.length) contactMatchClauses.push({ contactId: { $in: missingObjIdsForOpen } });
        if (missingStrIdsForOpen.length) contactMatchClauses.push({ contactId: { $in: missingStrIdsForOpen } });

        if (contactMatchClauses.length > 0) {
          const openAgg = await db.collection('campaign_events').aggregate([
            { $match: { $and: [ campaignIdMatch, { type: 'open' }, { $or: contactMatchClauses } ] } },
            { $group: { _id: '$contactId', lastOpenAt: { $max: '$createdAt' }, opens: { $sum: 1 } } },
          ]).toArray();

          for (const o of openAgg) {
            const k = (o._id && o._id.toString) ? o._id.toString() : String(o._id);
            openLookup[k] = o.lastOpenAt ? (new Date(o.lastOpenAt)).toISOString() : null;
          }
        }
      } catch (e) {
        console.warn('Failed to aggregate opens for contacts page (fallback)', e);
      }

      // Aggregate clicks for missing keys
      try {
        const contactMatchClauses2: any[] = [];
        if (missingObjIdsForClick.length) contactMatchClauses2.push({ contactId: { $in: missingObjIdsForClick } });
        if (missingStrIdsForClick.length) contactMatchClauses2.push({ contactId: { $in: missingStrIdsForClick } });

        if (contactMatchClauses2.length > 0) {
          const clickAgg = await db.collection('campaign_events').aggregate([
            { $match: { $and: [ campaignIdMatch, { type: 'click' }, { $or: contactMatchClauses2 } ] } },
            { $group: { _id: '$contactId', lastClickAt: { $max: '$createdAt' }, clicks: { $sum: 1 } } },
          ]).toArray();

          for (const c of clickAgg) {
            const k = (c._id && c._id.toString) ? c._id.toString() : String(c._id);
            clickLookup[k] = c.lastClickAt ? (new Date(c.lastClickAt)).toISOString() : null;
          }
        }
      } catch (e) {
        console.warn('Failed to aggregate clicks for contacts page (fallback)', e);
      }
    }

    // -------------------------
    // NEW: aggregate followup_sent & followup_skipped events for these page contactIds
    // -------------------------
    const followupLookup: Record<string, { completed: number; lastSkippedAt?: string | null; lastSkippedReason?: string | null }> = {};
    try {
      if (contactIdValues.length > 0) {
        // Build contact match clauses that accept ObjectId or string ids
        const contactMatchClauses: any[] = [];
        if (contactObjIds.length) contactMatchClauses.push({ contactId: { $in: contactObjIds } });
        if (stringContactIds.length) contactMatchClauses.push({ contactId: { $in: stringContactIds } });

        if (contactMatchClauses.length > 0) {
          const campaignIdMatch: any = campaignIdObj ? { $or: [{ campaignId: campaignIdObj }, { campaignId: campaignIdStr }] } : { campaignId: campaignIdStr };

          const fuAgg = await db.collection('campaign_events').aggregate([
            { $match: { $and: [ campaignIdMatch, { type: { $in: ['followup_sent', 'followup_skipped'] } }, { $or: contactMatchClauses } ] } },
            { $group: {
                _id: '$contactId',
                completed: { $sum: 1 },
                lastSkippedAt: { $max: { $cond: [ { $eq: ['$type', 'followup_skipped'] }, '$createdAt', null ] } },
                lastSkippedReason: { $max: { $cond: [ { $eq: ['$type', 'followup_skipped'] }, '$skippedReason', null ] } }
              }
            }
          ]).toArray();

          for (const f of fuAgg) {
            const k = (f._id && f._id.toString) ? f._id.toString() : String(f._id);
            followupLookup[k] = {
              completed: (typeof f.completed === 'number' ? f.completed : Number(f.completed || 0)),
              lastSkippedAt: f.lastSkippedAt ? (new Date(f.lastSkippedAt)).toISOString() : null,
              lastSkippedReason: f.lastSkippedReason ?? null,
            };
          }
        }
      }
    } catch (e) {
      console.warn('Failed to aggregate followup events for contacts page', e);
    }

    // Fetch campaign document to know how many followups are configured (safe fallback)
    let campaignDoc: any = null;
    try {
      campaignDoc = await db.collection('campaigns').findOne({ _id: campaignIdObj ?? campaignIdStr });
    } catch (e) {
      // ignore
    }
    const followUpCount = Array.isArray(campaignDoc?.followUps) ? campaignDoc.followUps.length : 0;

    // Build response items
    const items = rows.map((r: any) => {
      const cidKey = r.contactId && r.contactId.toString ? r.contactId.toString() : String(r.contactId ?? '');
      const contactDoc = contactDocsMap[cidKey];
      const email = contactDoc?.email ?? r.email ?? null;

      const lastOpenAt = openLookup[cidKey] ?? null;
      const lastClickAt = clickLookup[cidKey] ?? null;

      const fuInfo = followupLookup[cidKey] ?? { completed: 0 };

      // Prefer ledger followUpPlan when present for accurate progression
      const ledgerPlan = Array.isArray(r.followUpPlan) ? r.followUpPlan : null;
      let completedFromLedger: number | null = null;
      let lastSkippedAtFromLedger: string | null = null;
      let lastSkippedReasonFromLedger: string | null = null;
      let planCountLocal = followUpCount;

      if (ledgerPlan) {
        planCountLocal = ledgerPlan.length;
        completedFromLedger = ledgerPlan.filter((p: any) => {
          if (p == null) return false;
          const status = (p.status || '').toString().toLowerCase();
          if (status === 'sent' || !!p.sentAt) return true;
          return false;
        }).length;

        // find last skipped entry if any
        const skippedEntries = ledgerPlan.filter((p: any) => (p && (p.status === 'skipped' || p.skippedAt)));
        if (skippedEntries.length) {
          // pick latest skippedAt (if present) else null
          const latest = skippedEntries.reduce((acc: any, cur: any) => {
            const curDate = cur.skippedAt ? new Date(cur.skippedAt) : null;
            if (!acc) return cur;
            const accDate = acc.skippedAt ? new Date(acc.skippedAt) : null;
            if (curDate && accDate) return curDate.getTime() > accDate.getTime() ? cur : acc;
            if (curDate && !accDate) return cur;
            return acc;
          }, null);
          if (latest) {
            lastSkippedAtFromLedger = latest.skippedAt ? (new Date(latest.skippedAt)).toISOString() : null;
            lastSkippedReasonFromLedger = latest.skippedReason ?? null;
          }
        }
      }

      const completed = (typeof completedFromLedger === 'number') ? completedFromLedger : (fuInfo.completed ?? 0);
      const nextStep = (typeof completed === 'number' && completed < (planCountLocal || 0)) ? (completed + 1) : null;

      // Normalize followUpPlan for API output (convert Date-like to ISO strings)
      const followUpPlanOut = (ledgerPlan || []).map((p: any) => {
        return {
          index: p.index ?? null,
          name: p.name ?? null,
          rule: p.rule ?? null,
          status: p.status ?? null,
          scheduledFor: p.scheduledFor ? (new Date(p.scheduledFor)).toISOString() : (p.scheduledFor ? String(p.scheduledFor) : null),
          sentAt: p.sentAt ? (new Date(p.sentAt)).toISOString() : null,
          skippedAt: p.skippedAt ? (new Date(p.skippedAt)).toISOString() : null,
          skippedReason: p.skippedReason ?? null,
          delayMinutes: typeof p.delayMinutes === 'number' ? p.delayMinutes : (p.delayMinutes ? Number(p.delayMinutes) : null),
        };
      });

      // Step-scoped counters (new model): prefer explicit currentStep fields if present, otherwise fall back.
      const currentStepIndex = (typeof r.currentStepIndex === 'number' || (r.currentStepIndex != null)) ? Number(r.currentStepIndex) : (r.currentStepIndex ? Number(r.currentStepIndex) : null);
      const currentStepName = r.currentStepName ?? null;
      const currentStepAttempts = typeof r.currentStepAttempts === 'number' ? r.currentStepAttempts : (typeof r.attempts === 'number' ? r.attempts : 0);
      const currentStepBgAttempts = typeof r.currentStepBgAttempts === 'number' ? r.currentStepBgAttempts : (typeof r.bgAttempts === 'number' ? r.bgAttempts : 0);

      return {
        id: r._id?.toString?.() ?? String(r._id),
        contactId: r.contactId && r.contactId.toString ? r.contactId.toString() : String(r.contactId ?? null),
        email,
        status: r.status ?? null,
        // attempts reflect active step attempts under the new model; keep explicit fields as well
        attempts: typeof r.attempts === 'number' ? r.attempts : 0,
        bgAttempts: typeof r.bgAttempts === 'number' ? r.bgAttempts : 0,
        lastAttemptAt: r.lastAttemptAt ? new Date(r.lastAttemptAt).toISOString() : null,
        lastError: r.lastError ?? null,
        opened: !!lastOpenAt,
        lastOpenAt,
        clicked: !!lastClickAt,
        lastClickAt,
        // replies (existing)
        replied: !!(r.replied || r.repliesCount || r.lastReplyAt),
        lastReplyAt: r.lastReplyAt ?? null,
        lastReplySnippet: r.lastReplySnippet ?? null,
        repliesCount: typeof r.repliesCount === 'number' ? r.repliesCount : (r.replied ? 1 : 0),
        // followup progress (new / improved)
        followupsCompleted: completed,
        nextFollowUpStep: nextStep, // 1-based index or null
        // ledger-backed plan + progression fields
        followUpPlan: followUpPlanOut,
        nextFollowUpAt: r.nextFollowUpAt ? (new Date(r.nextFollowUpAt)).toISOString() : null,
        followUpStatus: r.followUpStatus ?? null,
        lastStepSentAt: r.lastStepSentAt ? (new Date(r.lastStepSentAt)).toISOString() : null,
        currentStepIndex,
        currentStepName,
        currentStepAttempts,
        currentStepBgAttempts,
        // skipped info: prefer ledger values, fall back to aggregated events
        lastFollowupSkippedAt: lastSkippedAtFromLedger ?? (fuInfo.lastSkippedAt ?? null),
        lastFollowupSkippedReason: lastSkippedReasonFromLedger ?? (fuInfo.lastSkippedReason ?? null),
      };
    });

    // Available statuses
    let availableStatuses: string[] = [];
    try {
      availableStatuses = await db.collection('campaign_contacts').distinct('status', { campaignId: campaignIdObj ?? campaignIdStr });
      availableStatuses = (availableStatuses || []).filter(Boolean);
    } catch (e) {
      // ignore
    }

    const pages = Math.max(1, Math.ceil(total / pageSize));

    return res.status(200).json({
      items,
      total,
      page,
      pages,
      pageSize,
      availableStatuses,
    });
  } catch (err) {
    console.error('Contacts API error', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}
