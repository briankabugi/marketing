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
            const key = o._1?._toString ? o._1.toString() : (o._id && o._id.toString ? o._id.toString() : String(o._id));
            // Some drivers may return _id as an ObjectId or primitive; normalize
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

    // Build response items
    const items = rows.map((r: any) => {
      const cidKey = r.contactId && r.contactId.toString ? r.contactId.toString() : String(r.contactId ?? '');
      const contactDoc = contactDocsMap[cidKey];
      const email = contactDoc?.email ?? r.email ?? null;

      const lastOpenAt = openLookup[cidKey] ?? null;
      const lastClickAt = clickLookup[cidKey] ?? null;

      return {
        id: r._id?.toString?.() ?? String(r._id),
        contactId: r.contactId && r.contactId.toString ? r.contactId.toString() : String(r.contactId ?? null),
        email,
        status: r.status ?? null,
        attempts: typeof r.attempts === 'number' ? r.attempts : 0,
        bgAttempts: typeof r.bgAttempts === 'number' ? r.bgAttempts : 0,
        lastAttemptAt: r.lastAttemptAt ? new Date(r.lastAttemptAt).toISOString() : null,
        lastError: r.lastError ?? null,
        opened: !!lastOpenAt,
        lastOpenAt,
        clicked: !!lastClickAt,
        lastClickAt,
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
