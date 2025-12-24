// pages/api/campaign/[id]/contacts.ts

import type { NextApiRequest, NextApiResponse } from 'next';
import clientPromise from '../../../../lib/mongo';
import { redis } from '../../../../lib/redis';
import { ObjectId } from 'mongodb';

const MAX_PAGE_SIZE = 200;
const MAX_ATTEMPTS = Number(process.env.MAX_ATTEMPTS || 3);

function safeNum(v: any, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : fallback;
}

/**
 * Build a simple preview object from either the step or the campaign definition.
 * Kept for compatibility with earlier clients; not required for the current UI.
 */
async function buildPreviewForRow(campaignId: string, campaignDoc: any, row: any) {
  try {
    const key = `campaign:${campaignId}:definition`;
    const raw = await redis.get(key);
    if (raw) {
      const definition = JSON.parse(raw);
      if (row.step && row.step.body) {
        return { subject: row.step.subject || definition.initial?.subject || campaignDoc?.name || '', body: row.step.body || '' };
      }
      return { subject: definition.initial?.subject || campaignDoc?.name || '', body: definition.initial?.body || campaignDoc?.body || '' };
    }
  } catch (e) {
    // ignore and continue to fallback
  }

  const subject = row.step?.subject || campaignDoc?.subject || campaignDoc?.title || campaignDoc?.name || `Campaign ${campaignId}`;
  const body = row.step?.body || campaignDoc?.body || campaignDoc?.content || '';
  return { subject, body };
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { id } = req.query;
  if (!id || typeof id !== 'string') return res.status(400).json({ error: 'Invalid campaign id' });

  // Query params: page, pageSize, status, search, sort, viewAll
  const page = safeNum(req.query.page, 1);
  const pageSize = Math.min(MAX_PAGE_SIZE, Math.max(1, safeNum(req.query.pageSize, 25)));
  const status = typeof req.query.status === 'string' && req.query.status.length ? req.query.status : undefined;
  const search = typeof req.query.search === 'string' && req.query.search.length ? req.query.search : undefined;
  const sort = req.query.sort === 'asc' ? 1 : -1;
  const viewAll = req.query.viewAll === 'true' || req.query.viewAll === '1';

  try {
    const client = await clientPromise;
    const db = client.db('PlatformData');
    const campaignId = new ObjectId(id);

    const campaignDoc = await db.collection('campaigns').findOne({ _id: campaignId });
    if (!campaignDoc) return res.status(404).json({ error: 'Campaign not found' });

    const filter: any = { campaignId };

    if (status) filter.status = status;
    if (search) {
      // search against email or contact snapshot fields
      filter.$or = [
        { email: { $regex: new RegExp(search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i') } },
        { contactIdStr: { $regex: new RegExp(search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i') } },
      ];
    }

    // Pagination calculation
    const total = await db.collection('campaign_contacts').countDocuments(filter);

    // If viewAll requested, we'll return up to a server cap
    let effectiveLimit = pageSize;
    let skip = (page - 1) * pageSize;
    if (viewAll) {
      const viewAllCap = Math.min(5000, Math.max(pageSize, total));
      effectiveLimit = viewAllCap;
      skip = 0;
    }

    const cursor = db
      .collection('campaign_contacts')
      .find(filter, {
        projection: {
          _id: 1,
          contactId: 1,
          email: 1,
          status: 1,
          attempts: 1,
          lastAttemptAt: 1,
          lastError: 1,
          step: 1,
        },
      })
      .sort({ lastAttemptAt: sort, email: 1 })
      .skip(skip)
      .limit(effectiveLimit);

    const rows = await cursor.toArray();

    // Collect contactIds that need email backfill
    const contactIdToRows: Record<string, any[]> = {};
    const objectIdsToLookup: ObjectId[] = [];
    for (const r of rows) {
      // Normalize contactId to string for mapping
      const cidRaw = r.contactId ?? r.contactIdStr ?? (r._id ? r._id.toString() : null);
      const cidStr = cidRaw ? String(cidRaw) : null;

      // if email exists in ledger row, no need to lookup
      if (!r.email && cidStr) {
        // try to parse as ObjectId for lookup
        try {
          const oid = new ObjectId(cidStr);
          objectIdsToLookup.push(oid);
          contactIdToRows[cidStr] = contactIdToRows[cidStr] || [];
          contactIdToRows[cidStr].push(r);
        } catch {
          // not an ObjectId — still record so we can attempt fallback later
          contactIdToRows[cidStr] = contactIdToRows[cidStr] || [];
          contactIdToRows[cidStr].push(r);
        }
      }
    }

    // Query contacts collection for ObjectId matches
    const idToEmail: Record<string, string> = {};
    if (objectIdsToLookup.length > 0) {
      const contactsRaw = await db
        .collection('contacts')
        .find({ _id: { $in: objectIdsToLookup } }, { projection: { _id: 1, email: 1 } })
        .toArray();

      for (const c of contactsRaw) {
        if (!c || !c._id) continue;
        idToEmail[c._id.toString()] = c.email ?? '';
      }
    }

    // Build response items — fill email from ledger or from contacts lookup, or null
    const items: any[] = [];
    for (const r of rows) {
      const cidRaw = r.contactId ?? r.contactIdStr ?? (r._id ? r._id.toString() : null);
      const cidStr = cidRaw ? String(cidRaw) : null;
      let email = r.email ?? null;
      if (!email && cidStr) {
        email = idToEmail[cidStr] ?? null;
      }

      // As a last resort, try to see if the ledger row stores an email in another field
      if (!email && (r.contactEmail || r.emailSnapshot)) {
        email = r.contactEmail ?? r.emailSnapshot ?? null;
      }

      // keep preview for compatibility (may be empty)
      const preview = await buildPreviewForRow(id, campaignDoc, r);

      items.push({
        id: r._id?.toString?.() ?? r._id,
        contactId: cidStr,
        email,
        status: r.status,
        attempts: r.attempts ?? 0,
        lastAttemptAt: r.lastAttemptAt ?? null,
        lastError: r.lastError ?? null,
        previewSubject: preview.subject ?? '',
        previewBody: preview.body ?? '',
      });
    }

    return res.status(200).json({
      campaignId: id,
      total,
      page,
      pageSize: effectiveLimit,
      pages: viewAll ? 1 : Math.max(1, Math.ceil(total / pageSize)),
      items,
      maxAttempts: MAX_ATTEMPTS,
      viewAll: !!viewAll,
    });
  } catch (err) {
    console.error('Contacts API error', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}
