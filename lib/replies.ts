// lib/replies.ts
import clientPromise from './mongo';
import { ObjectId } from 'mongodb';

/**
 * Convert an incoming value to ObjectId when possible.
 */
export function tryParseObjectId(maybeId: any) {
  if (maybeId == null) return maybeId;
  if (typeof maybeId === 'object' && maybeId instanceof ObjectId) return maybeId;
  if (typeof maybeId === 'string' && /^[0-9a-fA-F]{24}$/.test(maybeId)) {
    try {
      return new ObjectId(maybeId);
    } catch {
      return maybeId;
    }
  }
  return maybeId;
}

/**
 * Record an inbound reply into `replies` collection.
 * Returns insertedId.
 *
 * Payload example:
 * {
 *   campaignId?: string|ObjectId,
 *   contactId?: string|ObjectId,
 *   from?: string,
 *   to?: string,
 *   subject?: string,
 *   snippet?: string,
 *   messageId?: string,
 *   raw?: any
 * }
 */
export async function recordReply(payload: Record<string, any>) {
  const client = await clientPromise;
  const db = client.db('PlatformData');

  const doc: any = {
    createdAt: new Date(),
    inboundAt: payload.inboundAt ? new Date(payload.inboundAt) : new Date(),
    from: payload.from ?? null,
    to: payload.to ?? null,
    subject: payload.subject ?? null,
    snippet: payload.snippet ?? null,
    messageId: payload.messageId ?? null,
    raw: payload.raw ?? null,
    webhookSource: payload.webhookSource ?? null,
    metadata: payload.metadata ?? null,
  };

  if (payload.campaignId) doc.campaignId = tryParseObjectId(payload.campaignId);
  if (payload.contactId) doc.contactId = tryParseObjectId(payload.contactId);

  const r = await db.collection('replies').insertOne(doc);
  return r.insertedId;
}

/**
 * Return true if at least one reply exists matching campaignId+contactId.
 * If campaignId omitted, matches by contactId only.
 */
export async function hasReplyForCampaignContact(campaignId?: string | ObjectId, contactId?: string | ObjectId) {
  if (!campaignId && !contactId) return false;
  const client = await clientPromise;
  const db = client.db('PlatformData');

  const filter: any = {};
  if (campaignId) filter.campaignId = tryParseObjectId(campaignId);
  if (contactId) filter.contactId = tryParseObjectId(contactId);

  try {
    const doc = await db.collection('replies').findOne(filter, { projection: { _id: 1 } });
    return !!doc;
  } catch (e) {
    console.warn('hasReplyForCampaignContact error', e);
    return false;
  }
}

/**
 * Fetch recent replies for a contact (optionally filtered by campaign).
 */
export async function getRepliesForContact(contactId: string | ObjectId, opts?: { limit?: number, campaignId?: string | ObjectId }) {
  const client = await clientPromise;
  const db = client.db('PlatformData');
  const filter: any = { contactId: tryParseObjectId(contactId) };
  if (opts?.campaignId) filter.campaignId = tryParseObjectId(opts.campaignId);
  const limit = opts?.limit ?? 50;
  const rows = await db.collection('replies').find(filter).sort({ inboundAt: -1 }).limit(limit).toArray();
  return rows;
}
