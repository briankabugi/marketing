// pages/api/track/open/[campaign]/[contact].ts
import type { NextApiRequest, NextApiResponse } from 'next';
import clientPromise from '../../../../../lib/mongo';
import { redis } from '../../../../../lib/redis';
import { ObjectId } from 'mongodb';

function tryParseObjectId(maybeId: any) {
  if (!maybeId) return null;
  if (typeof maybeId === 'object' && maybeId instanceof ObjectId) return maybeId;
  if (typeof maybeId === 'string' && /^[0-9a-fA-F]{24}$/.test(maybeId)) {
    try { return new ObjectId(maybeId); } catch { return null; }
  }
  return null;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { campaign, contact } = req.query;
  const trace = (req.query.t as string) || String(Date.now());

  try {
    if (!campaign || !contact) {
      // respond pixel anyway to avoid breaking images
      const pixel = Buffer.from('R0lGODlhAQABAIABAP///wAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==', 'base64');
      res.setHeader('Content-Type', 'image/gif');
      res.setHeader('Cache-Control', 'no-store');
      return res.status(200).end(pixel);
    }

    const campaignIdStr = String(campaign);
    const contactIdStr = String(contact);

    const campaignIdObj = tryParseObjectId(campaignIdStr);
    const contactIdObj = tryParseObjectId(contactIdStr);

    const client = await clientPromise;
    const db = client.db('PlatformData');

    const ua = (req.headers['user-agent'] as string) || '';
    const ip =
      (req.headers['x-forwarded-for'] as string)?.split(',')[0] ||
      req.socket.remoteAddress ||
      null;
    const now = new Date();

    // Insert event (campaign_events)
    try {
      const ev: any = {
        campaignId: campaignIdObj ?? campaignIdStr,
        contactId: contactIdObj ?? contactIdStr,
        type: 'open',
        ua,
        ip,
        createdAt: now,
        trace,
      };
      const r = await db.collection('campaign_events').insertOne(ev);
      console.log('[OPEN][TRACE:%s] campaign_events.insertedId=%s', trace, r.insertedId?.toString?.());
    } catch (e) {
      console.warn('[OPEN] failed to insert campaign_events', e);
    }

    // Update campaign_contacts: set openedAt and $max lastActivityAt (if present)
    try {
      const updateDoc: any = {
        $set: { openedAt: now },
        $max: { lastActivityAt: now },
      };

      // Try multiple matching strategies (ObjectId or string keys)
      let resUpdate = null;
      if (campaignIdObj && contactIdObj) {
        resUpdate = await db.collection('campaign_contacts').updateOne({ campaignId: campaignIdObj, contactId: contactIdObj }, updateDoc);
      }
      if ((!resUpdate || resUpdate.matchedCount === 0) && campaignIdObj) {
        resUpdate = await db.collection('campaign_contacts').updateOne({ campaignId: campaignIdObj, contactId: contactIdStr }, updateDoc);
      }
      if ((!resUpdate || resUpdate.matchedCount === 0)) {
        resUpdate = await db.collection('campaign_contacts').updateOne({ campaignId: campaignIdStr, contactId: contactIdStr }, updateDoc);
      }

      if (!resUpdate || resUpdate.matchedCount === 0) {
        console.warn('[OPEN] campaign_contacts no match for', { campaignIdStr, contactIdStr });
      } else {
        // optionally log modifiedCount
      }
    } catch (e) {
      console.warn('[OPEN] campaign_contacts update failed', e);
    }

    // Redis counters & publish to contact_update channel (UI listens to this channel)
    try {
      await redis.hincrby(`campaign:${campaignIdStr}:metrics`, 'opens', 1);
      const payload = { campaignId: campaignIdStr, contactId: contactIdStr, event: 'open', openedAt: now.toISOString(), trace, ts: now.toISOString() };
      await redis.publish(`campaign:${campaignIdStr}:contact_update`, JSON.stringify(payload));
      console.log('[OPEN] published contact_update', payload);
    } catch (e) {
      console.warn('[OPEN] redis publish failed', e);
    }

    // Return 1x1 pixel
    const pixel = Buffer.from('R0lGODlhAQABAIABAP///wAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==', 'base64');
    res.setHeader('Content-Type', 'image/gif');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0, s-maxage=0');
    return res.status(200).end(pixel);
  } catch (err) {
    console.error('[OPEN] unexpected', err);
    const pixel = Buffer.from('R0lGODlhAQABAIABAP///wAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==', 'base64');
    res.setHeader('Content-Type', 'image/gif');
    res.setHeader('Cache-Control', 'no-store');
    return res.status(200).end(pixel);
  }
}
