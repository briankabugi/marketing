// pages/api/track/click/[campaign]/[contact].ts
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

function safeLog(...args: any[]) {
  try { console.log(...args); } catch {}
}

/**
 * Try multiple ways to decode the `u` parameter to the real destination URL.
 * We encoded with: encodeURIComponent(Buffer.from(url).toString('base64'))
 * But email clients / proxies may double-encode, rewrite, or pass the raw URL.
 */
function decodeDestination(uRaw?: string | string[] | undefined): string | null {
  if (!uRaw) return null;
  const u = Array.isArray(uRaw) ? uRaw[0] : uRaw;

  // heuristic: if it already looks like an http(s) URL, return raw
  if (/^https?:\/\//i.test(u)) return u;

  // Try decodeURIComponent then base64
  const attempts: string[] = [];

  try {
    attempts.push(decodeURIComponent(u));
  } catch (e) {
    // skip
  }
  attempts.push(u);

  // Also unescape plus/space issues
  attempts.push(u.replace(/\s/g, ''));
  attempts.push(u.replace(/\+/g, ' '));

  for (const a of attempts) {
    if (!a) continue;
    try {
      // Clean possible double-encoding (strip surrounding quotes)
      const cleaned = String(a).trim().replace(/^"+|"+$/g, '').replace(/^'+|'+$/g, '');
      // if looks like base64, try decode
      // base64 should only contain A-Za-z0-9+/= but URL-safe variants may appear
      const candidate = cleaned.replace(/ /g, '+'); // sometimes spaces replace +
      const buf = Buffer.from(candidate, 'base64');
      const str = buf.toString('utf8');
      if (str && /^https?:\/\//i.test(str)) return str;
    } catch (e) {
      // try next
    }
  }

  // Last-chance: maybe it's plain URL but percent-encoded differently
  try {
    const asDecoded = decodeURIComponent(String(u));
    if (/^https?:\/\//i.test(asDecoded)) return asDecoded;
  } catch (e) {}

  return null;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { campaign, contact } = req.query;
  const uRaw = req.query.u as string | undefined;
  const trace = (req.query.t as string) || String(Date.now());

  safeLog('[CLICK] incoming request', { url: req.url, query: req.query, headers: {
    'user-agent': req.headers['user-agent'],
    referer: req.headers.referer || req.headers.referrer,
    host: req.headers.host,
    'x-forwarded-for': req.headers['x-forwarded-for'],
  } });

  try {
    if (!campaign || !contact) {
      safeLog('[CLICK] missing campaign/contact path params');
      return res.status(400).send('Missing params');
    }

    const campaignIdStr = String(campaign);
    const contactIdStr = String(contact);
    const campaignIdObj = tryParseObjectId(campaignIdStr);
    const contactIdObj = tryParseObjectId(contactIdStr);

    const dest = decodeDestination(uRaw) ?? (() => {
      // fallback: maybe u contains a raw url despite no base64
      const fallback = Array.isArray(uRaw) ? uRaw[0] : uRaw;
      return (typeof fallback === 'string' && /^https?:\/\//i.test(fallback)) ? fallback : null;
    })();

    if (!dest) {
      safeLog('[CLICK] failed to decode destination', { uRaw });
      // Log an event so we can inspect
      try {
        const client = await clientPromise;
        const db = client.db('PlatformData');
        await db.collection('campaign_events').insertOne({
          campaignId: campaignIdObj ?? campaignIdStr,
          contactId: contactIdObj ?? contactIdStr,
          type: 'click',
          url: null,
          note: 'decode_failed',
          raw_u: uRaw,
          trace,
          createdAt: new Date(),
        });
      } catch (e) {
        safeLog('[CLICK] failed recording decode-failed event', e);
      }
      return res.status(400).send('Invalid destination');
    }

    // Persist event + update ledger + redis + publish
    const client = await clientPromise;
    const db = client.db('PlatformData');

    const ua = (req.headers['user-agent'] as string) || '';
    const ip = (req.headers['x-forwarded-for'] as string)?.split(',')[0] || req.socket.remoteAddress || null;
    const now = new Date();

    try {
      const ev: any = {
        campaignId: campaignIdObj ?? campaignIdStr,
        contactId: contactIdObj ?? contactIdStr,
        type: 'click',
        url: dest,
        ua,
        ip,
        createdAt: now,
        trace,
      };
      const r = await db.collection('campaign_events').insertOne(ev);
      safeLog('[CLICK] campaign_events.insertOne ->', r.insertedId?.toString?.());
    } catch (e) {
      safeLog('[CLICK] failed inserting campaign_events', e);
    }

    // Update campaign_contacts: lastClickAt and lastActivityAt (use $max to avoid path conflict)
    try {
      const updateDoc: any = { $set: { lastClickAt: now }, $max: { lastActivityAt: now } };

      let resUpdate = null;
      if (campaignIdObj && contactIdObj) {
        resUpdate = await db.collection('campaign_contacts').updateOne({ campaignId: campaignIdObj, contactId: contactIdObj }, updateDoc);
      }
      if ((!resUpdate || (resUpdate.matchedCount ?? 0) === 0) && campaignIdObj) {
        resUpdate = await db.collection('campaign_contacts').updateOne({ campaignId: campaignIdObj, contactId: contactIdStr }, updateDoc);
      }
      if ((!resUpdate || (resUpdate.matchedCount ?? 0) === 0)) {
        resUpdate = await db.collection('campaign_contacts').updateOne({ campaignId: campaignIdStr, contactId: contactIdStr }, updateDoc);
      }

      if (!resUpdate || (resUpdate.matchedCount ?? 0) === 0) {
        safeLog('[CLICK] campaign_contacts no match for', { campaignIdStr, contactIdStr });
      }
    } catch (e) {
      safeLog('[CLICK] campaign_contacts update failed', e);
    }

    // Redis counters and publish
    try {
      await redis.hincrby(`campaign:${campaignIdStr}:metrics`, 'clicks', 1);
      const payload = { campaignId: campaignIdStr, contactId: contactIdStr, event: 'click', url: dest, trace, ts: now.toISOString() };
      await redis.publish(`campaign:${campaignIdStr}:contact_update`, JSON.stringify(payload));
      safeLog('[CLICK] published contact_update', payload);
    } catch (e) {
      safeLog('[CLICK] redis publish failed', e);
    }

    // Redirect to dest
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate, max-age=0, s-maxage=0');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    return res.redirect(302, dest);
  } catch (err) {
    safeLog('[CLICK] unexpected error', err);
    return res.status(500).send('error');
  }
}
