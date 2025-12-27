import type { NextApiRequest, NextApiResponse } from 'next';
import clientPromise from '../../../lib/mongo';
import crypto from 'crypto';

/**
 * Zoho → Webhook → This endpoint
 *
 * Expected JSON:
 * {
 *   "from": "Jane <jane@company.com>",
 *   "to": "sales+CAMPAIGNID+CONTACTID@yourdomain.com",
 *   "subject": "Re: Hello",
 *   "text": "...",
 *   "html": "<p>...</p>",
 *   "messageId": "<abcd@zoho.com>"
 * }
 */

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    if (req.method !== 'POST') {
      return res.status(405).json({ error: 'Method not allowed' });
    }

    /* -------------------------------
       1) Authenticate Zoho
    -------------------------------- */
    const secret = req.headers['x-inbound-secret'];
    if (!secret || secret !== process.env.INBOUND_WEBHOOK_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const {
      from,
      to,
      subject,
      text,
      html,
      messageId,
    } = req.body || {};

    if (!to || !from) {
      return res.status(400).json({ error: 'Missing to/from' });
    }

    /* -------------------------------
       2) Extract campaign + contact
       from plus-addressing
       local+campaignId+contactId@domain
    -------------------------------- */
    const toAddress = extractEmail(to);
    const plusMatch = toAddress.match(/^([^+]+)\+([^+]+)\+([^@]+)@/);

    if (!plusMatch) {
      console.warn('Inbound email missing plus-address:', toAddress);
      return res.status(200).json({ ignored: true });
    }

    const campaignId = decodeURIComponent(plusMatch[2]);
    const contactId = decodeURIComponent(plusMatch[3]);

    /* -------------------------------
       3) Generate idempotency key
    -------------------------------- */
    const fingerprint =
      messageId ||
      crypto
        .createHash('sha256')
        .update(`${from}|${to}|${subject}|${text || ''}`)
        .digest('hex');

    const client = await clientPromise;
    const db = client.db('PlatformData');

    /* -------------------------------
       4) Prevent duplicate replies
    -------------------------------- */
    const existing = await db.collection('replies').findOne({ fingerprint });
    if (existing) {
      return res.status(200).json({ duplicate: true });
    }

    const now = new Date();

    /* -------------------------------
       5) Store reply
    -------------------------------- */
    await db.collection('replies').insertOne({
      fingerprint,
      campaignId,
      contactId,
      from,
      to: toAddress,
      subject,
      text,
      html,
      messageId,
      receivedAt: now,
    });

    /* -------------------------------
       6) Log campaign event
    -------------------------------- */
    await db.collection('campaign_events').insertOne({
      campaignId,
      contactId,
      type: 'reply',
      from,
      subject,
      receivedAt: now,
    });

    /* -------------------------------
       7) Mark contact as replied
    -------------------------------- */
    await db.collection('campaign_contacts').updateOne(
      { campaignId, contactId },
      {
        $set: {
          replied: true,
          repliedAt: now,
          lastReplySnippet: (text || html || '').slice(0, 500),
        },
      }
    );

    return res.status(200).json({ success: true });
  } catch (err) {
    console.error('Inbound reply error:', err);
    return res.status(500).json({ error: 'Internal error' });
  }
}

/* -----------------------------------
   Helpers
------------------------------------ */

function extractEmail(raw: string): string {
  // "Jane <jane@x.com>" → jane@x.com
  const match = raw.match(/<([^>]+)>/);
  return (match ? match[1] : raw).trim().toLowerCase();
}
