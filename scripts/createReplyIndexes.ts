// scripts/createReplyIndexes.ts
import clientPromise from '../lib/mongo';

async function run() {
  const client = await clientPromise;
  const db = client.db('PlatformData');

  console.log('Creating indexes for replies collection...');
  try {
    await db.collection('replies').createIndexes([
      { key: { campaignId: 1, contactId: 1, inboundAt: -1 }, name: 'campaign_contact_inboundAt' },
      { key: { contactId: 1, inboundAt: -1 }, name: 'contact_inboundAt' },
      { key: { messageId: 1 }, name: 'messageId_idx', sparse: true },
      { key: { inboundAt: -1 }, name: 'inboundAt_idx' },
    ]);
    console.log('Replies indexes created');
  } catch (e) {
    console.error('Failed to create replies indexes', e);
    process.exit(1);
  } finally {
    try { await client.close(); } catch {}
  }
}

run().catch((e) => {
  console.error('Script failed', e);
  process.exit(1);
});
