import 'dotenv/config';
import clientPromise from '../lib/mongo';

async function run() {
  const client = await clientPromise;
  const db = client.db('PlatformData');

  const col = db.collection('campaign_events');

  console.log('Ensuring indexes on campaign_events...');

  // For campaign analytics: open & click timelines
  await col.createIndex(
    { campaignId: 1, type: 1, createdAt: 1 },
    { name: 'campaign_type_time' }
  );

  // For per-contact analytics (did this person open / click?)
  await col.createIndex(
    { campaignId: 1, contactId: 1, type: 1 },
    { name: 'campaign_contact_type' }
  );

  // For fast campaign dashboards (all events sorted by time)
  await col.createIndex(
    { campaignId: 1, createdAt: -1 },
    { name: 'campaign_time_desc' }
  );

  // Optional: for IP abuse / debugging
  await col.createIndex(
    { ip: 1, createdAt: -1 },
    { name: 'ip_time', sparse: true }
  );

  console.log('Indexes ensured for campaign_events');

  const indexes = await col.indexes();
  console.table(indexes.map(i => ({
    name: i.name,
    key: JSON.stringify(i.key),
  })));

  process.exit(0);
}

run().catch(err => {
  console.error('Index creation failed', err);
  process.exit(1);
});
