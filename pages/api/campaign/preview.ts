import clientPromise from '../../../lib/mongo';

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).end();

  const { contacts } = req.body;
  if (!contacts || !['all', 'segment'].includes(contacts.type)) {
    return res.status(400).json({ error: 'Invalid contact selection' });
  }

  const client = await clientPromise;
  const db = client.db('PlatformData');

  const query =
    contacts.type === 'all'
      ? {}
      : { segments: contacts.value };

  const cursor = db.collection('contacts').find(query).project({ email: 1 });

  const all = await cursor.toArray();

  res.status(200).json({
    count: all.length,
    sample: all.slice(0, 10).map(c => c.email),
  });
}
