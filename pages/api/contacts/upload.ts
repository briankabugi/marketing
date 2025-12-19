import clientPromise from '../../../lib/mongo';

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const contacts = req.body;
  if (!Array.isArray(contacts) || !contacts.length)
    return res.status(400).json({ error: 'No contacts to upload' });

  try {
    const client = await clientPromise;
    const db = client.db('PlatformData');
    await db.collection('contacts').insertMany(contacts);
    res.status(200).json({ success: true, inserted: contacts.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
