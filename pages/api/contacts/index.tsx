import clientPromise from '../../../lib/mongo';

export default async function handler(req, res) {
  const client = await clientPromise;
  const db = client.db('PlatformData');

  if (req.method === 'GET') {
    const contacts = await db
      .collection('contacts')
      .find({})
      .toArray();

    // Ensure segments is always an array
    const normalized = contacts.map(c => ({
      ...c,
      segments: Array.isArray(c.segments) ? c.segments : [],
    }));

    res.status(200).json(normalized);
  } else if (req.method === 'POST') {
    const contact = req.body;
    // Ensure segments exists
    contact.segments = Array.isArray(contact.segments) ? contact.segments : [];
    try {
      const result = await db.collection('contacts').insertOne(contact);
      res.status(201).json({ ...contact, _id: result.insertedId });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  } else {
    res.status(405).json({ error: 'Method not allowed' });
  }
}
