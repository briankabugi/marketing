import clientPromise from '../../../lib/mongo';
import { ObjectId } from 'mongodb';

function normalizeSegments(input: any): string[] {
  if (!input) return [];
  if (Array.isArray(input)) return input.map(String).map(s => s.trim()).filter(Boolean);
  return String(input)
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
}

export default async function handler(req, res) {
  const client = await clientPromise;
  const db = client.db('PlatformData');
  const { id } = req.query;

  if (!id) return res.status(400).json({ error: 'No ID provided' });

  if (req.method === 'PUT') {
    const { _id, segments, ...updateData } = req.body;

    if (segments !== undefined) {
      updateData.segments = normalizeSegments(segments);
    }

    try {
      const result = await db.collection('contacts').updateOne(
        { _id: new ObjectId(id as string) },
        { $set: updateData }
      );

      if (result.matchedCount === 0)
        return res.status(404).json({ error: 'Contact not found' });

      res.status(200).json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  } else if (req.method === 'DELETE') {
    try {
      await db.collection('contacts').deleteOne({ _id: new ObjectId(id as string) });
      res.status(200).json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  } else {
    res.status(405).json({ error: 'Method not allowed' });
  }
}
