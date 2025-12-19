import clientPromise from '../../../lib/mongo';

export default async function handler(req, res) {
  const client = await clientPromise;
  const db = client.db('PlatformData');

  const contacts = await db
    .collection('contacts')
    .find({ segments: { $exists: true, $ne: [] } })
    .project({ segments: 1 })
    .toArray();

  const set = new Set<string>();
  contacts.forEach(c => {
    (c.segments || []).forEach((s: string) => set.add(s));
  });

  res.status(200).json({ segments: Array.from(set).sort() });
}
