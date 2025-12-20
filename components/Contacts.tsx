import Link from 'next/link';
import { useEffect, useState } from 'react';

type Contact = {
  id: string;
  email: string;
  segments?: string[];
};

export default function Contacts() {
  const [contacts, setContacts] = useState<Contact[]>([]);
  const [email, setEmail] = useState('');
  const [segmentsInput, setSegmentsInput] = useState(''); // comma separated
  const [loading, setLoading] = useState(true);

  async function loadContacts() {
    setLoading(true);
    try {
      const res = await fetch('/api/contacts'); // updated URL
      const data = await res.json();
      // Map _id → id for frontend
      const formatted = data.map((c: any) => ({
        id: c._id,
        email: c.email,
        segments: Array.isArray(c.segments) ? c.segments : [],
      }));
      setContacts(formatted);
    } catch (err) {
      console.error(err);
      setContacts([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadContacts();
  }, []);

  async function createContact() {
    if (!email) {
      alert('Email required');
      return;
    }

    const segments = segmentsInput
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);

    try {
      const res = await fetch('/api/contacts', { // updated URL
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, segments }),
      });

      if (!res.ok) {
        const err = await res.json();
        alert('Failed: ' + (err?.error ?? res.statusText));
        return;
      }

      setEmail('');
      setSegmentsInput('');
      loadContacts();
    } catch (err) {
      console.error(err);
      alert('Failed to create contact');
    }
  }

  return (
    <div style={{ marginTop: 24, marginBottom: 24 }}>
      <h2>Contacts</h2>
      <Link href="/contacts" className='mb-4'>See all Contacts</Link>
      <div style={{ marginBottom: 8 }}>
        <input
          placeholder="Email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          style={{ width: 300, marginRight: 8 }}
        />
        <input
          placeholder="Segments (comma separated e.g. leads,beta)"
          value={segmentsInput}
          onChange={(e) => setSegmentsInput(e.target.value)}
          style={{ width: 360, marginRight: 8 }}
        />
        <button onClick={createContact}>Create</button>
      </div>

      {loading ? (
        <div>Loading contacts…</div>
      ) : (
        <div style={{ maxWidth: 760 }}>
          {contacts.length === 0 && <div>No contacts yet.</div>}
          {contacts.map((c) => (
            <div key={c.id} style={{ borderBottom: '1px solid #eee', padding: 8 }}>
              <div><strong>{c.email}</strong></div>
              <div style={{ fontSize: 13, color: '#666' }}>
                Segments: {(c.segments || []).join(', ')}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
