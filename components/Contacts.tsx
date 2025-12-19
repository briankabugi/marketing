import Link from 'next/link';
import { useEffect, useState } from 'react';

type Contact = {
  _id: string;
  name?: string;
  email: string;
  company?: string;
};

export default function Contacts() {
  const [contacts, setContacts] = useState<Contact[]>([]);
  const [email, setEmail] = useState('');
  const [name, setName] = useState('');

  async function load() {
    const res = await fetch('/api/contacts');
    setContacts(await res.json());
  }

  async function addContact() {
    await fetch('/api/contacts', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, name }),
    });
    setEmail('');
    setName('');
    load();
  }

  async function remove(id: string) {
    await fetch(`/api/contacts/${id}`, { method: 'DELETE' });
    load();
  }

  useEffect(() => {
    load();
  }, []);

  return (
    <div>
      <div className='flex flex-row justify-between items-center mb-4'>
        <h2>Contacts</h2>
        <Link href="/contacts">See all</Link>
      </div>
      <div style={{ marginBottom: 12 }}>
        <input
          placeholder="Name"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <input
          placeholder="Email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
        />
        <button onClick={addContact}>Add</button>
      </div>

      {contacts.map((c) => {

        return (
          <div key={c._id} style={{ borderBottom: '1px solid #eee', padding: 6 }}>
            {c.name} — {c.email}
            <button onClick={() => remove(c._id)}>Delete</button>
          </div>
        )
      })}
    </div>
  );
}
