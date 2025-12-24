// pages/contacts.tsx
'use client';
import { useEffect, useState } from 'react';
import ContactsTable from '../components/ContactsTable';
import ContactModal from '../components/ContactModal';
import ImportModal from '../components/ImportModal';
import Link from 'next/link';

export default function ContactsPage() {
  const [contacts, setContacts] = useState([]);
  const [adding, setAdding] = useState(false);
  const [importing, setImporting] = useState(false);

  const fetchContacts = async () => {
    const res = await fetch('/api/contacts');
    const data = await res.json();
    setContacts(data);
  };

  useEffect(() => { fetchContacts(); }, []);

  return (
    <div className="p-4">
      <h1 className="text-2xl font-bold mb-4">Contacts</h1>
      <Link href="/" className='mb-4'>Back to HomePage</Link>
      <div className="flex gap-2 mb-4 mt-5">
        <button onClick={() => setAdding(true)} className="bg-green-500 text-white px-3 py-1 rounded">
          Add Contact
        </button>
        <button onClick={() => setImporting(true)} className="bg-purple-500 text-white px-3 py-1 rounded">
          Import Contacts
        </button>
      </div>

      {adding && (
        <ContactModal
          isOpen={adding}
          onSave={async (data) => {
            await fetch('/api/contacts', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(data),
            });
            setAdding(false);
            fetchContacts();
          }}
          onClose={() => setAdding(false)}
        />
      )}

      {importing && (
        <ImportModal
          isOpen={importing}
          onClose={() => setImporting(false)}
          refresh={fetchContacts}
        />
      )}

      <ContactsTable contacts={contacts} refresh={fetchContacts} />
    </div>
  );
}
