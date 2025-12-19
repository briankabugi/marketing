'use client';
import { useState } from 'react';
import ContactModal from './ContactModal';

export default function ContactsTable({ contacts, refresh }) {
  const [modalOpen, setModalOpen] = useState(false);
  const [editingContact, setEditingContact] = useState<any>(null);

  const handleEdit = (contact: any) => {
    setEditingContact(contact);
    setModalOpen(true);
  };

  const handleSave = async (data: any) => {
    if (editingContact?._id) {
      const { _id, ...updateData } = data;
      await fetch(`/api/contacts/${editingContact._id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updateData),
      });
    } else {
      await fetch('/api/contacts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
    }
    setModalOpen(false);
    setEditingContact(null);
    refresh();
  };

  const handleDelete = async (id: string) => {
    if (!confirm('Delete this contact?')) return;
    await fetch(`/api/contacts/${id}`, { method: 'DELETE' });
    refresh();
  };

  return (
    <div>
      <ContactModal
        contact={editingContact}
        isOpen={modalOpen}
        onSave={handleSave}
        onClose={() => setModalOpen(false)}
      />
      <table className="table-auto border-collapse border border-gray-300 w-full mt-4">
        <thead>
          <tr>
            <th className="border px-2 py-1">Name</th>
            <th className="border px-2 py-1">Email</th>
            <th className="border px-2 py-1">Phone</th>
            <th className="border px-2 py-1">WhatsApp</th>
            <th className="border px-2 py-1">Location</th>
            <th className="border px-2 py-1">Segments</th>
            <th className="border px-2 py-1">Actions</th>
          </tr>
        </thead>
        <tbody>
          {contacts.map((c) => (
            <tr key={c._id}>
              <td className="border px-2 py-1">{c.name}</td>
              <td className="border px-2 py-1">{c.email}</td>
              <td className="border px-2 py-1">{c.phone}</td>
              <td className="border px-2 py-1">{c.whatsapp}</td>
              <td className="border px-2 py-1">{c.location}</td>
              <td className="border px-2 py-1">{c.segments?.join(' / ')}</td>
              <td className="border px-2 py-1 flex gap-2">
                <button onClick={() => handleEdit(c)} className="bg-yellow-400 px-2 py-1 rounded">Edit</button>
                <button onClick={() => handleDelete(c._id)} className="bg-red-500 text-white px-2 py-1 rounded">Delete</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
