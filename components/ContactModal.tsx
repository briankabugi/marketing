'use client';
import { useEffect, useState } from 'react';

type Contact = {
  name: string;
  email: string;
  phone?: string;
  whatsapp?: string;
  location?: string;
  segments: string[];
};

type ContactModalProps = {
  contact?: Contact;
  isOpen: boolean;
  onSave: (data: Contact) => void;
  onClose: () => void;
};

export default function ContactModal({ contact, isOpen, onSave, onClose }: ContactModalProps) {
  const [formData, setFormData] = useState<Contact>({
    name: '',
    email: '',
    phone: '',
    whatsapp: '',
    location: '',
    segments: [],
  });

  useEffect(() => {
    if (contact) setFormData(contact);
  }, [contact]);

  if (!isOpen) return null;

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>
  ) => {
    const { name, value } = e.target;

    setFormData(prev => ({
      ...prev,
      [name]: name === 'segments' ? value.split(',').map(s => s.trim()) : value,
    }));
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSave(formData);
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-6 w-96">
        <h2 className="text-xl font-bold mb-4">{contact ? 'Edit Contact' : 'Add Contact'}</h2>
        <form className="flex flex-col gap-2" onSubmit={handleSubmit}>
          <input
            name="name"
            value={formData.name}
            onChange={handleChange}
            placeholder="Name"
            className="border px-2 py-1 rounded"
            required
          />
          <input
            name="email"
            value={formData.email}
            onChange={handleChange}
            placeholder="Email"
            className="border px-2 py-1 rounded"
            required
          />
          <input
            name="phone"
            value={formData.phone}
            onChange={handleChange}
            placeholder="Phone"
            className="border px-2 py-1 rounded"
          />
          <input
            name="whatsapp"
            value={formData.whatsapp}
            onChange={handleChange}
            placeholder="WhatsApp"
            className="border px-2 py-1 rounded"
          />
          <input
            name="location"
            value={formData.location}
            onChange={handleChange}
            placeholder="Location"
            className="border px-2 py-1 rounded"
          />
          <input
            name="segments"
            value={formData.segments.join(', ')}
            onChange={handleChange}
            placeholder="Segments (comma separated)"
            className="border px-2 py-1 rounded"
          />
          <div className="flex justify-end gap-2 mt-2">
            <button type="button" onClick={onClose} className="px-3 py-1 rounded bg-gray-300">
              Cancel
            </button>
            <button type="submit" className="px-3 py-1 rounded bg-blue-500 text-white">
              Save
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}