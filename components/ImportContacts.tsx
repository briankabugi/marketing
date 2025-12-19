'use client';
import { useState } from 'react';

export default function ImportContacts({ refresh }: { refresh: () => void }) {
  const [validatedContacts, setValidatedContacts] = useState<any[]>([]);
  const [summary, setSummary] = useState<{ total: number; valid: number; rejected: number } | null>(null);

  const handleFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files?.length) return;
    const file = e.target.files[0];

    const formData = new FormData();
    formData.append('file', file);

    // Step 1: Validate
    const res = await fetch('/api/contacts/import', { method: 'POST', body: formData });
    if (!res.ok) {
      const err = await res.json();
      return alert('Validation failed: ' + err.error);
    }

    const data = await res.json();
    setValidatedContacts(data.contacts);
    setSummary({ total: data.total, valid: data.valid, rejected: data.rejected });
  };

  const handleUpload = async () => {
    if (!validatedContacts.length) return;
    const res = await fetch('/api/contacts/upload', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(validatedContacts),
    });

    if (res.ok) {
      alert(`Uploaded ${validatedContacts.length} contacts successfully`);
      setValidatedContacts([]);
      setSummary(null);
      refresh();
    } else {
      const err = await res.json();
      alert('Upload failed: ' + err.error);
    }
  };

  return (
    <div className="mb-4">
      <label className="bg-purple-500 text-white px-3 py-1 rounded cursor-pointer inline-block mb-2">
        Import Excel/CSV
        <input type="file" accept=".csv,.xlsx,.xls" onChange={handleFile} className="hidden" />
      </label>

      {summary && (
        <div className="p-3 border rounded bg-gray-100">
          <p>Total rows: {summary.total}</p>
          <p>Valid: {summary.valid}</p>
          <p>Rejected: {summary.rejected}</p>

          <h3 className="mt-2 font-semibold">Preview:</h3>
          <div className="max-h-64 overflow-auto border mt-1">
            <table className="table-auto border-collapse border w-full text-sm">
              <thead>
                <tr>
                  <th className="border px-2">Name</th>
                  <th className="border px-2">Email</th>
                  <th className="border px-2">Phone</th>
                  <th className="border px-2">WhatsApp</th>
                  <th className="border px-2">Location</th>
                  <th className="border px-2">Segments</th>
                </tr>
              </thead>
              <tbody>
                {validatedContacts.slice(0, 10).map((c, i) => (
                  <tr key={i}>
                    <td className="border px-2">{c.name}</td>
                    <td className="border px-2">{c.email}</td>
                    <td className="border px-2">{c.phone}</td>
                    <td className="border px-2">{c.whatsapp}</td>
                    <td className="border px-2">{c.location}</td>
                    <td className="border px-2">{c.segments?.join(' / ')}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {validatedContacts.length > 10 && (
              <p className="text-xs mt-1 text-gray-500">Showing first 10 contacts...</p>
            )}
          </div>

          <button
            onClick={handleUpload}
            className="bg-green-500 text-white px-3 py-1 rounded mt-2"
          >
            Upload {summary.valid} valid contacts
          </button>
        </div>
      )}
    </div>
  );
}
