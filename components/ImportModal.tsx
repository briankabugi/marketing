import { useState } from 'react';

export default function ImportModal({ isOpen, onClose, refresh }: { isOpen: boolean; onClose: () => void; refresh: () => void }) {
  const [step, setStep] = useState<'idle' | 'validating' | 'preview' | 'uploading' | 'done'>('idle');
  const [validatedContacts, setValidatedContacts] = useState<any[]>([]);
  const [summary, setSummary] = useState<{ total: number; valid: number; rejected: number } | null>(null);
  const [fileName, setFileName] = useState<string | null>(null);

  if (!isOpen) return null;

  const handleFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files?.length) return;
    const file = e.target.files[0];
    setFileName(file.name);
    setStep('validating');

    const formData = new FormData();
    formData.append('file', file);

    try {
      const res = await fetch('/api/contacts/import', { method: 'POST', body: formData });
      if (!res.ok) throw new Error('Validation failed');
      const data = await res.json();
      setValidatedContacts(data.contacts);
      setSummary({ total: data.total, valid: data.valid, rejected: data.rejected });
      setStep('preview');
    } catch (err: any) {
      alert(err.message);
      setStep('idle');
    }
  };

  const handleUpload = async () => {
    if (!validatedContacts.length) return;
    setStep('uploading');
    try {
      const res = await fetch('/api/contacts/upload', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(validatedContacts),
      });
      if (!res.ok) throw new Error('Upload failed');
      setStep('done');
      refresh();
    } catch (err: any) {
      alert(err.message);
      setStep('preview');
    }
  };

  const resetModal = () => {
    setStep('idle');
    setValidatedContacts([]);
    setSummary(null);
    setFileName(null);
    onClose();
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-6 w-96 max-h-[90vh] overflow-auto">
        <h2 className="text-xl font-bold mb-4">Import Contacts</h2>

        {step === 'idle' && (
          <div className="flex flex-col gap-2">
            <label className="bg-purple-500 text-white px-3 py-1 rounded cursor-pointer text-center">
              Select Excel/CSV File
              <input type="file" accept=".csv,.xlsx,.xls" onChange={handleFile} className="hidden" />
            </label>
          </div>
        )}

        {step === 'validating' && (
          <p className="text-center text-gray-600">Validating {fileName}...</p>
        )}

        {step === 'preview' && summary && (
          <div>
            <p>Total rows: {summary.total}</p>
            <p>Valid: {summary.valid}</p>
            <p>Rejected: {summary.rejected}</p>

            <h3 className="mt-2 font-semibold">Preview (first 10):</h3>
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

            <div className="flex justify-end gap-2 mt-2">
              <button onClick={resetModal} className="px-3 py-1 rounded bg-gray-300">Cancel</button>
              <button onClick={handleUpload} className="px-3 py-1 rounded bg-green-500 text-white">
                Upload {summary.valid} Contacts
              </button>
            </div>
          </div>
        )}

        {step === 'uploading' && <p className="text-center text-gray-600">Uploading contacts...</p>}
        {step === 'done' && (
          <div className="text-center">
            <p className="text-green-600 font-semibold">Contacts uploaded successfully!</p>
            <button onClick={resetModal} className="mt-2 px-3 py-1 rounded bg-blue-500 text-white">Close</button>
          </div>
        )}
      </div>
    </div>
  );
}
