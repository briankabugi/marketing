// pages/index.tsx
import { useEffect, useState } from 'react';
import Contacts from '../components/Contacts';

type Campaign = {
  id: string;
  name: string;
  total: number;
  processed: number;
  status: string;
  createdAt: string;
};

export default function Dashboard() {
  const [campaigns, setCampaigns] = useState<Campaign[]>([]);
  const [loading, setLoading] = useState(true);

  // form state
  const [showForm, setShowForm] = useState(false);
  const [name, setName] = useState('');
  const [subject, setSubject] = useState('');
  const [body, setBody] = useState('');
  const [contactType, setContactType] = useState<'all' | 'segment'>('all');
  const [segment, setSegment] = useState('');
  const [segmentsList, setSegmentsList] = useState<string[]>([]);
  const [previewCount, setPreviewCount] = useState<number | null>(null);
  const [previewSample, setPreviewSample] = useState<string[]>([]);

  async function loadCampaigns() {
    const res = await fetch('/api/campaign/list');
    const data = await res.json();
    setCampaigns(data.campaigns || []);
    setLoading(false);
  }

  async function loadSegments() {
    try {
      const res = await fetch('/api/contacts/segments');
      const data = await res.json();
      setSegmentsList(data.segments || []);
    } catch (e) {
      console.error('Failed to load segments', e);
    }
  }

  useEffect(() => {
    loadCampaigns();
    loadSegments();
    const i = setInterval(loadCampaigns, 2000);
    return () => clearInterval(i);
  }, []);

  async function previewCampaign() {
    if (!name || !subject || !body) {
      alert('Please fill name, subject and body to preview.');
      return;
    }

    if (contactType === 'segment' && !segment) {
      alert('Please choose a segment to preview.');
      return;
    }

    const payload = {
      contacts: contactType === 'all' ? { type: 'all' } : { type: 'segment', value: segment },
      initial: { subject, body },
      followUps: [],
    };

    const res = await fetch('/api/campaign/preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const err = await res.json();
      alert('Preview failed: ' + (err?.error ?? res.statusText));
      return;
    }

    const data = await res.json();
    setPreviewCount(data.count);
    setPreviewSample(data.sample || []);
  }

  async function startCampaign() {
    if (!name || !subject || !body) {
      alert('Please fill name, subject and body.');
      return;
    }

    if (contactType === 'segment' && !segment) {
      alert('Please choose a segment.');
      return;
    }

    const payload = {
      name,
      contacts: contactType === 'all' ? { type: 'all' } : { type: 'segment', value: segment },
      initial: { subject, body },
      followUps: [], // extend later with UI for follow-ups
    };

    const res = await fetch('/api/campaign/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const err = await res.json();
      alert('Launch failed: ' + (err?.error ?? res.statusText));
      return;
    }

    // reset form and refresh
    setName('');
    setSubject('');
    setBody('');
    setContactType('all');
    setSegment('');
    setPreviewCount(null);
    setPreviewSample([]);
    setShowForm(false);
    loadCampaigns();
    loadSegments();
  }

  return (
    <div style={{ padding: 24, fontFamily: 'sans-serif' }}>
      <h1>Marketing MVP Dashboard</h1>

      <button onClick={() => setShowForm(!showForm)} style={{ marginBottom: 12 }}>
        + Start Campaign
      </button>

      {showForm && (
        <div
          style={{
            border: '1px solid #ccc',
            padding: 16,
            marginTop: 12,
            borderRadius: 6,
            maxWidth: 680,
          }}
        >
          <h3>New Campaign</h3>

          <input
            placeholder="Campaign name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            style={{ width: '100%', marginBottom: 8 }}
          />

          <div style={{ display: 'flex', gap: 8 }}>
            <input
              placeholder="Email subject"
              value={subject}
              onChange={(e) => setSubject(e.target.value)}
              style={{ flex: 1, marginBottom: 8 }}
            />
          </div>

          <textarea
            placeholder="Email body (HTML allowed)"
            value={body}
            onChange={(e) => setBody(e.target.value)}
            rows={6}
            style={{ width: '100%', marginBottom: 8 }}
          />

          <label style={{ display: 'block', marginBottom: 8 }}>
            Send to:
            <select
              value={contactType}
              onChange={(e) => setContactType(e.target.value as any)}
              style={{ width: '100%', marginTop: 6 }}
            >
              <option value="all">All contacts</option>
              <option value="segment">Specific segment</option>
            </select>
          </label>

          {contactType === 'segment' && (
            <div style={{ marginBottom: 8 }}>
              <label>
                Segment:
                <select
                  value={segment}
                  onChange={(e) => setSegment(e.target.value)}
                  style={{ width: '100%', marginTop: 6 }}
                >
                  <option value="">-- choose segment --</option>
                  {segmentsList.map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
              </label>
              <div style={{ marginTop: 6, fontSize: 13, color: '#666' }}>
                If the segment is not listed you can create contacts with the segment and it will appear here.
              </div>
            </div>
          )}

          <div style={{ display: 'flex', gap: 8, marginTop: 8 }}>
            <button onClick={previewCampaign}>Preview</button>
            <button onClick={startCampaign}>Launch Campaign</button>
            <button onClick={() => { setShowForm(false); setPreviewCount(null); setPreviewSample([]); }}>Cancel</button>
          </div>

          {previewCount !== null && (
            <div style={{ marginTop: 12, padding: 12, border: '1px dashed #ddd', borderRadius: 6 }}>
              <strong>Preview</strong>
              <div>Recipients: {previewCount}</div>
              {previewSample.length > 0 && (
                <div style={{ marginTop: 6 }}>
                  <div style={{ fontSize: 13, color: '#444' }}>Sample recipients:</div>
                  <ul>
                    {previewSample.map((e) => (
                      <li key={e}>{e}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      <Contacts />

      <h2>Running Campaigns</h2>

      {loading && <p>Loading campaigns…</p>}

      {campaigns.length === 0 && <p>No campaigns yet.</p>}

      {campaigns.map((c) => (
        <div
          key={c.id}
          onClick={() => (window.location.href = `/campaigns/${c.id}`)}
          style={{
            border: '1px solid #ddd',
            padding: 12,
            marginBottom: 10,
            borderRadius: 6,
            maxWidth: 680,
            cursor: 'pointer',
          }}
        >
          <strong>{c.name}</strong>
          <div>Status: {c.status}</div>
          <div>
            Progress: {c.processed} / {c.total}
          </div>
          <div>
            Created: {new Date(c.createdAt).toLocaleString()}
          </div>
        </div>
      ))}
    </div>
  );
}
