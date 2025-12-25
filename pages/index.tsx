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

type AttachmentEntry = {
  name: string;
  url: string;
  contentType?: string | null;
  size?: number;
  source?: 'url' | 'path' | 'content';
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

  // attachments state for the new campaign
  const [attachments, setAttachments] = useState<AttachmentEntry[]>([]);
  const [uploading, setUploading] = useState(false);
  const [urlToAdd, setUrlToAdd] = useState('');
  const [urlName, setUrlName] = useState('');

  async function loadCampaigns() {
    try {
      const res = await fetch('/api/campaign/list');
      const data = await res.json();
      setCampaigns(data.campaigns || []);
    } catch (e) {
      console.error('Failed to load campaigns', e);
    } finally {
      setLoading(false);
    }
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
      initial: { subject, body, attachments },
      followUps: [],
    };

    const res = await fetch('/api/campaign/preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => null);
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

    // normalize attachments to include "source" so server validation passes
    const normalized = attachments.map(a => ({
      name: a.name,
      source: a.source ?? 'url',
      url: a.url,
      contentType: a.contentType ?? undefined,
      // note: start.ts will accept url/path/content per its validator
    }));

    const payload = {
      name,
      contacts: contactType === 'all' ? { type: 'all' } : { type: 'segment', value: segment },
      initial: { subject, body, attachments: normalized },
      followUps: [], // extend later with UI for follow-ups
    };

    const res = await fetch('/api/campaign/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => null);
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
    setAttachments([]);
    loadCampaigns();
    loadSegments();
  }

  // Helper: upload a file to server (base64)
  async function uploadFile(file: File) {
    setUploading(true);
    try {
      const reader = new FileReader();
      const p = new Promise<{ url: string; filename: string; contentType?: string; size?: number }>((resolve, reject) => {
        reader.onload = async () => {
          try {
            const dataUrl = reader.result as string;
            const base64 = dataUrl.split(',')[1] ?? dataUrl;

            const payload = {
              filename: file.name,
              content: base64,
              contentType: file.type || null,
            };

            const res = await fetch('/api/uploads', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(payload),
            });

            if (!res.ok) {
              const errText = await res.text().catch(() => null);
              let errJson = null;
              try { errJson = JSON.parse(errText || ''); } catch {}
              return reject(new Error(errJson?.error || errText || res.statusText || 'upload failed'));
            }

            const data = await res.json();
            resolve({ url: data.url, filename: data.filename, contentType: data.contentType, size: data.size });
          } catch (e) {
            reject(e);
          }
        };
        reader.onerror = () => reject(new Error('File read error'));
        reader.readAsDataURL(file);
      });

      const result = await p;
      // add to attachments; mark source as 'url' because uploaded files are hosted under /uploads
      setAttachments((cur) => [...cur, { name: file.name, url: result.url, contentType: result.contentType ?? null, size: result.size, source: 'url' }]);
    } catch (e: any) {
      console.error('Upload failed', e);
      alert('Upload failed: ' + (e?.message || String(e)));
    } finally {
      setUploading(false);
    }
  }

  function handleFileInput(e: React.ChangeEvent<HTMLInputElement>) {
    const files = e.target.files;
    if (!files || files.length === 0) return;
    // upload each file sequentially to limit concurrency
    (async () => {
      for (let i = 0; i < files.length; i++) {
        await uploadFile(files[i]);
      }
    })();
    // reset input
    e.currentTarget.value = '';
  }

  function removeAttachment(idx: number) {
    setAttachments((cur) => {
      const copy = [...cur];
      copy.splice(idx, 1);
      return copy;
    });
  }

  // Add an external URL as an attachment (source = 'url')
  function isValidUrl(u: string) {
    try {
      const parsed = new URL(u);
      return parsed.protocol === 'http:' || parsed.protocol === 'https:';
    } catch {
      return false;
    }
  }

  function addUrlAttachment() {
    if (!urlToAdd) {
      alert('Paste a URL first');
      return;
    }
    if (!isValidUrl(urlToAdd.trim())) {
      alert('Invalid URL (must be http or https)');
      return;
    }
    const name = urlName?.trim() || new URL(urlToAdd).pathname.split('/').filter(Boolean).pop() || urlToAdd;
    setAttachments((cur) => [...cur, { name, url: urlToAdd.trim(), contentType: null, source: 'url' }]);
    setUrlToAdd('');
    setUrlName('');
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

          <div style={{ marginTop: 8 }}>
            <div style={{ marginBottom: 6, fontWeight: 600 }}>Attachments</div>

            <div style={{ marginBottom: 8 }}>
              <input type="file" multiple onChange={handleFileInput} />
              <div style={{ marginTop: 6, fontSize: 13, color: '#666' }}>
                Files uploaded are stored under <code>/public/uploads</code> and included as URL attachments.
              </div>
            </div>

            <div style={{ marginBottom: 8 }}>
              <div style={{ marginBottom: 6, fontSize: 13 }}>Or add a remote URL</div>
              <input
                placeholder="https://example.com/file.pdf"
                value={urlToAdd}
                onChange={(e) => setUrlToAdd(e.target.value)}
                style={{ width: '100%', marginBottom: 6 }}
              />
              <input
                placeholder="Optional display filename (e.g. brochure.pdf)"
                value={urlName}
                onChange={(e) => setUrlName(e.target.value)}
                style={{ width: '100%', marginBottom: 6 }}
              />
              <div style={{ display: 'flex', gap: 8 }}>
                <button onClick={addUrlAttachment}>Add URL attachment</button>
              </div>
            </div>

            <div style={{ marginTop: 8 }}>
              {uploading && <div style={{ fontSize: 13 }}>Uploading…</div>}
              {attachments.length === 0 && <div style={{ fontSize: 13, color: '#666' }}>No attachments</div>}
              {attachments.length > 0 && (
                <ul>
                  {attachments.map((a, idx) => (
                    <li key={a.url + idx} style={{ marginBottom: 6 }}>
                      <a href={a.url} target="_blank" rel="noreferrer">{a.name}</a>
                      {a.size ? <span style={{ marginLeft: 8, color: '#666', fontSize: 13 }}>{Math.round((a.size||0)/1024)} KB</span> : null}
                      <button style={{ marginLeft: 8 }} onClick={() => removeAttachment(idx)}>Remove</button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>

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
