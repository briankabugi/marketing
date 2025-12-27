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

type FollowUpDraft = {
  id: string; // client-side id
  delayValue: number;
  delayUnit: 'minutes' | 'hours' | 'days';
  subject: string;
  body: string;
  attachments: AttachmentEntry[];
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

  // attachments state for the new campaign (initial)
  const [attachments, setAttachments] = useState<AttachmentEntry[]>([]);
  const [uploading, setUploading] = useState(false);
  const [urlToAdd, setUrlToAdd] = useState('');
  const [urlName, setUrlName] = useState('');

  // Follow-ups: dynamic editor
  const [followUps, setFollowUps] = useState<FollowUpDraft[]>([]);

  // convenience: create a fresh followup draft
  function newFollowUpDraft(): FollowUpDraft {
    return {
      id: String(Date.now()) + '-' + Math.floor(Math.random() * 100000),
      delayValue: 1,
      delayUnit: 'days',
      subject: '',
      body: '',
      attachments: [],
    };
  }

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

  // convert a delay value/unit to minutes
  function delayToMinutes(value: number, unit: 'minutes' | 'hours' | 'days') {
    const v = Number(value) || 0;
    if (v <= 0) return 0;
    if (unit === 'minutes') return Math.round(v);
    if (unit === 'hours') return Math.round(v * 60);
    return Math.round(v * 60 * 24);
  }

  // Preview campaign: call server preview endpoint
  async function previewCampaign() {
    if (!name || !subject || !body) {
      alert('Please fill name, subject and body to preview.');
      return;
    }

    if (contactType === 'segment' && !segment) {
      alert('Please choose a segment to preview.');
      return;
    }

    // prepare followups in the original model shape (top-level followUps with delayMinutes)
    const normalizedFollowUps = followUps.map(f => ({
      delayMinutes: delayToMinutes(f.delayValue, f.delayUnit),
      subject: f.subject,
      body: f.body,
      attachments: f.attachments.map(a => ({
        name: a.name,
        source: a.source ?? 'url',
        url: a.url,
        contentType: a.contentType ?? undefined,
      })),
    }));

    const payload = {
      name,
      contacts: contactType === 'all' ? { type: 'all' } : { type: 'segment', value: segment },
      initial: { subject, body, attachments },
      followUps: normalizedFollowUps,
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

  // Start campaign: send start request to server using original model (top-level followUps)
  async function startCampaign() {
    if (!name || !subject || !body) {
      alert('Please fill name, subject and body.');
      return;
    }

    if (contactType === 'segment' && !segment) {
      alert('Please choose a segment.');
      return;
    }

    // validate followups: ensure non-empty subject/body and positive delay
    for (const f of followUps) {
      if (!f.subject && !f.body) {
        alert('Each follow-up must have at least a subject or body.');
        return;
      }
      const dm = delayToMinutes(f.delayValue, f.delayUnit);
      if (dm <= 0) {
        alert('Follow-up delays must be greater than zero.');
        return;
      }
    }

    // normalize followups for API (original shape expected by your worker)
    const normalizedFollowUps = followUps.map(f => ({
      delayMinutes: delayToMinutes(f.delayValue, f.delayUnit),
      subject: f.subject,
      body: f.body,
      attachments: f.attachments.map(a => ({
        name: a.name,
        source: a.source ?? 'url',
        url: a.url,
        contentType: a.contentType ?? undefined,
      })),
    }));

    const payload = {
      name,
      contacts: contactType === 'all' ? { type: 'all' } : { type: 'segment', value: segment },
      initial: { subject, body, attachments },
      followUps: normalizedFollowUps,
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
    setFollowUps([]);
    loadCampaigns();
    loadSegments();
  }

  // Helper: upload a file to server (base64)
  // target: undefined => initial attachments; { type: 'followup', idx } => followup attachments for followUps[idx]
  async function uploadFile(file: File, target?: { type: 'followup'; idx: number } | undefined) {
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

      const entry: AttachmentEntry = {
        name: file.name,
        url: result.url,
        contentType: result.contentType ?? null,
        size: result.size,
        source: 'url',
      };

      if (!target) {
        setAttachments((cur) => [...cur, entry]);
      } else {
        setFollowUps(cur => {
          const copy = [...cur];
          const fu = copy[target.idx];
          if (!fu) return cur;
          fu.attachments = [...fu.attachments, entry];
          return copy;
        });
      }
    } catch (e: any) {
      console.error('Upload failed', e);
      alert('Upload failed: ' + (e?.message || String(e)));
    } finally {
      setUploading(false);
    }
  }

  function handleInitialFileInput(e: React.ChangeEvent<HTMLInputElement>) {
    const files = e.target.files;
    if (!files || files.length === 0) return;
    (async () => {
      for (let i = 0; i < files.length; i++) {
        await uploadFile(files[i]);
      }
    })();
    e.currentTarget.value = '';
  }

  function handleFollowUpFileInput(e: React.ChangeEvent<HTMLInputElement>, idx: number) {
    const files = e.target.files;
    if (!files || files.length === 0) return;
    (async () => {
      for (let i = 0; i < files.length; i++) {
        await uploadFile(files[i], { type: 'followup', idx });
      }
    })();
    e.currentTarget.value = '';
  }

  function removeAttachment(idx: number) {
    setAttachments((cur) => {
      const copy = [...cur];
      copy.splice(idx, 1);
      return copy;
    });
  }

  function addUrlAttachment() {
    if (!urlToAdd) {
      alert('Paste a URL first');
      return;
    }
    try {
      const u = new URL(urlToAdd.trim());
      if (u.protocol !== 'http:' && u.protocol !== 'https:') {
        alert('Invalid URL (must be http or https)');
        return;
      }
    } catch {
      alert('Invalid URL (must be http or https)');
      return;
    }
    const name = urlName?.trim() || new URL(urlToAdd).pathname.split('/').filter(Boolean).pop() || urlToAdd;
    setAttachments((cur) => [...cur, { name, url: urlToAdd.trim(), contentType: null, source: 'url' }]);
    setUrlToAdd('');
    setUrlName('');
  }

  // FollowUp helpers
  function addFollowUp() {
    setFollowUps(cur => [...cur, newFollowUpDraft()]);
  }

  function removeFollowUp(idx: number) {
    setFollowUps(cur => {
      const copy = [...cur];
      copy.splice(idx, 1);
      return copy;
    });
  }

  function updateFollowUp(idx: number, patch: Partial<FollowUpDraft>) {
    setFollowUps(cur => {
      const copy = [...cur];
      if (!copy[idx]) return cur;
      copy[idx] = { ...copy[idx], ...patch };
      return copy;
    });
  }

  function addUrlAttachmentToFollowUp(idx: number, url: string, name?: string) {
    if (!url) return;
    try {
      const u = new URL(url.trim());
      if (u.protocol !== 'http:' && u.protocol !== 'https:') {
        alert('Invalid URL (must be http or https)');
        return;
      }
    } catch {
      alert('Invalid URL (must be http or https)');
      return;
    }
    const displayName = name?.trim() || new URL(url).pathname.split('/').filter(Boolean).pop() || url;
    const entry: AttachmentEntry = { name: displayName, url: url.trim(), contentType: null, source: 'url' };
    setFollowUps(cur => {
      const copy = [...cur];
      if (!copy[idx]) return cur;
      copy[idx].attachments = [...copy[idx].attachments, entry];
      return copy;
    });
  }

  function removeFollowUpAttachment(fuIdx: number, attIdx: number) {
    setFollowUps(cur => {
      const copy = [...cur];
      if (!copy[fuIdx]) return cur;
      const arr = [...copy[fuIdx].attachments];
      arr.splice(attIdx, 1);
      copy[fuIdx].attachments = arr;
      return copy;
    });
  }

  // Add a remote URL attachment to a follow-up (inline UI will pass values)
  // Note: we will use small inline inputs per followup below.

  // UI render
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
            maxWidth: 900,
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
            <div style={{ marginBottom: 6, fontWeight: 600 }}>Attachments (Initial Message)</div>

            <div style={{ marginBottom: 8 }}>
              <input type="file" multiple onChange={handleInitialFileInput} />
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

          {/* Follow-ups editor */}
          <div style={{ marginTop: 16, borderTop: '1px dashed #ddd', paddingTop: 12 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h4 style={{ margin: 0 }}>Follow-ups</h4>
              <div>
                <button onClick={addFollowUp}>+ Add follow-up</button>
              </div>
            </div>

            {followUps.length === 0 && (
              <div style={{ marginTop: 8, color: '#666' }}>
                No follow-ups configured (optional). Add follow-ups to automatically send subsequent messages.
              </div>
            )}

            {followUps.map((f, idx) => (
              <div key={f.id} style={{ marginTop: 12, border: '1px solid #eee', padding: 12, borderRadius: 6 }}>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 8 }}>
                  <div>
                    <label style={{ display: 'block', fontSize: 13 }}>Delay</label>
                    <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
                      <input
                        type="number"
                        min={1}
                        value={f.delayValue}
                        onChange={(e) => updateFollowUp(idx, { delayValue: Number(e.target.value || 0) })}
                        style={{ width: 100 }}
                      />
                      <select
                        value={f.delayUnit}
                        onChange={(e) => updateFollowUp(idx, { delayUnit: e.target.value as any })}
                      >
                        <option value="minutes">minutes</option>
                        <option value="hours">hours</option>
                        <option value="days">days</option>
                      </select>
                    </div>
                    <div style={{ fontSize: 12, color: '#666', marginTop: 6 }}>
                      Time after original send when this follow-up will be evaluated.
                    </div>
                  </div>

                  <div style={{ flex: 1 }}>
                    <label style={{ display: 'block', fontSize: 13 }}>Subject</label>
                    <input
                      value={f.subject}
                      onChange={(e) => updateFollowUp(idx, { subject: e.target.value })}
                      style={{ width: '100%', marginBottom: 6 }}
                      placeholder="Follow-up subject (optional)"
                    />
                    <label style={{ display: 'block', fontSize: 13 }}>Body</label>
                    <textarea
                      value={f.body}
                      onChange={(e) => updateFollowUp(idx, { body: e.target.value })}
                      rows={4}
                      style={{ width: '100%' }}
                      placeholder="Follow-up body (HTML allowed)"
                    />
                  </div>

                  <div style={{ minWidth: 80 }}>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                      <button onClick={() => removeFollowUp(idx)} style={{ background: '#fff', border: '1px solid #ddd' }}>Remove</button>
                    </div>
                  </div>
                </div>

                {/* Attachments for this follow-up */}
                <div style={{ marginTop: 8 }}>
                  <div style={{ marginBottom: 6, fontWeight: 600 }}>Attachments (for this follow-up)</div>
                  <div style={{ marginBottom: 6 }}>
                    <input type="file" multiple onChange={(e) => handleFollowUpFileInput(e, idx)} />
                  </div>

                  <FollowUpUrlAttachmentInput idx={idx} onAdd={(url, name) => addUrlAttachmentToFollowUp(idx, url, name)} />

                  <div style={{ marginTop: 8 }}>
                    {f.attachments.length === 0 && <div style={{ color: '#666', fontSize: 13 }}>No attachments</div>}
                    {f.attachments.length > 0 && (
                      <ul>
                        {f.attachments.map((a, aidx) => (
                          <li key={String(a.url) + aidx} style={{ marginBottom: 6 }}>
                            <a href={a.url} target="_blank" rel="noreferrer">{a.name}</a>
                            {a.size ? <span style={{ marginLeft: 8, color: '#666', fontSize: 13 }}>{Math.round((a.size||0)/1024)} KB</span> : null}
                            <button style={{ marginLeft: 8 }} onClick={() => removeFollowUpAttachment(idx, aidx)}>Remove</button>
                          </li>
                        ))}
                      </ul>
                    )}
                  </div>
                </div>
              </div>
            ))}
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

/**
 * Helper component: small inputs for adding a URL attachment to a given follow-up.
 * Keeps that logic modular to keep main file tidy.
 */
function FollowUpUrlAttachmentInput({ idx, onAdd }: { idx: number; onAdd: (url: string, name?: string) => void }) {
  const [u, setU] = useState('');
  const [n, setN] = useState('');
  return (
    <div style={{ marginTop: 8 }}>
      <div style={{ fontSize: 13, marginBottom: 6 }}>Or add a remote URL for this follow-up</div>
      <input placeholder="https://example.com/file.pdf" value={u} onChange={(e) => setU(e.target.value)} style={{ width: '100%', marginBottom: 6 }} />
      <input placeholder="Optional display filename" value={n} onChange={(e) => setN(e.target.value)} style={{ width: '100%', marginBottom: 6 }} />
      <div style={{ display: 'flex', gap: 8 }}>
        <button onClick={() => { onAdd(u.trim(), n.trim() || undefined); setU(''); setN(''); }}>Add URL attachment</button>
      </div>
    </div>
  );
}
