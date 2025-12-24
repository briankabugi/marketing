// pages/campaigns/[id].tsx
import react from 'react';
import { useRouter } from 'next/router';
import Link from 'next/link';

type ContactRow = {
  id: string;
  contactId: string | null;
  email: string | null;
  status: string | null;
  attempts: number;
  lastAttemptAt?: string | null;
  lastError?: string | null;
};

export default function CampaignInsight() {
  const router = useRouter();
  const { id } = router.query as { id?: string };

  // ... (omitted earlier state fields for brevity — recreate same as you had)
  const [campaign, setCampaign] = react.useState<any>(null);
  const [breakdown, setBreakdown] = react.useState<any>(null);
  const [totals, setTotals] = react.useState<any>(null);
  const [recentFailures, setRecentFailures] = react.useState<any[]>([]);
  const [maxAttempts, setMaxAttempts] = react.useState<number>(3);

  const [items, setItems] = react.useState<ContactRow[]>([]);
  const [total, setTotal] = react.useState<number>(0);
  const [pages, setPages] = react.useState<number>(1);
  const [tableLoading, setTableLoading] = react.useState(false);
  const [availableStatuses, setAvailableStatuses] = react.useState<string[]>([]);
  const [actionInProgress, setActionInProgress] = react.useState(false);
  const [refreshing, setRefreshing] = react.useState(false);

  // URL-driven params (fallbacks)
  const query = router.query || {};
  const statusQuery = (typeof query.status === 'string' ? query.status : 'all') || 'all';
  const pageQuery = Math.max(1, Number(query.page ? query.page : 1));
  const pageSizeQuery = Math.max(1, Number(query.pageSize ? query.pageSize : 25));

  // ----------------------
  // Data loading functions
  // ----------------------
  async function loadInsight(silent = false) {
    if (!id) return;
    if (!silent) setRefreshing(true);
    try {
      const res = await fetch(`/api/campaign/${id}/insight`);
      if (!res.ok) throw new Error('Failed to load insight');
      const body = await res.json();
      setCampaign(body.campaign);
      setBreakdown(body.breakdown);
      setTotals(body.totals);
      setRecentFailures(body.recentFailures || []);
      setMaxAttempts(body.maxAttempts ?? 3);
    } catch (e) {
      console.error(e);
    } finally {
      if (!silent) setRefreshing(false);
    }
  }

  async function loadContacts(opts?: { status?: string; page?: number; pageSize?: number; silent?: boolean }) {
    if (!id) return;
    const silent = opts?.silent === true;
    if (!silent) setTableLoading(true);
    try {
      const s = opts?.status ?? statusQuery;
      const p = opts?.page ?? pageQuery;
      const ps = opts?.pageSize ?? pageSizeQuery;

      const qp = new URLSearchParams();
      if (s && s !== 'all') qp.set('status', s);
      qp.set('page', String(p));
      qp.set('pageSize', String(ps));

      const res = await fetch(`/api/campaign/${id}/contacts?${qp.toString()}`);
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: 'Unknown' }));
        throw new Error(err.error || JSON.stringify(err));
      }
      const body = await res.json();
      setItems((body.items || []).map((it: any) => ({
        id: it.id,
        contactId: it.contactId ?? null,
        email: it.email ?? null,
        status: it.status ?? null,
        attempts: it.attempts ?? 0,
        lastAttemptAt: it.lastAttemptAt ?? null,
        lastError: it.lastError ?? null,
      })));
      setTotal(body.total ?? 0);
      setPages(body.pages ?? 1);

      if (Array.isArray(body.availableStatuses)) setAvailableStatuses(body.availableStatuses);
      if (typeof body.maxAttempts === 'number') setMaxAttempts(body.maxAttempts);
    } catch (e) {
      console.error('Contacts load error', e);
      if (!silent) { setItems([]); setTotal(0); setPages(1); }
    } finally {
      if (!silent) setTableLoading(false);
    }
  }

  // initial load + insight poll (insight only)
  react.useEffect(() => {
    if (!id) return;
    loadInsight();
    loadContacts();

    const interval = setInterval(() => {
      loadInsight(true); // silent only update counts
    }, 5000);
    return () => clearInterval(interval);
  }, [id]);

  // reload contacts whenever URL query changes (status/page/pageSize)
  react.useEffect(() => {
    if (!id) return;
    loadContacts();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id, router.asPath]);

  // ----------------------
  // EventSource (SSE) hookup
  // ----------------------
  react.useEffect(() => {
    if (!id) return;
    const es = new EventSource(`/api/campaign/${id}/events`);

    es.addEventListener('contact', (ev: any) => {
      try {
        const payload = JSON.parse(ev.data);
        // payload: { campaignId, contactId, status, attempts, lastAttemptAt, lastError, ... }
        const { contactId: updatedContactId } = payload;
        const contactIdStr = String(updatedContactId);

        let matched = false;
        setItems((prev) => {
          const next = prev.map((row) => {
            if (row.contactId && String(row.contactId) === contactIdStr) {
              matched = true;
              return {
                ...row,
                status: payload.status ?? row.status,
                attempts: typeof payload.attempts === 'number' ? payload.attempts : row.attempts,
                lastAttemptAt: payload.lastAttemptAt ?? row.lastAttemptAt,
                lastError: payload.lastError ?? row.lastError,
              };
            }
            return row;
          });
          return next;
        });

        // If not matched (not on current page), refresh counts only
        if (!matched) {
          loadInsight();
        }
      } catch (e) {
        console.warn('Malformed contact SSE payload', e);
      }
    });

    es.addEventListener('campaign', (ev: any) => {
      try {
        const payload = JSON.parse(ev.data);
        // some payloads may be campaign-level (status changes), so refresh counts and visible page
        loadInsight();
        loadContacts({ silent: true });
      } catch (e) {
        console.warn('Malformed campaign SSE payload', e);
      }
    });

    es.addEventListener('error', (e) => {
      // On error, we leave it to the browser to retry; optionally we can reconnect manually
      console.warn('EventSource error', e);
    });

    return () => {
      try { es.close(); } catch {}
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id, /* not including items to avoid re-creating EventSource */]);

  // Utility: update query params shallowly and reload contacts immediately
  function updateQuery(params: Record<string, any>) {
    const next = { ...router.query, ...params };
    if (next.status === 'all' || next.status == null) delete next.status;
    if (next.page == null || Number(next.page) === 1) delete next.page;
    if (next.pageSize == null || Number(next.pageSize) === 25) delete next.pageSize;

    router.replace({ pathname: router.pathname, query: next }, undefined, { shallow: true })
      .then(() => {
        loadContacts({ status: next.status ?? 'all', page: Number(next.page ?? 1), pageSize: Number(next.pageSize ?? 25) });
      })
      .catch(() => {
        loadContacts({ status: params.status ?? statusQuery, page: params.page ?? pageQuery, pageSize: params.pageSize ?? pageSizeQuery });
      });
  }

  // Control and retry handlers (same as before)
  async function updateCampaign(action: 'pause' | 'resume' | 'cancel' | 'delete') {
    if (!id || actionInProgress) return;
    if (action === 'delete') { if (!confirm('Delete campaign permanently? This cannot be undone.')) return; }
    setActionInProgress(true);
    try {
      const payload: any = { action };
      if (action === 'delete') payload.confirm = true;
      const res = await fetch(`/api/campaign/${id}/control`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      const body = await res.json();
      if (!res.ok) throw new Error(body.error || JSON.stringify(body));
      if (action === 'delete') { router.push('/'); return; }
      setCampaign((c:any) => c ? { ...c, status: (body.action === 'resumed' ? 'running' : body.action) } : c);
      loadInsight();
      loadContacts();
    } catch (e:any) { alert('Action failed: ' + (e?.message || String(e))); } finally { setActionInProgress(false); }
  }

  async function retryContact(contactId: string | null) {
    if (!id || !contactId || actionInProgress) return;
    if (!confirm('Retry this contact?')) return;
    setActionInProgress(true);
    try {
      const res = await fetch(`/api/campaign/${id}/control`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action: 'retryContact', contactId }) });
      const body = await res.json();
      if (!res.ok) throw new Error(body.error || JSON.stringify(body));
      // wait for SSE to update this row; also proactively refresh counts
      loadInsight();
    } catch (e:any) {
      alert('Retry failed: ' + (e?.message || String(e)));
    } finally { setActionInProgress(false); }
  }

  async function retryAllFailed() {
    if (!id || actionInProgress) return;
    if (!confirm('Retry all failed contacts eligible for retry?')) return;
    setActionInProgress(true);
    try {
      const res = await fetch(`/api/campaign/${id}/control`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action: 'retryFailed' }) });
      const body = await res.json();
      if (!res.ok) throw new Error(body.error || JSON.stringify(body));
      alert(`Retry requested. Requeued ${body.retried ?? 0} contacts.`);
      loadInsight();
      loadContacts();
    } catch (e:any) {
      alert('Retry failed: ' + (e?.message || String(e)));
    } finally { setActionInProgress(false); }
  }

  // UI render (similar to prior)
  const statusOptions = availableStatuses.length > 0 ? ['all', ...availableStatuses] : ['all', 'pending', 'sending', 'sent', 'failed', 'bounced', 'cancelled'];

  return (
    <div style={{ padding: 24, fontFamily: 'sans-serif', maxWidth: 1200 }}>
      <h1>Campaign Insight: {campaign?.name ?? '—'}</h1>
      <Link href="/">Back to HomePage</Link>

      <div style={{ marginTop: 12 }}>
        <strong>Status:</strong> {campaign?.status ?? '—'} <br />
        <strong>Created:</strong> {campaign?.createdAt ? new Date(campaign.createdAt).toLocaleString() : '—'}
      </div>

      <div style={{ marginTop: 12, display: 'flex', gap: 8 }}>
        {campaign?.status === 'running' && <button onClick={() => updateCampaign('pause')} disabled={actionInProgress}>Pause</button>}
        {campaign?.status === 'paused' && <button onClick={() => updateCampaign('resume')} disabled={actionInProgress}>Resume</button>}
        {['running', 'paused'].includes(campaign?.status ?? '') && <button onClick={() => updateCampaign('cancel')} disabled={actionInProgress} style={{ color: 'red' }}>Cancel</button>}
        <button onClick={() => updateCampaign('delete')} disabled={actionInProgress} style={{ color: 'red' }}>Delete</button>
        <div style={{ marginLeft: 'auto' }}>
          <button onClick={() => { loadInsight(); loadContacts(); }} disabled={refreshing}>Refresh</button>
        </div>
      </div>

      <div style={{ marginTop: 16, display: 'flex', gap: 8 }}>
        <div style={{ border: '1px solid #ddd', padding: 12, borderRadius: 6 }}>
          <div><strong>Intended</strong></div>
          <div>{totals?.intended ?? '–'}</div>
        </div>
        <div style={{ border: '1px solid #ddd', padding: 12, borderRadius: 6 }}>
          <div><strong>Processed</strong></div>
          <div>{totals ? totals.processed : '–'}</div>
        </div>
        <div style={{ border: '1px solid #ddd', padding: 12, borderRadius: 6 }}>
          <div><strong>Sent</strong></div>
          <div>{breakdown?.sent ?? '–'}</div>
        </div>
        <div style={{ border: '1px solid #ddd', padding: 12, borderRadius: 6 }}>
          <div><strong>Failed</strong></div>
          <div>{breakdown?.failed ?? '–'}</div>
        </div>
        <div style={{ marginLeft: 'auto' }}>
          {breakdown && breakdown.failed > 0 && <button onClick={retryAllFailed} disabled={actionInProgress}>Retry all failed</button>}
        </div>
      </div>

      {/* Filters */}
      <div style={{ marginTop: 16, display: 'flex', gap: 8, alignItems: 'center' }}>
        <label>
          Status:
          <select value={statusQuery} onChange={(e) => updateQuery({ status: e.target.value, page: 1 })} style={{ marginLeft: 8 }}>
            {statusOptions.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </label>

        <label>
          Page size:
          <select value={pageSizeQuery} onChange={(e) => updateQuery({ pageSize: Number(e.target.value), page: 1 })} style={{ marginLeft: 8 }}>
            {[10, 25, 50, 100].map(n => <option key={n} value={n}>{n}</option>)}
          </select>
        </label>

        <div style={{ marginLeft: 'auto' }}>
          <button onClick={() => { loadContacts(); loadInsight(); }} disabled={tableLoading || refreshing}>Refresh table</button>
        </div>
      </div>

      {/* Table */}
      <div style={{ marginTop: 12, border: '1px solid #ddd', borderRadius: 6, padding: 12 }}>
        <div style={{ marginBottom: 8 }}>
          <strong>Total:</strong> {total} • <strong>Page:</strong> {pageQuery} / {pages}
        </div>

        {tableLoading ? <div>Loading contacts…</div> : (
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #ccc' }}>
                <th style={{ textAlign: 'left', padding: 8 }}>Email / ContactId</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Status</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Attempts</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Last Attempt</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Error</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {items.length === 0 ? <tr><td colSpan={6} style={{ padding: 12, textAlign: 'center' }}>No records</td></tr> :
                items.map(it => (
                <tr key={it.id} style={{ borderBottom: '1px solid #eee' }}>
                  <td style={{ padding: 8 }}>{it.email ?? it.contactId ?? '—'}</td>
                  <td style={{ padding: 8 }}>{it.status ?? '—'}</td>
                  <td style={{ padding: 8 }}>{it.attempts} / {maxAttempts}</td>
                  <td style={{ padding: 8 }}>{it.lastAttemptAt ? new Date(it.lastAttemptAt).toLocaleString() : '—'}</td>
                  <td style={{ padding: 8, color: 'red' }}>{it.lastError ?? '—'}</td>
                  <td style={{ padding: 8 }}>
                    <button disabled={actionInProgress || it.attempts >= maxAttempts || it.status !== 'failed'} onClick={() => retryContact(it.contactId)} title={it.attempts >= maxAttempts ? 'Reached max attempts' : 'Retry this contact'}>Retry</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}

        <div style={{ marginTop: 12, display: 'flex', gap: 8, alignItems: 'center' }}>
          <button onClick={() => updateQuery({ page: Math.max(1, pageQuery - 1) })} disabled={pageQuery <= 1}>Prev</button>
          <div>Page {pageQuery} / {pages}</div>
          <button onClick={() => updateQuery({ page: Math.min(pages, pageQuery + 1) })} disabled={pageQuery >= pages}>Next</button>
        </div>
      </div>
    </div>
  );
}
