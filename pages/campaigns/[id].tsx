// pages/campaigns/[id].tsx
import React from 'react';
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
  bgAttempts?: number;
  // engagement
  opened?: boolean;
  clicked?: boolean;
  lastOpenAt?: string | null;
  lastClickAt?: string | null;
};

type InsightPayload = {
  campaign: {
    id: string;
    name: string;
    status: string;
    createdAt?: string;
    completedAt?: string | null;
  };
  totals: {
    intended: number;
    processed: number;
    sent: number;
    failed: number;
  };
  breakdown: {
    pending: number;
    sent: number;
    failed: number;
  };
  recentFailures: Array<any>;
  maxAttempts: number;
  engagement?: {
    opens?: { total: number; unique: number; rate: number };
    clicks?: { total: number; unique: number; rate: number };
    links?: Array<{ url?: string; clicks: number }>;
  };
};

export default function CampaignInsight() {
  const router = useRouter();
  const { id } = router.query as { id?: string };

  const [campaign, setCampaign] = React.useState<any>(null);
  const [breakdown, setBreakdown] = React.useState<any>(null);
  const [totals, setTotals] = React.useState<any>(null);
  const [recentFailures, setRecentFailures] = React.useState<any[]>([]);
  const [maxAttempts, setMaxAttempts] = React.useState<number>(3);

  const [engagement, setEngagement] = React.useState<InsightPayload['engagement'] | null>(null);

  const [items, setItems] = React.useState<ContactRow[]>([]);
  const [total, setTotal] = React.useState<number>(0);
  const [pages, setPages] = React.useState<number>(1);
  const [tableLoading, setTableLoading] = React.useState(false);
  const [availableStatuses, setAvailableStatuses] = React.useState<string[]>([]);
  const [actionInProgress, setActionInProgress] = React.useState(false);
  const [refreshing, setRefreshing] = React.useState(false);

  // engine / job UI state
  const [sseLog, setSseLog] = React.useState<any[]>([]);
  const sseLogRef = React.useRef<any[]>([]);
  const [engineLoading, setEngineLoading] = React.useState(false);
  const [lastReconcileAt, setLastReconcileAt] = React.useState<string | null>(null);
  const [queuedEstimate, setQueuedEstimate] = React.useState<number | null>(null);

  // keep refs for mounted & current fetch abort controllers to avoid race state updates
  const mountedRef = React.useRef(true);
  const insightAbortRef = React.useRef<AbortController | null>(null);
  const contactsAbortRef = React.useRef<AbortController | null>(null);

  React.useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      if (insightAbortRef.current) insightAbortRef.current.abort();
      if (contactsAbortRef.current) contactsAbortRef.current.abort();
    };
  }, []);

  // URL-driven params (derived, memoized to avoid needless recalculation)
  const query = router.query || {};
  const statusQuery = React.useMemo(() => (typeof query.status === 'string' ? query.status : 'all') || 'all', [router.query]);
  const pageQuery = Math.max(1, Number(query.page ? query.page : 1));
  const pageSizeQuery = Math.max(1, Number(query.pageSize ? query.pageSize : 25));

  // ----------------------
  // Helpers
  // ----------------------
  async function safeJson(res: Response) {
    const text = await res.text();
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  }

  // ----------------------
  // Data loading functions
  // ----------------------
  async function loadInsight(silent = false) {
    if (!id) return;
    if (!silent) setRefreshing(true);

    // abort previous insight request if any
    if (insightAbortRef.current) {
      try { insightAbortRef.current.abort(); } catch { }
    }
    insightAbortRef.current = new AbortController();
    const signal = insightAbortRef.current.signal;

    try {
      const res = await fetch(`/api/campaign/${id}/insight`, { signal });
      if (!res.ok) {
        const body = await safeJson(res).catch(() => ({ error: 'Unknown' }));
        throw new Error((body && (body as any).error) ? (body as any).error : `Failed to load insight (${res.status})`);
      }
      const body: InsightPayload = await res.json();
      if (!mountedRef.current) return;

      setCampaign(body.campaign);
      setBreakdown(body.breakdown);
      setTotals(body.totals);
      setRecentFailures(body.recentFailures || []);
      setMaxAttempts(body.maxAttempts ?? 3);
      setEngagement(body.engagement ?? null);

      // Quick queued estimate (intended - processed)
      const intended = Number(body.totals?.intended ?? 0);
      const processed = Number(body.totals?.processed ?? 0);
      setQueuedEstimate(Number.isFinite(intended - processed) ? Math.max(0, intended - processed) : null);
    } catch (e) {
      // don't clear UI on insight error; log for debugging.
      console.error('Insight load error', e);
    } finally {
      if (!silent) setRefreshing(false);
    }
  }

  async function loadContacts(opts?: { status?: string; page?: number; pageSize?: number; silent?: boolean }) {
    if (!id) return;
    const silent = opts?.silent === true;
    if (!silent) setTableLoading(true);

    // abort previous contacts request if any
    if (contactsAbortRef.current) {
      try { contactsAbortRef.current.abort(); } catch { }
    }
    contactsAbortRef.current = new AbortController();
    const signal = contactsAbortRef.current.signal;

    try {
      const s = opts?.status ?? statusQuery;
      const p = opts?.page ?? pageQuery;
      const ps = opts?.pageSize ?? pageSizeQuery;

      const qp = new URLSearchParams();
      if (s && s !== 'all') qp.set('status', s);
      qp.set('page', String(p));
      qp.set('pageSize', String(ps));

      const res = await fetch(`/api/campaign/${id}/contacts?${qp.toString()}`, { signal });
      if (!res.ok) {
        const err = await safeJson(res).catch(() => ({ error: 'Unknown' }));
        throw new Error((err && (err as any).error) ? (err as any).error : `Failed to load contacts (${res.status})`);
      }
      const body = await res.json();

      if (!mountedRef.current) return;

      // ensure attempts/bgAttempts are at least 1 for display consistency
      const normalizedItems = (body.items || []).map((it: any) => ({
        id: it.id,
        contactId: it.contactId ?? null,
        email: it.email ?? null,
        status: it.status ?? null,
        attempts: (typeof it.attempts === 'number' ? Math.max(1, it.attempts) : 1),
        lastAttemptAt: it.lastAttemptAt ?? null,
        lastError: it.lastError ?? null,
        bgAttempts: (typeof it.bgAttempts === 'number' ? Math.max(1, it.bgAttempts) : (typeof it.bgAttempts === 'undefined' ? 0 : Number(it.bgAttempts))),
        // engagement fields (if the API returns them)
        opened: !!it.opened,
        clicked: !!it.clicked,
        lastOpenAt: it.lastOpenAt ?? null,
        lastClickAt: it.lastClickAt ?? null,
      })) as ContactRow[];

      // update atomically to reduce flicker
      setItems(normalizedItems);
      setTotal(body.total ?? 0);
      setPages(body.pages ?? 1);

      if (Array.isArray(body.availableStatuses)) setAvailableStatuses(body.availableStatuses);
      if (typeof body.maxAttempts === 'number') setMaxAttempts(body.maxAttempts);
    } catch (e) {
      // Preserve prior items on error to avoid flicker; surface console message.
      console.error('Contacts load error', e);
    } finally {
      if (!silent) setTableLoading(false);
    }
  }

  // initial load + insight poll
  React.useEffect(() => {
    if (!id) return;
    loadInsight();
    loadContacts();

    const interval = setInterval(() => {
      loadInsight(true); // silent only update counts
    }, 5000);
    return () => clearInterval(interval);
  }, [id]);

  // reload contacts whenever URL query changes (status/page/pageSize)
  React.useEffect(() => {
    if (!id) return;
    loadContacts();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id, router.asPath]);

  // ----------------------
  // Silent table refresh debounce
  // ----------------------
  const refreshTimerRef = React.useRef<ReturnType<typeof setTimeout> | null>(null);

  function scheduleSilentTableRefresh(delayMs = 300) {
    if (refreshTimerRef.current) return;
    refreshTimerRef.current = setTimeout(() => {
      refreshTimerRef.current = null;
      loadContacts({ silent: true });
    }, delayMs);
  }

  // ----------------------
  // EventSource (SSE) hookup
  // ----------------------
  React.useEffect(() => {
    if (!id) return;
    const es = new EventSource(`/api/campaign/${id}/events`);

    es.onopen = () => {
      loadInsight(true);
      loadContacts({ silent: true });
    };

    es.addEventListener('contact', (ev: any) => {
      console.log('[SSE] contact event', ev.data)
      try {
        let payloadRaw = ev.data;
        let payload: any;
        try { payload = JSON.parse(payloadRaw); } catch { payload = { raw: String(payloadRaw) }; }

        const updatedContactId = payload.contactId ?? payload.contact?.contactId ?? payload.contactIdStr ?? null;
        if (!updatedContactId) {
          loadInsight(true);
          return;
        }
        const contactIdStr = String(updatedContactId);

        setItems((prev) => {
          console.log('[UI] attempting to patch row for', contactIdStr)
          let matched = false;
          const next = prev.map((row) => {
            const rowContactId = row.contactId != null ? String(row.contactId) : null;
            const rowIdStr = row.id != null ? String(row.id) : null;
            if ((rowContactId && rowContactId === contactIdStr) || (rowIdStr && rowIdStr === contactIdStr)) {
              matched = true;
              const newAttempts = typeof payload.attempts === 'number' ? Math.max(1, payload.attempts) : row.attempts ?? 1;
              const newBgAttempts = typeof payload.bgAttempts === 'number' ? Math.max(0, payload.bgAttempts) : row.bgAttempts ?? 0;
              const newStatus = payload.status ?? row.status;
              const newLastAttemptAt = payload.lastAttemptAt ?? row.lastAttemptAt;
              const newLastError = payload.lastError ?? row.lastError;

              // Engagement updates from SSE contact payloads
              let opened = row.opened ?? false;
              let clicked = row.clicked ?? false;
              let lastOpenAt = row.lastOpenAt ?? null;
              let lastClickAt = row.lastClickAt ?? null;

              // Normalize event timestamp fields robustly:
              // prefer explicit openedAt/clickedAt, then lastOpenAt/lastClickAt, then ts, then fallback to now
              const normalizedOpenTs =
                payload.openedAt ??
                payload.lastOpenAt ??
                payload.ts ??
                payload.opened_at ??
                null;
              const normalizedClickTs =
                payload.clickedAt ??
                payload.lastClickAt ??
                payload.ts ??
                payload.clicked_at ??
                null;

              if (payload.event === 'open' || normalizedOpenTs) {
                opened = true;
                lastOpenAt = String(normalizedOpenTs ?? new Date().toISOString());
              }

              if (payload.event === 'click' || normalizedClickTs) {
                clicked = true;
                lastClickAt = String(normalizedClickTs ?? new Date().toISOString());
              }

              // Some publishers might send boolean flags directly
              if (typeof payload.opened === 'boolean') opened = payload.opened;
              if (typeof payload.clicked === 'boolean') clicked = payload.clicked;
              if (payload.lastOpenAt) lastOpenAt = payload.lastOpenAt;
              if (payload.lastClickAt) lastClickAt = payload.lastClickAt;

              return {
                ...row,
                status: newStatus,
                attempts: newAttempts,
                bgAttempts: newBgAttempts,
                lastAttemptAt: newLastAttemptAt,
                lastError: newLastError,
                opened,
                clicked,
                lastOpenAt,
                lastClickAt,
              };
            }
            return row;
          });

          if (!matched) {
            console.log('[UI] contact not in current page, triggering refresh')
            scheduleSilentTableRefresh();
          } else {
            // refresh counts silently so metrics like unique opens update without flicker
            console.log('[UI] matched row, updating')
            loadInsight(true);
          }

          return next;
        });

        // push to SSE log
        const logEntry = {
          ts: new Date().toISOString(),
          type: 'contact',
          payload,
        };
        sseLogRef.current = [logEntry, ...sseLogRef.current].slice(0, 200);
        setSseLog(sseLogRef.current);
      } catch (e) {
        console.warn('Malformed contact SSE payload', e);
      }
    });

    es.addEventListener('campaign', (ev: any) => {
      console.log('[SSE] campaign event', ev.data)
      try {
        const payloadRaw = ev.data;
        let payload: any;
        try { payload = JSON.parse(payloadRaw); } catch { payload = { raw: String(payloadRaw) }; }

        const serverStatus = payload?.status ?? payload?.state ?? null;
        if (serverStatus && serverStatus !== (campaign?.status ?? null)) {
          loadInsight(false);
          loadContacts();
          return;
        }

        loadInsight(true);
        if (payload?.refreshContacts) {
          loadContacts({ silent: true });
        }

        // sse log
        const logEntry = {
          ts: new Date().toISOString(),
          type: 'campaign',
          payload,
        };
        sseLogRef.current = [logEntry, ...sseLogRef.current].slice(0, 200);
        setSseLog(sseLogRef.current);
      } catch (e) {
        console.warn('Malformed campaign SSE payload', e);
      }
    });

    es.addEventListener('ping', () => { });

    es.addEventListener('error', (e) => {
      console.warn('EventSource error', e);
    });

    return () => {
      try { es.close(); } catch { }
      if (refreshTimerRef.current) {
        clearTimeout(refreshTimerRef.current);
        refreshTimerRef.current = null;
      }
    };
    // Intentionally excluding campaign & items in deps to avoid recreating EventSource frequently
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id]);

  // Utility: update query params shallowly and reload contacts immediately (silent update to avoid flicker)
  function updateQuery(params: Record<string, any>) {
    const next = { ...router.query, ...params };
    if (next.status === 'all' || next.status == null) delete next.status;
    if (next.page == null || Number(next.page) === 1) delete next.page;
    if (next.pageSize == null || Number(next.pageSize) === 25) delete next.pageSize;

    router.replace({ pathname: router.pathname, query: next }, undefined, { shallow: true })
      .then(() => {
        // load silently to avoid clearing the table (prevents flicker while keeping data fresh)
        loadContacts({ status: next.status ?? 'all', page: Number(next.page ?? 1), pageSize: Number(next.pageSize ?? 25), silent: true });
      })
      .catch(() => {
        // fallback: attempt best-effort silent load
        loadContacts({ status: params.status ?? statusQuery, page: params.page ?? pageQuery, pageSize: params.pageSize ?? pageSizeQuery, silent: true });
      });
  }

  // Control and retry handlers
  async function updateCampaign(action: 'pause' | 'resume' | 'cancel' | 'delete') {
    if (!id || actionInProgress) return;
    if (action === 'delete') { if (!confirm('Delete campaign permanently? This cannot be undone.')) return; }
    setActionInProgress(true);
    try {
      const payload: any = { action };
      if (action === 'delete') payload.confirm = true;
      const res = await fetch(`/api/campaign/${id}/control`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      const body = await safeJson(res);
      if (!res.ok) throw new Error((body && (body as any).error) ? (body as any).error : JSON.stringify(body));
      if (action === 'delete') { router.push('/'); return; }
      setCampaign((c: any) => c ? { ...c, status: (body.action === 'resumed' ? 'running' : body.action) } : c);
      loadInsight();
      loadContacts();
    } catch (e: any) { alert('Action failed: ' + (e?.message || String(e))); } finally { setActionInProgress(false); }
  }

  async function retryContact(contactId: string | null) {
    if (!id || !contactId || actionInProgress) return;
    if (!confirm('Retry this contact?')) return;
    setActionInProgress(true);
    try {
      const res = await fetch(`/api/campaign/${id}/control`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action: 'retryContact', contactId }) });
      const body = await safeJson(res);
      if (!res.ok) throw new Error((body && (body as any).error) ? (body as any).error : JSON.stringify(body));
      loadInsight();
      // silent contacts refresh to avoid flicker
      loadContacts({ silent: true });
    } catch (e: any) {
      alert('Retry failed: ' + (e?.message || String(e)));
    } finally { setActionInProgress(false); }
  }

  async function retryAllFailed() {
    if (!id || actionInProgress) return;
    if (!confirm('Retry all failed contacts eligible for retry?')) return;
    setActionInProgress(true);
    try {
      const res = await fetch(`/api/campaign/${id}/control`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action: 'retryFailed' }) });
      const body = await safeJson(res);
      if (!res.ok) throw new Error((body && (body as any).error) ? (body as any).error : JSON.stringify(body));
      alert(`Retry requested. Requeued ${((body as any).retried ?? 0)} contacts.`);
      loadInsight();
      loadContacts();
    } catch (e: any) {
      alert('Retry failed: ' + (e?.message || String(e)));
    } finally { setActionInProgress(false); }
  }

  // Delivery engine actions
  async function reconcileNow() {
    if (!id || actionInProgress) return;
    if (!confirm('Run reconciliation now? This will recompute campaign status from ledger rows and persist results.')) return;
    setEngineLoading(true);
    try {
      const res = await fetch(`/api/campaign/${id}/control`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action: 'reconcile' }) });
      const body = await safeJson(res);
      if (!res.ok) {
        throw new Error((body && (body as any).error) ? (body as any).error : JSON.stringify(body));
      }
      setLastReconcileAt(new Date().toISOString());
      loadInsight();
      loadContacts({ silent: true });
      // log
      const logEntry = { ts: new Date().toISOString(), type: 'reconcile', payload: body };
      sseLogRef.current = [logEntry, ...sseLogRef.current].slice(0, 200);
      setSseLog(sseLogRef.current);
    } catch (e: any) {
      alert('Reconcile failed: ' + (e?.message || String(e)));
    } finally {
      setEngineLoading(false);
    }
  }

  async function forceRequeuePending() {
    if (!id || actionInProgress) return;
    if (!confirm('Force requeue of pending contacts (will attempt to enqueue pending rows up to a server limit).')) return;
    setEngineLoading(true);
    try {
      // resume endpoint re-enqueues pending contacts in server implementation
      const res = await fetch(`/api/campaign/${id}/control`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action: 'resume' }) });
      const body = await safeJson(res);
      if (!res.ok) throw new Error((body && (body as any).error) ? (body as any).error : JSON.stringify(body));
      loadInsight();
      loadContacts({ silent: true });
      const logEntry = { ts: new Date().toISOString(), type: 'force_requeue', payload: body };
      sseLogRef.current = [logEntry, ...sseLogRef.current].slice(0, 200);
      setSseLog(sseLogRef.current);
      alert('Requeue attempted — check job / SSE log for details.');
    } catch (e: any) {
      alert('Force requeue failed: ' + (e?.message || String(e)));
    } finally {
      setEngineLoading(false);
    }
  }

  async function fetchQueueSnapshot() {
    if (!id) return;
    setEngineLoading(true);
    try {
      const res = await fetch(`/api/campaign/${id}/insight`);
      if (!res.ok) throw new Error('Failed to fetch insight for queue snapshot');
      const body: InsightPayload = await res.json();
      const intended = Number(body.totals?.intended ?? 0);
      const processed = Number(body.totals?.processed ?? 0);
      setQueuedEstimate(Math.max(0, intended - processed));
      const logEntry = { ts: new Date().toISOString(), type: 'queue_snapshot', payload: { intended, processed, queued: Math.max(0, intended - processed) } };
      sseLogRef.current = [logEntry, ...sseLogRef.current].slice(0, 200);
      setSseLog(sseLogRef.current);
    } catch (e) {
      console.error('Queue snapshot failed', e);
      alert('Queue snapshot failed: ' + ((e as any)?.message ?? String(e)));
    } finally {
      setEngineLoading(false);
    }
  }

  // UI render
  const statusOptions = availableStatuses.length > 0 ? ['all', ...availableStatuses] : ['all', 'pending', 'sending', 'sent', 'failed', 'bounced', 'cancelled'];

  // derive top-links safe list
  const topLinks = (engagement?.links || []).map(l => ({ url: l.url || '—', clicks: l.clicks }));
  const maxClicks = topLinks.length > 0 ? Math.max(...topLinks.map(t => t.clicks || 0)) : 1;

  return (
    <div style={{ padding: 24, fontFamily: 'sans-serif', maxWidth: 1200 }}>
      <h1>Campaign Insight: {campaign?.name ?? '—'}</h1>
      <Link href="/">Back to HomePage</Link>

      <div style={{ marginTop: 12 }}>
        <strong>Status:</strong> {campaign?.status ?? '—'} <br />
        <strong>Created:</strong> {campaign?.createdAt ? new Date(campaign.createdAt).toLocaleString() : '—'}
      </div>

      <div style={{ marginTop: 12, display: 'flex', gap: 8 }}>

        <div style={{ border: '1px solid #ddd', padding: 12, borderRadius: 6, minWidth: 120 }}>
          <div><strong>Intended</strong></div>
          <div>{totals?.intended ?? '–'}</div>
        </div>

        <div style={{ border: '1px solid #ddd', padding: 12, borderRadius: 6, minWidth: 120 }}>
          <div><strong>Processed</strong></div>
          <div>{totals ? totals.processed : '–'}</div>
        </div>

        <div style={{ border: '1px solid #ddd', padding: 12, borderRadius: 6, minWidth: 120 }}>
          <div><strong>Sent</strong></div>
          <div>{breakdown?.sent ?? '–'}</div>
        </div>

        <div style={{ border: '1px solid #ddd', padding: 12, borderRadius: 6, minWidth: 120 }}>
          <div><strong>Failed</strong></div>
          <div>{breakdown?.failed ?? '–'}</div>
        </div>

        {/* Engagement summary */}
        <div style={{ border: '1px solid #ddd', padding: 12, borderRadius: 6, minWidth: 220 }}>
          <div style={{ fontSize: 12, color: '#555' }}>Opens (unique / total)</div>
          <div style={{ fontSize: 18 }}>
            {engagement?.opens ? `${engagement.opens.unique} / ${engagement.opens.total}` : '—'}
          </div>
          <div style={{ fontSize: 12, color: '#666' }}>
            Rate: {engagement?.opens ? `${Math.round(engagement.opens.rate * 1000) / 10}%` : '—'}
          </div>
        </div>

        <div style={{ border: '1px solid #ddd', padding: 12, borderRadius: 6, minWidth: 220 }}>
          <div style={{ fontSize: 12, color: '#555' }}>Clicks (unique / total)</div>
          <div style={{ fontSize: 18 }}>
            {engagement?.clicks ? `${engagement.clicks.unique} / ${engagement.clicks.total}` : '—'}
          </div>
          <div style={{ fontSize: 12, color: '#666' }}>
            CTR: {engagement?.clicks ? `${Math.round(engagement.clicks.rate * 1000) / 10}%` : '—'}
          </div>
        </div>

        <div style={{ marginLeft: 'auto' }}>
          {breakdown && breakdown.failed > 0 && <button onClick={retryAllFailed} disabled={actionInProgress}>Retry all failed</button>}
        </div>
      </div>

      {/* Top links heatmap */}
      <div style={{ marginTop: 12, display: 'flex', gap: 12 }}>
        <div style={{ flex: 1, border: '1px solid #eee', padding: 12, borderRadius: 6 }}>
          <div style={{ fontWeight: 600, marginBottom: 8 }}>Top Links</div>
          {topLinks.length === 0 ? <div style={{ color: '#666' }}>No clicks yet</div> :
            topLinks.map((t, idx) => {
              const pct = maxClicks > 0 ? Math.round((t.clicks / maxClicks) * 100) : 0;
              return (
                <div key={idx} style={{ marginBottom: 8 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 13 }}>
                    <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 520 }}>{t.url}</div>
                    <div style={{ marginLeft: 12, color: '#333' }}>{t.clicks}</div>
                  </div>
                  <div style={{ height: 8, background: '#f0f0f0', borderRadius: 6, marginTop: 6 }}>
                    <div style={{ width: `${pct}%`, height: '100%', borderRadius: 6, background: '#4a90e2' }} />
                  </div>
                </div>
              );
            })
          }
        </div>

        {/* Delivery Engine */}
        <div style={{ width: 420 }}>
          <div style={{ border: '1px solid #eee', padding: 12, borderRadius: 6 }}>
            <div style={{ fontWeight: 600, marginBottom: 8 }}>Delivery Engine</div>
            <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
              <div style={{ minWidth: 220 }}>
                <div style={{ fontSize: 12, color: '#555' }}>Queued (estimate)</div>
                <div style={{ fontSize: 18 }}>{queuedEstimate ?? '—'}</div>
              </div>

              <div style={{ minWidth: 220 }}>
                <div style={{ fontSize: 12, color: '#555' }}>Failure rate</div>
                <div style={{ fontSize: 18 }}>
                  {totals && totals.processed > 0 ? `${Math.round(((totals.failed || 0) / Math.max(1, totals.processed)) * 1000) / 10}%` : '—'}
                </div>
                <div style={{ fontSize: 11, color: '#666' }}>{`(failed ${totals?.failed ?? 0} / processed ${totals?.processed ?? 0})`}</div>
              </div>
            </div>

            <div style={{ marginTop: 8, fontSize: 12, color: '#666' }}>
              <div>Last reconcile: {lastReconcileAt ?? 'never'}</div>
              <div>Engine loading: {engineLoading ? 'yes' : 'no'}</div>
            </div>

            <div style={{ marginTop: 8, display: 'flex', gap: 8 }}>
              <button onClick={reconcileNow} disabled={engineLoading || actionInProgress}>Reconcile now</button>
              <button onClick={forceRequeuePending} disabled={engineLoading || actionInProgress}>Force requeue pending</button>
              <button onClick={fetchQueueSnapshot} disabled={engineLoading || actionInProgress}>Queue snapshot</button>
            </div>
          </div>

          {/* Job / SSE Log */}
          <div style={{ marginTop: 12 }}>
            <h4 style={{ marginBottom: 8 }}>Recent job / SSE log (diagnostic)</h4>
            <div style={{ maxHeight: 240, overflow: 'auto', border: '1px solid #eee', padding: 8, borderRadius: 6, background: '#fafafa' }}>
              {sseLog.length === 0 ? <div style={{ fontSize: 12, color: '#666' }}>No recent events</div> :
                sseLog.map((l, idx) => (
                  <div key={idx} style={{ borderBottom: '1px solid #f0f0f0', padding: '6px 0' }}>
                    <div style={{ fontSize: 11, color: '#333' }}><strong>{l.type}</strong> • {new Date(l.ts).toLocaleString()}</div>
                    <pre style={{ margin: '6px 0 0', whiteSpace: 'pre-wrap', fontSize: 12 }}>{typeof l.payload === 'string' ? l.payload : JSON.stringify(l.payload, null, 2)}</pre>
                  </div>
                ))
              }
            </div>
          </div>
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
          <button onClick={() => { loadContacts(); loadInsight(); }} disabled={tableLoading || refreshing}>Refresh</button>
        </div>
      </div>

      {/* Table */}
      <div style={{ marginTop: 12, border: '1px solid #ddd', borderRadius: 6, padding: 12, position: 'relative' }}>
        <div style={{ marginBottom: 8 }}>
          <strong>Total:</strong> {total} • <strong>Page:</strong> {pageQuery} / {pages}
        </div>

        {/* Keep table present at all times to avoid layout shifts; show subtle overlay when loading */}
        <div style={{ opacity: tableLoading ? 0.6 : 1, transition: 'opacity 120ms linear' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #ccc' }}>
                <th style={{ textAlign: 'left', padding: 8 }}>Email / ContactId</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Status</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Attempts</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Last Attempt</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Last Known Error</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {items.length === 0 ? <tr><td colSpan={6} style={{ padding: 12, textAlign: 'center' }}>No records</td></tr> :
                items.map(it => {
                  const bgDone = (typeof it.bgAttempts === 'number') ? it.bgAttempts >= maxAttempts : true;
                  const canManualRetry = it.status === 'failed' && (it.attempts < maxAttempts) && bgDone;

                  return (
                    <tr key={it.id} style={{ borderBottom: '1px solid #eee' }}>
                      <td style={{ padding: 8 }}>
                        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                          <div style={{ minWidth: 320, overflow: 'hidden' }}>
                            <div style={{ fontSize: 14, fontWeight: 500 }}>{it.email ?? it.contactId ?? '—'}</div>
                            <div style={{ fontSize: 12, color: '#666' }}>{it.contactId ?? ''}</div>
                          </div>

                          <div style={{ display: 'flex', gap: 6 }}>
                            {it.opened ? (
                              <div title={it.lastOpenAt ? `Opened: ${new Date(it.lastOpenAt).toLocaleString()}` : 'Opened'} style={{ background: '#e6ffed', color: '#0f8a3f', padding: '4px 8px', borderRadius: 999, fontSize: 12 }}>Opened</div>
                            ) : null}
                            {it.clicked ? (
                              <div title={it.lastClickAt ? `Clicked: ${new Date(it.lastClickAt).toLocaleString()}` : 'Clicked'} style={{ background: '#eef6ff', color: '#0366d6', padding: '4px 8px', borderRadius: 999, fontSize: 12 }}>Clicked</div>
                            ) : null}
                          </div>
                        </div>
                      </td>
                      <td style={{ padding: 8 }}>{it.status ?? '—'}</td>
                      <td style={{ padding: 8 }}>
                        {it.attempts} / {maxAttempts}
                        {typeof it.bgAttempts === 'number' ? <div style={{ fontSize: 11, color: '#666' }}>bg: {it.bgAttempts} / {maxAttempts}</div> : null}
                      </td>
                      <td style={{ padding: 8 }}>{it.lastAttemptAt ? new Date(it.lastAttemptAt).toLocaleString() : '—'}</td>
                      <td style={{ padding: 8, color: 'red' }}>{it.lastError ?? '—'}</td>
                      <td style={{ padding: 8 }}>
                        <button disabled={actionInProgress || !canManualRetry} onClick={() => retryContact(it.contactId)} title={!canManualRetry ? (it.attempts >= maxAttempts ? 'Reached max attempts' : 'Background retries in progress or not failed') : 'Retry this contact'}>Retry</button>
                      </td>
                    </tr>
                  );
                })
              }
            </tbody>
          </table>
        </div>

        {/* overlay loader to avoid flicker when tableLoading */}
        {tableLoading && (
          <div style={{
            position: 'absolute', left: 12, right: 12, top: 60, bottom: 12,
            display: 'flex', alignItems: 'center', justifyContent: 'center', pointerEvents: 'none'
          }}>
            <div style={{
              background: 'rgba(255,255,255,0.9)', padding: 8, borderRadius: 6, boxShadow: '0 2px 6px rgba(0,0,0,0.08)',
              fontSize: 13, color: '#333', pointerEvents: 'none'
            }}>
              Loading contacts…
            </div>
          </div>
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
