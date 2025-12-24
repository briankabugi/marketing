// pages/campaigns/[id].tsx

import react from 'react';
import { useRouter } from 'next/router';
import Link from 'next/link';

type Campaign = {
  id: string;
  name: string;
  status: string;
  createdAt: string;
  completedAt?: string | null;
};

type Totals = {
  intended: number;
  processed: number;
  sent: number;
  failed: number;
};

type Breakdown = {
  pending: number;
  sent: number;
  failed: number;
};

type FailureSample = {
  contactId: string;
  email: string;
  attempts: number;
  lastAttemptAt: string;
  error?: string;
};

export default function CampaignInsight() {
  const router = useRouter();
  const { id } = router.query;

  const [loading, setLoading] = react.useState(true);
  const [campaign, setCampaign] = react.useState<Campaign | null>(null);
  const [totals, setTotals] = react.useState<Totals | null>(null);
  const [breakdown, setBreakdown] = react.useState<Breakdown | null>(null);
  const [failures, setFailures] = react.useState<FailureSample[]>([]);
  const [error, setError] = react.useState<string | null>(null);

  const [refreshing, setRefreshing] = react.useState(false);
  const [actionInProgress, setActionInProgress] = react.useState(false);
  const initialLoadDone = react.useRef(false);

  async function loadInsight(options?: { silent?: boolean; manual?: boolean }) {
    if (!id) return;
    const silent = options?.silent === true;
    const manual = options?.manual === true;

    if (!silent && !initialLoadDone.current) setLoading(true);
    if (manual) setRefreshing(true);
    setError(null);

    try {
      const res = await fetch(`/api/campaign/${id}/insight`);
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err?.error || 'Failed to load campaign insight');
      }

      const data = await res.json();
      setCampaign(data.campaign);
      setBreakdown(data.breakdown);
      setFailures(data.recentFailures || []);

      // --- Use breakdown to compute totals for consistency ---
      setTotals({
        intended: data.totals.intended,
        processed: data.breakdown.sent + data.breakdown.failed,
        sent: data.breakdown.sent,
        failed: data.breakdown.failed,
      });
    } catch (e: any) {
      console.error(e);
      setError(e.message);
    } finally {
      if (!silent && !initialLoadDone.current) {
        initialLoadDone.current = true;
        setLoading(false);
      }
      if (manual) setRefreshing(false);
    }
  }

  react.useEffect(() => {
    if (!id) return;
    loadInsight();
    const interval = setInterval(() => loadInsight({ silent: true }), 5000);
    return () => clearInterval(interval);
  }, [id]);

  async function updateCampaign(action: 'pause' | 'resume' | 'cancel' | 'delete') {
    if (!id || actionInProgress) return;
    if (action === 'delete') {
      if (!confirm('Delete campaign permanently? This cannot be undone.')) return;
    }

    setActionInProgress(true);
    try {
      const payload: any = { action };
      if (action === 'delete') payload.confirm = true;

      const res = await fetch(`/api/campaign/${id}/control`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: 'Unknown' }));
        throw new Error(err.error || JSON.stringify(err));
      }

      const body = await res.json();
      if (action === 'delete') {
        router.push('/');
        return;
      }
      const newStatus = body.action === 'resumed' ? 'running' : body.action;
      setCampaign((prev) => (prev ? { ...prev, status: newStatus } : prev));
      loadInsight({ manual: true });
    } catch (err: any) {
      alert(err.message);
    } finally {
      setActionInProgress(false);
    }
  }

  if (loading && !initialLoadDone.current) return <div style={{ padding: 24 }}>Loading campaign insight…</div>;
  if (error) return <div style={{ padding: 24, color: 'red' }}>Error: {error}</div>;
  if (!campaign) return <div style={{ padding: 24 }}>Campaign not found.</div>;

  return (
    <div style={{ padding: 24, fontFamily: 'sans-serif', maxWidth: 800 }}>
      <h1>Campaign Insight: {campaign.name}</h1>
      <Link href="/" className="mb-4">Back to HomePage</Link>

      <div style={{ marginBottom: 16 }}>
        <strong>Status:</strong> {campaign.status} <br />
        <strong>Created:</strong> {new Date(campaign.createdAt).toLocaleString()} <br />
        {campaign.completedAt && (
          <>
            <strong>Completed:</strong> {new Date(campaign.completedAt).toLocaleString()}
          </>
        )}
      </div>

      {/* Control Buttons */}
      <div style={{ marginBottom: 16 }}>
        {campaign.status === 'running' && <button onClick={() => updateCampaign('pause')} disabled={actionInProgress} style={{ marginRight: 8 }}>Pause</button>}
        {campaign.status === 'paused' && <button onClick={() => updateCampaign('resume')} disabled={actionInProgress} style={{ marginRight: 8 }}>Resume</button>}
        {['running', 'paused'].includes(campaign.status) && <button onClick={() => updateCampaign('cancel')} disabled={actionInProgress} style={{ marginRight: 8, color: 'red' }}>Cancel</button>}
        <button onClick={() => updateCampaign('delete')} disabled={actionInProgress} style={{ color: 'red' }}>Delete</button>
      </div>

      {/* Totals */}
      {totals && (
        <div style={{ marginBottom: 16, border: '1px solid #ddd', padding: 12, borderRadius: 6 }}>
          <h3>Totals</h3>
          <div>Intended: {totals.intended}</div>
          <div>Processed: {totals.processed}</div>
          <div>Sent: {totals.sent}</div>
          <div>Failed: {totals.failed}</div>
        </div>
      )}

      {/* Breakdown */}
      {breakdown && (
        <div style={{ marginBottom: 16, border: '1px solid #ddd', padding: 12, borderRadius: 6 }}>
          <h3>Current Breakdown</h3>
          <div>Pending: {breakdown.pending}</div>
          <div>Sent: {breakdown.sent}</div>
          <div>Failed: {breakdown.failed}</div>
        </div>
      )}

      {/* Recent Failures */}
      {failures.length > 0 && (
        <div style={{ marginBottom: 16, border: '1px solid #ddd', padding: 12, borderRadius: 6 }}>
          <h3>Recent Failures (last {failures.length})</h3>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #ccc' }}>
                <th style={{ textAlign: 'left', padding: 6 }}>Email</th>
                <th style={{ textAlign: 'left', padding: 6 }}>Attempts</th>
                <th style={{ textAlign: 'left', padding: 6 }}>Last Attempt</th>
                <th style={{ textAlign: 'left', padding: 6 }}>Error</th>
              </tr>
            </thead>
            <tbody>
              {failures.map(f => (
                <tr key={f.contactId} style={{ borderBottom: '1px solid #eee' }}>
                  <td style={{ padding: 6 }}>{f.email}</td>
                  <td style={{ padding: 6 }}>{f.attempts}</td>
                  <td style={{ padding: 6 }}>{new Date(f.lastAttemptAt).toLocaleString()}</td>
                  <td style={{ padding: 6, color: 'red' }}>{f.error ?? '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Refresh */}
      <button
        onClick={() => loadInsight({ manual: true })}
        disabled={refreshing || actionInProgress}
        style={{ padding: '8px 16px', borderRadius: 4, border: '1px solid #888', opacity: refreshing || actionInProgress ? 0.6 : 1, cursor: refreshing || actionInProgress ? 'not-allowed' : 'pointer' }}
      >
        {refreshing ? 'Refreshing…' : 'Refresh'}
      </button>
    </div>
  );
}