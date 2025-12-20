import { useEffect, useState, useRef } from 'react';
import { useRouter } from 'next/router';

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

  // Page-level state
  const [loading, setLoading] = useState(true);
  const [campaign, setCampaign] = useState<Campaign | null>(null);
  const [totals, setTotals] = useState<Totals | null>(null);
  const [breakdown, setBreakdown] = useState<Breakdown | null>(null);
  const [failures, setFailures] = useState<FailureSample[]>([]);
  const [error, setError] = useState<string | null>(null);

  // UX state
  const [refreshing, setRefreshing] = useState(false);
  const initialLoadDone = useRef(false);

  async function loadInsight(options?: {
    silent?: boolean;
    manual?: boolean;
  }) {
    if (!id) return;

    const silent = options?.silent === true;
    const manual = options?.manual === true;

    if (!silent && !initialLoadDone.current) {
      setLoading(true);
    }

    if (manual) {
      setRefreshing(true);
    }

    setError(null);

    try {
      const res = await fetch(`/api/campaign/${id}/insight`);
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err?.error || 'Failed to load campaign insight');
      }

      const data = await res.json();

      setCampaign(data.campaign);
      setTotals(data.totals);
      setBreakdown(data.breakdown);
      setFailures(data.recentFailures || []);
    } catch (e: any) {
      console.error(e);
      setError(e.message);
    } finally {
      if (!silent && !initialLoadDone.current) {
        initialLoadDone.current = true;
        setLoading(false);
      }
      if (manual) {
        setRefreshing(false);
      }
    }
  }

  useEffect(() => {
    if (!id) return;

    // Initial load
    loadInsight();

    // Silent background refresh
    const interval = setInterval(() => {
      loadInsight({ silent: true });
    }, 5000);

    return () => clearInterval(interval);
  }, [id]);

  if (loading && !initialLoadDone.current) {
    return <div style={{ padding: 24 }}>Loading campaign insight…</div>;
  }

  if (error) {
    return (
      <div style={{ padding: 24, color: 'red' }}>
        Error: {error}
      </div>
    );
  }

  if (!campaign) {
    return <div style={{ padding: 24 }}>Campaign not found.</div>;
  }

  return (
    <div style={{ padding: 24, fontFamily: 'sans-serif', maxWidth: 800 }}>
      <h1>Campaign Insight: {campaign.name}</h1>

      <div style={{ marginBottom: 16 }}>
        <strong>Status:</strong> {campaign.status} <br />
        <strong>Created:</strong>{' '}
        {new Date(campaign.createdAt).toLocaleString()} <br />
        {campaign.completedAt && (
          <>
            <strong>Completed:</strong>{' '}
            {new Date(campaign.completedAt).toLocaleString()}
          </>
        )}
      </div>

      {totals && (
        <div
          style={{
            marginBottom: 16,
            border: '1px solid #ddd',
            padding: 12,
            borderRadius: 6,
          }}
        >
          <h3>Totals</h3>
          <div>Intended: {totals.intended}</div>
          <div>Processed: {totals.processed}</div>
          <div>Sent: {totals.sent}</div>
          <div>Failed: {totals.failed}</div>
        </div>
      )}

      {breakdown && (
        <div
          style={{
            marginBottom: 16,
            border: '1px solid #ddd',
            padding: 12,
            borderRadius: 6,
          }}
        >
          <h3>Current Breakdown</h3>
          <div>Pending: {breakdown.pending}</div>
          <div>Sent: {breakdown.sent}</div>
          <div>Failed: {breakdown.failed}</div>
        </div>
      )}

      {failures.length > 0 && (
        <div
          style={{
            marginBottom: 16,
            border: '1px solid #ddd',
            padding: 12,
            borderRadius: 6,
          }}
        >
          <h3>Recent Failures (last {failures.length})</h3>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #ccc' }}>
                <th style={{ textAlign: 'left', padding: 6 }}>Email</th>
                <th style={{ textAlign: 'left', padding: 6 }}>Attempts</th>
                <th style={{ textAlign: 'left', padding: 6 }}>
                  Last Attempt
                </th>
                <th style={{ textAlign: 'left', padding: 6 }}>Error</th>
              </tr>
            </thead>
            <tbody>
              {failures.map((f) => (
                <tr
                  key={f.contactId}
                  style={{ borderBottom: '1px solid #eee' }}
                >
                  <td style={{ padding: 6 }}>{f.email}</td>
                  <td style={{ padding: 6 }}>{f.attempts}</td>
                  <td style={{ padding: 6 }}>
                    {new Date(f.lastAttemptAt).toLocaleString()}
                  </td>
                  <td style={{ padding: 6, color: 'red' }}>
                    {f.error ?? '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <button
        onClick={() => loadInsight({ manual: true })}
        disabled={refreshing}
        style={{
          padding: '8px 16px',
          borderRadius: 4,
          border: '1px solid #888',
          opacity: refreshing ? 0.6 : 1,
          cursor: refreshing ? 'not-allowed' : 'pointer',
        }}
      >
        {refreshing ? 'Refreshing…' : 'Refresh'}
      </button>
    </div>
  );
}
