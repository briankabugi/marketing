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

  async function loadCampaigns() {
    const res = await fetch('/api/campaign/list');
    const data = await res.json();
    setCampaigns(data.campaigns);
    setLoading(false);
  }

  useEffect(() => {
    loadCampaigns();
    const i = setInterval(loadCampaigns, 2000); // simple live refresh
    return () => clearInterval(i);
  }, []);

  return (
    <div style={{ padding: 24, fontFamily: 'sans-serif' }}>
      <h1>Marketing MVP Dashboard</h1>

      {loading && <p>Loading campaigns…</p>}

      <Contacts/>

      <h2>Running Campaigns</h2>

      {campaigns.length === 0 && <p>No campaigns yet.</p>}

      {campaigns.map((c) => (
        <div
          key={c.id}
          style={{
            border: '1px solid #ddd',
            padding: 12,
            marginBottom: 10,
            borderRadius: 6,
          }}
        >
          <strong>{c.name}</strong>
          <div>Status: {c.status}</div>
          <div>
            Progress: {c.processed} / {c.total}
          </div>
          <div>Created: {new Date(c.createdAt).toLocaleString()}</div>
        </div>
      ))}
    </div>
  );
}