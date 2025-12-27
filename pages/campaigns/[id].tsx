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
  // replies
  replied?: boolean;
  lastReplyAt?: string | null;
  lastReplySnippet?: string | null;
  repliesCount?: number;
  // runtime SSE hints (may be present from SSE payloads)
  followupSkipped?: boolean;
  skippedReason?: string | null;
  skippedAt?: string | null;
  stepName?: string | null;

  // New model fields (step-scoped)
  followUpPlan?: Array<{
    index?: number | null;
    name?: string | null;
    rule?: string | null;
    status?: string | null;
    scheduledFor?: string | null;
    sentAt?: string | null;
    skippedAt?: string | null;
    skippedReason?: string | null;
    delayMinutes?: number | null;
  }>;
  followupsCompleted?: number; // aggregated followup_sent / followup_skipped
  nextFollowUpStep?: number | null; // 1-based index
  nextFollowUpAt?: string | null;
  followUpStatus?: string | null; // scheduled|done|skipped|...
  lastStepSentAt?: string | null;

  // current step scoped counters (explicit)
  currentStepIndex?: number | null;
  currentStepName?: string | null;
  currentStepAttempts?: number;
  currentStepBgAttempts?: number;
};

type InsightPayload = {
  campaign: {
    id: string;
    name: string;
    status: string;
    createdAt?: string;
    completedAt?: string | null;
    followUps?: Array<{
      name?: string | null;
      delayMinutes?: number;
      rule?: string | any;
      subject?: string;
      body?: string;
      attachments?: Array<{ name?: string; url?: string; contentType?: string | null; size?: number }>;
    }>;
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
  replies?: {
    total?: number;
    uniqueContacts?: number;
  };
};

type ReplyDoc = {
  _id?: string;
  fingerprint?: string;
  campaignId?: string;
  contactId?: string;
  from?: string;
  to?: string;
  subject?: string;
  text?: string | null;
  html?: string | null;
  messageId?: string | null;
  receivedAt?: string | Date;
  source?: string;
  attachments?: Array<{
    filename?: string;
    contentType?: string | null;
    size?: number;
    sha256?: string;
    content?: string | null;
  }>;
};

export default function CampaignInsight() {
  const router = useRouter();
  const { id } = router.query as { id?: string };

  const [campaign, setCampaign] = React.useState<any>(null);
  const [followUps, setFollowUps] = React.useState<any[]>([]);
  const [breakdown, setBreakdown] = React.useState<any>(null);
  const [totals, setTotals] = React.useState<any>(null);
  const [recentFailures, setRecentFailures] = React.useState<any[]>([]);
  const [maxAttempts, setMaxAttempts] = React.useState<number>(3);

  const [engagement, setEngagement] = React.useState<InsightPayload['engagement'] | null>(null);
  const [repliesSummary, setRepliesSummary] = React.useState<{ total?: number; uniqueContacts?: number } | null>(null);

  const [items, setItems] = React.useState<ContactRow[]>([]);
  const [total, setTotal] = React.useState<number>(0);
  const [pages, setPages] = React.useState<number>(1);
  const [tableLoading, setTableLoading] = React.useState(false);
  const [availableStatuses, setAvailableStatuses] = React.useState<string[]>([]);
  const [actionInProgress, setActionInProgress] = React.useState(false);
  const [refreshing, setRefreshing] = React.useState(false);

  // Replies modal state
  const [repliesModalOpen, setRepliesModalOpen] = React.useState(false);
  const [repliesModalContact, setRepliesModalContact] = React.useState<{ contactId: string | null; email: string | null } | null>(null);
  const [repliesForContact, setRepliesForContact] = React.useState<ReplyDoc[]>([]);
  const [loadingReplies, setLoadingReplies] = React.useState(false);

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

  function humanizeDelay(minutesMaybe?: number | null) {
    const minutes = Number(minutesMaybe ?? 0);
    if (!minutes || minutes <= 0) return '0 minutes';
    if (minutes < 60) return `${minutes} minute${minutes === 1 ? '' : 's'}`;
    const hours = minutes / 60;
    if (hours < 24) {
      const h = Math.round(hours * 10) / 10;
      return `${h} hour${h === 1 ? '' : 's'}`;
    }
    const days = Math.round((hours / 24) * 10) / 10;
    return `${days} day${days === 1 ? '' : 's'}`;
  }

  // Compute per-contact follow-up state derived from ledger + followUps array.
  // Prefer explicit ledger/currentStep fields when present (new model).
  function computeFollowupState(it: ContactRow) {
    const steps = followUps || [];
    if (!steps || steps.length === 0) {
      return {
        lastSentLabel: 'Initial',
        followupsSent: 0,
        nextIndex: null as number | null,
        nextStepLabel: '—',
        nextDueAt: null as Date | null,
        followupStatus: 'no_sequence' as 'no_sequence' | 'complete' | 'scheduled' | 'due' | 'skipped' | 'unknown',
        skipReason: null as string | null,
        nextRule: null as string | null,
      };
    }

    // Prefer explicit completed count if available
    let completed = typeof it.followupsCompleted === 'number' ? it.followupsCompleted : null;

    // Prefer ledger plan to compute completions if explicit completed not present
    const ledgerPlan = Array.isArray(it.followUpPlan) ? it.followUpPlan : null;
    if (completed === null && ledgerPlan) {
      completed = ledgerPlan.filter((p) => {
        if (!p) return false;
        const st = (p.status || '').toString().toLowerCase();
        if (st === 'sent' || !!p.sentAt) return true;
        return false;
      }).length;
    }

    // Fallback: legacy heuristic based on attempts/bgAttempts
    if (completed === null) {
      const sentCount = Math.max(it.attempts || 0, it.bgAttempts || 0);
      // ensure at least 1 (initial)
      const sent = Math.max(1, sentCount);
      completed = Math.max(0, sent - 1);
    }

    const followupsSent = completed;

    const lastSentLabel = followupsSent === 0 ? 'Initial' : `Follow-up ${followupsSent} (${(steps[followupsSent - 1]?.name) ?? 'step ' + followupsSent})`;

    // Next follow-up index in `steps` (0-based index into steps)
    const nextIndex = (typeof followupsSent === 'number' && followupsSent < steps.length) ? followupsSent : null;

    if (nextIndex === null) {
      return {
        lastSentLabel,
        followupsSent,
        nextIndex: null,
        nextStepLabel: '—',
        nextDueAt: null,
        followupStatus: 'complete' as const,
        skipReason: null,
        nextRule: null,
      };
    }

    const fu = steps[nextIndex];
    const nextStepLabel = `Follow-up ${nextIndex + 1}${fu?.name ? ` — ${fu.name}` : ''}`;
    const delayMinutes = Number(fu?.delayMinutes ?? 0);

    // Next due: prefer ledgerPlan[nextIndex].scheduledFor if present
    let nextDueAt: Date | null = null;
    try {
      if (ledgerPlan && ledgerPlan[nextIndex] && ledgerPlan[nextIndex].scheduledFor) {
        const d = new Date(ledgerPlan[nextIndex].scheduledFor as any);
        if (!isNaN(d.getTime())) nextDueAt = d;
      }
      // fallback: use lastStepSentAt or lastAttemptAt + delay
      if (!nextDueAt) {
        const lastTsStr = it.lastStepSentAt ?? it.lastAttemptAt ?? null;
        if (lastTsStr) {
          const lastTs = new Date(lastTsStr).getTime();
          if (!Number.isNaN(lastTs)) {
            nextDueAt = new Date(lastTs + Math.round(delayMinutes * 60_000));
          }
        }
      }
    } catch {
      nextDueAt = null;
    }

    // Determine rule type for next step
    let ruleType = 'always';
    try {
      if (typeof fu?.rule === 'string') ruleType = fu.rule;
      else if (fu && typeof fu.rule === 'object' && fu.rule?.type) ruleType = fu.rule.type;
    } catch {
      ruleType = 'always';
    }

    // Determine skip reason: prefer explicit ledger / API fields
    let skipReason: string | null = null;
    if (it.followUpStatus === 'skipped' && it.lastFollowupSkippedReason) {
      skipReason = it.lastFollowupSkippedReason;
    } else if (ledgerPlan) {
      const skipped = ledgerPlan[nextIndex] && (ledgerPlan[nextIndex].status === 'skipped' || ledgerPlan[nextIndex].skippedAt);
      if (skipped) {
        skipReason = ledgerPlan[nextIndex].skippedReason ?? 'skipped';
      }
    }

    // Also account for reply-based rules (no_reply/replied) with quick inference if possible
    if (!skipReason && ruleType === 'no_reply' && it.replied) {
      skipReason = 'replied';
    } else if (!skipReason && ruleType === 'replied' && !it.replied) {
      skipReason = 'requires-reply';
    }

    // Consider SSE-provided hint
    if (!skipReason && it.followupSkipped) {
      skipReason = it.skippedReason ?? 'rule';
    }

    // followUpStatus: prefer explicit if present
    let followupStatus: 'no_sequence' | 'complete' | 'scheduled' | 'due' | 'skipped' | 'unknown' = 'unknown';
    if (typeof it.followUpStatus === 'string') {
      const s = it.followUpStatus.toLowerCase();
      if (s === 'scheduled') followupStatus = 'scheduled';
      else if (s === 'done' || s === 'completed') followupStatus = 'complete';
      else if (s === 'skipped') followupStatus = 'skipped';
      else followupStatus = s as any;
    } else {
      if (skipReason) followupStatus = 'skipped';
      else if (!nextDueAt) followupStatus = 'scheduled';
      else {
        followupStatus = (Date.now() >= nextDueAt.getTime()) ? 'due' : 'scheduled';
      }
    }

    return {
      lastSentLabel,
      followupsSent,
      nextIndex,
      nextStepLabel,
      nextDueAt,
      followupStatus,
      skipReason,
      nextRule: ruleType,
    };
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
      setRepliesSummary(body.replies ?? null);

      // set followups if present
      setFollowUps(body.campaign?.followUps ?? []);

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

      // ensure attempts/bgAttempts are normalized and include new step-scoped fields
      const normalizedItems = (body.items || []).map((it: any) => {
        const repliedFromApi = !!(it.replied || it.repliesCount || it.lastReplyAt);
        return {
          id: it.id,
          contactId: it.contactId ?? null,
          email: it.email ?? null,
          status: it.status ?? null,
          // 'attempts' under new model is the active step attempts; ensure sensible default
          attempts: (typeof it.attempts === 'number' ? Math.max(1, it.attempts) : 1),
          lastAttemptAt: it.lastAttemptAt ?? null,
          lastError: it.lastError ?? null,
          bgAttempts: (typeof it.bgAttempts === 'number' ? Math.max(0, it.bgAttempts) : (typeof it.bgAttempts === 'undefined' ? 0 : Number(it.bgAttempts))),
          opened: !!it.opened,
          clicked: !!it.clicked,
          lastOpenAt: it.lastOpenAt ?? null,
          lastClickAt: it.lastClickAt ?? null,
          replied: repliedFromApi,
          lastReplyAt: it.lastReplyAt ?? null,
          lastReplySnippet: it.lastReplySnippet ?? null,
          repliesCount: typeof it.repliesCount === 'number' ? it.repliesCount : (repliedFromApi ? 1 : 0),
          followupSkipped: !!it.followupSkipped,
          skippedReason: it.skippedReason ?? null,
          skippedAt: it.skippedAt ?? null,
          stepName: it.stepName ?? null,

          // New model / ledger-backed fields
          followUpPlan: Array.isArray(it.followUpPlan) ? it.followUpPlan.map((p: any) => ({
            index: p?.index ?? null,
            name: p?.name ?? null,
            rule: p?.rule ?? null,
            status: p?.status ?? null,
            scheduledFor: p?.scheduledFor ? String(p.scheduledFor) : null,
            sentAt: p?.sentAt ? String(p.sentAt) : null,
            skippedAt: p?.skippedAt ? String(p.skippedAt) : null,
            skippedReason: p?.skippedReason ?? null,
            delayMinutes: (typeof p?.delayMinutes === 'number') ? p.delayMinutes : (p?.delayMinutes ? Number(p.delayMinutes) : null),
          })) : undefined,

          followupsCompleted: typeof it.followupsCompleted === 'number' ? it.followupsCompleted : undefined,
          nextFollowUpStep: (typeof it.nextFollowUpStep === 'number' || it.nextFollowUpStep == null) ? it.nextFollowUpStep : undefined,
          nextFollowUpAt: it.nextFollowUpAt ?? null,
          followUpStatus: it.followUpStatus ?? null,
          lastStepSentAt: it.lastStepSentAt ?? null,

          currentStepIndex: (typeof it.currentStepIndex === 'number' || it.currentStepIndex == null) ? it.currentStepIndex : undefined,
          currentStepName: it.currentStepName ?? null,
          currentStepAttempts: typeof it.currentStepAttempts === 'number' ? it.currentStepAttempts : undefined,
          currentStepBgAttempts: typeof it.currentStepBgAttempts === 'number' ? it.currentStepBgAttempts : undefined,
        } as ContactRow;
      }) as ContactRow[];

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
              let replied = row.replied ?? false;
              let lastReplyAt = row.lastReplyAt ?? null;
              let lastReplySnippet = row.lastReplySnippet ?? null;
              let repliesCount = row.repliesCount ?? 0;

              // Normalize event timestamp fields robustly:
              const normalizedOpenTs =
                payload.openedAt ?? payload.lastOpenAt ?? payload.ts ?? payload.opened_at ?? null;
              const normalizedClickTs =
                payload.clickedAt ?? payload.lastClickAt ?? payload.ts ?? payload.clicked_at ?? null;
              const normalizedReplyTs =
                payload.repliedAt ?? payload.replyAt ?? payload.ts ?? null;

              if (payload.event === 'open' || normalizedOpenTs) {
                opened = true;
                lastOpenAt = String(normalizedOpenTs ?? new Date().toISOString());
              }
              if (payload.event === 'click' || normalizedClickTs) {
                clicked = true;
                lastClickAt = String(normalizedClickTs ?? new Date().toISOString());
              }
              if (payload.event === 'reply' || normalizedReplyTs || payload.repliesCount) {
                replied = true;
                lastReplyAt = String(normalizedReplyTs ?? payload.lastReplyAt ?? new Date().toISOString());
                if (payload.snippet || payload.lastReplySnippet) lastReplySnippet = payload.snippet ?? payload.lastReplySnippet;
                if (typeof payload.repliesCount === 'number') repliesCount = payload.repliesCount;
                else repliesCount = Math.max(1, repliesCount);
              }

              // followup skip hints from server
              const followupSkipped = typeof payload.followupSkipped === 'boolean' ? payload.followupSkipped : row.followupSkipped;
              const skippedReason = payload.skippedReason ?? row.skippedReason ?? null;
              const skippedAt = payload.skippedAt ?? row.skippedAt ?? null;
              const stepName = payload.stepName ?? row.stepName ?? null;

              // Merge new model fields if present in SSE payload
              const mergedFollowUpPlan = payload.followUpPlan ? payload.followUpPlan : row.followUpPlan;
              const mergedFollowupsCompleted = (typeof payload.followupsCompleted === 'number') ? payload.followupsCompleted : row.followupsCompleted;
              const mergedNextFollowUpAt = payload.nextFollowUpAt ?? row.nextFollowUpAt;
              const mergedFollowUpStatus = payload.followUpStatus ?? row.followUpStatus;
              const mergedLastStepSentAt = payload.lastStepSentAt ?? row.lastStepSentAt;

              const mergedCurrentStepIndex = (typeof payload.currentStepIndex === 'number') ? payload.currentStepIndex : (typeof row.currentStepIndex === 'number' ? row.currentStepIndex : undefined);
              const mergedCurrentStepName = payload.currentStepName ?? row.currentStepName;
              const mergedCurrentStepAttempts = typeof payload.currentStepAttempts === 'number' ? payload.currentStepAttempts : row.currentStepAttempts;
              const mergedCurrentStepBgAttempts = typeof payload.currentStepBgAttempts === 'number' ? payload.currentStepBgAttempts : row.currentStepBgAttempts;

              // Some publishers might send boolean flags directly
              if (typeof payload.opened === 'boolean') opened = payload.opened;
              if (typeof payload.clicked === 'boolean') clicked = payload.clicked;
              if (typeof payload.replied === 'boolean') replied = payload.replied;
              if (payload.lastOpenAt) lastOpenAt = payload.lastOpenAt;
              if (payload.lastClickAt) lastClickAt = payload.lastClickAt;
              if (payload.lastReplyAt) lastReplyAt = payload.lastReplyAt;
              if (payload.lastReplySnippet) lastReplySnippet = payload.lastReplySnippet;
              if (typeof payload.repliesCount === 'number') repliesCount = payload.repliesCount;

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
                replied,
                lastReplyAt,
                lastReplySnippet,
                repliesCount,
                followupSkipped,
                skippedReason,
                skippedAt,
                stepName,

                // merge step-scoped fields
                followUpPlan: mergedFollowUpPlan,
                followupsCompleted: mergedFollowupsCompleted,
                nextFollowUpAt: mergedNextFollowUpAt,
                followUpStatus: mergedFollowUpStatus,
                lastStepSentAt: mergedLastStepSentAt,

                currentStepIndex: mergedCurrentStepIndex,
                currentStepName: mergedCurrentStepName,
                currentStepAttempts: mergedCurrentStepAttempts,
                currentStepBgAttempts: mergedCurrentStepBgAttempts,
              };
            }
            return row;
          });

          if (!matched) {
            scheduleSilentTableRefresh();
          } else {
            loadInsight(true);
          }

          // push to SSE log
          const logEntry = {
            ts: new Date().toISOString(),
            type: 'contact',
            payload,
          };
          sseLogRef.current = [logEntry, ...sseLogRef.current].slice(0, 200);
          setSseLog(sseLogRef.current);

          return next;
        });
      } catch (e) {
        console.warn('Malformed contact SSE payload', e);
      }
    });

    es.addEventListener('campaign', (ev: any) => {
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

    es.addEventListener('reply', (ev: any) => {
      // Some servers might publish reply events separately; handle defensively
      try {
        let payloadRaw = ev.data;
        let payload: any;
        try { payload = JSON.parse(payloadRaw); } catch { payload = { raw: String(payloadRaw) }; }

        const updatedContactId = payload.contactId ?? payload.contact?.contactId ?? null;
        if (!updatedContactId) {
          scheduleSilentTableRefresh();
          return;
        }

        const contactIdStr = String(updatedContactId);
        setItems(prev => {
          let matched = false;
          const next = prev.map(row => {
            const rowContactId = row.contactId != null ? String(row.contactId) : null;
            const rowIdStr = row.id != null ? String(row.id) : null;
            if ((rowContactId && rowContactId === contactIdStr) || (rowIdStr && rowIdStr === contactIdStr)) {
              matched = true;
              const repliesCount = Math.max(1, (typeof payload.repliesCount === 'number' ? payload.repliesCount : (row.repliesCount ?? 0) + 1));
              return {
                ...row,
                replied: true,
                lastReplyAt: payload.receivedAt ?? payload.repliedAt ?? new Date().toISOString(),
                lastReplySnippet: payload.snippet ?? payload.lastReplySnippet ?? row.lastReplySnippet,
                repliesCount,
              };
            }
            return row;
          });
          if (!matched) scheduleSilentTableRefresh(); else loadInsight(true);
          const logEntry = { ts: new Date().toISOString(), type: 'reply', payload };
          sseLogRef.current = [logEntry, ...sseLogRef.current].slice(0, 200);
          setSseLog(sseLogRef.current);
          return next;
        });
      } catch (e) {
        console.warn('Malformed reply SSE payload', e);
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
        loadContacts({ status: next.status ?? 'all', page: Number(next.page ?? 1), pageSize: Number(next.pageSize ?? 25), silent: true });
      })
      .catch(() => {
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

  // ---- Replies UI ----
  async function openRepliesModalFor(row: ContactRow) {
    setRepliesModalOpen(true);
    setRepliesModalContact({ contactId: row.contactId, email: row.email });
    setRepliesForContact([]);
    setLoadingReplies(true);
    try {
      // Attempt multiple query strategies:
      let url = `/api/campaign/${id}/replies?`;
      if (row.contactId) url += `contactId=${encodeURIComponent(String(row.contactId))}`;
      else url += `contactId=${encodeURIComponent(String(row.id))}`;
      const res = await fetch(url);
      if (!res.ok) {
        const body = await safeJson(res).catch(() => ({ error: 'Unknown' }));
        throw new Error((body && (body as any).error) ? (body as any).error : `Failed to fetch replies (${res.status})`);
      }
      const body = await res.json();
      const docs: ReplyDoc[] = Array.isArray(body.items) ? body.items : (Array.isArray(body) ? body : []);
      setRepliesForContact(docs);
    } catch (e) {
      console.error('Failed loading replies for contact', e);
      setRepliesForContact([]);
      alert('Failed to load replies: ' + ((e as any).message ?? String(e)));
    } finally {
      setLoadingReplies(false);
    }
  }

  function closeRepliesModal() {
    setRepliesModalOpen(false);
    setRepliesModalContact(null);
    setRepliesForContact([]);
  }

  // Helper to render attachment download link; will create data URL if inline base64 provided
  function renderAttachmentLink(att: any) {
    const filename = att.filename || att.name || 'attachment';
    const contentType = att.contentType || att.mimeType || 'application/octet-stream';
    const size = att.size ? `${Math.round(att.size / 1024)} KB` : null;

    if (att.content && typeof att.content === 'string') {
      // assume base64 content
      const href = `data:${contentType};base64,${att.content}`;
      return (<a href={href} download={filename}>{filename}{size ? ` (${size})` : ''}</a>);
    }

    if (att.url) {
      return (<a href={att.url} target="_blank" rel="noreferrer">{filename}{size ? ` (${size})` : ''}</a>);
    }

    // fallback: only metadata available
    return (<span title={att.sha256 || ''}>{filename}{size ? ` (${size})` : ''}</span>);
  }

  // UI render
  const statusOptions = (availableStatuses.length > 0 ? ['all', ...availableStatuses] : ['all', 'pending', 'sending', 'sent', 'failed', 'bounced', 'cancelled']).concat(['replied', 'no_reply']);

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

      {/* Follow-ups summary */}
      <div style={{ marginTop: 12 }}>
        <div style={{ border: '1px solid #eee', padding: 12, borderRadius: 6 }}>
          <div style={{ fontWeight: 600, marginBottom: 8 }}>Follow-up Sequence</div>
          {followUps.length === 0 ? (
            <div style={{ color: '#666' }}>No follow-ups configured for this campaign.</div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {followUps.map((fu, idx) => (
                <div key={idx} style={{ border: '1px solid #f0f0f0', padding: 8, borderRadius: 6, display: 'flex', gap: 12 }}>
                  <div style={{ minWidth: 120 }}>
                    <div style={{ fontSize: 12, color: '#666' }}>Step</div>
                    <div style={{ fontWeight: 600 }}>{idx + 1}{fu.name ? ` — ${fu.name}` : ''}</div>
                  </div>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 13 }}>{fu.subject ?? (fu.body ? (String(fu.body).slice(0, 120) + (String(fu.body).length > 120 ? '…' : '')) : '—')}</div>
                    <div style={{ marginTop: 6, fontSize: 12, color: '#666' }}>
                      Rule: {typeof fu.rule === 'string' ? fu.rule : (fu.rule?.type ?? 'always')} • Delay: {humanizeDelay(Number(fu.delayMinutes || 0))}
                    </div>
                    {fu.attachments && fu.attachments.length > 0 && (
                      <div style={{ marginTop: 6 }}>
                        <div style={{ fontSize: 12, color: '#666' }}>Attachments:</div>
                        <ul>
                          {fu.attachments.map((a: any, ai: number) => (
                            <li key={ai}>
                              {a.url ? <a href={a.url} target="_blank" rel="noreferrer">{a.name || a.filename || String(a.url)}</a> : (a.name || a.filename || 'attachment')}
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
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

      {/* Top links heatmap & Delivery Engine */}
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

        <div style={{ opacity: tableLoading ? 0.6 : 1, transition: 'opacity 120ms linear' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #ccc' }}>
                <th style={{ textAlign: 'left', padding: 8 }}>Email / ContactId</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Status</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Attempts</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Step</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Next follow-up</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Next FU status</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Last Attempt</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Last Known Error</th>
                <th style={{ textAlign: 'left', padding: 8 }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {items.length === 0 ? <tr><td colSpan={9} style={{ padding: 12, textAlign: 'center' }}>No records</td></tr> :
                items.map(it => {
                  // prefer currentStepBgAttempts when available
                  const bgDone = (typeof it.currentStepBgAttempts === 'number') ? it.currentStepBgAttempts >= maxAttempts : (typeof it.bgAttempts === 'number' ? it.bgAttempts >= maxAttempts : true);
                  // can manual retry when status failed and attempts < maxAttempts (attempts is active step attempts)
                  const canManualRetry = it.status === 'failed' && (it.attempts < maxAttempts) && bgDone;

                  const fuState = computeFollowupState(it);
                  const nextDueStr = fuState.nextDueAt ? new Date(fuState.nextDueAt).toLocaleString() : (fuState.nextIndex === null ? '—' : 'unknown');

                  // status badge for next FU
                  let fuBadge = '';
                  if (fuState.followupStatus === 'complete') fuBadge = 'Complete';
                  else if (fuState.followupStatus === 'no_sequence') fuBadge = 'No sequence';
                  else if (fuState.followupStatus === 'skipped') fuBadge = `Skipped${fuState.skipReason ? ` (${fuState.skipReason})` : ''}`;
                  else if (fuState.followupStatus === 'scheduled') fuBadge = `Scheduled • ${nextDueStr}`;
                  else if (fuState.followupStatus === 'due') fuBadge = 'Due';
                  else fuBadge = '—';

                  return (
                    <tr key={it.id} style={{ borderBottom: '1px solid #eee' }}>
                      <td style={{ padding: 8 }}>
                        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                          <div style={{ minWidth: 320, overflow: 'hidden' }}>
                            <div style={{ fontSize: 14, fontWeight: 500 }}>{it.email ?? it.contactId ?? '—'}</div>
                            <div style={{ fontSize: 12, color: '#666' }}>{it.contactId ?? ''}</div>
                            {it.replied && (
                              <div style={{ marginTop: 6 }}>
                                <span style={{ background: '#fff7e6', color: '#a66f00', padding: '4px 8px', borderRadius: 999, fontSize: 12 }}>Replied</span>
                                {it.repliesCount ? <span style={{ marginLeft: 8, fontSize: 12, color: '#666' }}>{it.repliesCount} reply(ies)</span> : null}
                                {it.lastReplyAt ? <div style={{ fontSize: 12, color: '#666', marginTop: 4 }}>Last reply: {new Date(it.lastReplyAt).toLocaleString()}</div> : null}
                                {it.lastReplySnippet ? <div style={{ marginTop: 6, fontSize: 13, color: '#333' }}>{String(it.lastReplySnippet).slice(0, 200)}{String(it.lastReplySnippet).length > 200 ? '…' : ''}</div> : null}
                              </div>
                            )}
                          </div>

                          <div style={{ display: 'flex', gap: 6, flexDirection: 'column' }}>
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
                        <div>{it.attempts} / {maxAttempts}</div>
                        {(typeof it.currentStepAttempts === 'number' || typeof it.currentStepBgAttempts === 'number') && (
                          <div style={{ fontSize: 11, color: '#666' }}>
                            {typeof it.currentStepAttempts === 'number' ? `step: ${it.currentStepAttempts}` : null}
                            {typeof it.currentStepBgAttempts === 'number' ? <span style={{ marginLeft: 8 }}>bg: {it.currentStepBgAttempts}</span> : null}
                          </div>
                        )}
                        {typeof it.bgAttempts === 'number' && (typeof it.currentStepBgAttempts !== 'number') ? <div style={{ fontSize: 11, color: '#666' }}>bg: {it.bgAttempts}</div> : null}
                      </td>

                      <td style={{ padding: 8 }}>
                        <div style={{ fontSize: 13 }}>{fuState.lastSentLabel}</div>
                        {it.currentStepName ? <div style={{ fontSize: 12, color: '#666', marginTop: 4 }}>Active: {it.currentStepName} (index {it.currentStepIndex ?? '—'})</div> : null}
                      </td>

                      <td style={{ padding: 8 }}>
                        <div style={{ fontSize: 13 }}>{fuState.nextStepLabel}</div>
                        {fuState.nextIndex !== null && followUps[fuState.nextIndex] && (
                          <div style={{ marginTop: 6, fontSize: 12, color: '#666' }}>
                            Delay: {humanizeDelay(Number(followUps[fuState.nextIndex].delayMinutes || 0))}
                            {fuState.nextRule ? <span style={{ marginLeft: 8 }}>Rule: {fuState.nextRule}</span> : null}
                          </div>
                        )}
                      </td>

                      <td style={{ padding: 8 }}>
                        <div style={{ fontSize: 13 }}>{fuBadge}</div>
                        {fuState.followupStatus === 'scheduled' && fuState.nextDueAt ? <div style={{ fontSize: 12, color: '#666', marginTop: 4 }}>Due: {new Date(fuState.nextDueAt).toLocaleString()}</div> : null}
                        {it.nextFollowUpAt && !fuState.nextDueAt ? <div style={{ fontSize: 12, color: '#666', marginTop: 4 }}>Next: {new Date(it.nextFollowUpAt).toLocaleString()}</div> : null}
                      </td>

                      <td style={{ padding: 8 }}>{it.lastAttemptAt ? new Date(it.lastAttemptAt).toLocaleString() : '—'}</td>
                      <td style={{ padding: 8, color: 'red' }}>{it.lastError ?? '—'}</td>
                      <td style={{ padding: 8 }}>
                        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                          <button disabled={actionInProgress || !canManualRetry} onClick={() => retryContact(it.contactId)} title={!canManualRetry ? (it.attempts >= maxAttempts ? 'Reached max attempts' : 'Background retries in progress or not failed') : 'Retry this contact'}>Retry</button>
                          <button onClick={() => openRepliesModalFor(it)} title="View replies for this contact">View replies</button>
                        </div>
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

      {/* Replies modal */}
      {repliesModalOpen && (
        <div style={{
          position: 'fixed', left: 0, right: 0, top: 0, bottom: 0, background: 'rgba(0,0,0,0.4)',
          display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000
        }}>
          <div style={{ width: '90%', maxWidth: 900, maxHeight: '90%', overflow: 'auto', background: '#fff', borderRadius: 8, padding: 16 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div>
                <strong>Replies for</strong>
                <div style={{ fontSize: 13, color: '#666' }}>{repliesModalContact?.email ?? repliesModalContact?.contactId ?? '—'}</div>
              </div>
              <div>
                <button onClick={closeRepliesModal}>Close</button>
              </div>
            </div>

            <div style={{ marginTop: 12 }}>
              {loadingReplies ? <div>Loading replies…</div> : (
                repliesForContact.length === 0 ? <div style={{ color: '#666' }}>No replies found.</div> :
                  repliesForContact.map((r, idx) => (
                    <div key={r._id ?? r.fingerprint ?? idx} style={{ border: '1px solid #eee', padding: 12, borderRadius: 6, marginBottom: 12 }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                        <div>
                          <div style={{ fontWeight: 600 }}>{r.subject ?? '(no subject)'}</div>
                          <div style={{ fontSize: 12, color: '#666' }}>{r.from} • {r.receivedAt ? new Date(r.receivedAt).toLocaleString() : ''}</div>
                        </div>
                        <div style={{ fontSize: 12, color: '#666' }}>{r.source ?? ''}</div>
                      </div>

                      {r.text ? (
                        <div style={{ marginTop: 8, whiteSpace: 'pre-wrap', fontFamily: 'monospace', fontSize: 13 }}>{r.text}</div>
                      ) : r.html ? (
                        <div style={{ marginTop: 8 }}>
                          <div dangerouslySetInnerHTML={{ __html: r.html ?? '' }} />
                        </div>
                      ) : null}

                      {r.attachments && r.attachments.length > 0 && (
                        <div style={{ marginTop: 8 }}>
                          <div style={{ fontWeight: 600 }}>Attachments</div>
                          <ul>
                            {r.attachments.map((a, ai) => (
                              <li key={ai}>
                                {renderAttachmentLink(a)}
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}
                    </div>
                  ))
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
