import { MetricsSkeleton, BusyLabel } from './LoadingState';
import { useEffect, useId, useLayoutEffect, useRef, useState, type RefObject } from 'react';
import { useQuery } from '@tanstack/react-query';
import { request } from '../http';
import { memoryBudgetSource, memoryBytes, memorySeries, memoryStatus, type MemorySnapshot } from '../memory';

const ranges = [{ seconds: 300, label: '5m' }, { seconds: 900, label: '15m' }, { seconds: 3600, label: '1h' },
  { seconds: 21600, label: '6h' }, { seconds: 86400, label: '24h' }];
const sampleTime = (us: number) => new Date(us / 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

function usePageVisible() {
  const [visible, setVisible] = useState(() => !document.hidden);
  useEffect(() => {
    const update = () => setVisible(!document.hidden);
    document.addEventListener('visibilitychange', update);
    return () => document.removeEventListener('visibilitychange', update);
  }, []);
  return visible;
}

export function MemoryMonitorButton() {
  const [open, setOpen] = useState(false), [windowSeconds, setWindowSeconds] = useState(900);
  const anchor = useRef<HTMLButtonElement>(null), panelId = useId();
  const visible = usePageVisible();
  const summary = useQuery({ queryKey: ['memory-current'], queryFn: ({ signal }) => request<MemorySnapshot>('/memory', signal),
    enabled: visible && !open, refetchInterval: 10_000, retry: false, gcTime: 30_000, refetchOnWindowFocus: true });
  const detail = useQuery({ queryKey: ['memory-history', windowSeconds],
    queryFn: ({ signal }) => request<MemorySnapshot>(`/memory?${new URLSearchParams({ window_seconds: String(windowSeconds), points: '600' })}`, signal),
    enabled: visible && open, retry: false, gcTime: 0, refetchOnWindowFocus: true,
    refetchInterval: query => query.state.data?.enabled === false ? 10_000 : Math.max(
      (query.state.data?.interval_seconds ?? 1) * 1000, windowSeconds >= 21600 ? 30_000 : windowSeconds >= 3600 ? 10_000 : 2000) });
  const snapshot = open ? detail.data ?? summary.data : summary.data;
  const error = open ? detail.error : summary.error;
  const status = memoryStatus(snapshot, Boolean(error));
  const pressure = error || snapshot?.stale ? 'unknown' : snapshot?.latest?.pressure ?? 'unknown';
  return <>
    <button ref={anchor} type="button" className={`memory-trigger memory-${pressure}`} aria-haspopup="dialog" aria-expanded={open}
      aria-controls={open ? panelId : undefined} aria-label="Open memory monitor" title={`Server memory · ${status}`}
      onClick={() => setOpen(value => !value)}>
      <span className="memory-status-dot" /><span className="memory-trigger-label">Memory</span>
      <strong>{snapshot?.enabled && snapshot.latest ? memoryBytes(snapshot.latest.process.rss_bytes) : status}</strong>
      <span className="memory-trigger-status">{snapshot?.enabled && snapshot.latest ? status : ''}</span>
    </button>
    {open ? <MemoryPanel id={panelId} anchor={anchor} snapshot={snapshot} error={error?.message} loading={detail.isFetching}
      windowSeconds={windowSeconds} onRange={setWindowSeconds} onRefresh={() => void detail.refetch()} onClose={() => setOpen(false)} /> : null}
  </>;
}

function MemoryPanel({ id, anchor, snapshot, error, loading, windowSeconds, onRange, onRefresh, onClose }: {
  id: string; anchor: RefObject<HTMLButtonElement | null>; snapshot?: MemorySnapshot; error?: string; loading: boolean;
  windowSeconds: number; onRange: (value: number) => void; onRefresh: () => void; onClose: () => void;
}) {
  const dialog = useRef<HTMLDialogElement>(null), title = useId();
  useLayoutEffect(() => {
    const popup = dialog.current, trigger = anchor.current;
    if (!popup || !trigger) return;
    const previous = document.activeElement;
    popup.showModal();
    const position = () => {
      const box = trigger.getBoundingClientRect();
      popup.style.left = `${Math.max(12, Math.min(box.right - popup.offsetWidth, window.innerWidth - popup.offsetWidth - 12))}px`;
      popup.style.top = `${Math.max(12, Math.min(box.bottom + 8, window.innerHeight - popup.offsetHeight - 12))}px`;
    };
    position();
    const observer = new ResizeObserver(position); observer.observe(popup); observer.observe(trigger);
    window.addEventListener('resize', position);
    return () => { observer.disconnect(); window.removeEventListener('resize', position); popup.close();
      if (previous instanceof HTMLElement && previous.isConnected) previous.focus({ preventScroll: true }); };
  }, [anchor]);
  const latest = snapshot?.latest, host = latest?.host, queue = host?.persistence, projections = host?.projections;
  return <dialog id={id} ref={dialog} className="memory-panel" aria-labelledby={title} onCancel={event => { event.preventDefault(); onClose(); }}
    onClick={event => { if (event.target !== dialog.current) return; const box = event.currentTarget.getBoundingClientRect();
      if (event.clientX < box.left || event.clientX > box.right || event.clientY < box.top || event.clientY > box.bottom) onClose(); }}>
    <header><div><h2 id={title}>Memory monitor</h2><span>Server process{latest ? ` · PID ${latest.pid}` : ''}</span></div>
      <div className="memory-panel-actions"><button type="button" onClick={onRefresh} disabled={loading} aria-label="Refresh memory monitor"><BusyLabel busy={loading && !!snapshot} label="Refreshing">Refresh</BusyLabel></button>
        <button type="button" onClick={onClose} aria-label="Close memory monitor">×</button></div></header>
    {error ? <p className="memory-notice" role="alert">{error} <button type="button" onClick={onRefresh}>Retry</button></p> : null}
    {!snapshot ? (error ? null : <MetricsSkeleton label="Loading memory telemetry" />) : !snapshot.enabled ? <div className="memory-empty"><h3>Monitoring is off</h3>
      <p>Set this in the project’s .env file, then restart the Server.</p><code>AUTOAGENT_MEMORY_MONITOR_ENABLED=true</code></div> : <>
      <div className={`memory-health memory-${error || snapshot.stale ? 'unknown' : latest?.pressure ?? 'unknown'}`}>
        <span className="memory-status-dot" /><strong>{memoryStatus(snapshot, Boolean(error))}</strong>
        <span>{snapshot.sample_age_seconds == null ? 'No sample yet' : `Sample ${snapshot.sample_age_seconds.toFixed(1)}s old`}</span>
        {snapshot.stale ? <span role="status">Waiting for a fresh sample</span> : null}</div>
      <dl className="memory-cards"><Metric label="Process RSS" value={memoryBytes(latest?.process.rss_bytes)} />
        <Metric label="Peak RSS" value={memoryBytes(latest?.process.peak_rss_bytes)} />
        <Metric label="Host budget" value={memoryBytes(latest?.budget_bytes)} />
        <Metric label="Budget used" value={latest?.budget_ratio == null ? 'Unavailable' : `${(latest.budget_ratio * 100).toFixed(1)}%`} /></dl>
      <p className="memory-caption">Budget: {memoryBudgetSource(latest?.resources.budget_source)}
        {latest?.resources.capacity_bytes != null ? ` · Effective capacity ${memoryBytes(latest.resources.capacity_bytes)}` : ''}.
        </p>
      <section className="memory-trend"><div className="memory-section-header"><h3>RSS history</h3><div className="memory-ranges" role="group" aria-label="Memory history range">
        {ranges.map(range => <button type="button" key={range.seconds} aria-pressed={windowSeconds === range.seconds} onClick={() => onRange(range.seconds)}>{range.label}</button>)}</div></div>
        <MemoryChart snapshot={snapshot} windowSeconds={windowSeconds} />
        <p className="memory-caption">{snapshot.history_count.toLocaleString()} samples</p>
      </section>
      <section><div className="memory-section-header"><h3>Host & persistence</h3><small>Cached component estimates</small></div>
        <dl className="memory-details"><Metric label="Resident graphs" value={host?.resident_graphs} /><Metric label="Checkpoint jobs" value={host?.checkpoint_jobs} />
          <Metric label="Writer queue" value={queue?.queued_count} /><Metric label="Queue memory" value={memoryBytes(queue?.retained_bytes)} />
          <Metric label="Writer state" value={queue?.state} /><Metric label="Uncommitted records" value={queue ? queue.accepted_frontier - queue.durable_frontier : undefined} />
          <Metric label="Projection cache" value={memoryBytes(projections?.bytes)} /><Metric label="Cache entries" value={projections?.entries} />
          <Metric label="State estimate" value={memoryBytes(host?.residency.estimated_bytes)} /><Metric label="Active requests" value={host?.requests} /></dl>
        {queue?.fault ? <p className="memory-notice" role="alert">Writer fault: {queue.fault}</p> : null}
        </section>
      {latest?.cgroup ? <section><div className="memory-section-header"><h3>Cgroup</h3><small>Shared usage, including other processes and file cache</small></div>
        <dl className="memory-details"><Metric label="Current" value={memoryBytes(latest.cgroup.current_bytes)} /><Metric label="Effective limit" value={memoryBytes(latest.cgroup.limit_bytes)} />
          <Metric label="Available headroom" value={memoryBytes(latest.cgroup.available_bytes)} />
          {latest.cgroup.limit_path && latest.cgroup.limit_path !== latest.cgroup.path ? <Metric label="Limit scope usage" value={memoryBytes(latest.cgroup.limit_scope_current_bytes)} /> : null}
          <Metric label="OOM kills" value={latest.cgroup.events.oom_kill} /></dl></section> : null}
      <section><div className="memory-section-header"><h3>System memory</h3><small>Shared with other applications</small></div>
        <dl className="memory-details"><Metric label="Physical capacity" value={memoryBytes(latest?.resources.system_total_bytes)} />
          <Metric label="Available" value={memoryBytes(latest?.resources.system_available_bytes)} />
          <Metric label="Shared capacity used" value={latest ? `${(latest.resources.shared_pressure_ratio * 100).toFixed(1)}%` : undefined} /></dl>
        </section>
      <section><div className="memory-section-header"><h3>Largest resident Sessions</h3><small>Cached estimates</small></div>
        {host?.residency.top_sessions.length ? <div className="memory-session-list">{host.residency.top_sessions.map(session => <div key={session.session_id}>
          <code title={session.session_id}>{session.session_id}</code><span>{memoryBytes(session.estimated_bytes)}</span>
          <small>{session.can_unload ? 'Unloadable' : 'In use'}</small></div>)}</div> : <p className="memory-caption">No Session estimates available yet.</p>}</section>
      <footer className="memory-caption">Sampled every {snapshot.interval_seconds}s · Up to {(snapshot.history_retention_seconds / 3600).toFixed(1)}h / {snapshot.history_capacity.toLocaleString()} samples
        <br />History buffer {memoryBytes(snapshot.history_storage_bytes)} · {snapshot.history_retained_samples.toLocaleString()} retained · Cleared on Server restart</footer>
    </>}
  </dialog>;
}

function Metric({ label, value }: { label: string; value: string | number | null | undefined }) {
  return <div><dt>{label}</dt><dd>{value ?? 'Unavailable'}</dd></div>;
}

function MemoryChart({ snapshot, windowSeconds }: { snapshot: MemorySnapshot; windowSeconds: number }) {
  const points = snapshot.history;
  if (!points.length) return <p className="memory-empty" role="status">History is collecting. New samples appear automatically.</p>;
  const end = snapshot.latest?.timestamp_us ?? points[points.length - 1].timestamp_us, start = end - windowSeconds * 1_000_000;
  let ceiling = 1024 ** 2;
  for (const point of points) ceiling = Math.max(ceiling, point.rss_bytes ?? 0, point.budget_bytes ?? 0);
  ceiling *= 1.1;
  return <><svg className="memory-chart" viewBox="0 0 660 190" role="img" aria-label={`Server RSS over the last ${windowSeconds / 60} minutes; current ${memoryBytes(snapshot.latest?.process.rss_bytes)}`}>
    {[0, .5, 1].map(fraction => <g key={fraction}><line x1="44" x2="644" y1={160 - fraction * 140} y2={160 - fraction * 140} /><text x="39" y={164 - fraction * 140} textAnchor="end">{(ceiling * fraction / 1024 ** 2).toFixed(0)}</text></g>)}
    <text x="4" y="11">MiB</text>
    <path className="memory-budget-line" d={memorySeries(points, 'budget_bytes', start, end, ceiling)} />
    <path className="memory-rss-line" d={memorySeries(points, 'rss_bytes', start, end, ceiling)} />
    {points.length === 1 && points[0].rss_bytes != null ? <circle className="memory-rss-point" cx="644" cy={160 - points[0].rss_bytes / ceiling * 140} r="3" /> : null}
    <text x="44" y="183">{sampleTime(start)}</text><text x="644" y="183" textAnchor="end">{sampleTime(end)}</text>
  </svg><div className="memory-chart-legend"><span>― RSS</span><span>┄ Host budget</span></div></>;
}
