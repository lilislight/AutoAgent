export type MemoryPressure = 'unknown' | 'normal' | 'high' | 'critical';
export interface MemoryPoint {
  timestamp_us: number; rss_bytes: number | null; budget_bytes: number | null;
  cgroup_current_bytes: number | null; persistence_retained_bytes: number | null;
  projection_bytes: number | null; resident_graphs: number | null; pressure: MemoryPressure;
}
export interface MemorySample {
  timestamp_us: number; pid: number; pressure: MemoryPressure; budget_bytes: number | null;
  budget_ratio: number | null; sampling_ms: number;
  process: { rss_bytes: number | null; peak_rss_bytes: number | null; cpu_seconds: number };
  resources: { budget_source: 'explicit' | 'cgroup' | 'physical' | 'fallback'; capacity_bytes: number | null;
    capacity_source: 'cgroup' | 'physical' | null; system_total_bytes: number | null; system_available_bytes: number | null;
    shared_pressure_ratio: number; cgroup_available_bytes: number | null };
  cgroup: { current_bytes: number | null; limit_bytes: number | null; path: string; limit_path: string | null;
    limit_scope_current_bytes: number | null; available_bytes: number | null; events: Record<string, number> } | null;
  host: {
    sampled_at_us: number; resident_graphs: number; loading_graphs: number; checkpoint_jobs: number;
    requests: number; request_bytes: number;
    persistence: { queued_count: number; retained_bytes: number; state: string; fault: string | null; accepted_frontier: number; durable_frontier: number };
    projections: { bytes?: number; budget_bytes?: number; entries?: number; hits?: number; misses?: number; evictions?: number };
    residency: { estimated_bytes: number; estimated_graphs: number; tail_bytes: number; tail_events: number;
      top_sessions: { session_id: string; estimated_bytes: number; can_unload: boolean }[] };
  } | null;
}

export function memoryBudgetSource(source: MemorySample['resources']['budget_source'] | undefined): string {
  switch (source) {
    case 'explicit': return 'Configured';
    case 'cgroup': return 'Automatic · Cgroup capacity × 80%';
    case 'physical': return 'Automatic · Physical capacity × 80%';
    case 'fallback': return 'Fallback · Capacity detection unavailable';
    default: return 'Unavailable';
  }
}
export interface MemorySnapshot {
  enabled: boolean; monitor_id: string; interval_seconds: number; sample_age_seconds: number | null; stale: boolean;
  history_capacity: number; history_retention_seconds: number; history_retained_samples: number;
  history_storage_bytes: number; history_count: number; history_points_limit: number;
  latest: MemorySample | null; history: MemoryPoint[];
}

export function memoryBytes(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return 'Unavailable';
  if (value >= 1024 ** 3) return `${(value / 1024 ** 3).toFixed(2)} GiB`;
  return `${(value / 1024 ** 2).toFixed(1)} MiB`;
}

export function memoryStatus(snapshot: MemorySnapshot | undefined, failed = false): string {
  if (failed) return 'Unavailable';
  if (!snapshot) return 'Loading';
  if (!snapshot.enabled) return 'Off';
  if (snapshot.stale) return 'Stale';
  const pressure = snapshot.latest?.pressure ?? 'unknown';
  return pressure[0].toUpperCase() + pressure.slice(1);
}

export function memorySeries(points: MemoryPoint[], field: 'rss_bytes' | 'budget_bytes', start: number, end: number, ceiling: number): string {
  let pen = false;
  const path: string[] = [];
  for (const point of points) {
    const value = point[field];
    if (value == null || !Number.isFinite(value)) { pen = false; continue; }
    const x = 44 + Math.max(0, Math.min(1, (point.timestamp_us - start) / Math.max(1, end - start))) * 600;
    const y = 160 - Math.max(0, Math.min(1, value / ceiling)) * 140;
    path.push(`${pen ? 'L' : 'M'}${x.toFixed(1)},${y.toFixed(1)}`); pen = true;
  }
  return path.join(' ');
}
