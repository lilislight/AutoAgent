import type { TimelineOverview, TimelineRow } from './types.js';

export interface TimeWindow { start: number; end: number }
export function fullWindow(overview: TimelineOverview): TimeWindow | null {
  if (overview.start_at_us === null || overview.end_at_us === null) return null;
  return { start: overview.start_at_us, end: Math.max(overview.start_at_us + 1, overview.end_at_us) };
}
export function moveWindow(full: TimeWindow, start: number, width: number): TimeWindow {
  const size = Math.min(full.end - full.start, Math.max(1, Math.round(width)));
  const left = Math.max(full.start, Math.min(full.end - size, Math.round(start)));
  return { start: left, end: left + size };
}
export function zoomWindow(full: TimeWindow, window: TimeWindow, factor: number, anchor = .5): TimeWindow {
  const ratio = Math.max(0, Math.min(1, anchor));
  const width = Math.min(full.end - full.start, Math.max(1, Math.round((window.end - window.start) * factor)));
  return moveWindow(full, window.start + (window.end - window.start) * ratio - width * ratio, width);
}
export function selectedWindow(full: TimeWindow, start: number, end: number): TimeWindow {
  return moveWindow(full, Math.min(start, end), Math.max(1, Math.abs(end - start)));
}
export function resizeWindow(full: TimeWindow, window: TimeWindow, edge: 'start' | 'end', time: number): TimeWindow {
  return edge === 'start' ? { start: Math.max(full.start, Math.min(window.end - 1, Math.round(time))), end: window.end }
    : { start: window.start, end: Math.min(full.end, Math.max(window.start + 1, Math.round(time))) };
}
export function focusWindow(full: TimeWindow, row: Pick<TimelineRow, 'start_at_us' | 'end_at_us' | 'status'>, observed: number): TimeWindow {
  const elapsed = spanGeometry(row, full, observed).elapsed ?? 0;
  const size = Math.max(1, elapsed * 1.2, Math.min(1000, full.end - full.start));
  return moveWindow(full, row.start_at_us - (size - elapsed) / 2, size);
}
export type EventCategory = 'invocation' | 'node' | 'call' | 'wait' | 'routing' | 'data' | 'error' | 'runtime' | 'mixed';
export function eventCategory(name: string): EventCategory {
  const parts = name.split('.');
  if (parts.some(part => ['failed', 'faulted', 'error', 'lost', 'interrupted'].includes(part))) return 'error';
  const prefix = parts[0];
  if (prefix === 'invocation' || prefix === 'child_invocation' || prefix === 'session') return 'invocation';
  if (prefix === 'node' || prefix === 'node_occurrence') return 'node';
  if (prefix === 'operator_call' || prefix === 'operator') return 'call';
  if (prefix === 'wait' || prefix === 'signal' || prefix === 'timer') return 'wait';
  if (prefix === 'routing' || prefix === 'edge') return 'routing';
  if (prefix === 'input' || prefix === 'output' || prefix === 'capability' || prefix === 'context') return 'data';
  return 'runtime';
}
export function spanGeometry(row: Pick<TimelineRow, 'start_at_us' | 'end_at_us' | 'status'>, window: TimeWindow, observed: number) {
  const open = row.end_at_us === null && ['ready', 'running', 'waiting'].includes(row.status ?? '');
  const end = row.end_at_us ?? (open ? Math.max(row.start_at_us, observed) : row.start_at_us);
  const point = end === row.start_at_us;
  const left = Math.max(row.start_at_us, window.start);
  const right = Math.min(end, window.end);
  const visible = point ? row.start_at_us >= window.start && row.start_at_us < window.end : end > window.start && row.start_at_us < window.end;
  return { visible, point, open, left: (left - window.start) / (window.end - window.start) * 100,
    width: Math.max(0, right - left) / (window.end - window.start) * 100,
    elapsed: row.end_at_us === null && !open ? null : end - row.start_at_us,
    clippedStart: row.start_at_us < window.start, clippedEnd: end > window.end };
}
export function elapsedLabel(us: number): string {
  if (us < 1000) return `${Number(us.toFixed(2))} µs`;
  if (us < 1_000_000) return `${Number((us / 1000).toFixed(2))} ms`;
  if (us < 60_000_000) return `${Number((us / 1_000_000).toFixed(2))} s`;
  return `${Math.floor(us / 60_000_000)}m ${Number((us % 60_000_000 / 1_000_000).toFixed(2))}s`;
}

export function timelineScale(window: TimeWindow) {
  const width = window.end - window.start;
  return width < 1_000_000 ? { unit: 'ms', divisor: 1000 }
    : width < 120_000_000 ? { unit: 's', divisor: 1_000_000 }
    : { unit: 'min', divisor: 60_000_000 };
}
export function timelineTimestamp(us: number): string {
  const value = new Date(Math.floor(us / 1000)).toISOString();
  return value.replace(/\.\d{3}Z$/, `.${String(us % 1_000_000).padStart(6, '0')}Z`);
}
