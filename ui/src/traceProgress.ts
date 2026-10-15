/** Download progress is presentation-only; incomplete datasets never become queryable. */
export interface TraceProgress { phase: 'cache' | 'download' | 'index'; loaded: number; total: number }
const states = new Map<string, TraceProgress>();
const listeners = new Set<() => void>();
export const subscribeTraceProgress = (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; };
export const getTraceProgress = (key: string) => states.get(key);
export function setTraceProgress(key: string, value?: TraceProgress) {
  if (value) states.set(key, value); else states.delete(key);
  for (const listener of listeners) listener();
}
export function traceProgressLabel(progress?: TraceProgress) {
  if (!progress) return 'Preparing trace data…';
  if (progress.phase === 'cache') return 'Checking saved trace data…';
  if (progress.phase === 'index') return 'Preparing local queries…';
  return `Loading trace data · ${progress.loaded.toLocaleString('en-US')} / ${progress.total.toLocaleString('en-US')} records`;
}
