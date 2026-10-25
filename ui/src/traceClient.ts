import { ApiError, config, request } from './http';
import type { ExecutionTrace, TraceDataset, TraceDatasetManifest, TraceDatasetPage, TimelineRow, TimelineMarker, ReplayEvent, LocalReplayResult, TraceEdgeUpdate, FamilyLink } from './types';
import { readTraceCache, writeTraceCache } from './traceStorage';
import { setTraceProgress } from './traceProgress';

const source = () => new URL(config.apiPrefix, location.origin).href;
let worker: Worker | undefined;
let serial = 0;
const pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();
function query<T>(op: string, key: string, args: unknown): Promise<T> {
  if (!worker) {
    worker = new Worker(new URL('./trace.worker.ts', import.meta.url), { type: 'module' });
    worker.onmessage = event => {
      const entry = pending.get(event.data.id);
      if (!entry) return;
      pending.delete(event.data.id);
      if (event.data.error) entry.reject(new Error(event.data.error)); else entry.resolve(event.data.value);
    };
    worker.onerror = () => {
      for (const entry of pending.values()) entry.reject(new Error('Trace Worker could not run. Reload this page.'));
      pending.clear(); entries.clear(); current.clear(); worker?.terminate(); worker = undefined;
    };
  }
  return new Promise<T>((resolve, reject) => {
    const id = ++serial;
    pending.set(id, { resolve: resolve as (value: unknown) => void, reject });
    try { worker!.postMessage({ id, op, key, args }); }
    catch (error) { pending.delete(id); reject(error instanceof Error ? error : new Error(String(error))); }
  });
}
interface ReplayCache { key: string; boundary: string; loading?: Promise<void>; ready: boolean; bytes: number }
interface Entry {
  manifest: TraceDatasetManifest; key: string; bytes: number;
  pinned?: boolean; replay?: ReplayCache; syncing?: Promise<ExecutionTrace | null>;
}
const entries = new Map<string, Entry>();
const current = new Map<string, Entry>();
const readers = new Map<string, number>();
const known = new Map<string, ExecutionTrace>();
const loading = new Map<string, Promise<ExecutionTrace>>();
const headerKey = (trace: ExecutionTrace) => `family-links:4|${source()}|${trace.source_id ?? 'unknown'}|${trace.invocation.invocation_id}|${trace.last_event?.event_id ?? 'empty'}|${trace.invocation.status}`;
const entryKey = (manifest: TraceDatasetManifest) => `${source()}|${manifest.invocation_id}|${manifest.version}`;

async function release(manifest: TraceDatasetManifest) {
  try { await request(`/trace-dataset?${new URLSearchParams({ snapshot_id: manifest.snapshot_id })}`, AbortSignal.timeout(5000), { method: 'DELETE' }); }
  catch { /* The bounded server TTL also reclaims abandoned exports. */ }
}
async function download(manifest: TraceDatasetManifest, signal: AbortSignal, progressKey: string) {
  const total = Object.values(manifest.sections).reduce((sum, section) => sum + section.count, 0);
  let loaded = 0;
  setTraceProgress(progressKey, { phase: 'download', loaded, total });
  const parts: Record<string, TraceDatasetPage['items']> = {};
  for (const [section, info] of Object.entries(manifest.sections)) {
    parts[section] = [];
    for (let page = 0; page < info.pages; page++) {
      signal.throwIfAborted();
      const value = await request<TraceDatasetPage>(`/trace-dataset-page?${new URLSearchParams({ snapshot_id: manifest.snapshot_id, section, page: String(page) })}`, signal);
      if (value.snapshot_id !== manifest.snapshot_id || value.page !== page || value.section !== section) throw new Error('Trace snapshot page does not match its manifest.');
      parts[section].push(...value.items);
      loaded += value.items.length;
      setTraceProgress(progressKey, { phase: 'download', loaded, total });
    }
    if (parts[section].length !== info.count) throw new Error('Trace snapshot download is incomplete.');
  }
  return parts;
}
function dataset(manifest: TraceDatasetManifest, parts: Record<string, TraceDatasetPage['items']>): TraceDataset {
  return { manifest, node: (parts.node ?? []) as TimelineRow[], call: (parts.call ?? []) as TimelineRow[], wait: (parts.wait ?? []) as TimelineRow[], event: (parts.event ?? []) as TimelineMarker[], edge: (parts.edge ?? []) as TraceEdgeUpdate[], link:(parts.link??[]) as FamilyLink[] };
}
async function datasetBytes(data: TraceDataset | { manifest: TraceDatasetManifest; events: ReplayEvent[] }) {
  let bytes = JSON.stringify(data.manifest).length * 2, chunks = 0;
  const sections = 'events' in data ? [data.events] : [data.node, data.call, data.wait, data.event, data.link??[]];
  for (const rows of sections) for (let offset = 0; offset < rows.length; offset += 200) {
    bytes += JSON.stringify(rows.slice(offset, offset + 200)).length * 2;
    if (++chunks % 32 === 0) await new Promise(resolve => setTimeout(resolve, 0));
  }
  return bytes;
}
function dropReplay(entry: Entry) {
  if (!entry.replay) return;
  entry.bytes -= entry.replay.bytes;
  void query('drop-replay', entry.replay.key, null);
  entry.replay = undefined;
}
function evictEntries(retained: Entry) {
  let total = [...entries.values()].reduce((sum, entry) => sum + entry.bytes, 0);
  for (const [key, entry] of entries) if (entry !== retained && !entry.pinned && !(readers.get(entry.manifest.invocation_id) && current.get(entry.manifest.invocation_id)===entry) && (entry.manifest.invocation_id === retained.manifest.invocation_id || entries.size > 4 || total > 128 * 1024 * 1024)) {
    entries.delete(key); total -= entry.bytes;
    if (current.get(entry.manifest.invocation_id) === entry) current.delete(entry.manifest.invocation_id);
    dropReplay(entry); void query('drop', key, null);
  }
}
function remember(entry: Entry) {
  known.set(entry.manifest.invocation_id, entry.manifest.trace);
  if (known.size > 128) known.delete(known.keys().next().value!);
}
export function retainTrace(iid: string) {
  readers.set(iid,(readers.get(iid) ?? 0)+1);
  return () => { const count=(readers.get(iid) ?? 1)-1;if(count)readers.set(iid,count);else readers.delete(iid); };
}
export function currentTrace(iid: string) { return current.get(iid)?.manifest.trace; }

export async function traceDataset(trace: ExecutionTrace, signal: AbortSignal, restore = false): Promise<ExecutionTrace> {
  const iid = trace.invocation.invocation_id, key = headerKey(trace), previous = current.get(iid);
  if (previous && headerKey(previous.manifest.trace) === key) return previous.manifest.trace;
  const inflight = loading.get(key);
  if (inflight) return inflight;
  signal.throwIfAborted();
  const downloadSignal = new AbortController().signal;
  const operation = (async () => {
    setTraceProgress(iid, { phase: 'cache', loaded: 0, total: 0 });
    const terminal = ['completed','failed','cancelled'].includes(trace.invocation.status ?? '');
    const cached = terminal || restore ? await readTraceCache<TraceDataset>(key) : null;
    let data = cached;
    let manifest: TraceDatasetManifest | undefined;
    try {
      if (!data) {
        manifest = await request<TraceDatasetManifest>(`/trace-dataset?${new URLSearchParams({ invocation_id: iid })}`, downloadSignal);
        data = dataset(manifest, await download(manifest, downloadSignal, iid));
      }
      setTraceProgress(iid, { phase: 'index', loaded: 0, total: 0 });
      const modelKey = entryKey(data.manifest), entry: Entry = { manifest: data.manifest, key: modelKey, bytes: await datasetBytes(data) };
      await new Promise(resolve => setTimeout(resolve, 0));
      await query('load', modelKey, data);
      entries.set(modelKey, entry); current.set(iid, entry); remember(entry);
      if (!cached) void writeTraceCache(key, data, entry.bytes);
      evictEntries(entry);
      return data.manifest.trace;
    } finally { if (manifest) void release(manifest); }
  })();
  loading.set(key, operation);
  try { return await operation; } finally { loading.delete(key); setTraceProgress(iid); }
}

/** Download once and atomically patch the existing Worker indexes. */
export async function syncTrace(iid: string, signal: AbortSignal, allowed: () => boolean): Promise<ExecutionTrace | null> {
  const entry = current.get(iid);
  if (!entry) throw new Error('Trace data is not loaded.');
  if (entry.syncing) return entry.syncing;
  const operation = (async () => {
    let manifest: TraceDatasetManifest | undefined;
    try {
      manifest = await request<TraceDatasetManifest>(`/trace-dataset?${new URLSearchParams({ invocation_id: iid, cursor: entry.manifest.cursor })}`, signal);
      const delta = dataset(manifest, await download(manifest, signal, iid));
      if (!allowed() || entry.pinned) return null;
      const result = await query<{ trace: ExecutionTrace; manifest: TraceDatasetManifest; bytesDelta: number }>('apply', entry.key, delta);
      if (entry.manifest.cursor !== result.manifest.cursor) dropReplay(entry);
      entry.manifest = result.manifest; entry.bytes = Math.max(0, entry.bytes + result.bytesDelta);
      remember(entry); evictEntries(entry);
      // Persist complete small terminal snapshots once, not a full version per tick.
      if (['completed','failed','cancelled'].includes(result.trace.invocation.status ?? '') && entry.bytes < 16 * 1024 * 1024) {
        void query<TraceDataset>('data', entry.key, {}).then(data => writeTraceCache(headerKey(result.trace), data, entry.bytes)).catch(() => { /* Optional browser cache cannot interrupt live viewing. */ });
      }
      return result.trace;
    } catch (error) {
      if (error instanceof ApiError && error.status === 409 && allowed()) {
        const trace = await request<ExecutionTrace>(`/execution-trace?${new URLSearchParams({ invocation_id: iid })}`, signal);
        if (!allowed()) return null;
        return traceDataset(trace, signal);
      }
      throw error;
    } finally { if (manifest) void release(manifest); setTraceProgress(iid); }
  })();
  entry.syncing = operation;
  try { return await operation; } finally { entry.syncing = undefined; }
}
export async function traceQuery<T>(iid: string, op: string, args: unknown, signal: AbortSignal): Promise<T> {
  signal.throwIfAborted();
  if (!current.has(iid)) {
    const trace = known.get(iid) ?? await request<ExecutionTrace>(`/execution-trace?${new URLSearchParams({ invocation_id: iid })}`, signal);
    await traceDataset(trace, signal, true);
  }
  const value = await query<T>(op, current.get(iid)!.key, args);
  signal.throwIfAborted(); return value;
}

export async function seekLocalReplay(trace: ExecutionTrace, position: { sequence?: number; at_us?: number }, signal: AbortSignal): Promise<LocalReplayResult> {
  const iid = trace.invocation.invocation_id;
  if (!current.has(iid)) await traceDataset(trace, signal, true);
  const entry = current.get(iid)!;
  entry.pinned = true;
  const boundary = trace.last_event?.event_id ?? 'empty';
  if (!entry.replay || entry.replay.boundary !== boundary) {
    dropReplay(entry);
    entry.replay = { key: `${entry.key}|replay:${boundary}`, boundary, ready: false, bytes: 0 };
  }
  const cache = entry.replay;
  if (!cache.loading) cache.loading = (async () => {
    const progressKey = `replay:${iid}`, cacheKey = `replay|${cache.key}`;
    setTraceProgress(progressKey, { phase: 'cache', loaded: 0, total: 0 });
    let manifest: TraceDatasetManifest | undefined;
    try {
      let saved = await readTraceCache<{ manifest: TraceDatasetManifest; events: ReplayEvent[] }>(cacheKey);
      if (!saved) {
        const downloadSignal = new AbortController().signal;
        manifest = await request<TraceDatasetManifest>(`/trace-dataset?${new URLSearchParams({ invocation_id: iid, replay: 'true', through_sequence: String(trace.last_event?.sequence ?? 0) })}`, downloadSignal);
        if (!manifest.history_complete || !manifest.initial_trace) throw new Error('Saved history is incomplete. Replay is unavailable.');
        const parts = await download(manifest, downloadSignal, progressKey);
        saved = { manifest, events: parts.event as ReplayEvent[] };
      }
      if (entry.replay !== cache) throw new Error('Replay snapshot was superseded.');
      setTraceProgress(progressKey, { phase: 'index', loaded: 0, total: 0 });
      const bytes = await datasetBytes(saved);
      if (entry.replay !== cache) throw new Error('Replay snapshot was superseded.');
      cache.bytes = bytes; entry.bytes += bytes; evictEntries(entry);
      void writeTraceCache(cacheKey, saved, cache.bytes);
      await query('replay-load', cache.key, saved);
      if (entry.replay !== cache) {
        void query('drop-replay', cache.key, null);
        throw new Error('Replay snapshot was superseded.');
      }
      cache.ready = true;
    } catch (error) { if (entry.replay === cache) cache.loading = undefined; throw error; }
    finally { if (manifest) void release(manifest); setTraceProgress(progressKey); }
  })();
  await cache.loading; signal.throwIfAborted();
  const result = await query<LocalReplayResult>('replay-seek', cache.key, position);
  signal.throwIfAborted(); return result;
}
export function releaseLocalReplay(trace: ExecutionTrace | null) {
  if (trace) { const entry = current.get(trace.invocation.invocation_id); if (entry) entry.pinned = false; }
}
export function localReplayReady(trace: ExecutionTrace) {
  const cache = current.get(trace.invocation.invocation_id)?.replay;
  return Boolean(cache?.ready && cache.boundary === trace.last_event?.event_id);
}
