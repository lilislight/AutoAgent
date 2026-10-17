import type { ExecutionTrace, TimelineIdle, TimelineMarker } from './types.js';

export const terminalStatus = (status?: string | null) => ['completed', 'failed', 'cancelled'].includes(status ?? '');
export function quiescentTrace(trace: ExecutionTrace) {
  return trace.invocation.control_state === 'paused' ||
    ['waiting', 'settling'].includes(trace.invocation.status ?? '') && !trace.nodes.some(node => node.status === 'running');
}
export function observationInterval(trace: ExecutionTrace) {
  return terminalStatus(trace.invocation.status) ? 0 : quiescentTrace(trace) ? 1000 : 500;
}

/** Only clock consumers subscribe; ticking must not rerender the Graph workspace. */
export class ObservationClock {
  private value: number;
  private server: number;
  private local: number;
  private listeners = new Set<() => void>();
  constructor(server: number, private monotonic: () => number = () => performance.now()) {
    this.value = this.server = server; this.local = monotonic();
  }
  get = () => this.value;
  subscribe = (listener: () => void) => { this.listeners.add(listener); return () => { this.listeners.delete(listener); }; };
  calibrate(server: number, publish = true) {
    this.server = server; this.local = this.monotonic();
    if (publish) this.publish(Math.max(this.value, server));
  }
  tick = () => this.publish(Math.max(this.value, this.server + (this.monotonic() - this.local) * 1000));
  private publish(value: number) {
    if (value === this.value) return;
    this.value = value; for (const listener of this.listeners) listener();
  }
}

export interface PauseInterval { start_at_us: number; end_at_us: number | null }
export function appendPauseIntervals(previous: PauseInterval[], events: TimelineMarker[]): PauseInterval[] {
  const intervals = previous.map(interval=>({...interval}));
  let open = intervals.at(-1)?.end_at_us===null ? intervals.at(-1) : undefined;
  for (const event of events) {
    if (event.control_state === 'paused' && !open) {
      open = { start_at_us: event.occurred_at_us, end_at_us: null }; intervals.push(open);
    }
    if (open && (event.control_state === 'active' || terminalStatus(event.invocation_status))) {
      open.end_at_us = Math.max(open.start_at_us, event.occurred_at_us); open = undefined;
    }
  }
  return intervals;
}
export const pauseIntervals = (events:TimelineMarker[]) => appendPauseIntervals([],events);

/** Tracks committed boundaries once; heartbeat/local clocks never enter this index. */
export class TimelineActivity {
  private at = 0;
  private waitingSince: number | null = null;
  private status: string | undefined;
  append(events: TimelineMarker[]) {
    const waiting = (status?: string) => status === 'waiting' || status === 'settling';
    for (const event of events) {
      this.at = Math.max(this.at, event.occurred_at_us);
      if (event.invocation_status !== undefined) {
        if (waiting(event.invocation_status)) {
          if (!waiting(this.status)) this.waitingSince = event.occurred_at_us;
        } else this.waitingSince = null;
        this.status = event.invocation_status;
      }
    }
  }
  idle(trace: ExecutionTrace, recorded: number, pauses: PauseInterval[]): TimelineIdle | null {
    if (terminalStatus(trace.invocation.status) || !quiescentTrace(trace)) return null;
    const at = Math.max(recorded, this.at, trace.last_event?.occurred_at_us ?? 0);
    const paused = trace.invocation.control_state === 'paused';
    const last = pauses.at(-1);
    const since = paused ? last?.end_at_us === null ? last.start_at_us : at : this.waitingSince ?? at;
    return { state: paused ? 'paused' : 'waiting', since_at_us: Math.min(since, at), at_us: at };
  }
}
export function pausedDuration(pauses: PauseInterval[], start: number, end: number) {
  return pauses.reduce((total,pause)=>total+Math.max(0,Math.min(end,pause.end_at_us ?? end)-Math.max(start,pause.start_at_us)),0);
}
