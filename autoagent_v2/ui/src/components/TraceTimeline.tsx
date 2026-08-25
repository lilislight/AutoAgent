import { AlertTriangle, CheckCircle2, CircleDashed, PauseCircle, XCircle } from "lucide-react";
import type { TraceEvent } from "../types";

interface Props {
  events: TraceEvent[];
  selectedId: string | null;
  hiddenCount: number;
  onSelect: (event: TraceEvent) => void;
}

export function TraceTimeline({ events, selectedId, hiddenCount, onSelect }: Props) {
  if (!events.length) {
    return <div className="empty-state"><CircleDashed size={18} /><span>No Trace Events yet.</span></div>;
  }
  const origin = events[0].occurred_at_ns;
  return (
    <div className="timeline" role="list">
      {hiddenCount > 0 && <div className="timeline-truncated">Trace cursor starts after {hiddenCount.toLocaleString()} earlier positions</div>}
      {events.map((event) => (
        <button
          type="button"
          role="listitem"
          className={`timeline-row ${selectedId === event.id ? "selected" : ""}`}
          key={event.id}
          onClick={() => onSelect(event)}
        >
          <span className={`event-icon status-${event.status ?? "neutral"}`}>{eventIcon(event)}</span>
          <span className="event-copy">
            <strong>{event.kind}</strong>
            <small>{eventSubject(event)}</small>
          </span>
          <span className="event-time">+{formatElapsed(event.occurred_at_ns, origin)}</span>
          <span className="event-sequence">#{event.trace_sequence}</span>
        </button>
      ))}
    </div>
  );
}

function eventIcon(event: TraceEvent) {
  if (event.error || event.status === "failed") return <AlertTriangle size={14} />;
  if (event.status === "completed") return <CheckCircle2 size={14} />;
  if (event.status === "cancelled") return <XCircle size={14} />;
  if (event.status === "waiting") return <PauseCircle size={14} />;
  return <CircleDashed size={14} />;
}

function eventSubject(event: TraceEvent): string {
  const subjects = Object.entries(event.subject_ids);
  return subjects.length ? subjects.map(([key, value]) => `${key}: ${value}`).join(" · ") : event.status ?? "runtime";
}

function formatElapsed(value: string, origin: string): string {
  const nanoseconds = BigInt(value) - BigInt(origin);
  if (nanoseconds <= 0n) return "0μs";
  if (nanoseconds < 1_000_000n) return `${nanoseconds / 1_000n}μs`;
  if (nanoseconds < 1_000_000_000n) {
    return `${(Number(nanoseconds) / 1_000_000).toFixed(1)}ms`;
  }
  return `${(Number(nanoseconds) / 1_000_000_000).toFixed(2)}s`;
}
