import { useMemo, useRef } from "react";
import { Clock3, CornerDownRight } from "lucide-react";

import type {
  RuntimeEvent,
  TimelineSpan,
  TimelineView,
  TraceSelection,
} from "../types";

interface ExecutionTimelineProps {
  timeline: TimelineView;
  events: RuntimeEvent[];
  cursorSequence: number;
  onCursorChange: (sequence: number) => void;
  onSelect: (selection: TraceSelection) => void;
}

export function ExecutionTimeline({
  timeline,
  events,
  cursorSequence,
  onCursorChange,
  onSelect,
}: ExecutionTimelineProps) {
  const trackRef = useRef<HTMLDivElement>(null);
  const range = useMemo(() => timelineRange(timeline, events), [events, timeline]);
  const cursorEvent = [...events]
    .reverse()
    .find((event) => event.sequence <= cursorSequence);
  const cursorTime = cursorEvent?.occurred_at_ms ?? range.start;
  const cursorLeft = percent(cursorTime, range.start, range.end);

  const moveCursor = (clientX: number) => {
    const bounds = trackRef.current?.getBoundingClientRect();
    if (!bounds) return;
    const ratio = Math.min(1, Math.max(0, (clientX - bounds.left) / bounds.width));
    const time = range.start + (range.end - range.start) * ratio;
    onCursorChange(sequenceAt(events, time));
  };

  return (
    <section className="timeline-panel" aria-label="Invocation timeline">
      <div className="timeline-header">
        <div>
          <Clock3 size={15} />
          <strong>Execution timeline</strong>
          <span>{formatDuration(range.end - range.start)}</span>
        </div>
        <span className="timeline-cursor-label">
          {formatTimestamp(cursorTime)} · event {cursorSequence}
        </span>
      </div>
      <div className="timeline-table">
        <div className="timeline-label-column timeline-axis-label">Execution</div>
        <div
          className="timeline-track timeline-axis"
          ref={trackRef}
          onClick={(event) => moveCursor(event.clientX)}
        >
          {[0, 0.25, 0.5, 0.75, 1].map((tick) => (
            <span key={tick} style={{ left: `${tick * 100}%` }}>
              {formatDuration((range.end - range.start) * tick)}
            </span>
          ))}
        </div>
        {timeline.spans.map((span) => (
          <TimelineRow
            key={span.id}
            span={span}
            range={range}
            onSelect={() => {
              onSelect({ type: span.kind, id: span.id });
              onCursorChange(sequenceAt(events, span.started_at_ms));
            }}
          />
        ))}
        <div className="timeline-cursor-track" aria-hidden="true">
          <span className="timeline-cursor" style={{ left: `${cursorLeft}%` }} />
        </div>
      </div>
    </section>
  );
}

function TimelineRow({
  span,
  range,
  onSelect,
}: {
  span: TimelineSpan;
  range: { start: number; end: number };
  onSelect: () => void;
}) {
  const start = percent(span.started_at_ms, range.start, range.end);
  const end = percent(span.ended_at_ms ?? range.end, range.start, range.end);
  return (
    <>
      <button
        className={`timeline-label ${span.kind === "operator_call" ? "is-child" : ""}`}
        type="button"
        onClick={onSelect}
        title={span.label}
      >
        {span.kind === "operator_call" && <CornerDownRight size={12} />}
        <span>{span.label}</span>
      </button>
      <button
        className="timeline-track timeline-row"
        type="button"
        onClick={onSelect}
        title={`${span.label}: ${span.state}`}
      >
        <span
          className={`timeline-bar state-${span.state}`}
          style={{ left: `${start}%`, width: `${Math.max(0.35, end - start)}%` }}
        >
          <span>{formatDuration(span.duration_ms ?? 0)}</span>
        </span>
      </button>
    </>
  );
}

function timelineRange(
  timeline: TimelineView,
  events: RuntimeEvent[],
): { start: number; end: number } {
  const start = timeline.started_at_ms;
  const eventEnd = events.at(-1)?.occurred_at_ms ?? start;
  const spanEnd = Math.max(
    start,
    ...timeline.spans.map((span) => span.ended_at_ms ?? span.started_at_ms),
  );
  return { start, end: Math.max(start + 1, timeline.ended_at_ms ?? eventEnd, spanEnd) };
}

function sequenceAt(events: RuntimeEvent[], timestamp: number): number {
  let sequence = events[0]?.sequence ?? 0;
  for (const event of events) {
    if (event.occurred_at_ms > timestamp) break;
    sequence = event.sequence;
  }
  return sequence;
}

function percent(value: number, start: number, end: number): number {
  return Math.min(100, Math.max(0, ((value - start) / (end - start)) * 100));
}

function formatDuration(value: number): string {
  if (value < 1000) return `${Math.round(value)} ms`;
  if (value < 60_000) return `${(value / 1000).toFixed(2)} s`;
  return `${(value / 60_000).toFixed(1)} min`;
}

function formatTimestamp(value: number): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
  }).format(value);
}
