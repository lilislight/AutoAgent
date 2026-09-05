import { Braces, CircleDashed, Layers3 } from "lucide-react";
import { memo } from "react";
import type { RuntimeSpan } from "../runtimeProjection";

const MAX_VISIBLE_SPANS = 500;

interface Props {
  spans: readonly RuntimeSpan[];
}

export const RuntimeSpans = memo(function RuntimeSpans({ spans }: Props) {
  if (spans.length === 0) {
    return (
      <div className="runtime-spans-empty">
        <CircleDashed size={16} />
        <span>No Runtime State spans yet.</span>
      </div>
    );
  }
  const hiddenCount = Math.max(0, spans.length - MAX_VISIBLE_SPANS);
  const visible = spans.slice(-MAX_VISIBLE_SPANS);
  const origin = firstStart(spans);
  return (
    <div className="runtime-spans" role="list" aria-label="Runtime State spans">
      {hiddenCount > 0 && (
        <div className="runtime-spans-truncated">
          Showing the latest {MAX_VISIBLE_SPANS.toLocaleString()} of {spans.length.toLocaleString()} spans
        </div>
      )}
      {visible.map((span) => (
        <div
          className={`runtime-span runtime-span-${span.kind}`}
          key={span.id}
          role="listitem"
        >
          <span className="runtime-span-icon">
            {span.kind === "node" ? <Layers3 size={13} /> : <Braces size={13} />}
          </span>
          <span className="runtime-span-copy">
            <strong>{spanLabel(span)}</strong>
            <small>{spanDetail(span)}</small>
          </span>
          <span className={`status-pill status-${statusClass(span.status)}`}>
            {span.status}
          </span>
          <span className="runtime-span-time">
            {formatOffset(span.startedAtNs, origin)}
          </span>
          <span className="runtime-span-duration">
            {formatDuration(span.durationNs)}
          </span>
        </div>
      ))}
    </div>
  );
});

function spanLabel(span: RuntimeSpan): string {
  if (span.kind === "node") return span.nodeId ?? span.occurrenceId;
  return span.operatorId ?? span.id;
}

function spanDetail(span: RuntimeSpan): string {
  if (span.kind === "node") return span.occurrenceId;
  const unit = span.unitIndex === null ? "" : ` · unit ${span.unitIndex}`;
  return `${span.nodeId ?? "unknown node"}${unit}`;
}

function firstStart(spans: readonly RuntimeSpan[]): string | null {
  for (const span of spans) {
    if (span.startedAtNs !== null) return span.startedAtNs;
  }
  return null;
}

function formatOffset(value: string | null, origin: string | null): string {
  if (value === null || origin === null) return "—";
  return `+${formatNanoseconds(difference(value, origin))}`;
}

function formatDuration(value: string | null): string {
  return value === null ? "open" : formatNanoseconds(value);
}

function difference(value: string, origin: string): string {
  try {
    return String(BigInt(value) - BigInt(origin));
  } catch {
    return "0";
  }
}

function formatNanoseconds(value: string): string {
  let nanoseconds: bigint;
  try {
    nanoseconds = BigInt(value);
  } catch {
    return "—";
  }
  if (nanoseconds <= 0n) return "0μs";
  if (nanoseconds < 1_000_000n) return `${nanoseconds / 1_000n}μs`;
  if (nanoseconds < 1_000_000_000n) {
    return `${(Number(nanoseconds) / 1_000_000).toFixed(1)}ms`;
  }
  return `${(Number(nanoseconds) / 1_000_000_000).toFixed(2)}s`;
}

function statusClass(value: string): string {
  return value.replace(/[^a-z0-9_-]/gi, "-").toLowerCase();
}
