import { useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties } from "react";
import { ChevronDown, ChevronRight, Clock3, CornerDownRight, History, LoaderCircle } from "lucide-react";

import type {
  RuntimeEvent,
  TimelineSpan,
  TimelineView,
  TraceSelection,
} from "../types";

type RuntimeEventMarkerCluster = {
  id: string;
  left: number;
  x: number;
  category: string;
  sequence: number;
  firstLocalIndex: number;
  lastLocalIndex: number;
  events: RuntimeEvent[];
};

interface ExecutionTimelineProps {
  timeline: TimelineView;
  events: RuntimeEvent[];
  cursorSequence: number;
  onCursorChange: (sequence: number) => void;
  onSelect: (selection: TraceSelection) => void;
  historyAvailable: boolean;
  historyLoading: boolean;
  historyError: string | null;
  collapsed: boolean;
  onCollapsedChange: (collapsed: boolean) => void;
  height: number;
  onHeightChange: (height: number) => void;
  onLoadHistory: () => void;
}

export function ExecutionTimeline({
  timeline,
  events,
  cursorSequence,
  onCursorChange,
  onSelect,
  historyAvailable,
  historyLoading,
  historyError,
  collapsed,
  onCollapsedChange,
  height,
  onHeightChange,
  onLoadHistory,
}: ExecutionTimelineProps) {
  const trackRef = useRef<HTMLDivElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const [hoverLeft, setHoverLeft] = useState<number | null>(null);
  const [scrollLeft, setScrollLeft] = useState(0);
  const [draggingHeight, setDraggingHeight] = useState(false);
  const [draggingCursor, setDraggingCursor] = useState(false);
  const [collapsedNodes, setCollapsedNodes] = useState<Set<string>>(() => new Set());
  const range = useMemo(() => timelineRange(timeline, events), [events, timeline]);
  useEffect(
    () => () => {
      document.body.classList.remove("is-resizing-timeline");
      document.body.classList.remove("is-dragging-timeline-cursor");
    },
    [],
  );
  const cursorEvent = [...events]
    .reverse()
    .find((event) => event.sequence <= cursorSequence);
  const cursorRowId = timelineRowIdForEvent(cursorEvent);
  const cursorTime = cursorEvent?.occurred_at_ms ?? range.start;
  const cursorEventIndex = cursorEvent
    ? events.findIndex((event) => event.id === cursorEvent.id) + 1
    : 0;
  const trackWidth = timelineTrackWidth(timeline, events);
  const eventMarkers = useMemo(
    () => clusterRuntimeEventMarkers(events, range, trackWidth),
    [events, range, trackWidth],
  );
  const cursorMarker = markerForSequence(eventMarkers, cursorSequence);
  const cursorLeft = cursorMarker?.left ?? percent(cursorTime, range.start, range.end);
  const cursorX = (cursorLeft / 100) * trackWidth;
  const cursorOffset = (cursorLeft / 100) * trackWidth - scrollLeft;
  const hoverOffset = hoverLeft === null ? null : (hoverLeft / 100) * trackWidth - scrollLeft;

  useEffect(() => {
    const scroller = scrollRef.current;
    if (!scroller || events.length === 0) return;
    const leftPadding = 40;
    const rightPadding = 80;
    const visibleStart = scroller.scrollLeft;
    const visibleEnd = visibleStart + scroller.clientWidth;
    if (cursorX < visibleStart + leftPadding) {
      scroller.scrollTo({
        left: Math.max(0, cursorX - leftPadding),
        behavior: "smooth",
      });
    } else if (cursorX > visibleEnd - rightPadding) {
      scroller.scrollTo({
        left: Math.max(0, cursorX - scroller.clientWidth + rightPadding),
        behavior: "smooth",
      });
    }
  }, [cursorSequence, cursorX, events.length]);

  useEffect(() => {
    const scroller = scrollRef.current;
    if (!scroller || !cursorRowId) return;
    const target =
      findTimelineRow(scroller, cursorRowId) ??
      findTimelineRow(scroller, parentTimelineRowId(timeline.spans, cursorRowId));
    target?.scrollIntoView({
      block: "center",
      inline: "nearest",
      behavior: "smooth",
    });
  }, [cursorRowId, timeline.spans]);

  const pointerPosition = (clientX: number): { left: number; sequence: number; near: boolean } | null => {
    const bounds = trackRef.current?.getBoundingClientRect();
    if (!bounds) return null;
    const ratio = Math.min(1, Math.max(0, (clientX - bounds.left) / bounds.width));
    const x = ratio * trackWidth;
    const marker = nearestMarkerAt(eventMarkers, x);
    return {
      left: ratio * 100,
      sequence: marker?.sequence ?? 0,
      near: marker ? Math.abs(marker.x - x) <= 22 : false,
    };
  };
  const previewCursor = (clientX: number) => {
    const position = pointerPosition(clientX);
    if (position) setHoverLeft(position.left);
  };
  const commitCursor = (clientX: number, options: { requireNear?: boolean } = {}) => {
    const position = pointerPosition(clientX);
    if (!position || position.sequence === 0) return;
    if (options.requireNear && !position.near) return;
    onCursorChange(position.sequence);
  };
  const childSpansByParent = useMemo(() => {
    const values = new Map<string, TimelineSpan[]>();
    for (const span of timeline.spans) {
      if (span.kind !== "operator_call" || !span.parent_id) continue;
      const children = values.get(span.parent_id) ?? [];
      children.push(span);
      values.set(span.parent_id, children);
    }
    for (const children of values.values()) {
      children.sort(compareTimelineSpans);
    }
    return values;
  }, [timeline.spans]);
  const visibleSpans = useMemo(() => {
    const rows: TimelineSpan[] = [];
    const nodes = timeline.spans
      .filter((span) => span.kind === "node_execution")
      .sort(compareTimelineSpans);
    for (const node of nodes) {
      rows.push(node);
      if (!collapsedNodes.has(node.id)) {
        rows.push(...(childSpansByParent.get(node.id) ?? []));
      }
    }
    return rows;
  }, [childSpansByParent, collapsedNodes, timeline.spans]);
  const startHeightDrag = (clientY: number) => {
    const initialHeight = collapsed ? 42 : height;
    let moved = false;
    const onPointerMove = (event: PointerEvent) => {
      moved = true;
      const delta = clientY - event.clientY;
      onCollapsedChange(false);
      onHeightChange(clampTimelineHeight(initialHeight + delta));
    };
    const onPointerUp = () => {
      setDraggingHeight(false);
      document.body.classList.remove("is-resizing-timeline");
      window.removeEventListener("pointermove", onPointerMove);
      if (!moved) onCollapsedChange(!collapsed);
    };
    setDraggingHeight(true);
    document.body.classList.add("is-resizing-timeline");
    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", onPointerUp, { once: true });
  };

  const toggleNode = (id: string) => {
    setCollapsedNodes((current) => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const startCursorDrag = (clientX: number) => {
    let moved = false;
    let lastClientX = clientX;
    const onPointerMove = (event: PointerEvent) => {
      moved = true;
      lastClientX = event.clientX;
      previewCursor(event.clientX);
      commitCursor(event.clientX, { requireNear: true });
    };
    const onPointerUp = () => {
      setDraggingCursor(false);
      document.body.classList.remove("is-dragging-timeline-cursor");
      window.removeEventListener("pointermove", onPointerMove);
      if (moved) commitCursor(lastClientX, { requireNear: true });
    };
    setDraggingCursor(true);
    document.body.classList.add("is-dragging-timeline-cursor");
    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", onPointerUp, { once: true });
  };

  return (
    <section className={`timeline-panel ${collapsed ? "is-collapsed" : ""} ${draggingHeight ? "is-resizing" : ""}`} aria-label="Invocation timeline">
      <button
        className="timeline-resize-handle"
        type="button"
        aria-label="Resize or collapse timeline"
        title="Drag to resize. Click to collapse or expand."
        onPointerDown={(event) => {
          event.preventDefault();
          startHeightDrag(event.clientY);
        }}
      />
      <div className="timeline-header">
        <div>
          <Clock3 size={15} />
          <strong>Execution timeline</strong>
          {!collapsed && <span>{formatDuration(range.end - range.start)}</span>}
        </div>
        {!collapsed && <div className="timeline-actions">
          {historyError && <span className="timeline-history-error">History unavailable</span>}
          {historyAvailable && (
            <button type="button" onClick={onLoadHistory} disabled={historyLoading}>
              {historyLoading ? <LoaderCircle className="spin" size={13} /> : <History size={13} />}
              {historyLoading ? "Loading history" : "Load full history"}
            </button>
          )}
          <span className="timeline-cursor-label">
            {formatTimestamp(cursorTime)} · event {cursorEventIndex}/{events.length}
          </span>
        </div>}
      </div>
      {!collapsed && (
      <div
        className="timeline-table-shell"
        onPointerLeave={() => setHoverLeft(null)}
      >
        <div
          className="timeline-scroll-x"
          ref={scrollRef}
          onScroll={(event) => setScrollLeft(event.currentTarget.scrollLeft)}
        >
          <div
            className="timeline-table"
            style={{ "--timeline-track-width": `${trackWidth}px` } as CSSProperties}
          >
            <div className="timeline-label-column timeline-event-label">Runtime events</div>
            <div
              className="timeline-track timeline-event-row"
              onPointerMove={(event) => previewCursor(event.clientX)}
              onPointerDown={(event) => {
                event.preventDefault();
                startCursorDrag(event.clientX);
              }}
              onClick={(event) => commitCursor(event.clientX)}
            >
              {eventMarkers.map((marker) => (
                <button
                  key={marker.id}
                  className={`timeline-event-marker event-category-${marker.category} ${marker.events.length > 1 ? "is-cluster" : ""}`}
                  type="button"
                  style={{ left: `${marker.left}%` }}
                  title={eventMarkerTitle(marker, events.length)}
                  onPointerDown={(event) => {
                    event.stopPropagation();
                  }}
                  onClick={(event) => {
                    event.stopPropagation();
                    onCursorChange(marker.sequence);
                  }}
                >
                  {marker.events.length > 1 && <span>{marker.events.length}</span>}
                </button>
              ))}
            </div>
            <div className="timeline-label-column timeline-axis-label">Execution</div>
            <div
              className="timeline-track timeline-axis"
              ref={trackRef}
              onClick={(event) => commitCursor(event.clientX)}
              onPointerMove={(event) => previewCursor(event.clientX)}
              onPointerDown={(event) => {
                event.preventDefault();
                startCursorDrag(event.clientX);
              }}
            >
              {[0, 0.25, 0.5, 0.75, 1].map((tick) => (
                <span key={tick} style={{ left: `${tick * 100}%` }}>
                  {formatDuration((range.end - range.start) * tick)}
                </span>
              ))}
            </div>
            {visibleSpans.map((span) => (
              <TimelineRow
                key={span.id}
                span={span}
                range={range}
                collapsed={collapsedNodes.has(span.id)}
                hasChildren={(childSpansByParent.get(span.id)?.length ?? 0) > 0}
                onToggleNode={() => toggleNode(span.id)}
                onCursorPreview={previewCursor}
                onCursorCommit={commitCursor}
                onCursorDragStart={startCursorDrag}
                onSelect={() => {
                  onSelect({ type: span.kind, id: span.id });
                }}
              />
            ))}
          </div>
        </div>
          <div className="timeline-cursor-track" aria-hidden="true">
            <span
              className={`timeline-cursor ${draggingCursor ? "is-dragging" : ""}`}
              style={{ left: `calc(var(--timeline-label-width) + ${cursorOffset}px)` }}
            />
            {hoverOffset !== null && (
              <span
                className="timeline-hover-cursor"
                style={{ left: `calc(var(--timeline-label-width) + ${hoverOffset}px)` }}
              />
            )}
          </div>
      </div>
      )}
    </section>
  );
}

function TimelineRow({
  span,
  range,
  collapsed,
  hasChildren,
  onToggleNode,
  onCursorPreview,
  onCursorCommit,
  onCursorDragStart,
  onSelect,
}: {
  span: TimelineSpan;
  range: { start: number; end: number };
  collapsed: boolean;
  hasChildren: boolean;
  onToggleNode: () => void;
  onCursorPreview: (clientX: number) => void;
  onCursorCommit: (clientX: number, options?: { requireNear?: boolean }) => void;
  onCursorDragStart: (clientX: number) => void;
  onSelect: () => void;
}) {
  const start = percent(span.started_at_ms, range.start, range.end);
  const end = percent(span.ended_at_ms ?? range.end, range.start, range.end);
  const durationMs = span.ended_at_ms === null
    ? span.duration_ms
    : Math.max(0, span.ended_at_ms - span.started_at_ms);
  return (
    <>
      <button
        className={`timeline-label ${span.kind === "operator_call" ? "is-child" : ""} ${span.kind === "node_execution" ? "is-node" : ""}`}
        type="button"
        data-timeline-row-id={span.id}
        onClick={() => {
          if (span.kind === "node_execution" && hasChildren) onToggleNode();
          else onSelect();
        }}
        title={span.label}
      >
        {span.kind === "node_execution" && hasChildren && (
          collapsed ? <ChevronRight size={12} /> : <ChevronDown size={12} />
        )}
        {span.kind === "node_execution" && !hasChildren && <span className="timeline-label-spacer" />}
        {span.kind === "operator_call" && <CornerDownRight size={12} />}
        <span>{span.label}</span>
      </button>
      <button
        className="timeline-track timeline-row"
        type="button"
        data-timeline-row-id={span.id}
        onPointerMove={(event) => onCursorPreview(event.clientX)}
        onPointerDown={(event) => {
          event.preventDefault();
          onCursorDragStart(event.clientX);
        }}
        onClick={(event) => {
          onCursorCommit(event.clientX);
          onSelect();
        }}
        title={`${span.label}: ${span.state}`}
      >
        <span
          className={`timeline-bar state-${span.state}`}
          style={{ left: `${start}%`, width: `${Math.max(0.35, end - start)}%` }}
        >
          <span>{formatDuration(durationMs ?? 0)}</span>
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

function compareTimelineSpans(left: TimelineSpan, right: TimelineSpan): number {
  if (left.sequence !== right.sequence) return left.sequence - right.sequence;
  if (left.started_at_ms !== right.started_at_ms) {
    return left.started_at_ms - right.started_at_ms;
  }
  return left.id.localeCompare(right.id);
}

function percent(value: number, start: number, end: number): number {
  return Math.min(100, Math.max(0, ((value - start) / (end - start)) * 100));
}

function timelineTrackWidth(timeline: TimelineView, events: RuntimeEvent[]): number {
  const eventWidth = events.length * 18;
  const spanWidth = timeline.spans.length * 86;
  return Math.max(900, eventWidth, spanWidth);
}

function clusterRuntimeEventMarkers(
  events: RuntimeEvent[],
  range: { start: number; end: number },
  trackWidth: number,
): RuntimeEventMarkerCluster[] {
  const minGapPx = 13;
  const positioned = events
    .map((event, index) => {
      const left = percent(event.occurred_at_ms, range.start, range.end);
      return {
        event,
        localIndex: index + 1,
        left,
        leftPx: (left / 100) * trackWidth,
      };
    })
    .sort((left, right) => {
      if (left.leftPx !== right.leftPx) return left.leftPx - right.leftPx;
      return left.event.sequence - right.event.sequence;
    });

  const clusters: Array<{
    events: RuntimeEvent[];
    localIndexes: number[];
    leftPxTotal: number;
    leftPxMax: number;
  }> = [];
  for (const item of positioned) {
    const previous = clusters.at(-1);
    if (!previous || item.leftPx - previous.leftPxMax > minGapPx) {
      clusters.push({
        events: [item.event],
        localIndexes: [item.localIndex],
        leftPxTotal: item.leftPx,
        leftPxMax: item.leftPx,
      });
      continue;
    }
    previous.events.push(item.event);
    previous.localIndexes.push(item.localIndex);
    previous.leftPxTotal += item.leftPx;
    previous.leftPxMax = Math.max(previous.leftPxMax, item.leftPx);
  }

  return clusters.map((cluster) => {
    const averageLeftPx = cluster.leftPxTotal / cluster.events.length;
    const leftPx = Math.min(trackWidth - 14, Math.max(14, averageLeftPx));
    const localIndexes = [...cluster.localIndexes].sort((left, right) => left - right);
    const orderedEvents = cluster.events.sort((left, right) => left.sequence - right.sequence);
    return {
      id: cluster.events.map((event) => event.id).join(":"),
      left: percent(leftPx, 0, trackWidth),
      x: leftPx,
      category: clusterEventCategory(cluster.events),
      sequence: orderedEvents.at(-1)?.sequence ?? 0,
      firstLocalIndex: localIndexes[0] ?? 0,
      lastLocalIndex: localIndexes.at(-1) ?? 0,
      events: orderedEvents,
    };
  });
}

function nearestMarkerAt(
  markers: RuntimeEventMarkerCluster[],
  x: number,
): { x: number; sequence: number } | null {
  let closest = markers[0];
  if (!closest) return null;
  for (const marker of markers) {
    const currentDistance = Math.abs(marker.x - x);
    const closestDistance = Math.abs(closest.x - x);
    if (currentDistance < closestDistance) closest = marker;
    if (marker.x > x && currentDistance > closestDistance) break;
  }
  return { x: closest.x, sequence: closest.sequence };
}

function markerForSequence(
  markers: RuntimeEventMarkerCluster[],
  sequence: number,
): RuntimeEventMarkerCluster | null {
  return (
    markers.find((marker) =>
      marker.events.some((event) => event.sequence === sequence),
    ) ?? null
  );
}

function timelineRowIdForEvent(event: RuntimeEvent | undefined): string | null {
  if (!event) return null;
  if (event.entity_type === "node_execution" || event.entity_type === "operator_call") {
    return event.entity_id;
  }
  if (event.entity_type === "edge") {
    const sourceExecutionId = event.payload.node_execution_id;
    return sourceExecutionId === undefined || sourceExecutionId === null
      ? null
      : String(sourceExecutionId);
  }
  return null;
}

function parentTimelineRowId(spans: TimelineSpan[], rowId: string | null): string | null {
  if (!rowId) return null;
  return spans.find((span) => span.id === rowId)?.parent_id ?? null;
}

function findTimelineRow(scroller: HTMLElement, rowId: string | null): HTMLElement | null {
  if (!rowId) return null;
  return (
    Array.from(scroller.querySelectorAll<HTMLElement>("[data-timeline-row-id]"))
      .find((element) => element.dataset.timelineRowId === rowId) ?? null
  );
}

function eventMarkerTitle(marker: RuntimeEventMarkerCluster, eventCount: number): string {
  if (marker.events.length === 1) {
    const event = marker.events[0];
    return `Event ${marker.firstLocalIndex}/${eventCount}: ${event.type}; session sequence ${event.sequence}`;
  }
  const eventTypes = [...new Set(marker.events.map((event) => event.type))].join(", ");
  const sequences = `${marker.events[0].sequence}-${marker.events.at(-1)?.sequence}`;
  return `Events ${marker.firstLocalIndex}-${marker.lastLocalIndex}/${eventCount}: ${eventTypes}; session sequences ${sequences}`;
}

function clusterEventCategory(events: RuntimeEvent[]): string {
  if (
    events.some((event) =>
      ["failed", "cancelled", "interrupted"].includes(String(event.payload.to ?? event.payload.state ?? "")),
    )
  ) {
    return "error";
  }
  const priority = ["output", "edge", "operator", "node", "invocation", "context", "runtime"];
  const categories = new Set(events.map(eventCategory));
  return priority.find((category) => categories.has(category)) ?? "runtime";
}

function eventCategory(event: RuntimeEvent): string {
  if (event.entity_type === "invocation") return "invocation";
  if (event.entity_type === "node_execution") return "node";
  if (event.entity_type === "operator_call") return "operator";
  if (event.entity_type === "edge") return "edge";
  if (event.entity_type === "output") return "output";
  if (
    event.entity_type === "invocation_context" ||
    event.entity_type === "session_context"
  ) {
    return "context";
  }
  return "runtime";
}

function formatDuration(value: number): string {
  if (value < 1000) return `${Math.round(value)} ms`;
  if (value < 60_000) return `${(value / 1000).toFixed(2)} s`;
  return `${(value / 60_000).toFixed(1)} min`;
}

function clampTimelineHeight(value: number): number {
  return Math.min(520, Math.max(42, value));
}

function formatTimestamp(value: number): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
  }).format(value);
}
