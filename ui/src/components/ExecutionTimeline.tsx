import { useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties } from "react";
import { createPortal } from "react-dom";
import {
  Clock3,
  CirclePause,
  History,
  LoaderCircle,
  Play,
  Radio,
  Square,
} from "lucide-react";

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

type TimelineScale = {
  minBarWidthPercent: number;
  left: (timeMs: number) => number;
  ticks: Array<{ left: number; label: string }>;
};

type ClusterPopoverState = {
  marker: RuntimeEventMarkerCluster;
  left: number;
  top: number;
};

type DurationTooltipState = {
  span: TimelineSpan;
  durationLabel: string;
  left: number;
  top: number;
};

// Both a single event and a merged event use the same 14px marker. Events are
// merged only when their rendered controls would overlap; zooming therefore
// expands clusters gradually instead of jumping from a few markers to all of
// them after a small wheel movement.
const RUNTIME_EVENT_CLUSTER_GAP_PX = 14;
const MIN_TIMELINE_ZOOM = 1;
const MAX_TIMELINE_ZOOM = 18;
const PLAYBACK_BASE_DELAY_MS = 500;
const MIN_PLAYBACK_SPEED = 0.25;
const MAX_PLAYBACK_SPEED = 4;

interface ExecutionTimelineProps {
  timeline: TimelineView;
  events: RuntimeEvent[];
  cursorSequence: number;
  followLive: boolean;
  onCursorChange: (sequence: number) => void;
  onSelect: (selection: TraceSelection) => void;
  historyAvailable: boolean;
  historyLoading: boolean;
  historyError: string | null;
  bufferedEventCount: number;
  totalEventCount: number;
  collapsed: boolean;
  onCollapsedChange: (collapsed: boolean) => void;
  height: number;
  onHeightChange: (height: number) => void;
  onLoadHistory: () => void;
  onFlushBufferedEvents: () => void;
  onToggleFollow: () => void;
}

export function ExecutionTimeline({
  timeline,
  events,
  cursorSequence,
  followLive,
  onCursorChange,
  onSelect,
  historyAvailable,
  historyLoading,
  historyError,
  bufferedEventCount,
  totalEventCount,
  collapsed,
  onCollapsedChange,
  height,
  onHeightChange,
  onLoadHistory,
  onFlushBufferedEvents,
  onToggleFollow,
}: ExecutionTimelineProps) {
  const shellRef = useRef<HTMLDivElement>(null);
  const trackRef = useRef<HTMLDivElement>(null);
  const [hoverLeft, setHoverLeft] = useState<number | null>(null);
  const [trackViewportWidthPx, setTrackViewportWidthPx] = useState(900);
  const [draggingHeight, setDraggingHeight] = useState(false);
  const [draggingCursor, setDraggingCursor] = useState(false);
  const [zoomLevel, setZoomLevel] = useState(1);
  const [isPlaying, setIsPlaying] = useState(false);
  const [playbackSpeed, setPlaybackSpeed] = useState(1);
  const [clusterPopover, setClusterPopover] = useState<ClusterPopoverState | null>(null);
  const [clusterPopoverOpen, setClusterPopoverOpen] = useState(false);
  const [durationTooltip, setDurationTooltip] = useState<DurationTooltipState | null>(null);
  const clusterPopoverHideTimerRef = useRef<number | null>(null);
  const clusterPopoverRemoveTimerRef = useRef<number | null>(null);
  const range = useMemo(() => timelineRange(timeline, events), [events, timeline]);
  const scale = useMemo(
    () => createTimelineScale(range, timeline, events),
    [events, range, timeline],
  );
  const trackWidthPx = Math.max(1, trackViewportWidthPx * zoomLevel);
  useEffect(() => {
    if (collapsed) return;
    const element = shellRef.current;
    if (!element) return;
    const updateWidth = () => {
      setTrackViewportWidthPx(Math.max(240, element.clientWidth - timelineLabelWidth(element)));
    };
    updateWidth();
    const observer = new ResizeObserver(updateWidth);
    observer.observe(element);
    return () => observer.disconnect();
  }, [collapsed]);
  useEffect(
    () => () => {
      document.body.classList.remove("is-resizing-timeline");
      document.body.classList.remove("is-dragging-timeline-cursor");
      if (clusterPopoverHideTimerRef.current !== null) {
        window.clearTimeout(clusterPopoverHideTimerRef.current);
      }
      if (clusterPopoverRemoveTimerRef.current !== null) {
        window.clearTimeout(clusterPopoverRemoveTimerRef.current);
      }
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
  const eventMarkers = useMemo(
    () => clusterRuntimeEventMarkers(
      events,
      scale,
      trackWidthPx,
      RUNTIME_EVENT_CLUSTER_GAP_PX,
    ),
    [events, scale, trackWidthPx],
  );
  const cursorMarker = markerForSequence(eventMarkers, cursorSequence);
  const cursorLeft = cursorMarker?.left ?? scale.left(cursorTime);
  const cursorOffset = (cursorLeft / 100) * trackWidthPx;
  const hoverOffset = hoverLeft === null ? null : (hoverLeft / 100) * trackWidthPx;

  useEffect(() => {
    if (followLive || collapsed) setIsPlaying(false);
  }, [collapsed, followLive]);

  useEffect(() => {
    if (!isPlaying || followLive || collapsed) return;
    const nextEvent = events.find((event) => event.sequence > cursorSequence);
    if (nextEvent === undefined) {
      setIsPlaying(false);
      return;
    }
    const timer = window.setTimeout(
      () => onCursorChange(nextEvent.sequence),
      Math.round(PLAYBACK_BASE_DELAY_MS / playbackSpeed),
    );
    return () => window.clearTimeout(timer);
  }, [collapsed, cursorSequence, events, followLive, isPlaying, onCursorChange, playbackSpeed]);

  const togglePlayback = () => {
    if (isPlaying) {
      setIsPlaying(false);
      return;
    }
    // A replay cursor can be empty for an invocation with no manually selected
    // event. Start there instead of requiring the user to click a marker first.
    if (!cursorEvent) {
      const firstEvent = events[0];
      if (!firstEvent) return;
      onCursorChange(firstEvent.sequence);
    }
    setIsPlaying(true);
  };

  const cancelClusterPopoverClose = () => {
    if (clusterPopoverHideTimerRef.current !== null) {
      window.clearTimeout(clusterPopoverHideTimerRef.current);
      clusterPopoverHideTimerRef.current = null;
    }
    if (clusterPopoverRemoveTimerRef.current !== null) {
      window.clearTimeout(clusterPopoverRemoveTimerRef.current);
      clusterPopoverRemoveTimerRef.current = null;
    }
    setClusterPopoverOpen(true);
  };

  const openClusterPopover = (
    marker: RuntimeEventMarkerCluster,
    anchor: HTMLButtonElement,
  ) => {
    cancelClusterPopoverClose();
    const bounds = anchor.getBoundingClientRect();
    const preferredHalfWidth = 210;
    const viewportMargin = Math.min(preferredHalfWidth, window.innerWidth / 2);
    const left = clamp(
      bounds.left + bounds.width / 2,
      viewportMargin,
      Math.max(viewportMargin, window.innerWidth - viewportMargin),
    );
    setClusterPopover({
      marker,
      left,
      top: Math.max(8, bounds.top - 8),
    });
  };

  const scheduleClusterPopoverClose = () => {
    if (clusterPopoverHideTimerRef.current !== null) {
      window.clearTimeout(clusterPopoverHideTimerRef.current);
    }
    clusterPopoverHideTimerRef.current = window.setTimeout(() => {
      setClusterPopoverOpen(false);
      clusterPopoverRemoveTimerRef.current = window.setTimeout(() => {
        setClusterPopover(null);
      }, 150);
    }, 180);
  };

  const showDurationTooltip = (
    span: TimelineSpan,
    durationLabel: string,
    anchor: HTMLElement,
  ) => {
    const bounds = anchor.getBoundingClientRect();
    setDurationTooltip({
      span,
      durationLabel,
      left: clamp(bounds.left + bounds.width / 2, 132, window.innerWidth - 132),
      top: Math.max(8, bounds.top - 7),
    });
  };

  useEffect(() => {
    const shell = shellRef.current;
    if (!shell || events.length === 0) return;
    const visibleTrackWidth = Math.max(1, shell.clientWidth - timelineLabelWidth(shell));
    const cursorX = (cursorLeft / 100) * trackWidthPx;
    const visibleStart = shell.scrollLeft;
    const visibleEnd = visibleStart + visibleTrackWidth;
    const padding = 48;
    if (cursorX < visibleStart + padding) {
      shell.scrollTo({ left: Math.max(0, cursorX - padding), behavior: "smooth" });
    } else if (cursorX > visibleEnd - padding) {
      shell.scrollTo({
        left: Math.max(0, cursorX - visibleTrackWidth + padding),
        behavior: "smooth",
      });
    }
    // Intentionally do not depend on trackWidthPx. Zooming should preserve the
    // mouse anchor instead of auto-scrolling the selected cursor back into view.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cursorSequence, events.length]);

  useEffect(() => {
    const shell = shellRef.current;
    if (!shell || !cursorRowId) return;
    const target = findTimelineRow(shell, cursorRowId);
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
    const x = ratio * bounds.width;
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
  const shouldZoomFromWheelTarget = (target: EventTarget | null): boolean => {
    return target instanceof Element && Boolean(target.closest(".timeline-zoom-wheel-zone"));
  };
  useEffect(() => {
    const shell = shellRef.current;
    if (!shell) return;
    const onWheel = (event: WheelEvent) => {
      if (!shouldZoomFromWheelTarget(event.target)) return;
      event.preventDefault();
      event.stopPropagation();
      event.stopImmediatePropagation();
      const labelWidth = timelineLabelWidth(shell);
      const shellBounds = shell.getBoundingClientRect();
      const visibleTrackWidth = Math.max(1, shell.clientWidth - labelWidth);
      const visibleTrackX = clamp(
        event.clientX - shellBounds.left - labelWidth,
        0,
        visibleTrackWidth,
      );
      setZoomLevel((current) => {
        const currentTrackWidth = Math.max(1, trackViewportWidthPx * current);
        const worldX = clamp(shell.scrollLeft + visibleTrackX, 0, currentTrackWidth);
        const anchorRatio = worldX / currentTrackWidth;
        const next = event.deltaY < 0 ? current * 1.24 : current / 1.24;
        const zoom = Number(clamp(next, MIN_TIMELINE_ZOOM, MAX_TIMELINE_ZOOM).toFixed(2));
        const nextTrackWidth = Math.max(1, trackViewportWidthPx * zoom);
        window.requestAnimationFrame(() => {
          shell.scrollLeft = Math.max(0, anchorRatio * nextTrackWidth - visibleTrackX);
        });
        return zoom;
      });
    };
    shell.addEventListener("wheel", onWheel, { passive: false, capture: true });
    return () => shell.removeEventListener("wheel", onWheel, { capture: true });
  }, [trackViewportWidthPx]);
  const commitCursor = (clientX: number, options: { requireNear?: boolean } = {}) => {
    const position = pointerPosition(clientX);
    if (!position || position.sequence === 0) return;
    if (options.requireNear && !position.near) return;
    onCursorChange(position.sequence);
  };
  const visibleSpans = useMemo(
    () => [...timeline.spans].sort(compareTimelineSpans),
    [timeline.spans],
  );
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
          <button
            type="button"
            className={followLive ? "is-active" : ""}
            onClick={onToggleFollow}
            title={followLive ? "Enter replay mode" : "Return to the latest event"}
          >
            {followLive ? <Radio size={13} /> : <CirclePause size={13} />}
            {followLive ? "Following" : "Replay"}
          </button>
          {!followLive && (
            <div className="timeline-replay-controls">
              <button
                type="button"
                onClick={togglePlayback}
                disabled={events.length === 0}
                title={isPlaying ? "Stop replay" : "Play replay from the current event, or the first event"}
              >
                {isPlaying ? <Square size={13} /> : <Play size={13} />}
                {isPlaying ? "Stop" : "Play"}
              </button>
              <label className="timeline-playback-speed">
                <span>{formatPlaybackSpeed(playbackSpeed)}</span>
                <input
                  type="range"
                  min={MIN_PLAYBACK_SPEED}
                  max={MAX_PLAYBACK_SPEED}
                  step={0.25}
                  value={playbackSpeed}
                  onChange={(event) => setPlaybackSpeed(Number(event.target.value))}
                  aria-label="Replay speed"
                />
              </label>
            </div>
          )}
          <span className="timeline-zoom-label" title="Wheel over Runtime events or Execution to zoom the timeline. Zooming in expands clustered events and span widths.">
            Zoom {zoomLevel.toFixed(2)}x
          </span>
          {historyError && <span className="timeline-history-error">History unavailable</span>}
          {bufferedEventCount > 0 && (
            <button type="button" onClick={onFlushBufferedEvents}>
              Flush {bufferedEventCount}
            </button>
          )}
          {historyAvailable && (
            <button type="button" onClick={onLoadHistory} disabled={historyLoading}>
              {historyLoading ? <LoaderCircle className="spin" size={13} /> : <History size={13} />}
              {historyLoading ? "Loading events" : "Load next events"}
            </button>
          )}
          <span className="timeline-cursor-label">
            {formatTimestamp(cursorTime)} · event {cursorEventIndex}/{events.length}
            {totalEventCount > events.length ? ` loaded · ${totalEventCount} current` : ""}
          </span>
        </div>}
      </div>
      {!collapsed && (
      <div
        className="timeline-table-shell"
        ref={shellRef}
        onPointerLeave={() => setHoverLeft(null)}
        onScroll={() => {
          scheduleClusterPopoverClose();
          setDurationTooltip(null);
        }}
      >
          <div
            className="timeline-table"
            style={{ "--timeline-track-width": `${trackWidthPx}px` } as CSSProperties}
          >
            <div
              className="timeline-label-column timeline-event-label timeline-zoom-wheel-zone"
            >
              Runtime events
            </div>
            <div
              className="timeline-track timeline-event-row timeline-zoom-wheel-zone"
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
                  className={`timeline-event-marker event-category-${marker.category} ${marker.events.length > 1 ? "is-cluster" : ""} ${marker.x <= 10 ? "is-track-start" : ""} ${marker.x >= trackWidthPx - 10 ? "is-track-end" : ""}`}
                  type="button"
                  style={{ left: `${marker.left}%` }}
                  title={eventMarkerTitle(marker, events.length)}
                  onPointerDown={(event) => {
                    event.stopPropagation();
                  }}
                  onPointerEnter={(event) => {
                    openClusterPopover(marker, event.currentTarget);
                  }}
                  onPointerLeave={() => {
                    scheduleClusterPopoverClose();
                  }}
                  onClick={(event) => {
                    event.stopPropagation();
                    onCursorChange(marker.sequence);
                    if (marker.events.length === 1) {
                      const runtimeEvent = marker.events[0];
                      onSelect({
                        type: "event",
                        id: runtimeEvent.id,
                        sequence: runtimeEvent.sequence,
                      });
                    }
                  }}
                >
                  {marker.events.length > 1 && <span>{marker.events.length}</span>}
                </button>
              ))}
            </div>
            <div
              className="timeline-label-column timeline-axis-label timeline-zoom-wheel-zone"
            >
              Execution
            </div>
            <div
              className="timeline-track timeline-axis timeline-zoom-wheel-zone"
              ref={trackRef}
              onClick={(event) => commitCursor(event.clientX)}
              onPointerMove={(event) => previewCursor(event.clientX)}
              onPointerDown={(event) => {
                event.preventDefault();
                startCursorDrag(event.clientX);
              }}
            >
              {scale.ticks.map((tick, index) => (
                <span key={`${tick.left}:${index}`} style={{ left: `${tick.left}%` }}>
                  {tick.label}
                </span>
              ))}
            </div>
            {visibleSpans.map((span) => (
              <TimelineRow
                key={span.id}
                span={span}
                scale={scale}
                onCursorPreview={previewCursor}
                onCursorCommit={commitCursor}
                onCursorDragStart={startCursorDrag}
                onDurationHover={showDurationTooltip}
                onDurationLeave={() => setDurationTooltip(null)}
                onSelect={() => {
                  onSelect({ type: span.kind, id: span.id });
                }}
              />
            ))}
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
      </div>
      )}
      {clusterPopover && createPortal(
        <div
          className={`timeline-event-cluster-menu ${clusterPopoverOpen ? "is-open" : ""}`}
          role="menu"
          aria-label={
            clusterPopover.marker.events.length > 1
              ? `${clusterPopover.marker.events.length} merged runtime events`
              : "Runtime event"
          }
          style={{ left: clusterPopover.left, top: clusterPopover.top }}
          onPointerEnter={cancelClusterPopoverClose}
          onPointerLeave={scheduleClusterPopoverClose}
        >
          <span className="timeline-event-cluster-menu-title">
            {clusterPopover.marker.events.length > 1
              ? `${clusterPopover.marker.events.length} merged events`
              : "Runtime event"}
          </span>
          <div className="timeline-event-cluster-strip">
            {clusterPopover.marker.events.map((runtimeEvent) => (
              <button
                key={runtimeEvent.id}
                className={`timeline-event-cluster-item event-category-${eventDisplayCategory(runtimeEvent)}`}
                type="button"
                role="menuitem"
                title={`Event #${runtimeEvent.sequence}: ${runtimeEvent.type}`}
                onClick={() => {
                  onCursorChange(runtimeEvent.sequence);
                  onSelect({
                    type: "event",
                    id: runtimeEvent.id,
                    sequence: runtimeEvent.sequence,
                  });
                }}
              >
                <span>#{runtimeEvent.sequence}</span>
                <small>{shortEventType(runtimeEvent.type)}</small>
                <time>{formatTimestamp(runtimeEvent.occurred_at_ms)}</time>
                <em>
                  {runtimeEvent.status ?? "recorded"}
                  {runtimeEvent.elapsed_ns == null
                    ? ""
                    : ` · ${formatDuration(runtimeEvent.elapsed_ns / 1_000_000)}`}
                </em>
              </button>
            ))}
          </div>
        </div>,
        document.body,
      )}
      {durationTooltip && createPortal(
        <div
          className="timeline-duration-tooltip"
          role="tooltip"
          style={{ left: durationTooltip.left, top: durationTooltip.top }}
        >
          <strong>{durationTooltip.span.label}</strong>
          <span>
            <i className={`state-${durationTooltip.span.state}`} />
            {formatRuntimeState(durationTooltip.span.state)}
            <b>{durationTooltip.durationLabel}</b>
          </span>
          <span>
            <small>Started</small>
            <b>{formatTimestamp(durationTooltip.span.started_at_ms)}</b>
          </span>
          {durationTooltip.span.ended_at_ms !== null && (
            <span>
              <small>Ended</small>
              <b>{formatTimestamp(durationTooltip.span.ended_at_ms)}</b>
            </span>
          )}
          <span>
            <small>Event</small>
            <b>#{durationTooltip.span.sequence}</b>
          </span>
        </div>,
        document.body,
      )}
    </section>
  );
}

function TimelineRow({
  span,
  scale,
  onCursorPreview,
  onCursorCommit,
  onCursorDragStart,
  onDurationHover,
  onDurationLeave,
  onSelect,
}: {
  span: TimelineSpan;
  scale: TimelineScale;
  onCursorPreview: (clientX: number) => void;
  onCursorCommit: (clientX: number, options?: { requireNear?: boolean }) => void;
  onCursorDragStart: (clientX: number) => void;
  onDurationHover: (span: TimelineSpan, durationLabel: string, anchor: HTMLElement) => void;
  onDurationLeave: () => void;
  onSelect: () => void;
}) {
  const start = scale.left(span.started_at_ms);
  const end = scale.left(span.ended_at_ms ?? span.started_at_ms);
  const durationMs = span.ended_at_ms === null
    ? span.duration_ms
    : Math.max(0, span.ended_at_ms - span.started_at_ms);
  const durationLabel = formatDuration(durationMs ?? 0);
  const width = Math.max(
    0.15,
    Math.min(100 - start, Math.max(scale.minBarWidthPercent, end - start)),
  );
  // A zero-duration final span still needs a visible bar. Keep its right edge
  // inside the track rather than letting min-width create horizontal overflow.
  const left = Math.min(start, 100 - width);
  return (
    <>
      <button
        className="timeline-label is-node"
        type="button"
        data-timeline-row-id={span.id}
        onClick={onSelect}
        title={span.label}
      >
        <span className="timeline-label-spacer" />
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
          style={{ left: `${left}%`, width: `${width}%` }}
          onPointerEnter={(event) => onDurationHover(span, durationLabel, event.currentTarget)}
          onPointerLeave={onDurationLeave}
        >
          <span className="timeline-bar-duration">{durationLabel}</span>
        </span>
      </button>
    </>
  );
}

function formatRuntimeState(value: string): string {
  return value.replaceAll("_", " ");
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
  // The horizontal range represents visible runtime activity. The persisted
  // Invocation completion timestamp may be later than every event/span (for
  // example after final bookkeeping), which otherwise leaves a misleading gap
  // after the final visible node.
  return { start, end: Math.max(start + 1, eventEnd, spanEnd) };
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

function createTimelineScale(
  range: { start: number; end: number },
  timeline: TimelineView,
  events: RuntimeEvent[],
): TimelineScale {
  const anchors = timelineAnchors(timeline, events, range);
  const linearLeft = (timeMs: number) => percent(timeMs, range.start, range.end);
  const readableLeft = (timeMs: number) => {
    if (anchors.length <= 1) return linearLeft(timeMs);
    if (timeMs <= anchors[0]) return 0;
    if (timeMs >= anchors.at(-1)!) return 100;
    const index = upperBound(anchors, timeMs);
    const leftIndex = Math.max(0, index - 1);
    const rightIndex = Math.min(anchors.length - 1, index);
    const leftTime = anchors[leftIndex];
    const rightTime = anchors[rightIndex];
    const localRatio = rightTime === leftTime
      ? 0
      : (timeMs - leftTime) / (rightTime - leftTime);
    const rank = leftIndex + localRatio;
    const rankLeft = (rank / (anchors.length - 1)) * 100;
    return clamp(rankLeft * 0.84 + linearLeft(timeMs) * 0.16, 0, 100);
  };

  return {
    minBarWidthPercent: 0.5,
    left: readableLeft,
    ticks: [0, 0.25, 0.5, 0.75, 1].map((tick) => {
      const anchor = anchors[Math.min(anchors.length - 1, Math.round(tick * (anchors.length - 1)))] ?? range.start;
      return {
        left: readableLeft(anchor),
        label: formatDuration(anchor - range.start),
      };
    }),
  };
}

function timelineAnchors(
  timeline: TimelineView,
  events: RuntimeEvent[],
  range: { start: number; end: number },
): number[] {
  const values = new Set<number>([range.start, range.end]);
  for (const event of events) values.add(event.occurred_at_ms);
  for (const span of timeline.spans) {
    values.add(span.started_at_ms);
    values.add(span.ended_at_ms ?? span.started_at_ms);
  }
  return [...values].sort((left, right) => left - right);
}

function upperBound(values: number[], target: number): number {
  let low = 0;
  let high = values.length;
  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (values[mid] <= target) low = mid + 1;
    else high = mid;
  }
  return low;
}

function clusterRuntimeEventMarkers(
  events: RuntimeEvent[],
  scale: TimelineScale,
  trackWidthPx: number,
  minGapPx: number,
): RuntimeEventMarkerCluster[] {
  const positioned = events
    .map((event, index) => ({
      event,
      localIndex: index + 1,
      leftPx: (scale.left(event.occurred_at_ms) / 100) * trackWidthPx,
    }))
    .sort((left, right) => {
    if (left.leftPx !== right.leftPx) return left.leftPx - right.leftPx;
    return left.event.sequence - right.event.sequence;
  });

  const clusters: Array<{
    id: string;
    events: RuntimeEvent[];
    localIndexes: number[];
    leftPxTotal: number;
    leftPxMax: number;
  }> = [];
  for (const item of positioned) {
    const previous = clusters.at(-1);
    // Equal spacing is safe: adjacent 14px controls may touch, but they do not
    // overlap. Only merge markers that would actually cover one another.
    if (!previous || item.leftPx - previous.leftPxMax >= minGapPx) {
      clusters.push({
        id: item.event.id,
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

  const markers = clusters.map((cluster) => {
    const averageLeftPx = cluster.leftPxTotal / cluster.events.length;
    const markerHalfWidthPx = minGapPx / 2;
    const leftPx = Math.min(
      trackWidthPx - markerHalfWidthPx,
      Math.max(markerHalfWidthPx, averageLeftPx),
    );
    const localIndexes = [...cluster.localIndexes].sort((left, right) => left - right);
    const orderedEvents = cluster.events.sort((left, right) => left.sequence - right.sequence);
    return {
      id: cluster.id,
      left: percent(leftPx, 0, trackWidthPx),
      x: leftPx,
      category: clusterEventCategory(cluster.events),
      sequence: orderedEvents.at(-1)?.sequence ?? 0,
      firstLocalIndex: localIndexes[0] ?? 0,
      lastLocalIndex: localIndexes.at(-1) ?? 0,
      events: orderedEvents,
    };
  });
  return markers;
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
  if (event.entity_type === "node_execution") {
    return event.entity_id;
  }
  if (event.entity_type === "operator_call") {
    const nodeExecutionId = event.payload.node_execution_id;
    return nodeExecutionId === undefined || nodeExecutionId === null
      ? null
      : String(nodeExecutionId);
  }
  if (event.entity_type === "edge") {
    const sourceExecutionId = event.payload.node_execution_id;
    return sourceExecutionId === undefined || sourceExecutionId === null
      ? null
      : String(sourceExecutionId);
  }
  return null;
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

function eventDisplayCategory(event: RuntimeEvent): string {
  const state = String(event.payload.to ?? event.payload.state ?? "");
  return ["failed", "cancelled", "interrupted"].includes(state)
    ? "error"
    : eventCategory(event);
}

function shortEventType(type: string): string {
  const [entity, action] = type.split(".", 2);
  return action ? `${entity} ${action.replaceAll("_", " ")}` : type;
}

function formatPlaybackSpeed(value: number): string {
  return `${Number.isInteger(value) ? value : value.toFixed(2).replace(/0+$/, "")}x`;
}

function formatDuration(value: number): string {
  if (value < 1000) return `${Math.round(value)} ms`;
  if (value < 60_000) return `${(value / 1000).toFixed(2)} s`;
  return `${(value / 60_000).toFixed(1)} min`;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function timelineLabelWidth(element: HTMLElement): number {
  const value = getComputedStyle(element).getPropertyValue("--timeline-label-width").trim();
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : 220;
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
