import { useRef, useState } from 'react';
import { elapsedLabel, moveWindow, resizeWindow, selectedWindow, type TimeWindow } from '../timeline';

interface Props {
  full: TimeWindow; window: TimeWindow; bins: number[];
  onChange: (window: TimeWindow) => void; onReset: () => void;
  onPreview: (window: TimeWindow | null) => void; replayTime?: number | null;
}
type Mode = 'select' | 'move' | 'start' | 'end';
export function TimelineRange({ full, window, bins, onChange, onReset, onPreview, replayTime }: Props) {
  const track = useRef<HTMLDivElement>(null);
  const drag = useRef<{ pointer: number; mode: Mode; x: number; time: number; initial: TimeWindow; draft: TimeWindow } | null>(null);
  const [draft, setDraft] = useState<TimeWindow | null>(null);
  const current = draft ?? window;
  const total = full.end - full.start;
  const left = (current.start - full.start) / total * 100;
  const right = (current.end - full.start) / total * 100;
  const zoomed = window.start !== full.start || window.end !== full.end;
  const maximum = Math.max(1, ...bins);
  const at = (x: number) => {
    const bounds = track.current!.getBoundingClientRect();
    return Math.round(full.start + Math.max(0, Math.min(1, (x - bounds.left) / Math.max(1, bounds.width))) * total);
  };
  const stop = () => { drag.current = null; setDraft(null); onPreview(null); };
  const key = (edge: 'start' | 'end', event: React.KeyboardEvent<HTMLButtonElement>) => {
    const delta = Math.max(1, Math.round(total * (event.shiftKey ? .1 : .01)));
    let time = window[edge];
    if (event.key === 'ArrowLeft') time -= delta;
    else if (event.key === 'ArrowRight') time += delta;
    else if (event.key === 'Home') time = full.start;
    else if (event.key === 'End') time = full.end;
    else return;
    event.preventDefault(); onChange(resizeWindow(full, window, edge, time));
  };
  return <div className="timeline-overview">
    <div className="timeline-overview-labels"><span title="Each bar counts Node occurrences beginning in one time bucket. Heights are relative to the busiest bucket. Ready or recorded time is used when no start time was saved.">Execution overview <small>Node activity</small></span><span>{elapsedLabel(current.start - full.start)} – {elapsedLabel(current.end - full.start)} <small>· {elapsedLabel(current.end - current.start)} selected</small></span></div>
    <div ref={track} className={`timeline-overview-track ${zoomed ? 'is-focused' : ''} ${draft ? 'is-dragging' : ''}`} aria-label="Execution time range. Drag to select; drag the selection to move; drag either handle to resize. Double-click to show the full range."
      onDoubleClick={onReset}
      onPointerDown={event => {
        if (event.button !== 0) return;
        const action = (event.target as Element).closest<HTMLElement>('[data-range-action]')?.dataset.rangeAction as Mode | undefined;
        const mode = event.shiftKey ? 'select' : action === 'move' && !zoomed ? 'select' : action ?? 'select';
        const time = at(event.clientX);
        const next = mode === 'select' ? selectedWindow(full, time, time) : window;
        drag.current = { pointer: event.pointerId, mode, x: event.clientX, time, initial: window, draft: next };
        setDraft(next); event.currentTarget.setPointerCapture(event.pointerId); event.preventDefault();
      }}
      onPointerMove={event => {
        const start = drag.current;
        if (!start || start.pointer !== event.pointerId) return;
        const time = at(event.clientX);
        const next = start.mode === 'select' ? selectedWindow(full, start.time, time)
          : start.mode === 'move' ? moveWindow(full, start.initial.start + time - start.time, start.initial.end - start.initial.start)
          : resizeWindow(full, start.initial, start.mode, time);
        start.draft = next; setDraft(next); onPreview(next);
      }}
      onPointerUp={event => {
        const start = drag.current;
        if (!start || start.pointer !== event.pointerId) return;
        if (Math.abs(event.clientX - start.x) >= 3 || start.mode === 'start' || start.mode === 'end') onChange(start.draft);
        stop(); if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId);
      }} onPointerCancel={stop} onLostPointerCapture={stop}>
      <svg className="timeline-density" viewBox="0 0 640 32" preserveAspectRatio="none" aria-hidden="true">{bins.map((count, index) => count ? <rect key={index} x={index * 640 / bins.length + 1} y={32 - count / maximum * 27} width={Math.max(1, 640 / bins.length - 2)} height={count / maximum * 27} rx="1" /> : null)}</svg>
      <div className="timeline-overview-shade" style={{ left: 0, width: `${left}%` }} /><div className="timeline-overview-shade" style={{ left: `${right}%`, right: 0 }} />
      <div className="timeline-overview-window" data-range-action="move" style={{ left: `${left}%`, width: `${right - left}%` }} tabIndex={0} role="slider" aria-label="Selected time range" aria-valuemin={0} aria-valuemax={total - (current.end - current.start)} aria-valuenow={current.start - full.start} aria-valuetext={`${elapsedLabel(current.start - full.start)} to ${elapsedLabel(current.end - full.start)}`}
        onKeyDown={event => {
          const width = window.end - window.start;
          const step = Math.max(1, Math.round(width * (event.shiftKey ? .5 : .1)));
          const start = event.key === 'ArrowLeft' ? window.start - step : event.key === 'ArrowRight' ? window.start + step : event.key === 'Home' ? full.start : event.key === 'End' ? full.end - width : null;
          if (start !== null) { event.preventDefault(); onChange(moveWindow(full, start, width)); }
        }} />
      {replayTime != null ? <span className="timeline-overview-replay" style={{ left: `${Math.max(0, Math.min(100, (replayTime - full.start) / total * 100))}%` }} role="img" aria-label={`Replay position: ${elapsedLabel(replayTime - full.start)} since start`} /> : null}
      {(['start', 'end'] as const).map(edge => <button key={edge} className={`timeline-range-handle handle-${edge}`} data-range-action={edge} style={{ left: `${edge === 'start' ? left : right}%` }} role="slider" aria-label={`Selection ${edge}`} aria-valuemin={0} aria-valuemax={total} aria-valuenow={current[edge] - full.start} aria-valuetext={elapsedLabel(current[edge] - full.start)} onKeyDown={event => key(edge, event)} />)}
    </div>
    <div className="timeline-overview-ticks">{[0,.25,.5,.75,1].map(ratio => <span key={ratio}>{elapsedLabel(total * ratio)}</span>)}</div>
  </div>;
}
