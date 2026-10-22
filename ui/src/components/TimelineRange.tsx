import { useRef, useState } from 'react';
import { elapsedLabel, elapsedPosition, moveWindow, resizeWindow, selectedWindow, type TimeWindow } from '../timeline';

interface Props {
  full: TimeWindow; domain: TimeWindow; window: TimeWindow; bins: number[];
  onFocus: () => void; onPan: (start: number, remember: boolean) => void;
  onChange: (window: TimeWindow) => void;
  onPreview: (window: TimeWindow | null) => void; replayTime?: number | null;
}
type Mode = 'select' | 'move' | 'start' | 'end';
function OverviewLocator({ full, domain, onPan }: Pick<Props, 'full' | 'domain' | 'onPan'>) {
  const drag = useRef<{pointer:number;x:number;start:number;remember:boolean} | null>(null);
  const span = full.end - full.start, width = domain.end - domain.start;
  const clear = () => { drag.current = null; };
  return <div className="timeline-global-locator" role="slider" tabIndex={0} aria-label="Overview position in full execution"
    aria-valuemin={0} aria-valuemax={span-width} aria-valuenow={domain.start-full.start}
    aria-valuetext={`${elapsedPosition(domain.start-full.start,width)} to ${elapsedPosition(domain.end-full.start,width)} of ${elapsedLabel(span)}`}
    onKeyDown={event => {
      const next = event.key==='Home'?full.start:event.key==='End'?full.end-width:event.key==='ArrowLeft'?domain.start-width/2:event.key==='ArrowRight'?domain.start+width/2:null;
      if(next!==null){event.preventDefault();onPan(next,true);}
    }}
    onPointerDown={event => {
      if(event.button!==0)return;
      const rect=event.currentTarget.getBoundingClientRect(), offset=(event.clientX-rect.left)/rect.width;
      const inside=Boolean((event.target as Element).closest('.timeline-global-hit')) || offset*span>=domain.start-full.start && offset*span<=domain.end-full.start;
      const start=inside?domain.start:moveWindow(full,full.start+offset*span-width/2,width).start;
      drag.current={pointer:event.pointerId,x:event.clientX,start,remember:start===domain.start};
      if(start!==domain.start)onPan(start,true);
      event.currentTarget.setPointerCapture(event.pointerId);event.preventDefault();
    }}
    onPointerMove={event => {
      const origin=drag.current;if(!origin || origin.pointer!==event.pointerId)return;
      const delta=(event.clientX-origin.x)/event.currentTarget.getBoundingClientRect().width*span;
      const next=moveWindow(full,origin.start+delta,width).start;
      if(next===domain.start)return;
      onPan(next,origin.remember);origin.remember=false;
    }} onPointerUp={event => {clear();if(event.currentTarget.hasPointerCapture(event.pointerId))event.currentTarget.releasePointerCapture(event.pointerId);}}
    onPointerCancel={clear} onLostPointerCapture={clear}>
    <span className="timeline-global-domain" style={{left:`${(domain.start-full.start)/span*100}%`,width:`${width/span*100}%`}} />
    <span className="timeline-global-hit" style={{left:`${(domain.start-full.start+width/2)/span*100}%`}} />
  </div>;
}
export function TimelineRange({ full, domain, window, bins, onChange, onFocus, onPan, onPreview, replayTime }: Props) {
  const track = useRef<HTMLDivElement>(null);
  const drag = useRef<{ pointer: number; mode: Mode; x: number; time: number; initial: TimeWindow; draft: TimeWindow } | null>(null);
  const [draft, setDraft] = useState<TimeWindow | null>(null);
  const current = draft ?? window;
  const total = domain.end - domain.start;
  const left = (current.start - domain.start) / total * 100;
  const right = (current.end - domain.start) / total * 100;
  const zoomed = window.start !== domain.start || window.end !== domain.end;
  const maximum = Math.max(1, ...bins);
  const at = (x: number) => {
    const bounds = track.current!.getBoundingClientRect();
    return Math.round(domain.start + Math.max(0, Math.min(1, (x - bounds.left) / Math.max(1, bounds.width))) * total);
  };
  const stop = () => { drag.current = null; setDraft(null); onPreview(null); };
  const key = (edge: 'start' | 'end', event: React.KeyboardEvent<HTMLButtonElement>) => {
    const delta = Math.max(1, Math.round(total * (event.shiftKey ? .1 : .01)));
    let time = window[edge];
    if (event.key === 'ArrowLeft') time -= delta;
    else if (event.key === 'ArrowRight') time += delta;
    else if (event.key === 'Home') time = domain.start;
    else if (event.key === 'End') time = domain.end;
    else return;
    event.preventDefault(); onChange(resizeWindow(domain, window, edge, time));
  };
  return <div className="timeline-overview">
    <div className="timeline-overview-labels"><span title="Each bar counts Node occurrences beginning in one time bucket. Heights are relative to the busiest bucket. Skipped and inherited occurrences are excluded.">Execution overview <small>Node activity</small></span><button className="timeline-expand-selection" disabled={!zoomed} onClick={onFocus} title="Expand the selected interval to fill the overview">Expand selection</button><span>{elapsedPosition(current.start - full.start,total)} – {elapsedPosition(current.end - full.start,total)} <small>· {elapsedLabel(current.end - current.start)} selected</small></span></div>
    <div ref={track} className={`timeline-overview-track ${zoomed ? 'is-focused' : ''} ${draft ? 'is-dragging' : ''}`} aria-label="Execution time range. Scroll to zoom in or out. Drag to select; drag the selection to move; drag either handle to resize. Double-click to expand the selection."
      onDoubleClick={onFocus}
      onPointerDown={event => {
        if (event.button !== 0) return;
        const action = (event.target as Element).closest<HTMLElement>('[data-range-action]')?.dataset.rangeAction as Mode | undefined;
        const mode = event.shiftKey ? 'select' : action === 'move' && !zoomed ? 'select' : action ?? 'select';
        const time = at(event.clientX);
        const next = mode === 'select' ? selectedWindow(domain, time, time) : window;
        drag.current = { pointer: event.pointerId, mode, x: event.clientX, time, initial: window, draft: next };
        setDraft(next); event.currentTarget.setPointerCapture(event.pointerId); event.preventDefault();
      }}
      onPointerMove={event => {
        const start = drag.current;
        if (!start || start.pointer !== event.pointerId) return;
        const time = at(event.clientX);
        const next = start.mode === 'select' ? selectedWindow(domain, start.time, time)
          : start.mode === 'move' ? moveWindow(domain, start.initial.start + time - start.time, start.initial.end - start.initial.start)
          : resizeWindow(domain, start.initial, start.mode, time);
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
      <div className="timeline-overview-window" data-range-action="move" style={{ left: `${left}%`, width: `${right - left}%` }} tabIndex={0} role="slider" aria-label="Selected time range" aria-valuemin={0} aria-valuemax={total - (current.end - current.start)} aria-valuenow={current.start - domain.start} aria-valuetext={`${elapsedPosition(current.start - full.start,total)} to ${elapsedPosition(current.end - full.start,total)}`}
        onKeyDown={event => {
          const width = window.end - window.start;
          const step = Math.max(1, Math.round(width * (event.shiftKey ? .5 : .1)));
          const start = event.key === 'ArrowLeft' ? window.start - step : event.key === 'ArrowRight' ? window.start + step : event.key === 'Home' ? domain.start : event.key === 'End' ? domain.end - width : null;
          if (start !== null) { event.preventDefault(); onChange(moveWindow(domain, start, width)); }
        }} />
      {replayTime != null && replayTime >= domain.start && replayTime <= domain.end ? <span className="timeline-overview-replay" style={{ left: `${Math.max(0, Math.min(100, (replayTime - domain.start) / total * 100))}%` }} role="img" aria-label={`Replay position: ${elapsedLabel(replayTime - full.start)} since start`} /> : null}
      {(['start', 'end'] as const).map(edge => <button key={edge} className={`timeline-range-handle handle-${edge}`} data-range-action={edge} style={{ left: `${edge === 'start' ? left : right}%` }} role="slider" aria-label={`Selection ${edge}`} aria-valuemin={0} aria-valuemax={total} aria-valuenow={current[edge] - domain.start} aria-valuetext={elapsedLabel(current[edge] - full.start)} onKeyDown={event => key(edge, event)} />)}
    </div>
    <div className="timeline-overview-ticks">{[0,.25,.5,.75,1].map(ratio => <span key={ratio}>{elapsedPosition(domain.start - full.start + total * ratio,total)}</span>)}</div>
    {domain.start !== full.start || domain.end !== full.end ? <OverviewLocator full={full} domain={domain} onPan={onPan} /> : null}
  </div>;
}
