import { moveWindow, zoomWindow, type TimeWindow } from './timeline.js';

// A null domain follows the full execution; a null selection fills its domain.
export interface RangeFrame { domain: TimeWindow | null; window: TimeWindow | null }
export interface RangeNavigation { frame: RangeFrame; history: RangeFrame[] }
export const initialRange = (): RangeNavigation => ({ frame: { domain: null, window: null }, history: [] });
export const sameWindow = (a: TimeWindow, b: TimeWindow) => a.start === b.start && a.end === b.end;
export function resolveRange(full: TimeWindow, frame: RangeFrame) {
  const domain = frame.domain ? moveWindow(full, frame.domain.start, frame.domain.end - frame.domain.start) : full;
  const window = frame.window ? moveWindow(domain, frame.window.start, frame.window.end - frame.window.start) : domain;
  return { domain, window };
}
export function rememberRange(state: RangeNavigation, frame: RangeFrame, full: TimeWindow, remember = true): RangeNavigation {
  const before = resolveRange(full, state.frame), after = resolveRange(full, frame);
  if (sameWindow(before.domain, after.domain) && sameWindow(before.window, after.window)) return state;
  return { frame, history: remember ? [...state.history, state.frame].slice(-30) : state.history };
}
export function selectRange(state: RangeNavigation, full: TimeWindow, next: TimeWindow) {
  const { domain } = resolveRange(full, state.frame);
  const window = moveWindow(domain, next.start, next.end - next.start);
  return rememberRange(state, { domain: state.frame.domain, window: sameWindow(domain, window) ? null : window }, full);
}
export function focusRange(state: RangeNavigation, full: TimeWindow, next: TimeWindow, remember = true) {
  const domain = moveWindow(full, next.start, next.end - next.start);
  return rememberRange(state, { domain: sameWindow(full, domain) ? null : domain, window: null }, full, remember);
}
export function zoomRange(state: RangeNavigation, full: TimeWindow, factor: number, anchor = .5, remember = true) {
  return focusRange(state, full, zoomWindow(full, resolveRange(full, state.frame).window, factor, anchor), remember);
}
export function panRange(state: RangeNavigation, full: TimeWindow, start: number, remember = true) {
  const { domain, window } = resolveRange(full, state.frame);
  const next = moveWindow(full, start, domain.end - domain.start);
  const shift = next.start - domain.start;
  return rememberRange(state, { domain: sameWindow(full, next) ? null : next,
    window: state.frame.window ? { start: window.start + shift, end: window.end + shift } : null }, full, remember);
}
export function previousRange(state: RangeNavigation): RangeNavigation {
  const frame = state.history.at(-1);
  return frame ? { frame, history: state.history.slice(0, -1) } : state;
}
export function eventZoomWindow(full: TimeWindow, start: number, end: number): TimeWindow | null {
  // end is exclusive: Events at the same microsecond cannot be split by time.
  if (end - start <= 1) return null;
  const padding = Math.max(1, Math.round((end - start) * .15));
  return moveWindow(full, start - padding, end - start + padding * 2);
}
