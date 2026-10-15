import { useSyncExternalStore } from 'react';
import { DEFAULT_EXECUTION_LAYOUT, EXECUTION_LAYOUT_KEY, readExecutionLayout, type ExecutionLayout } from '../executionLayout';

let current = DEFAULT_EXECUTION_LAYOUT;
const listeners = new Set<() => void>();
try { current = readExecutionLayout(localStorage.getItem(EXECUTION_LAYOUT_KEY)); } catch { /* Storage may be disabled. */ }
const notify = () => listeners.forEach(listener => listener());
window.addEventListener('storage', event => {
  if (event.key === EXECUTION_LAYOUT_KEY || event.key === null) { current = readExecutionLayout(event.newValue); notify(); }
});
function subscribe(listener: () => void) { listeners.add(listener); return () => { listeners.delete(listener); }; }
export function setExecutionLayout(patch: Partial<ExecutionLayout>, persist = true) {
  current = { ...current, ...patch }; notify();
  if (persist) persistExecutionLayout();
}
export function persistExecutionLayout() {
  try { localStorage.setItem(EXECUTION_LAYOUT_KEY, JSON.stringify(current)); } catch { /* Keep in-memory preferences. */ }
}
export function getExecutionLayout() { return current; }
export function useExecutionLayout() { return useSyncExternalStore(subscribe, () => current); }
