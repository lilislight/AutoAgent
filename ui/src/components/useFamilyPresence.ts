import { useSyncExternalStore } from 'react';

// Transient presence is shared across retained Invocations, so navigating to a
// Child keeps the open strip under the pointer. It is never saved to storage.
let expanded = false;
let timer: ReturnType<typeof setTimeout> | undefined;
const listeners = new Set<() => void>();
const notify = () => listeners.forEach(listener => listener());
function reveal() { clearTimeout(timer); if (!expanded) { expanded = true; notify(); } }
function dismiss() { clearTimeout(timer); if (expanded) { expanded = false; notify(); } }
function dismissLater() { clearTimeout(timer); timer = setTimeout(dismiss, 1400); }
function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => { listeners.delete(listener); if (!listeners.size) { clearTimeout(timer); expanded = false; } };
}
export function useFamilyPresence() {
  return { expanded: useSyncExternalStore(subscribe, () => expanded), reveal, dismiss, dismissLater };
}
