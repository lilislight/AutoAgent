import { useEffect, useId, useSyncExternalStore } from 'react';

// Transient presence is shared across retained Invocations, so navigating to a
// Child keeps the open strip under the pointer. It is never saved to storage.
let expanded = false;
let timer: ReturnType<typeof setTimeout> | undefined;
const listeners = new Set<() => void>();
const interactions=new Set<string>();
const holds=new Set<symbol>();
export const FAMILY_DISMISS_DELAY=1400;
const notify = () => listeners.forEach(listener => listener());
function reveal() { clearTimeout(timer); if (!expanded) { expanded = true; notify(); } }
function dismiss() { clearTimeout(timer); if (expanded) { expanded = false; notify(); } }
function dismissLater() { clearTimeout(timer); if(!interactions.size && !holds.size)timer = setTimeout(dismiss, FAMILY_DISMISS_DELAY); }
function interaction(key:string,value:boolean) { if(value){interactions.add(key);reveal();}else{interactions.delete(key);dismissLater();} }
function hold() { const token=Symbol();holds.add(token);reveal();return()=>{holds.delete(token);dismissLater();}; }
function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => { listeners.delete(listener); if (!listeners.size) { clearTimeout(timer); expanded = false; interactions.clear(); holds.clear(); } };
}
export function useFamilyPresence() {
  const id=useId();
  useEffect(()=>()=>{interaction(id+':pointer',false);interaction(id+':focus',false);},[id]);
  return { expanded: useSyncExternalStore(subscribe, () => expanded), reveal, dismiss, dismissLater,hold,
    pointer:(value:boolean)=>interaction(id+':pointer',value),focus:(value:boolean)=>interaction(id+':focus',value) };
}
