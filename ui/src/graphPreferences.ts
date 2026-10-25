import type { Point } from './layout.js';
const KEY = 'autoagent.graph-presentation.v1';
interface Preferences { folded: string[]; layouts: Record<string, Record<string, Point>> }
export const foldKey = (ids: ReadonlySet<string>) => JSON.stringify([...ids].sort());
export function readGraphPreferences(revision: string): Preferences {
  try {
    const value = JSON.parse(localStorage.getItem(KEY) ?? '{}')[revision];
    if (!value || !Array.isArray(value.folded) || !value.folded.every((id:unknown)=>typeof id==='string')) return {folded:[],layouts:{}};
    const layouts: Preferences['layouts'] = {};
    for (const [key, positions] of Object.entries(value.layouts ?? {})) {
      if (!positions || typeof positions !== 'object') continue;
      layouts[key] = Object.fromEntries(Object.entries(positions).filter((entry): entry is [string,Point]=> {
        const p=entry[1];return p && typeof p==='object' && Number.isFinite(p.x) && Number.isFinite(p.y);
      }));
    }
    return {folded:value.folded,layouts};
  } catch { return {folded:[],layouts:{}}; }
}
export function writeGraphPreferences(revision: string, value: Preferences) {
  try {
    const all = JSON.parse(localStorage.getItem(KEY) ?? '{}'); delete all[revision];
    // Keep bounded layouts; storing fails harmlessly if browser quota is exhausted.
    all[revision] = {...value,layouts:Object.fromEntries(Object.entries(value.layouts).slice(-8).filter(([,p])=>Object.keys(p).length<=2000))};
    localStorage.setItem(KEY,JSON.stringify(Object.fromEntries(Object.entries(all).slice(-16))));
  } catch { /* Read-only or full browser storage: the current view still works. */ }
}
