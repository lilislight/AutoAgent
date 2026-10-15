/** Retain a few lightweight family views, always keeping the Root when available. */
export function retainedViews<T extends { key: string }>(current: T, previous: T[], group: (value: T) => string | null, limit: number, pinnedKey?: string): T[] {
  const family = group(current);
  if (!family || limit < 2) return [];
  const unique = new Map<string, T>([[current.key, current]]);
  for (const value of previous) if (group(value) === family && !unique.has(value.key)) unique.set(value.key, value);
  const views = [...unique.values()].slice(0, limit);
  const pinned = pinnedKey ? unique.get(pinnedKey) : undefined;
  if (pinned && !views.some(value => value.key === pinned.key)) views[views.length - 1] = pinned;
  return views.filter(value => value.key !== current.key);
}
