import { useState } from 'react';
// Business input stays in bounded page memory, never browser persistent storage.
const drafts = new Map<string, { value: unknown; bytes: number }>();
let usedBytes = 0;
export function useActionDraft<T>(key: string, initial: () => T): [T, (value: T | ((previous: T) => T)) => void] {
  const [state, setState] = useState<{ key: string; value: T }>(() => ({ key, value: drafts.has(key) ? drafts.get(key)!.value as T : initial() }));
  const value = state.key === key ? state.value : drafts.has(key) ? drafts.get(key)!.value as T : initial();
  const update = (next: T | ((previous: T) => T)) => {
    const updated = typeof next === 'function' ? (next as (previous: T) => T)(value) : next;
    usedBytes -= drafts.get(key)?.bytes ?? 0;
    drafts.delete(key);
    const bytes = JSON.stringify(updated).length * 2;
    drafts.set(key, { value: updated, bytes }); usedBytes += bytes;
    while (drafts.size > 60 || usedBytes > 8 * 1024 * 1024) {
      const oldest = drafts.keys().next().value!;
      usedBytes -= drafts.get(oldest)!.bytes; drafts.delete(oldest);
    }
    setState({ key, value: updated });
  };
  return [value, update];
}
