export type JsonPath = readonly (string | number)[];
export interface JsonMatch { path: JsonPath; name: string; value: unknown }

export function jsonPath(path: JsonPath): string {
  return '$' + path.map(part => typeof part === 'number' ? `[${part}]` : /^[A-Za-z_$][\w$]*$/.test(part) ? `.${part}` : `[${JSON.stringify(part)}]`).join('');
}
export function pathStartsWith(path: JsonPath, prefix: JsonPath): boolean {
  return prefix.length <= path.length && prefix.every((part, index) => part === path[index]);
}
export function fieldWindow(index: number, size = 50): number { return Math.max(0, Math.floor(index / size) * size); }

/** Literal, case-insensitive matches with offsets into the original text. */
export function jsonMatchRanges(text: string, query: string, limit = Infinity): { start: number; end: number }[] {
  const needle = query.trim().toLowerCase(), folded = text.toLowerCase();
  if (!needle) return [];
  // Some characters expand when lowercased (for example, İ). Keep display
  // offsets in the original UTF-16 string rather than the folded string.
  const offsets: { start: number; end: number }[] = [];
  if (folded.length !== text.length) {
    let start = 0;
    for (const char of text) {
      for (let index = 0; index < char.toLowerCase().length; index++) offsets.push({ start, end: start + char.length });
      start += char.length;
    }
  }
  const ranges: { start: number; end: number }[] = [];
  let cursor = 0;
  while (ranges.length < limit) {
    const index = folded.indexOf(needle, cursor);
    if (index < 0) break;
    const start = offsets.length ? offsets[index].start : index;
    const end = offsets.length ? offsets[index + needle.length - 1].end : index + needle.length;
    const previous = ranges[ranges.length - 1];
    if (previous && start < previous.end) previous.end = Math.max(previous.end, end);
    else ranges.push({ start, end });
    cursor = index + needle.length;
  }
  return ranges;
}

/** Keep long values bounded while making the first matching text visible. */
export function jsonTextPreview(text: string, query = '', limit = 160): string {
  if (text.length <= limit) return text;
  const match = query ? jsonMatchRanges(text, query, 1)[0] : undefined;
  const start = match ? Math.max(0, match.start - Math.floor(limit / 4)) : 0;
  const end = Math.min(text.length, Math.max(start + limit, match?.end ?? 0));
  return `${start ? '…' : ''}${text.slice(start, end)}${end < text.length ? '…' : ''}`;
}

/** Search loaded JSON cooperatively, retaining depth rather than all children. */
export async function searchJson(value: unknown, query: string, signal: AbortSignal, limit = 1000): Promise<{ matches: JsonMatch[]; truncated: boolean }> {
  const needle = query.trim().toLowerCase(), matches: JsonMatch[] = [];
  if (!needle) return { matches, truncated: false };
  type Frame = { value: unknown; path: (string | number)[]; keys: string[] | null; next: number };
  const frames: Frame[] = [{ value, path: [], keys: null, next: -1 }];
  const ancestors = new Set<object>();
  let processed = 0;
  while (frames.length) {
    signal.throwIfAborted();
    const frame = frames[frames.length - 1];
    if (frame.next === -1) {
      const object = frame.value !== null && typeof frame.value === 'object';
      const name = frame.path.length ? String(frame.path[frame.path.length - 1]) : '$';
      if (name.toLowerCase().includes(needle) || (!object && String(frame.value).toLowerCase().includes(needle))) {
        if (matches.length === limit) return { matches, truncated: true };
        matches.push({ path: frame.path, name, value: frame.value });
      }
      if (!object || ancestors.has(frame.value as object)) frames.pop();
      else { ancestors.add(frame.value as object); frame.keys = Object.keys(frame.value as object); frame.next = 0; }
    } else if (frame.next === frame.keys!.length) {
      ancestors.delete(frame.value as object); frames.pop();
    } else {
      const key = frame.keys![frame.next++];
      frames.push({ value: (frame.value as Record<string, unknown>)[key], path: [...frame.path, Array.isArray(frame.value) ? Number(key) : key], keys: null, next: -1 });
    }
    if (++processed % 1000 === 0) await new Promise<void>(resolve => setTimeout(resolve, 0));
  }
  return { matches, truncated: false };
}
export function jsonPreview(value: unknown, limit = 160, query = ''): string {
  if (value !== null && typeof value === 'object') return Array.isArray(value) ? `Array(${value.length})` : `Object(${Object.keys(value).length})`;
  const text = typeof value === 'string' ? value : String(value);
  return jsonTextPreview(text, query, limit);
}
