import type { TimelineEventGroup } from './types.js';
import { eventCategory, type EventCategory, type TimeWindow } from './timeline.js';

/** Uniform 18px circles leave at least two pixels between visible edges. */
export const EVENT_MIN_GAP = 20;
export type EventCluster = TimelineEventGroup & { left: number };
/** Weighted centres are checked again after each merge, including the previous cluster. */
export function clusterEventGroups(items: TimelineEventGroup[], window: TimeWindow, width: number): EventCluster[] {
  const groups: EventCluster[] = [];
  const position = (time: number) => Math.max(0, Math.min(100, (time - window.start) / Math.max(1, window.end - window.start) * 100));
  for (const item of [...items].sort((a,b) => a.occurred_at_us - b.occurred_at_us || a.first_sequence - b.first_sequence)) {
    // Cached whole-range aggregates can be reprojected during range dragging.
    // Never clamp off-screen Events into misleading piles at either endpoint.
    if (item.occurred_at_us < window.start || item.occurred_at_us > window.end) continue;
    groups.push({ ...item, names: { ...item.names }, left:position(item.occurred_at_us) });
    while (groups.length > 1 && (groups.at(-1)!.left - groups.at(-2)!.left) * width / 100 < EVENT_MIN_GAP) {
      const right = groups.pop()!, left = groups.pop()!;
      const count = left.count + right.count;
      const time = left.occurred_at_us + (right.occurred_at_us - left.occurred_at_us) * right.count / count;
      const names = { ...left.names };
      for (const [name, n] of Object.entries(right.names)) names[name] = (names[name] ?? 0) + n;
      groups.push({ count, names, occurred_at_us:time, start_us:Math.min(left.start_us,right.start_us),
        end_us:Math.max(left.end_us,right.end_us), first_sequence:Math.min(left.first_sequence,right.first_sequence),
        last_sequence:Math.max(left.last_sequence,right.last_sequence), marker:null, left:position(time) });
    }
  }
  return groups;
}
export function eventGroupCategory(group: TimelineEventGroup): EventCategory {
  const categories = new Set(Object.keys(group.names).map(eventCategory));
  return categories.has('error') ? 'error' : categories.size === 1 ? [...categories][0] : 'mixed';
}
