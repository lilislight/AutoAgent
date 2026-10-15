/** Metadata refresh never invalidates immutable definitions or business values. */
const metadataKinds = new Set(['execution-record', 'execution-timeline', 'timeline-items', 'timeline-position', 'timeline-markers', 'timeline-event-groups', 'timeline-marker-details', 'inspector-items', 'occurrence-stack', 'execution-family', 'family-children']);
export function metadataScope(invocation: string, epoch: number) {
  return { executionScope: invocation, executionEpoch: epoch };
}
export function belongsToSnapshot(key: readonly unknown[], meta: Record<string, unknown> | undefined, invocation: string, epoch: number) {
  return metadataKinds.has(String(key[0])) && meta?.executionScope === invocation && meta.executionEpoch === epoch;
}
export function snapshotTime(us: number | null | undefined) {
  return us == null ? 'No saved events' : new Date(us / 1000).toLocaleString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
}
