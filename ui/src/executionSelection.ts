import type { ExecutionRecordSelection, Selection, TraceRow } from './types.js';

export function recordIdentity(kind: ExecutionRecordSelection['kind'], row: TraceRow): string | undefined {
  if (kind === 'edge') return row.event_id != null && row.result_index != null ? `${row.event_id}:${row.result_index}` : undefined;
  return kind === 'node' ? row.occurrence_id : kind === 'call' ? row.call_id : row.wait_id;
}
export function selectRecord(kind: ExecutionRecordSelection['kind'], row: TraceRow, section?: ExecutionRecordSelection['section']): Selection {
  const id = kind === 'edge' ? row.edge_id as string | undefined : row.node_id;
  const identity = recordIdentity(kind, row);
  return id && identity ? { kind: kind === 'edge' ? 'edge' : 'node', id, record: { kind, id: identity, ...(section ? { section } : {}) } } : null;
}
export function recordMatches(selection: NonNullable<Selection>, row: TraceRow): boolean {
  const record = selection.record;
  return Boolean(record && recordIdentity(record.kind, row) === record.id && (record.kind === 'edge'
    ? selection.kind === 'edge' && row.edge_id === selection.id
    : selection.kind === 'node' && row.node_id === selection.id));
}
export function selectedInterval(kind: ExecutionRecordSelection['kind'], row: TraceRow) {
  const start = kind === 'edge' ? row.occurred_at_us as number | null : kind === 'wait' ? row.created_at_us as number | null
    : row.started_at_us ?? row.ready_at_us ?? row.completed_at_us;
  const end = kind === 'edge' ? start : kind === 'wait' ? row.end_at_us as number | null : row.completed_at_us;
  return typeof start === 'number' ? { start_at_us: start, end_at_us: typeof end === 'number' ? end : null, status: row.status } : null;
}
export function parentRecord(selection: NonNullable<Selection>, row: TraceRow): Selection {
  if (selection.record?.kind === 'wait' && row.wait_id) return { kind: 'node', id: selection.id, record: { kind: 'call', id: row.wait_id } };
  const id = row.node_id;
  const occurrence = row.occurrence_id ?? row.source_occurrence_id;
  return id && occurrence ? { kind: 'node', id, record: { kind: 'node', id: occurrence } } : null;
}
