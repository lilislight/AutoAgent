import type { TraceRow } from './types.js';

export const stackIndex = (position: number, total: number) => Math.max(0, Math.min(Math.max(0, total - 1), Math.round(position)));
export const stepStack = (position: number, direction: number, total: number) => stackIndex(stackIndex(position, total) + direction, total);

export function stackStatus(row: TraceRow, kind: 'node' | 'edge'): string {
  if (kind === 'edge') return row.condition_result ? 'selected' : 'skipped';
  return row.control_state === 'paused' ? 'paused' : row.status ?? 'planned';
}

/** Filtering retains the source ordinal used by the Inspector and position memory. */
export function stackRecords(rows: TraceRow[], total: number, kind: 'node' | 'edge') {
  return rows.map((row, sourceIndex) => ({ row, sourceIndex, ordinal: total - sourceIndex, status: stackStatus(row, kind) }));
}

export function stackJump(value: string, total: number): number | null {
  if (!/^\d+$/.test(value.trim())) return null;
  const ordinal = Number(value);
  return Number.isSafeInteger(ordinal) && ordinal >= 1 && ordinal <= total ? total - ordinal : null;
}
