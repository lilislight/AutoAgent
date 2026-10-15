export const EXECUTION_LAYOUT_KEY = 'autoagent.execution-layout.v1';
import type { ExecutionView } from './types.js';
export type ExecutionLayout = { orientation: 'horizontal' | 'vertical'; ratio: number; view: ExecutionView };
export const DEFAULT_EXECUTION_LAYOUT: ExecutionLayout = { orientation: 'vertical', ratio: .425, view: 'graph' };
export function readExecutionLayout(raw: string | null): ExecutionLayout {
  try {
    const value = JSON.parse(raw ?? 'null');
    if (!value || !['horizontal', 'vertical'].includes(value.orientation)) return DEFAULT_EXECUTION_LAYOUT;
    return { orientation: value.orientation, view: ['graph', 'timeline', 'split'].includes(value.view) ? value.view : 'graph',
      ratio: typeof value.ratio === 'number' && Number.isFinite(value.ratio) ? Math.max(.2, Math.min(.8, value.ratio)) : .425 };
  } catch { return DEFAULT_EXECUTION_LAYOUT; }
}
export function graphRatioAt(position: number, size: number, min: number, max: number) {
  const ratio = position / Math.max(1, size);
  return Math.max(min, Math.min(max, ratio));
}
