export const DEFAULT_INSPECTOR_WIDTH = 460;
export const INSPECTOR_WIDTH_KEY = 'autoagent:inspector-width';

export function constrainInspectorWidth(preferred: number, available: number): number {
  const maximum = Math.max(1, Math.min(900, available));
  const minimum = Math.min(320, maximum);
  return Math.round(Math.max(minimum, Math.min(maximum, Number.isFinite(preferred) ? preferred : DEFAULT_INSPECTOR_WIDTH)));
}
