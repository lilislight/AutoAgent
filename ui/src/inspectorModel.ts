import type { InspectorTab, TraceRow } from './types.js';

export const STAGE_LABELS: Record<string, string> = {
  'input.mapped': 'Input Mapping', 'capability.resolved': 'Capability Resolution',
  'node.aggregated': 'Aggregation', 'output.bound': 'Output Binding',
  'routing.resolved': 'Routing', 'node.faulted': 'Failure',
};
export function isSystemCall(row: TraceRow): boolean {
  return Boolean(row.operator_id?.startsWith('system_command:'));
}
export function executionTiming(row: TraceRow): number | null {
  const ns = row.execution_duration_ns;
  return ns == null || isSystemCall(row) && ns === 0 ? null : ns / 1000;
}
export function callPresentation(row: TraceRow, wait?: TraceRow | null): TraceRow {
  if (isSystemCall(row) && row.status === 'running' && (wait?.status === 'waiting' || wait?.status === 'cancelled')) {
    return { ...row, status: wait.status };
  }
  return row;
}
export function valueMessage(row: TraceRow, field: 'input' | 'output'): string {
  if (row.status === 'skipped') return 'This occurrence was skipped. No input was processed or output produced.';
  if (field === 'input' && row.error_phase === 'input_mapping') return 'Input Mapping failed before a result was recorded.';
  if (field === 'output' && ['ready', 'created', 'running', 'waiting', 'settling'].includes(row.status ?? '')) return 'Output will be available when this execution completes.';
  if (field === 'output' && ['failed', 'cancelled', 'lost'].includes(row.status ?? '')) return 'This execution ended without a recorded output.';
  return `${field === 'input' ? 'Input' : 'Output'} was not recorded.`;
}
export function stageDefinitionPath(row: TraceRow): string[] | undefined {
  const name = row.event_name === 'node.faulted' ? row.error_phase : row.event_name;
  switch (name) {
    case 'input.mapped': case 'input_mapping': return ['input_mapping'];
    case 'capability.resolved': case 'capability_resolution': case 'operator': return ['executable'];
    case 'node.aggregated': case 'aggregation': return ['map', 'aggregate'];
    case 'output.bound': case 'output_binding': return ['output_binding'];
    default: return undefined;
  }
}
/** Only expose sections that this stage actually owns. Errors belong to Summary. */
export function stageTabs(row: TraceRow): InspectorTab[] {
  const tabs: InspectorTab[] = ['Summary'];
  if (row.values.some(field => field !== 'error' && field !== 'conditions')) tabs.push('Input / Output');
  if (row.event_name === 'routing.resolved') tabs.push('Execution');
  if (stageDefinitionPath(row)) tabs.push('Definition');
  return tabs;
}
export function systemCommandId(id?: string): string {
  return id?.startsWith('system_command:') ? id : `system_command:${id ?? 'wait'}`;
}
export function pickDefinition(value: unknown, path?: string[]): unknown {
  for (const key of path ?? []) value = value && typeof value === 'object' ? (value as Record<string, unknown>)[key] : undefined;
  return value;
}
export function beforeCalls(stage: TraceRow): boolean {
  return ['input.mapped', 'capability.resolved'].includes(String(stage.event_name))
    || stage.event_name === 'node.faulted' && ['input_mapping', 'capability_resolution'].includes(String(stage.error_phase));
}
