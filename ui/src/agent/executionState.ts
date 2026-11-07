import type { Controls } from './types.js';

export function executionStatus(status: string, controls?: Pick<Controls, 'control_state' | 'recovery_reason'>): string {
  if (status === 'completed' || status === 'failed' || status === 'cancelled') return status;
  if (controls?.control_state === 'paused') return 'paused';
  if (status === 'running' && controls?.recovery_reason === 'interrupted') return 'interrupted';
  return status;
}
