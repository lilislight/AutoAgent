import type { Controls } from './types.js';

export function executionStatus(status: string, controls?: Pick<Controls, 'control_state'>): string {
  if (status === 'completed' || status === 'failed' || status === 'cancelled') return status;
  if (controls?.control_state === 'pausing') return 'pausing';
  if (controls?.control_state === 'paused') return 'paused';
  // A Host recovery hint can describe a nonresident graph or a restore in progress.
  return status;
}
