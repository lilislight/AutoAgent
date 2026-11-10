/** Brief retries absorb propagation races on reads; mutations are never retried. */
export function retryAgentRead(failures: number, error: Error): boolean {
  const status = error && typeof error === 'object' && 'status' in error ? error.status : null;
  const changed = 'code' in error && error.code === 'approval_changed';
  return failures < 2 && (changed || status === 404 || typeof status === 'number' && status >= 500 || error instanceof TypeError);
}
export const agentReadRetryDelay = (attempt: number) => Math.min(250 * 2 ** attempt, 1000);
