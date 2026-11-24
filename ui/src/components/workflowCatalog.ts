import type { QueryClient } from '@tanstack/react-query';

const catalogQueries = new Set(['workflow-catalog', 'workflow-revisions', 'filter-revisions', 'run-registered-workflows']);

/** Preserve ready content and reread snapshots that predate a registration notice. */
export async function refreshWorkflowCatalog(client: QueryClient, active: () => boolean) {
  const filter = { predicate: (query: { queryKey: readonly unknown[] }) => catalogQueries.has(String(query.queryKey[0])) };
  const pending = new Set(client.getQueryCache().findAll(filter)
    .filter(query => query.state.fetchStatus === 'fetching').map(query => query.queryHash));
  await client.invalidateQueries(filter, { cancelRefetch: false });
  if (active() && pending.size) {
    await client.invalidateQueries({ predicate: query => pending.has(query.queryHash) }, { cancelRefetch: false });
  }
}
