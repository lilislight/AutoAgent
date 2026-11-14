import { entityKey, type ConversationState } from './conversationState.js';

export interface ContextUsage {
  usedTokens: number | null;
  windowTokens: number | null;
  source: 'empty' | 'usage' | 'compaction' | 'unavailable';
  compacting: boolean;
}

const tokens = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value) && value >= 0;

/** Context is the latest active view, never the sum of the Session's billed calls. */
export function contextUsage(state: ConversationState, policyWindow?: number | null, modelInputLimit?: number | null): ContextUsage {
  const limits = [policyWindow, modelInputLimit].filter((value): value is number => tokens(value) && value > 0);
  const windowTokens = limits.length ? Math.min(...limits) : null;
  let latest: (typeof state.rows)[string] | undefined;
  let streaming = false;
  for (const row of Object.values(state.rows)) {
    if (row.type !== 'response' && row.type !== 'compaction') continue;
    // A partial response has no measured Usage yet. Keep the last known view.
    if (row.type === 'response' && !state.responses[entityKey(row.invocation, row.responseId)]) { streaming = true; continue; }
    if (!latest || row.at > latest.at || row.at === latest.at && row.id > latest.id) latest = row;
  }
  if (!latest) return { usedTokens: streaming ? null : 0, windowTokens, source: streaming ? 'unavailable' : 'empty', compacting: false };
  if (latest.type === 'compaction') {
    const value = state.compactions[entityKey(latest.invocation, latest.compactionId)];
    const usedTokens = value?.state === 'completed' ? value.after_tokens : value?.before_tokens;
    return { usedTokens: tokens(usedTokens) ? usedTokens : null, windowTokens,
      source: tokens(usedTokens) ? 'compaction' : 'unavailable', compacting: value?.state === 'running' };
  }
  const usage = latest.type === 'response' ? state.responses[entityKey(latest.invocation, latest.responseId)]?.usage : null;
  // Cached input and reasoning are subsets; adding them again overstates occupancy.
  const usedTokens = tokens(usage?.input_tokens) ? usage.input_tokens + (tokens(usage?.output_tokens) ? usage.output_tokens : 0)
    : tokens(usage?.total_tokens) ? usage.total_tokens : null;
  return { usedTokens, windowTokens, source: usedTokens === null ? 'unavailable' : 'usage', compacting: false };
}

export function contextPercentage(value: ContextUsage): number | null {
  return value.usedTokens === null || value.windowTokens === null ? null : value.usedTokens / value.windowTokens * 100;
}

export const compactTokens = (value: number | null) => value === null ? '—' : new Intl.NumberFormat('en', {
  notation: 'compact', maximumFractionDigits: 1,
}).format(value);
