import { entityKey, sortedRows, type ConversationState, type FeedRow } from './conversationState.js';
import type { AgentInvocation, ResponseBlock, ResponseSnapshot } from './types.js';

export interface ConversationTurn { type: 'turn'; id: string; invocation: string; at: number; rows: FeedRow[] }
export type ConversationEntry = ConversationTurn | { type: 'message'; id: string; at: number; row: Extract<FeedRow, { type: 'message' }> };

/** Group model rounds without hiding or reordering messages received mid-task. */
export function conversationEntries(state: ConversationState, invocations: AgentInvocation[] = []): ConversationEntry[] {
  const entries: ConversationEntry[] = [], turns = new Map<string, ConversationTurn>();
  const messages = new Map<string, number>();
  for (const row of sortedRows(state)) {
    const turn = turns.get(row.invocation);
    if (turn) { turn.rows.push(row); continue; }
    if (row.type === 'message' && row.message.source === 'user') {
      entries.push({ type: 'message', id: row.id, at: row.at, row });
      messages.set(row.invocation, row.at);
    } else {
      const created: ConversationTurn = { type: 'turn', id: `turn:${row.invocation}`, invocation: row.invocation, at: row.at, rows: [row] };
      turns.set(row.invocation, created); entries.push(created);
    }
  }
  for (const item of invocations) {
    if (turns.has(item.invocation_id) || !item.status || (!messages.has(item.invocation_id) && !['running', 'waiting', 'failed', 'cancelled'].includes(item.status))) continue;
    entries.push({ type: 'turn', id: `turn:${item.invocation_id}`, invocation: item.invocation_id,
      at: messages.has(item.invocation_id) ? messages.get(item.invocation_id)! + 1 : item.started_at_us ?? 0, rows: [] });
  }
  return entries.sort((a, b) => a.at - b.at || a.id.localeCompare(b.id));
}

export function isAnswerBlock(block: ResponseBlock): boolean {
  return block.type === 'media' || (block.type === 'text' || block.type === 'refusal') && !!block.text.trim();
}

export function finalResponse(state: ConversationState, invocation: string, status?: string): ResponseSnapshot | undefined {
  if (status === 'failed' || status === 'cancelled') return undefined;
  const complete = state.completed[invocation];
  const response = complete && state.responses[entityKey(invocation, complete.final_response_id)];
  return response && response.blocks.some(isAnswerBlock) ? response : undefined;
}

export function hasLogContent(row: FeedRow, state: ConversationState, finalId?: string): boolean {
  if (row.type === 'history') return true;
  if (row.type !== 'response') return row.type !== 'message' || row.message.source === 'agent';
  const key = entityKey(row.invocation, row.responseId), response = state.responses[key];
  if (!response) return !!state.live[key]?.parts.some(part => part.text.trim());
  return response.blocks.some(block => block.type === 'reasoning' ? !!block.text?.trim() || block.summary.some(text => text.trim())
    : block.type === 'tool' || row.responseId !== finalId && isAnswerBlock(block));
}

export function formatElapsed(microseconds: number): string {
  const seconds = Math.max(0, Math.floor(microseconds / 1_000_000));
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor(seconds % 3600 / 60)}m`;
}
