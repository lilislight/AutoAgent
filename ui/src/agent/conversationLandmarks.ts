import { entityKey, sortedRows, type ConversationState } from './conversationState.js';
import { finalResponse } from './conversationTurns.js';
import type { AgentInvocation, AgentTurnIndexItem } from './types.js';

export interface ConversationLandmark { id: string; anchor: string; invocation: string; at: number; question: string; answer: string }

export function messageAnchor(id: string) { return `agent-message-${encodeURIComponent(id)}`; }

function excerpt(text: string, maximum: number) {
  const value = text.slice(0, maximum * 3).replace(/\s+/g, ' ').trim();
  const shortened = value.length > maximum || text.length > maximum * 3;
  let result = value.slice(0, maximum);
  if (/[\uD800-\uDBFF]/.test(result.slice(-1))) result = result.slice(0, -1);
  return result + (shortened ? '…' : '');
}

/** Only user questions form landmarks; reasoning and collaboration stay private to logs. */
export function conversationLandmarks(state: ConversationState, metadata: Map<string, AgentInvocation>): ConversationLandmark[] {
  return sortedRows(state).flatMap(row => {
    if (row.type !== 'message' || row.message.source !== 'user') return [];
    const status = metadata.get(row.invocation)?.status;
    const response = finalResponse(state, row.invocation, status);
    const question = row.message.content.filter(part => part.type === 'text').map(part => part.text?.slice(0, 720) ?? '').join(' ');
    const answer = response?.blocks.flatMap(block => block.type === 'text' || block.type === 'refusal' ? [block.text.slice(0, 1200)] : []).join(' ');
    const pending = status === 'failed' ? 'Task failed · No final answer' : status === 'cancelled' ? 'Task stopped · No final answer'
      : status === 'completed' ? 'No text answer' : status === 'waiting' ? 'Waiting for input…' : 'Working…';
    return [{ id: row.id, anchor: messageAnchor(row.id), invocation: row.invocation, at: row.at, question: excerpt(question, 180) || 'Message with attachments',
      answer: answer ? excerpt(answer, 320) : response ? 'Answer contains attachments' : pending }];
  });
}

export function mergeLandmarks(index: AgentTurnIndexItem[], visible: ConversationLandmark[]) {
  const items = new Map<string, ConversationLandmark>();
  for (const item of index) {
    const id = entityKey(item.invocation_id, item.message_id);
    items.set(id, { id, anchor: messageAnchor(id), invocation: item.invocation_id, at: item.at,
      question: item.question, answer: item.answer });
  }
  for (const item of visible) items.set(item.id, item);
  return [...items.values()].sort((a, b) => a.at - b.at || a.id.localeCompare(b.id));
}

/** The question at or immediately above the reading position remains selected through long answers. */
export function landmarkAt(positions: number[], readingTop: number) {
  let low = 0, high = positions.length - 1, result = 0;
  while (low <= high) {
    const middle = (low + high) >>> 1;
    if (positions[middle] <= readingTop) { result = middle; low = middle + 1; }
    else high = middle - 1;
  }
  return result;
}
