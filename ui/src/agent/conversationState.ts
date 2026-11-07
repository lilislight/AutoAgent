import type { AgentEvent, AgentMessage, Compaction, Interaction, ResponseSnapshot, ToolSnapshot } from './types.js';

export const entityKey = (iid: string, id: string) => JSON.stringify([iid, id]);
interface RowBase { id: string; invocation: string; at: number }
export type FeedRow = RowBase & ({ type: 'message'; message: AgentMessage } | { type: 'response'; responseId: string } | { type: 'compaction'; compactionId: string } | { type: 'error'; error: { type: string; message: string }; handled: boolean });
export interface LiveResponse { streamId: string; parts: { key: string; kind: string; text: string }[] }
export interface ConversationState {
  rows: Record<string, FeedRow>; responses: Record<string, ResponseSnapshot>; live: Record<string, LiveResponse>;
  tools: Record<string, ToolSnapshot & { committed: boolean }>; interactions: Record<string, Interaction>;
  compactions: Record<string, Compaction>; seen: Set<string>; completed: Set<string>;
}
export function emptyConversation(): ConversationState { return { rows: {}, responses: {}, live: {}, tools: {}, interactions: {}, compactions: {}, seen: new Set(), completed: new Set() }; }
export function applyEvents(previous: ConversationState, events: AgentEvent[]): ConversationState {
  const state: ConversationState = { rows: { ...previous.rows }, responses: { ...previous.responses }, live: { ...previous.live }, tools: { ...previous.tools }, interactions: { ...previous.interactions }, compactions: { ...previous.compactions }, seen: new Set(previous.seen), completed: new Set(previous.completed) };
  for (const event of events) {
    if (!event || event.payload?.v !== 1 || (event.persist && state.seen.has(event.id))) continue;
    if (event.persist) state.seen.add(event.id);
    const key = (id: string) => entityKey(event.invocation_id, id);
    const row = (id: string) => ({ id, invocation: event.invocation_id, at: event.occurred_at_us });
    switch (event.kind) {
      case 'agent.messages':
        for (const message of event.payload.messages) { const id = key(message.message_id); state.rows[id] = { ...row(id), type: 'message', message }; }
        break;
      case 'agent.response.delta': {
        const delta = event.payload, id = key(delta.response_id);
        if (state.responses[id]) break;
        const prior = state.live[id];
        const value = prior?.streamId === delta.stream_id ? prior : { streamId: delta.stream_id, parts: [] };
        const partKey = JSON.stringify([delta.item_key, delta.content_index, delta.kind]);
        const index = value.parts.findIndex(part => part.key === partKey);
        const parts = [...value.parts];
        if (index < 0) parts.push({ key: partKey, kind: delta.kind, text: delta.delta });
        else parts[index] = { ...parts[index], text: parts[index].text + delta.delta };
        state.live[id] = { ...value, parts };
        state.rows[id] ??= { ...row(id), type: 'response', responseId: delta.response_id };
        break;
      }
      case 'agent.response': {
        const id = key(event.payload.response_id);
        state.responses[id] = event.payload; delete state.live[id];
        state.rows[id] = { ...row(id), type: 'response', responseId: event.payload.response_id };
        break;
      }
      case 'agent.tools':
        for (const tool of event.payload.tools) {
          const id = key(tool.tool_id);
          if (state.tools[id]?.committed && !event.persist) continue;
          state.tools[id] = { ...tool, committed: event.persist };
        }
        break;
      case 'agent.interaction': {
        const id = key(event.payload.interaction_id);
        if (state.interactions[id]?.state === 'resolved' && event.payload.state === 'pending') break;
        state.interactions[id] = event.payload; break;
      }
      case 'agent.compaction': {
        const value = event.payload, id = key(value.compaction_id);
        if (state.compactions[id]?.state === 'completed' && value.state === 'running') break;
        state.compactions[id] = value;
        state.rows[id] ??= { ...row(id), type: 'compaction', compactionId: value.compaction_id };
        break;
      }
      case 'agent.error': {
        const id = key(event.id); state.rows[id] = { ...row(id), type: 'error', error: event.payload.error, handled: event.payload.handled }; break;
      }
      case 'agent.completed': state.completed.add(event.invocation_id); break;
    }
  }
  return state;
}

export function resetTransient(state: ConversationState): ConversationState {
  const liveIds = new Set(Object.keys(state.live));
  return { ...state, live: {}, rows: Object.fromEntries(Object.entries(state.rows).filter(([id]) => !liveIds.has(id))),
    tools: Object.fromEntries(Object.entries(state.tools).filter(([, tool]) => tool.committed)),
    interactions: Object.fromEntries(Object.entries(state.interactions).filter(([, value]) => value.state === 'resolved')),
    compactions: Object.fromEntries(Object.entries(state.compactions).filter(([, value]) => value.state === 'completed')) };
}

export function sortedRows(state: ConversationState) { return Object.values(state.rows).sort((a, b) => a.at - b.at || a.id.localeCompare(b.id)); }
