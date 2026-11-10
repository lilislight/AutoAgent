import type { AgentEvent } from './types.js';

/** Render tool/text events locally; read Host controls only at lifecycle boundaries. */
export function eventRefresh(events: AgentEvent[]) {
  const lifecycle = events.some(event => event.kind === 'agent.completed' || event.kind === 'agent.error');
  const message = events.some(event => event.kind === 'agent.messages');
  const interaction = events.some(event => event.kind === 'agent.interaction');
  const waiting = events.some(event => event.kind === 'agent.tools' && event.payload.tools.some(tool => tool.state === 'waiting' || ['spawn_agent', 'run_agent'].includes(tool.name)));
  return { execution: lifecycle || interaction || waiting, invocations: lifecycle || interaction || message, directory: lifecycle || message };
}
