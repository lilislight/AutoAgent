import type { AgentIconName } from './AgentIcon.js';
import type { Json, ToolSnapshot } from './types.js';

type Category = 'read' | 'command' | 'search' | 'write' | 'skill' | 'agent' | 'process' | 'other';
export interface ToolPresentation { title: string; icon: AgentIconName; category: Category }
const object = (value: Json | undefined): Record<string, Json> => value && typeof value === 'object' && !Array.isArray(value) ? value : {};
function text(record: Record<string, Json>, name: string): string { return typeof record[name] === 'string' ? record[name] : ''; }
export function toolArguments(raw: string): Record<string, Json> {
  try { return object(JSON.parse(raw)); } catch { return {}; }
}
export function presentTool(tool: Pick<ToolSnapshot, 'name' | 'arguments' | 'state'>): ToolPresentation {
  const args = toolArguments(tool.arguments), path = text(args, 'path');
  const done = tool.state === 'completed';
  switch (tool.name) {
    case 'read_file': return { title: `Read ${path || 'file'}`, icon: 'file', category: 'read' };
    case 'list_files': return { title: `Browse ${path || 'Workspace'}`, icon: 'folder', category: 'search' };
    case 'search_files': return { title: `Search ${path || 'Workspace'}${text(args, 'query') ? ` for “${text(args, 'query')}”` : ''}`, icon: 'search', category: 'search' };
    case 'write_file': return { title: `${done ? args.overwrite ? 'Updated' : 'Created' : 'Write'} ${path || 'file'}`, icon: 'edit', category: 'write' };
    case 'edit_file': return { title: `${done ? 'Edited' : 'Edit'} ${path || 'file'}`, icon: 'edit', category: 'write' };
    case 'manage_files': {
      const request = object(args.request);
      return { title: `${text(request, 'operation') || 'Manage files'} ${text(request, 'path')}`.trim(), icon: 'folder', category: 'write' };
    }
    case 'execute_command': {
      const command = typeof args.mode === 'string' ? args : object(args.command);
      const argv = Array.isArray(command.args) ? command.args.filter(arg => typeof arg === 'string').join(' ') : '';
      const display = text(command, 'mode') === 'shell' ? text(command, 'script') : [text(command, 'program'), argv].filter(Boolean).join(' ');
      return { title: `${done ? 'Ran' : tool.state === 'running' ? 'Running' : 'Run'} ${display.replace(/\s+/g, ' ').trim().slice(0, 180) || 'command'}`, icon: 'terminal', category: 'command' };
    }
    case 'manage_process': {
      const request = object(args.request);
      return { title: `${text(request, 'action') || 'Manage'} process${text(request, 'process_id') ? ` ${text(request, 'process_id')}` : ''}`, icon: 'terminal', category: 'process' };
    }
    case 'skill_load': case 'skill_read_resource': case 'skill_list_resources':
      return { title: `${tool.name === 'skill_load' ? done ? 'Loaded' : 'Load' : tool.name === 'skill_read_resource' ? 'Read' : 'Browse'} ${text(args, 'name') || text(args, 'skill_name') || 'skill'}${path ? ` · ${path}` : ''}`, icon: 'skills', category: 'skill' };
    case 'spawn_agent': case 'run_agent': return { title: `${tool.name === 'spawn_agent' ? 'Start' : 'Run'} Agent${text(args, 'task') ? ` · ${text(args, 'task')}` : ''}`, icon: 'agent', category: 'agent' };
    case 'cancel_agent': return { title: `Stop Agent ${text(args, 'agent_id')}`.trim(), icon: 'agent', category: 'agent' };
    case 'get_agents': return { title: 'Inspect Agents', icon: 'agent', category: 'agent' };
    case 'send_message': return { title: `Send message${text(args, 'target') ? ` to ${text(args, 'target')}` : ''}`, icon: 'message', category: 'agent' };
    case 'wait_for': return { title: 'Wait for an Agent update', icon: 'agent', category: 'agent' };
    case 'ask_user': return { title: 'Ask you a question', icon: 'message', category: 'other' };
    default: return { title: tool.name.replaceAll('_', ' '), icon: 'wrench', category: 'other' };
  }
}

export function toolGroupSummary(tools: Pick<ToolSnapshot, 'name' | 'arguments' | 'state'>[]): string {
  if (!tools.length) return 'Tool calls';
  const counts = new Map<Category, number>();
  for (const tool of tools) { const category = presentTool(tool).category; counts.set(category, (counts.get(category) ?? 0) + 1); }
  const phrases: Partial<Record<Category, (count: number) => string>> = {
    read: count => `Read ${count} ${count === 1 ? 'file' : 'files'}`,
    command: count => `${count} ${count === 1 ? 'command' : 'commands'}`,
    search: count => `${count} ${count === 1 ? 'search' : 'searches'}`,
    write: count => `${count} file ${count === 1 ? 'change' : 'changes'}`,
    skill: count => `${count} ${count === 1 ? 'skill' : 'skills'}`,
    agent: count => `${count} Agent ${count === 1 ? 'operation' : 'operations'}`,
    process: count => `${count} process ${count === 1 ? 'operation' : 'operations'}`,
    other: count => `${count} ${count === 1 ? 'tool' : 'tools'}`,
  };
  const ordered = [...counts.entries()].sort((a, b) => b[1] - a[1]), selected = ordered.slice(0, 2);
  const more = ordered.slice(2).reduce((sum, [, count]) => sum + count, 0);
  return [...selected.map(([category, count]) => phrases[category]!(count)), ...(more ? [`${more} more`] : [])].join(' · ');
}

export function toolStateLabel(tool: ToolSnapshot | undefined, lifecycle?: string): string | undefined {
  if (!tool) return 'No saved result';
  if (['queued', 'running', 'waiting'].includes(tool.state)) {
    if (lifecycle === 'cancelled') return 'Stopped';
    if (lifecycle === 'failed') return 'Interrupted';
    if (lifecycle === 'paused') return 'Paused';
  }
  return tool.state === 'completed' ? undefined : ({ queued: 'Queued', running: 'Running', waiting: 'Waiting', failed: 'Failed', denied: 'Denied', cancelled: 'Stopped' }[tool.state]);
}
