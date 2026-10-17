import type { RunWorkflowInput } from './types.js';
export function runInput(args: {workflow: string; revision: string; entry: string; text: string; sessionMode: 'new'|'existing'; session: string}): RunWorkflowInput {
  if (!args.entry) throw new Error('Choose an Entry node.');
  let input: unknown;
  try { input = JSON.parse(args.text); } catch { throw new Error('Input must be valid JSON.'); }
  const body: RunWorkflowInput = { workflow_id: args.workflow, revision_id: args.revision, entry_node_id: args.entry, input, session_mode: args.sessionMode };
  if (args.sessionMode === 'existing' && !args.session.trim()) throw new Error('Choose an existing Session ID.');
  if (args.session.trim()) body.session_id = args.session.trim();
  return body;
}
