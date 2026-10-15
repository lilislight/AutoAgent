import type { RunWorkflowInput } from './types.js';

export function runInput(args: {workflow: string; revision: string; entry: string; text: string; sessionMode: 'new'|'existing'; session: string; context: string; includeContext: boolean}): RunWorkflowInput {
  if(!args.entry)throw new Error('Choose an Entry node.');
  let input: unknown;
  try{input=JSON.parse(args.text);}catch{throw new Error('Input must be valid JSON.');}
  const body: RunWorkflowInput={workflow_id:args.workflow,revision_id:args.revision,entry_node_id:args.entry,input,session_mode:args.sessionMode};
  if(args.sessionMode==='existing'){
    if(!args.session.trim())throw new Error('Enter an existing Session ID.');
    body.session_id=args.session.trim();
  }else if(args.includeContext){
    let value:unknown;
    try{value=JSON.parse(args.context);}catch{throw new Error('Session context must be valid JSON.');}
    if(value===null || typeof value!=='object' || Array.isArray(value))throw new Error('Session context must be a JSON object.');
    body.session_context=value as Record<string,unknown>;
  }
  return body;
}
