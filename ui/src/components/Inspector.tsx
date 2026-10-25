import type { AcceptedExecution, Selection, WorkflowGraphData } from '../types';
import { InspectorFrame } from './InspectorFrame';
import { DefinitionDetails, GroupDefinitionInspector } from './DefinitionDetails';
import { RunWorkflowForm } from './RunWorkflowForm';
export function Inspector({ workflow, selection, onClose, onEntry, onRunAccepted }: { workflow: WorkflowGraphData; selection: NonNullable<Selection>; onClose: () => void; onEntry: (id: string) => void; onRunAccepted: (receipt: AcceptedExecution) => void }) {
  const runnable = workflow.runnable && selection.kind === 'node' && workflow.definition.entry_node_ids.includes(selection.id);
  if(selection.kind==='group')return <GroupDefinitionInspector workflow={workflow} identity={selection.id} onClose={onClose}/>;
  return <InspectorFrame kind={selection.kind} title={selection.kind === 'workflow' ? workflow.workflow_id : selection.id} context={<><span>v{workflow.workflow_version}</span><code title={workflow.workflow_revision_id}>{workflow.definition_hash.slice(0, 8)}</code></>} tabs={runnable ? ['Definition', 'Actions'] : ['Definition']} onClose={onClose}>
    {tab => tab === 'Actions' && runnable ? <RunWorkflowForm workflow={workflow} entry={selection.id} onEntry={onEntry} onAccepted={onRunAccepted} /> : <DefinitionDetails workflow={workflow} kind={selection.kind} identity={selection.id} />}
  </InspectorFrame>;
}
