import type { AcceptedExecution, Selection, WorkflowGraphData } from '../types';
import { InspectorFrame } from './InspectorFrame';
import { DefinitionDetails, GroupDefinitionInspector } from './DefinitionDetails';
import { RunWorkflowForm } from './RunWorkflowForm';
import { CopyId } from './InspectorFrame';
export function Inspector({ workflow, selection, onClose, onEntry, onRunAccepted }: { workflow: WorkflowGraphData; selection: NonNullable<Selection>; onClose: () => void; onEntry: (id: string) => void; onRunAccepted: (receipt: AcceptedExecution) => void }) {
  const runnable = workflow.runnable && selection.kind === 'node' && workflow.definition.entry_node_ids.includes(selection.id);
  if(selection.kind==='group')return <GroupDefinitionInspector workflow={workflow} identity={selection.id} onClose={onClose}/>;
  return <InspectorFrame kind={selection.kind} title={selection.kind === 'workflow' ? workflow.workflow_id : selection.id} context={<><CopyId label="Version" value={workflow.workflow_version} displayValue={`Version ${workflow.workflow_version}`}/><CopyId label="Revision ID" value={workflow.workflow_revision_id} displayValue={`Revision ${workflow.definition_hash.slice(0,8)}`}/></>} tabs={runnable ? ['Definition', 'Actions'] : ['Definition']} onClose={onClose}>
    {tab => tab === 'Actions' && runnable ? <RunWorkflowForm workflow={workflow} entry={selection.id} onEntry={onEntry} onAccepted={onRunAccepted} /> : <DefinitionDetails workflow={workflow} kind={selection.kind} identity={selection.id} />}
  </InspectorFrame>;
}
