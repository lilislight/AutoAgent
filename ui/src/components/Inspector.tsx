import type { Selection, WorkflowGraphData } from "../types";
import { InspectorFrame } from "./InspectorFrame";
import { DefinitionDetails } from "./DefinitionDetails";
export function Inspector({ workflow, selection, onClose }: { workflow: WorkflowGraphData; selection: NonNullable<Selection>; onClose: () => void }) {
  return <InspectorFrame kind={selection.kind} title={selection.kind === 'workflow' ? workflow.workflow_id : selection.id} context={<><span>v{workflow.workflow_version}</span><code title={workflow.workflow_revision_id}>{workflow.definition_hash.slice(0, 8)}</code></>} tabs={['Definition']} initial="Definition" onClose={onClose} footer={<><span>{workflow.runnable ? 'Registered revision' : 'Saved revision'}</span><code>{workflow.definition_hash.slice(0, 8)}</code></>}>
    {() => <DefinitionDetails workflow={workflow} kind={selection.kind} identity={selection.id} />}
  </InspectorFrame>;
}
