import { useQuery } from "@tanstack/react-query";
import { getWorkflowObject, getWorkflowRevision } from "../api";
import type { WorkflowGraphData } from "../types";
import { JsonValue } from "./JsonValue";
import { pickDefinition } from '../inspectorModel';
import { InspectorFrame } from './InspectorFrame';
function LoadedDefinition({ revision, kind, identity, path }: { revision: string; kind: string; identity: string; path?: string[] }) {
  const detail = useQuery({ queryKey: ['definition', revision, kind, identity], queryFn: ({ signal }) => kind === 'workflow' ? getWorkflowRevision(revision, signal) : getWorkflowObject(revision, kind, identity, signal), staleTime: Infinity, gcTime: 30_000, refetchOnWindowFocus: false });
  if (detail.isPending) return <p className="muted" role="status">Loading definition…</p>;
  if (detail.error) return <div className="sidebar-error"><p>{detail.error.message}</p><button onClick={() => void detail.refetch()}>Retry</button></div>;
  const value = pickDefinition(detail.data, path);
  return value == null && path ? <p className="inspector-empty">No custom definition was recorded for this stage.</p> : <JsonValue value={value} label="definition" />;
}
export function DefinitionDetails({ workflow, kind, identity, path }: { workflow: WorkflowGraphData; kind: string; identity: string; path?: string[] }) {
  return <LoadedDefinition revision={workflow.workflow_revision_id} kind={kind} identity={identity} path={path} />;
}
export function GroupDefinitionInspector({workflow,identity,onClose}:{workflow:WorkflowGraphData;identity:string;onClose:()=>void}) {
  const group=workflow.definition.groups?.find(g=>g.id===identity);
  const path=group?[group.label]:[identity];let parent=group?.parent_group_id;
  while(parent){const owner=workflow.definition.groups?.find(g=>g.id===parent);if(!owner)break;path.unshift(owner.label);parent=owner.parent_group_id;}
  return <InspectorFrame kind="inline Workflow" title={path.join(' / ')} context={<><span>{group?.workflow_id}</span><span>v{group?.workflow_version}</span><span>{group?.node_ids.length??0} direct nodes</span></>} tabs={['Definition']} onClose={onClose}>
    {()=> <DefinitionDetails workflow={workflow} kind="group" identity={identity}/>}
  </InspectorFrame>;
}
