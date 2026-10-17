import { useQuery } from "@tanstack/react-query";
import { getWorkflowObject, getWorkflowRevision } from "../api";
import type { WorkflowGraphData } from "../types";
import { JsonValue } from "./JsonValue";
import { pickDefinition } from '../inspectorModel';
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
