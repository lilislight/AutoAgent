import { useQuery } from "@tanstack/react-query";
import { getWorkflowObject, getWorkflowRevision } from "../api";
import type { WorkflowGraphData } from "../types";
import { JsonValue } from "./JsonValue";
import { CopyId } from "./InspectorFrame";
import { formatTime } from './TraceDetails';
import { pickDefinition } from '../inspectorModel';
export function DefinitionSummary({ workflow, kind, identity }: { workflow: WorkflowGraphData; kind: string; identity: string }) {
  const node = workflow.definition.nodes.find(item => item.id === identity);
  const edge = workflow.definition.edges.find(item => item.id === identity);
  return <dl className="trace-facts"><dt>Workflow</dt><dd>{workflow.workflow_id}</dd><dt>Version</dt><dd>v{workflow.workflow_version}</dd><dt>Revision</dt><dd><CopyId value={workflow.workflow_revision_id} /></dd>
    {kind === "workflow" ? <><dt>Registration</dt><dd>{workflow.runnable ? "Registered" : "Unregistered"}</dd><dt>Revision saved</dt><dd>{formatTime(workflow.registered_at_us)}</dd><dt>Nodes / edges</dt><dd>{workflow.definition.nodes.length} / {workflow.definition.edges.length}</dd><dt>Entry nodes</dt><dd>{workflow.definition.entry_node_ids.join(', ') || '—'}</dd><dt>Exit nodes</dt><dd>{workflow.definition.exit_node_ids.join(', ') || '—'}</dd></> : node ? <><dt>Executable</dt><dd>{node.executable.id.startsWith(node.executable.kind + ":") ? node.executable.id : `${node.executable.kind}:${node.executable.id}`}</dd><dt>Map</dt><dd>{node.mapped ? "Enabled" : "None"}</dd><dt>Entry / exit</dt><dd>{[workflow.definition.entry_node_ids.includes(identity) ? "Entry" : null, workflow.definition.exit_node_ids.includes(identity) ? "Exit" : null].filter(Boolean).join(" / ") || "Neither"}</dd></> : edge ? <><dt>Source</dt><dd>{edge.source}</dd><dt>Target</dt><dd>{edge.target}</dd><dt>Type</dt><dd>{edge.on === 'error' ? 'Fail' : 'Complete'}</dd><dt>Condition</dt><dd>{edge.conditional ? 'Defined' : 'None'}</dd></> : null}
  </dl>;
}
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
