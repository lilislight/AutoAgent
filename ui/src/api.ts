import type { TimelineEventGroups, ExecutionFamily, TimelinePosition, ExecutionFilters, ExecutionTrace, RecordedValue, SessionPage, TimelineMarkerPage, TimelineOverview, TimelinePage, TracePage, TraceRow, WorkflowCatalogPage, WorkflowGraphData, WorkflowPage } from "./types";

import { config } from './http';
import { workflowRequest as request } from './workflowHttp';
export function getForkPlan(iid:string,sequence:number,signal:AbortSignal) {
  return request<import('./types').ForkPlan>(`/execution-fork-plan?${new URLSearchParams({invocation_id:iid,sequence:String(sequence)})}`,signal);
}
export function listExecutionEvents(iid:string,cursor:string|null,signal:AbortSignal) {
  const query=new URLSearchParams({invocation_id:iid,limit:'50'});if(cursor)query.set('cursor',cursor);
  return request<TracePage>(`/invocation-events?${query}`,signal);
}
export function forkExecution(iid:string,sequence:number,session:string|undefined,signal:AbortSignal) {
  return request<import('./types').AcceptedFork>('/execution-forks',signal,{method:'POST',
    headers:{'Content-Type':'application/json'},body:JSON.stringify({invocation_id:iid,sequence,session_id:session})});
}
export { ApiError, config } from './http';
import { traceQuery } from './traceClient';
import type { AcceptedExecution, RunWorkflowInput, WorkflowRunSpec } from './types';
import type { ExecutionActionInput, ExecutionActionSpec, ExecutionControls, ExecutionOperation } from './types';

export function getExecutionControls(iid:string,signal:AbortSignal,cursor?:string|null,target?:{target_invocation_id?:string;wait_id?:string}) {
  const query=new URLSearchParams({invocation_id:iid});if(cursor)query.set('cursor',cursor);
  if(target?.target_invocation_id)query.set('target_invocation_id',target.target_invocation_id);
  if(target?.wait_id)query.set('wait_id',target.wait_id);
  return request<ExecutionControls>(`/execution-controls?${query}`,signal);
}
export function getExecutionActionSpec(iid:string,action:string,options:Record<string,string>,signal:AbortSignal) {
  return request<ExecutionActionSpec>(`/execution-action-spec?${new URLSearchParams({invocation_id:iid,action,...options})}`,signal);
}
export function submitExecutionAction(body:ExecutionActionInput,signal:AbortSignal) {
  return request<ExecutionOperation>('/execution-actions',signal,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
}
export function getExecutionOperation(id:string,signal:AbortSignal) {
  return request<ExecutionOperation>(`/execution-operations/${encodeURIComponent(id)}`,signal);
}

export function getWorkflowRunSpec(workflowId: string, revision: string, entry: string, session: string | null, signal: AbortSignal) {
  const query = new URLSearchParams({workflow_id:workflowId,revision_id:revision});
  if(session)query.set('session_id',session);
  if(entry)query.set('entry_node_id',entry);
  return request<WorkflowRunSpec>(`/workflow-run-spec?${query}`,signal);
}
export function runWorkflow(input: RunWorkflowInput, signal: AbortSignal) {
  return request<AcceptedExecution>('/executions',signal,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(input)});
}

export function browseWorkflows(search: string, cursor: string | null, signal: AbortSignal, registeredOnly = false) {
  const query = new URLSearchParams({ q: search, limit: String(config.pageSize) });
  if (registeredOnly) query.set("registered_only", "true");
  if (cursor) query.set("cursor", cursor);
  return request<WorkflowCatalogPage>(`/workflow-catalog?${query}`, signal);
}

export function listWorkflowRevisions(workflowId: string, prefix: string | null, cursor: string | null, signal: AbortSignal) {
  const query = new URLSearchParams({ workflow_id: workflowId, limit: String(config.pageSize) });
  if (prefix) query.set("prefix", prefix);
  if (cursor) query.set("cursor", cursor);
  return request<WorkflowPage>(`/workflow-revisions?${query}`, signal);
}

export function getWorkflowGraph(revision: string, signal: AbortSignal) {
  return request<WorkflowGraphData>(`/workflow-graph?${new URLSearchParams({ revision_id: revision })}`, signal);
}

export function getWorkflowObject(revision: string, kind: string, identity: string, signal: AbortSignal) {
  return request<unknown>(`/workflow-object?${new URLSearchParams({ revision_id: revision, kind, identity })}`, signal);
}

export function getWorkflowRevision(revision: string, signal: AbortSignal) {
  return request<unknown>(`/workflow-revisions/${encodeURIComponent(revision)}`, signal);
}

function appendExecutionFilters(query: URLSearchParams, filters?: ExecutionFilters) {
  if (!filters) return;
  if (filters.query) query.set("q", filters.query);
  for (const id of filters.workflowIds) query.append("workflow_id", id);
  for (const id of filters.revisionIds) query.append("revision_id", id);
  for (const status of filters.statuses) query.append("status", status);
  if (filters.startedFrom) query.set('started_after_us', String(Date.parse(filters.startedFrom) * 1000));
  if (filters.startedTo) query.set('started_before_us', String(Date.parse(filters.startedTo) * 1000));
}

export function listExecutionSessions(cursor: string | null, signal: AbortSignal, session?: string, filters?: ExecutionFilters) {
  const query = new URLSearchParams({ limit: String(config.pageSize) });
  if (cursor) query.set("cursor", cursor);
  if (session) query.set("session_id", session);
  appendExecutionFilters(query, filters);
  return request<SessionPage>(`/execution-sessions?${query}`, signal);
}

export function listSessionInvocations(session: string, cursor: string | null, signal: AbortSignal, filters?: ExecutionFilters) {
  const query = new URLSearchParams({ session_id: session, limit: String(config.pageSize) });
  if (cursor) query.set("cursor", cursor);
  appendExecutionFilters(query, filters);
  return request<TracePage>(`/session-invocations?${query}`, signal);
}

export function getExecutionTrace(invocationId: string, signal: AbortSignal) {
  return request<ExecutionTrace>(`/execution-trace?${new URLSearchParams({ invocation_id: invocationId })}`, signal);
}
export function getExecutionFamily(invocationId: string, signal: AbortSignal) {
  return request<ExecutionFamily>(`/execution-family?${new URLSearchParams({ invocation_id: invocationId })}`, signal);
}

export function getExecutionTimeline(invocationId: string, signal: AbortSignal, observed?: number) {
  return traceQuery<TimelineOverview>(invocationId,'overview',{observed},signal);
}
export function getTimelineActivity(invocationId: string, start: number, end: number, signal: AbortSignal) {
  return traceQuery<number[]>(invocationId,'activity',{start,end,buckets:128},signal);
}
export function getTimelineItems(invocationId: string, kind: string, start: number, end: number, occurrence: string | null, cursor: string | null, signal: AbortSignal, nodeId?: string | null) {
  return traceQuery<TimelinePage>(invocationId,'page',{kind,start,end,occurrence,cursor,nodeId},signal);
}
export function getTimelinePosition(invocationId: string, kind: string, identity: string, start: number, end: number, occurrence: string | null, nodeId: string | null, pageIndex: number | undefined, signal: AbortSignal) {
  return traceQuery<TimelinePosition>(invocationId,'position',{kind,identity,start,end,occurrence,nodeId,pageIndex},signal);
}
export function getTimelineEventGroups(iid: string, start: number, end: number, buckets: number, through: number, signal: AbortSignal) {
  return traceQuery<TimelineEventGroups>(iid,'groups',{start,end,buckets,through},signal);
}
export function getTimelineMarkers(invocationId: string, start: number, end: number, cursor: string | null, signal: AbortSignal, through?: number) {
  return traceQuery<TimelineMarkerPage>(invocationId,'markers',{start,end,cursor,through},signal);
}
export function getExecutionRecord(invocationId: string, kind: string, identity: string, signal: AbortSignal) {
  return request<TraceRow>(`/execution-record?${new URLSearchParams({ invocation_id: invocationId, kind, identity })}`, signal);
}

export function getExecutionItems(invocationId: string, kind: string, filters: Record<string, string>, cursor: string | null, signal: AbortSignal) {
  const query = new URLSearchParams({ invocation_id: invocationId, kind, limit: "20", ...filters });
  if (cursor) query.set("cursor", cursor);
  return request<TracePage>(`/execution-items?${query}`, signal);
}

export function getExecutionValue(invocationId: string, kind: string, identity: string, field: string, signal: AbortSignal) {
  return request<RecordedValue>(`/execution-value?${new URLSearchParams({ invocation_id: invocationId, kind, identity, field })}`, signal);
}

export function getExecutionStages(invocationId: string, occurrenceId: string, cursor: string | null, signal: AbortSignal) {
  const query = new URLSearchParams({ invocation_id: invocationId, occurrence_id: occurrenceId, limit: '20' });
  if (cursor) query.set('cursor', cursor);
  return request<TracePage>(`/execution-stages?${query}`, signal);
}

export function getStageValue(invocationId: string, occurrenceId: string, eventId: string, field: string, signal: AbortSignal) {
  return request<RecordedValue>(`/execution-stage-value?${new URLSearchParams({ invocation_id: invocationId, occurrence_id: occurrenceId, event_id: eventId, field })}`, signal);
}

export function compareWorkflowRevisions(base: string, target: string, cursor: string | null, signal: AbortSignal) {
  const query = new URLSearchParams({ base_revision_id: base, target_revision_id: target, limit: String(config.pageSize) });
  if (cursor) query.set('cursor', cursor);
  return request<import('./types').RevisionComparisonPage>(`/workflow-comparison?${query}`, signal);
}
export function getRevisionComparisonObject(base: string, target: string, kind: string, identity: string, signal: AbortSignal) {
  return request<import('./types').ComparisonObject>(`/workflow-comparison-object?${new URLSearchParams({base_revision_id:base,target_revision_id:target,kind,identity})}`, signal);
}
export function getSessionInformation(session: string, signal: AbortSignal) {
  return request<import('./types').SessionInformation>(`/session-information?${new URLSearchParams({session_id:session})}`, signal);
}
export function getSessionHistory(session: string, cursor: string | null, revision: string | null, signal: AbortSignal) {
  const query = new URLSearchParams({ session_id:session, limit:String(config.pageSize) });
  if (cursor) query.set('cursor',cursor);
  if (revision) query.set('revision_id',revision);
  return request<import('./types').MetadataPage<import('./types').SessionHistoryRow>>(`/session-history?${query}`,signal);
}
export function getSessionWorkflows(session: string, cursor: string | null, signal: AbortSignal) {
  const query = new URLSearchParams({session_id:session,limit:String(config.pageSize)});
  if (cursor) query.set('cursor',cursor);
  return request<import('./types').MetadataPage<import('./types').SessionWorkflow>>(`/session-workflows?${query}`,signal);
}
export function listSessionCheckpoints(session: string, cursor: string | null, signal: AbortSignal) {
  const query = new URLSearchParams({session_id:session,limit:String(config.pageSize)});
  if (cursor) query.set('cursor',cursor);
  return request<import('./types').MetadataPage<import('./types').SessionCheckpoint>>(`/session-checkpoints?${query}`,signal);
}
export function getCheckpointDetails(id: string, signal: AbortSignal) {
  return request<import('./types').CheckpointDetails>(`/checkpoints/${encodeURIComponent(id)}`,signal);
}
export function createSessionCheckpoint(session: string, pinned: boolean, signal: AbortSignal, sequence?: number) {
  return request<import('./types').CheckpointDetails>('/session-checkpoints',signal,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({session_id:session,pinned,sequence})});
}
export function setCheckpointPinned(id: string, pinned: boolean, signal: AbortSignal) {
  return request<import('./types').CheckpointDetails>(`/checkpoints/${encodeURIComponent(id)}/pin`,signal,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({pinned})});
}
export function deleteCheckpoint(id: string, signal: AbortSignal) {
  return request<void>(`/checkpoints/${encodeURIComponent(id)}`,signal,{method:'DELETE'});
}
