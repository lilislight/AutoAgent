import { FactsSkeleton, RecordsSkeleton } from './LoadingState';
import { useState, type ReactNode } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ApiError, getExecutionItems, getExecutionRecord, getExecutionStages } from '../api';
import type { GraphEdge, TraceRow, WorkflowGraphData } from '../types';
import { beforeCalls, callPresentation, executionTiming, isSystemCall, stageDefinitionPath, stageTabs, STAGE_LABELS } from '../inspectorModel';
import { useExecutionSnapshot, useSnapshotMetadata } from './ExecutionSnapshot';
import { useExecutionNavigation } from './ExecutionNavigation';
import { CopyId, InspectorFrame, type InspectorTab } from './InspectorFrame';
import { DefinitionDetails } from './DefinitionDetails';
import { duration, formatTime, LazyValue, LoadedValue, RowDetails, RowValues, StatusBadge } from './TraceDetails';

export function useInspectorPage(iid: string, kind: string, filters: Record<string, string>, frontier: number, enabled = true) {
  const { active } = useExecutionNavigation();
  const snapshot = useExecutionSnapshot();
  const scope = JSON.stringify([iid, kind, filters]);
  const [position, setPosition] = useState({ scope, cursors: [null] as (string | null)[], index: 0 });
  const current = position.scope === scope ? position : { scope, cursors: [null], index: 0 };
  const result = useQuery({ queryKey: ['inspector-items', iid, kind, filters, current.cursors[current.index], snapshot.epoch], meta: snapshot.meta,
    queryFn: ({ signal }) => kind === 'stage' ? getExecutionStages(iid, filters.occurrence_id, current.cursors[current.index], signal)
      : getExecutionItems(iid, kind, { order: kind === 'child' ? 'asc' : 'desc', ...filters }, current.cursors[current.index], signal),
    enabled: active && enabled, staleTime: Infinity, gcTime: 30_000, refetchOnWindowFocus: false });
  const data = useSnapshotMetadata(JSON.stringify([scope, current.cursors[current.index]]), result.data);
  const query = { ...result, data, isPending: result.isPending && !data };
  return { query, rows: query.data?.items ?? [], index: current.index,
    previous: () => setPosition({ ...current, index: Math.max(0, current.index - 1) }),
    next: () => { if (query.data?.next_cursor) setPosition({ scope, cursors: [...current.cursors.slice(0, current.index + 1), query.data.next_cursor], index: current.index + 1 }); } };
}
export function PageFeedback({ page, empty }: { page: ReturnType<typeof useInspectorPage>; empty?: string }) {
  return page.query.isPending ? (page.query.isFetching ? <RecordsSkeleton label="Loading records" /> : null) : page.query.error ? <div className="sidebar-error"><p>{page.query.error.message}</p><button onClick={() => void page.query.refetch()}>Retry</button></div> : !page.rows.length && empty ? <p className="inspector-empty">{empty}</p> : null;
}
export function InspectorPages({ page, label }: { page: ReturnType<typeof useInspectorPage>; label: string }) {
  return !page.index && !page.query.data?.next_cursor ? null : <nav className="inspector-pagination" aria-label={`${label} pages`}><button disabled={!page.index || page.query.isFetching} onClick={page.previous}>Previous</button><span>Page {page.index + 1}</span><button disabled={!page.query.data?.next_cursor || page.query.isFetching} onClick={page.next}>Next</button></nav>;
}
export function InspectorSection({ title, count, children }: { title: string; count?: number; children: ReactNode }) {
  return <section className="inspector-section"><header><h3>{title}</h3>{count != null ? <span>{count}</span> : null}</header>{children}</section>;
}
export function RecordChoice({ title, caption, badge, durationLabel, onClick, active = false, type = 'stage' }: {
  title: string; caption?: string; badge?: ReactNode; durationLabel?: string; onClick: () => void; active?: boolean; type?: string;
}) {
  return <button className={`inspector-record-choice type-${type} ${active ? 'is-selected' : ''}`} onClick={onClick}><span className="record-kind-icon" aria-hidden="true"><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true">{type === 'call' ? <path d="m5 4-4 4 4 4m6-8 4 4-4 4M9 2 7 14" /> : type === 'command' ? <><rect x="1.5" y="1.5" width="13" height="13" rx="3" /><path d="m5 5 3 3-3 3m4 0h3" /></> : type === 'wait' ? <><circle cx="8" cy="8" r="6" /><path d="M8 4v4l3 2" /></> : type === 'edge' ? <path d="M3 13 13 3M5 3h8v8" /> : type === 'failure' ? <path d="m4 4 8 8m0-8-8 8" /> : <path d="m8 2 6 6-6 6-6-6Z" />}</svg></span><span className="record-choice-label"><strong>{title}</strong>{caption ? <small>{caption}</small> : null}</span><span className="record-choice-meta">{badge}{durationLabel ? <small>{durationLabel}</small> : null}</span><svg className="record-choice-arrow" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><path d="m6 3 5 5-5 5" /></svg></button>;
}
export function CallChoice({ row, onPick }: { row: TraceRow; onPick: (row: TraceRow) => void }) {
  return <RecordChoice type={isSystemCall(row) ? 'command' : 'call'} title={row.operator_id ?? 'Call'} caption={`Unit ${row.unit_index ?? 0}${isSystemCall(row) ? ' · System command' : ''}`} badge={<StatusBadge row={row} />} durationLabel={duration(row.started_at_us != null && row.completed_at_us != null ? row.completed_at_us - row.started_at_us : null)} onClick={() => onPick(row)} />;
}
export function StageChoice({ row, onPick }: { row: TraceRow; onPick: (row: TraceRow) => void }) {
  return <RecordChoice title={STAGE_LABELS[String(row.event_name)] ?? String(row.event_name)} caption={`${row.inherited ? 'Source Event' : 'Event'} #${row.sequence}${row.error_phase ? ` · ${String(row.error_phase)}` : ''}`} type={row.event_name === 'node.faulted' ? 'failure' : 'stage'} durationLabel={duration(row.duration_ns == null ? null : row.duration_ns / 1000)} badge={row.event_name === 'node.faulted' ? <StatusBadge row={row} /> : undefined} onClick={() => onPick(row)} />;
}
export function RouteChoice({ edge, onPick }: { edge: GraphEdge; onPick: (id: string) => void }) {
  return <RecordChoice title={edge.target} caption={`${edge.on === 'error' ? 'Fail' : 'Complete'} edge${edge.conditional ? ' · Conditional' : ''}`} type="edge" onClick={() => onPick(edge.id)} />;
}

export function CallInspector({ row, workflow, frontier, onBack, onClose, backLabel, initial }: {
  row: TraceRow; workflow: WorkflowGraphData; frontier: number; onBack: () => void; onClose: () => void;
  backLabel?: string; initial?: InspectorTab;
}) {
  const navigation = useExecutionNavigation();
  const { active } = navigation;
  const snapshot = useExecutionSnapshot();
  const iid = row.invocation_id;
  const command = isSystemCall(row);
  const detail = useQuery({ queryKey: ['execution-record', iid, 'call', row.call_id, snapshot.epoch], meta: snapshot.meta, queryFn: ({ signal }) => getExecutionRecord(iid, 'call', row.call_id!, signal), enabled: active, staleTime: Infinity, gcTime: 30_000, refetchOnWindowFocus: false });
  const record = detail.data ?? row;
  const executable = workflow.definition.nodes.find(node => node.id === row.node_id)?.executable;
  const nodeDefinitionAvailable = command || executable?.kind === 'operator' && executable.id === record.operator_id;
  const spawning = record.operator_id === 'system_command:spawn' || record.operator_id === 'system_command:await';
  const wait = useQuery({ queryKey: ['execution-record', iid, 'wait', row.call_id, snapshot.epoch], meta: snapshot.meta, queryFn: async ({ signal }) => {
    try { return await getExecutionRecord(iid, 'wait', row.call_id!, signal); }
    catch (error) { if (error instanceof ApiError && error.status === 404) return null; throw error; }
  }, enabled: active && command && !spawning, staleTime: Infinity, gcTime: 30_000, refetchOnWindowFocus: false });
  const status = callPresentation(record, wait.data);
  return <InspectorFrame kind={command ? 'system-command' : 'call'} title={record.operator_id?.replace(/^system_command:/, '') ?? record.call_id!} initial={initial} context={<><StatusBadge row={status} />{record.inherited_from ? <span className="inspector-chip">Inherited execution</span> : null}<span>Unit {String(record.unit_index ?? 0)}</span></>} tabs={command && (spawning || wait.data) ? ['Summary', 'Input / Output', 'Execution', 'Definition'] : ['Summary', 'Input / Output', 'Definition']} onClose={onClose} onBack={onBack} backLabel={backLabel}>
    {tab => {
      switch (tab) {
        case 'Summary': return <>
          {detail.isLoading ? <FactsSkeleton label="Loading Call metadata" /> : detail.error ? <div className="sidebar-error"><p>{detail.error.message}</p><button onClick={() => void detail.refetch()}>Retry</button></div> : null}
          {!detail.isPending ? <RowDetails row={status} /> : null}
          {command && !spawning && wait.error ? <div className="sidebar-error"><p>{wait.error.message}</p><button onClick={() => void wait.refetch()}>Retry Wait metadata</button></div> : null}
          {command && executionTiming(record) == null ? <p className="inspector-empty">Elapsed time includes command waits; no separate processing duration was recorded.</p> : null}
          {record.values.includes('error') ? <LazyValue invocationId={iid} kind="call" identity={record.call_id!} field="error" valueKey={record.value_keys?.error} label="Error" /> : null}
        </>;
        case 'Input / Output': return <RowValues row={status} kind="call" />;
        case 'Definition': return record.operator_definition_id ? <LoadedValue invocationId={iid} kind="call" identity={record.call_id!} field="definition" label="Definition" />
          : row.node_id && nodeDefinitionAvailable ? <DefinitionDetails workflow={workflow} kind="node" identity={row.node_id} path={['executable']} />
          : <p className="inspector-empty">Executable definition was not recorded.</p>;
        case 'Execution': return spawning ? <CallChildren row={record} frontier={frontier} /> : <InspectorSection title="Wait"><WaitFacts row={wait.data!} /></InspectorSection>;
        default: return null;
      }
    }}
  </InspectorFrame>;
}
function CallChildren({ row, frontier }: { row: TraceRow; frontier: number }) {
  const { openChild } = useExecutionNavigation();
  const page = useInspectorPage(row.invocation_id, 'child', { call_id: row.call_id! }, frontier);
  return <InspectorSection title="Child executions"><PageFeedback page={page} empty="No child execution has been recorded for this Unit." />
    {page.rows.map(child => <RecordChoice key={child.invocation_id} type="command" title={child.workflow_id ?? 'Child Workflow'} caption={`Unit ${child.unit_index ?? 0} · ${child.invocation_id.slice(0, 8)}`} badge={<StatusBadge row={child} />} durationLabel={duration(child.started_at_us != null && child.completed_at_us != null ? child.completed_at_us - child.started_at_us : null)} onClick={() => openChild?.(child)} />)}
    <InspectorPages page={page} label="Child executions" /></InspectorSection>;
}
export function CommandWaitDetails({ row }: { row: TraceRow }) {
  const snapshot = useExecutionSnapshot();
  const navigation = useExecutionNavigation();
  const wait = useQuery({ queryKey: ['execution-record', row.invocation_id, 'wait', row.call_id, snapshot.epoch], meta: snapshot.meta,
    queryFn: ({ signal }) => getExecutionRecord(row.invocation_id, 'wait', row.call_id!, signal), enabled: navigation.active,
    staleTime: Infinity, gcTime: 30_000, refetchOnWindowFocus: false });
  return wait.isPending ? (wait.isFetching ? <FactsSkeleton label="Loading Wait" /> : null) : wait.error ? <div className="sidebar-error"><p>{wait.error.message}</p><button onClick={() => void wait.refetch()}>Retry</button></div> : <WaitFacts row={wait.data} />;
}
function WaitFacts({ row }: { row: TraceRow }) {
  return <dl className="trace-facts"><dt>Wait ID</dt><dd><CopyId value={String(row.wait_id)} /></dd><dt>Status</dt><dd><StatusBadge row={row} /></dd><dt>Kind</dt><dd>{String(row.kind)}</dd><dt>Started</dt><dd>{formatTime(row.created_at_us as number, true)}</dd><dt>{row.status === 'cancelled' ? 'Cancelled' : 'Resolved'}</dt><dd>{formatTime((row.end_at_us ?? row.resumed_at_us) as number, true)}</dd>{row.deadline_at_us != null ? <><dt>Deadline</dt><dd>{formatTime(row.deadline_at_us as number, true)}</dd></> : null}</dl>;
}
export function StageInspector({ row, nodeId, workflow, onBack, onClose, onEdge }: {
  row: TraceRow; nodeId: string; workflow: WorkflowGraphData; onBack: () => void; onClose: () => void; onEdge: (id: string) => void;
}) {
  const title = STAGE_LABELS[String(row.event_name)] ?? String(row.event_name);
  const routing = row.event_name === 'routing.resolved';
  return <InspectorFrame kind={`stage:${String(row.event_name)}`} title={title} context={<><code>Event #{row.sequence}</code><span>{nodeId}</span><StatusBadge row={row} /></>} tabs={stageTabs(row)} onClose={onClose} onBack={onBack}>
    {tab => {
      switch (tab) {
        case 'Summary': return <>
          <dl className="trace-facts"><dt>Occurrence ID</dt><dd><CopyId value={row.occurrence_id} /></dd><dt>Event ID</dt><dd><CopyId value={String(row.event_id)} /></dd><dt>Recorded</dt><dd>{formatTime(row.occurred_at_us as number, true)}</dd><dt>Processing time</dt><dd>{duration(row.duration_ns == null ? null : row.duration_ns / 1000)}</dd>
            {row.capability_id ? <><dt>Capability</dt><dd>{String(row.capability_id)}</dd><dt>Resolved Operator</dt><dd>{row.operator_id}</dd></> : null}
            {row.error_type ? <><dt>Error type</dt><dd>{row.error_type}</dd><dt>Error phase</dt><dd>{String(row.error_phase ?? '—')}</dd></> : null}
          </dl>
          {row.values.includes('error') ? <LazyValue invocationId={row.invocation_id} kind="stage" identity={String(row.event_id)} occurrenceId={row.occurrence_id} field="error" valueKey={row.value_keys?.error} label="Error" /> : null}
          {routing ? <p className="inspector-empty">Inspect an outgoing edge in Execution for its decision in this occurrence.</p> : null}
        </>;
        case 'Execution': return <InspectorSection title="Outgoing edges">{workflow.definition.edges.filter(edge => edge.source === nodeId).map(edge => <RouteChoice key={edge.id} edge={edge} onPick={onEdge} />)}</InspectorSection>;
        case 'Definition': return <DefinitionDetails workflow={workflow} kind="node" identity={nodeId} path={stageDefinitionPath(row)} />;
        case 'Input / Output': return <>
          {row.event_name === 'output.bound' ? <p className="inspector-empty">Context patch produced by Output Binding; it commits when the Node completes.</p> : null}
          {row.values.filter(field => field !== 'error' && field !== 'conditions').map(field => <LazyValue key={field} invocationId={row.invocation_id} kind="stage" identity={String(row.event_id)} occurrenceId={row.occurrence_id} field={field} valueKey={row.value_keys?.[field]} label={({ mapped_input: 'Mapped input', output: 'Output', patch: 'Context patch' } as Record<string,string>)[field] ?? field} />)}
        </>;
        default: return null;
      }
    }}
  </InspectorFrame>;
}
export { beforeCalls };
