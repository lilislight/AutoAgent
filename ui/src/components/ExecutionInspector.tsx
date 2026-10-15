import { useEffect, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getExecutionRecord } from '../api';
import type { ExecutionTrace, Selection, TraceRow, WorkflowGraphData } from '../types';
import { isSystemCall, valueMessage } from '../inspectorModel';
import { duration, formatTime, LazyValue, MissingValue, RowDetails, RowValues, StatusBadge } from './TraceDetails';
import { CopyId, InspectorFrame, type InspectorTab } from './InspectorFrame';
import { DefinitionDetails, DefinitionSummary } from './DefinitionDetails';
import { useExecutionSnapshot } from './ExecutionSnapshot';
import { useExecutionNavigation } from './ExecutionNavigation';
import { executionRecordKey } from './OccurrenceStack';
import { stackPositions } from '../stackPositions';
import { JsonValue } from './JsonValue';
import { beforeCalls, CallChoice, CallInspector, InspectorPages, InspectorSection, PageFeedback,
  RecordChoice, RouteChoice, StageChoice, StageInspector, useInspectorPage, WaitInspector } from './ExecutionInspectorParts';

type Child = { kind: 'call' | 'stage' | 'wait'; row: TraceRow; initial?: InspectorTab } | { kind: 'edge'; id: string; occurrence: string };
function NodeValues({ row, frontier, onCall }: { row: TraceRow; frontier: number; onCall: (row: TraceRow, initial?: InspectorTab) => void }) {
  const skipped = row.status === 'skipped';
  const mapped = row.values.includes('input');
  const calls = useInspectorPage(row.invocation_id, 'call', { occurrence_id: row.occurrence_id! }, frontier, !skipped);
  if (skipped) return <><MissingValue title="Input" message={valueMessage(row, 'input')} /><MissingValue title="Output" message={valueMessage(row, 'output')} /></>;
  return <>
    {mapped ? <LazyValue invocationId={row.invocation_id} kind="node" identity={row.occurrence_id!} field="input" valueKey={row.value_keys?.input} label="Mapped input" badge="Custom mapping" description="Result of this occurrence’s Input Mapping." /> : null}
    <InspectorSection title={mapped ? 'Actual Call inputs' : 'Input'}>
      {!mapped ? <p className="inspector-empty">{row.error_phase === 'input_mapping' ? valueMessage(row, 'input') : 'No separate Input Mapping result was recorded. The inputs passed to each Call are shown below.'}</p> : null}
      <PageFeedback page={calls} empty={row.error_phase === 'input_mapping' ? undefined : ['ready', 'running', 'waiting'].includes(row.status ?? '') ? 'No Call input has been recorded yet.' : 'This occurrence has no recorded Call input.'} />
      {calls.rows.map(call => <div key={call.call_id} className="call-input-card"><header><span><strong>{call.operator_id}</strong><small>Unit {String(call.unit_index ?? 0)}</small></span><button className="inspector-link" onClick={() => onCall({ ...call, node_id: row.node_id }, 'Input / Output')}>Inspect Call <span aria-hidden="true">↗</span></button></header>{call.values.includes('input') ? <LazyValue invocationId={row.invocation_id} kind="call" identity={call.call_id!} field="input" valueKey={call.value_keys?.input} label="Call input" description={isSystemCall(call) ? 'Request passed to this command.' : 'Validated input for this Unit.'} /> : <p className="inspector-empty">This Call has no recorded input.</p>}</div>)}
      <InspectorPages page={calls} label="Call inputs" />
    </InspectorSection>
    {row.values.includes('output') ? <LazyValue invocationId={row.invocation_id} kind="node" identity={row.occurrence_id!} field="output" valueKey={row.value_keys?.output} label="Node output" description="Final result of this occurrence, after aggregation when configured." /> : <MissingValue title="Node output" message={valueMessage(row, 'output')} />}
  </>;
}
function NodeExecution({ row, frontier, workflow, onCall, onStage, onEdge }: {
  row: TraceRow; frontier: number; workflow: WorkflowGraphData; onCall: (row: TraceRow) => void; onStage: (row: TraceRow) => void; onEdge: (id: string) => void;
}) {
  const skipped = row.status === 'skipped';
  const stages = useInspectorPage(row.invocation_id, 'stage', { occurrence_id: row.occurrence_id! }, frontier, !skipped);
  const calls = useInspectorPage(row.invocation_id, 'call', { occurrence_id: row.occurrence_id! }, frontier, !skipped);
  if (skipped) return <p className="inspector-empty">This occurrence was skipped. No executable or execution stages ran.</p>;
  const command = workflow.definition.nodes.find(node => node.id === row.node_id)?.executable.kind === 'system_command';
  const routes = workflow.definition.edges.filter(edge => edge.source === row.node_id);
  return <div className="execution-pipeline">
    <InspectorSection title="Preparation"><PageFeedback page={stages} />{stages.rows.filter(beforeCalls).map(stage => <StageChoice key={String(stage.event_id)} row={stage} onPick={onStage} />)}{!stages.query.isPending && !stages.query.error && !stages.rows.some(beforeCalls) ? <p className="inspector-empty">No separate Input Mapping or Capability Resolution stage was recorded.</p> : null}</InspectorSection>
    <InspectorSection title={command ? 'System command Calls' : 'Executable Calls'} count={row.call_count}><PageFeedback page={calls} empty="No Calls were recorded for this occurrence." />{calls.rows.map(call => <CallChoice key={call.call_id} row={call} onPick={value => onCall({ ...value, node_id: row.node_id })} />)}<InspectorPages page={calls} label="Calls" /></InspectorSection>
    {stages.rows.some(stage => !beforeCalls(stage)) ? <InspectorSection title="Result processing">{stages.rows.filter(stage => !beforeCalls(stage)).map(stage => <StageChoice key={String(stage.event_id)} row={stage} onPick={onStage} />)}</InspectorSection> : null}
    <InspectorPages page={stages} label="Stages" />
    {routes.length ? <InspectorSection title="Outgoing edges" count={routes.length}>{routes.map(edge => <RouteChoice key={edge.id} edge={edge} onPick={onEdge} />)}</InspectorSection> : null}
  </div>;
}
function NodeInspector({ iid, id, record, trace, workflow, onClose, onReselect, initial }: { initial?: InspectorTab; iid: string; id: string; record?: TraceRow; trace: ExecutionTrace; workflow: WorkflowGraphData; onClose: () => void; onReselect?: () => void }) {
  const navigation = useExecutionNavigation();
  const query = useInspectorPage(iid, 'node', { node_id: id }, trace.frontier, !record);
  const row = record ?? query.rows[0];
  const pendingWait=navigation.pendingResponses?.find(wait=>wait.invocation_id===iid && wait.occurrence_id===row?.occurrence_id);
  const [children, setChildren] = useState<Child[]>([]);
  const child = children.at(-1);
  const count = trace.nodes.find(item => item.node_id === id)?.occurrences ?? 0;
  const push = (child: Child) => setChildren(current => {
    const parents = [...current];
    const parent = parents.at(-1);
    if (child.kind === 'wait' && parent?.kind === 'call') parents[parents.length - 1] = { ...parent, initial: undefined };
    return [...parents, child];
  });
  const pickCall = (value: TraceRow, initial?: InspectorTab) => navigation.pickRecord ? navigation.pickRecord('call', value, initial) : push({ kind: 'call', row: value, initial });
  const back = () => setChildren(current => current.slice(0, -1));
  const parent = children.at(-2);
  const backLabel = parent?.kind === 'call' ? parent.row.operator_id?.replace(/^system_command:/, '') ?? 'Call' : parent?.kind === 'stage' ? 'Execution stage' : 'Node occurrence';
  const selectEdge = (edge: string) => { if (row) push({ kind: 'edge', id: edge, occurrence: row.occurrence_id! }); };
  if (child?.kind === 'call') return <CallInspector key={child.row.call_id} row={child.row} workflow={workflow} frontier={trace.frontier} initial={child.initial} onBack={back} onClose={onClose} onWait={wait => push({ kind: 'wait', row: wait })} backLabel={backLabel} />;
  if (child?.kind === 'wait') return <WaitInspector key={String(child.row.wait_id)} row={child.row} onBack={back} onClose={onClose} backLabel={backLabel} />;
  if (child?.kind === 'stage') return <StageInspector key={String(child.row.event_id)} row={child.row} nodeId={id} workflow={workflow} onBack={back} onClose={onClose} onEdge={selectEdge} />;
  if (child?.kind === 'edge') return <EdgeInspector key={`${child.id}:${child.occurrence}`} iid={iid} id={child.id} occurrence={child.occurrence} trace={trace} workflow={workflow} onBack={back} backLabel={backLabel} onClose={onClose} />;
  return <InspectorFrame kind="node-execution" title={id} initial={initial} context={<>{row ? <StatusBadge row={row} /> : null}{pendingWait && navigation.canRespond?<button className="inspector-link" onClick={()=>navigation.respondToWait?.({...pendingWait,values:[]})}>Respond</button>:null}{count > 1 ? <button className="occurrence-switch" onClick={onReselect}>{row?.ordinal != null || row?.ui_ordinal != null ? `Occurrence #${String(row.ordinal ?? row.ui_ordinal)}` : 'Choose occurrence'} · ×{count}</button> : null}</>} tabs={['Summary', 'Input / Output', 'Execution', 'Definition']} onClose={onClose} footer={<><span>Full recorded occurrence</span><code>{workflow.definition_hash.slice(0, 8)}</code></>}>
    {tab => tab === 'Definition' ? <DefinitionDetails workflow={workflow} kind="node" identity={id} /> : !row ? <>{!record ? <PageFeedback page={query} empty="No occurrence has been recorded for this Node." /> : null}{tab === 'Summary' ? <DefinitionSummary workflow={workflow} kind="node" identity={id} /> : null}</> : tab === 'Summary' ? <><RowDetails row={row} />{row.values.includes('error') ? <LazyValue invocationId={iid} kind="node" identity={row.occurrence_id!} field="error" valueKey={row.value_keys?.error} label="Error" /> : null}</> : tab === 'Input / Output' ? <NodeValues row={{ ...row, node_id: id }} frontier={trace.frontier} onCall={pickCall} /> : <NodeExecution row={{ ...row, node_id: id }} frontier={trace.frontier} workflow={workflow} onCall={pickCall} onStage={stage => push({ kind: 'stage', row: stage })} onEdge={selectEdge} />}
  </InspectorFrame>;
}
function EvaluationSummary({ row }: { row: TraceRow }) {
  return <><div className={`evaluation-result ${row.condition_result ? 'is-taken' : 'is-not-taken'}`}><span className="evaluation-result-icon" aria-hidden="true">{row.condition_result ? '✓' : '×'}</span><div><strong>{row.condition_result ? 'Selected' : 'Not selected'}</strong><small>Recorded condition result</small></div></div><dl className="trace-facts"><dt>Source occurrence</dt><dd><CopyId value={row.source_occurrence_id} /></dd><dt>Source outcome</dt><dd>{row.source_status === 'complete' ? 'Completed' : row.source_status === 'error' ? 'Failed' : String(row.source_status ?? '—')}</dd><dt>Recorded</dt><dd>{formatTime(row.occurred_at_us as number, true)}</dd><dt>Measured time</dt><dd>{duration(row.duration_ns == null ? null : row.duration_ns / 1000)}</dd><dt>Event</dt><dd>#{row.sequence}</dd></dl></>;
}
function EdgeValues({ iid, occurrence, row, conditional, frontier }: { iid: string; occurrence?: string; row?: TraceRow; conditional: boolean; frontier: number }) {
  const snapshot = useExecutionSnapshot();
  const source = useQuery({ queryKey: ['execution-record', iid, 'node', occurrence, snapshot.epoch], meta: snapshot.meta,
    queryFn: ({ signal }) => getExecutionRecord(iid, 'node', occurrence!, signal), enabled: Boolean(occurrence),
    staleTime: Infinity, gcTime: 30_000, refetchOnWindowFocus: false });
  return <><InspectorSection title="Source values"><p className="inspector-empty">Recorded output or error from the selected source occurrence.</p>{!occurrence ? <p className="inspector-empty">Select an occurrence to inspect its source values.</p> : source.isPending ? <p className="inspector-empty" role="status">Loading source metadata…</p> : source.error ? <div className="sidebar-error"><p>{source.error.message}</p><button onClick={() => void source.refetch()}>Retry</button></div> : source.data ? source.data.values.some(field => ['output', 'error'].includes(field)) ? ['output', 'error'].filter(field => source.data.values.includes(field)).map(field => <LazyValue key={field} invocationId={iid} kind="node" identity={occurrence} field={field} valueKey={source.data.value_keys?.[field]} label={field === 'output' ? 'Source output' : 'Source error'} />) : <p className="inspector-empty">No source output or error was recorded.</p> : null}</InspectorSection><InspectorSection title="Condition result">{!conditional ? <p className="inspector-empty">This edge has no Condition and produces no separate Condition result.</p> : row ? <JsonValue value={row.condition_result} label="selected" /> : <p className="inspector-empty">No Condition result was recorded for this selection.</p>}</InspectorSection></>;
}
function EdgeInspector({ iid, id, record, occurrence, trace, workflow, onClose, onReselect, onBack, backLabel, initial }: { initial?: InspectorTab; iid: string; id: string; record?: TraceRow; occurrence?: string; trace: ExecutionTrace; workflow: WorkflowGraphData; onClose: () => void; onReselect?: () => void; onBack?: () => void; backLabel?: string }) {
  const navigation = useExecutionNavigation();
  const edge = workflow.definition.edges.find(edge => edge.id === id);
  const query = useInspectorPage(iid, 'edge', { edge_id: id, ...(occurrence ? { occurrence_id: occurrence } : {}) }, trace.frontier, Boolean(edge?.conditional));
  const [chosen, setChosen] = useState<TraceRow | undefined>();
  const row = chosen ?? record ?? query.rows[0];
  useEffect(() => { if (navigation.active && !record && row) navigation.pickRecord?.('edge', row); }, [navigation.active, record, row]);
  const count = trace.edges.find(item => item.edge_id === id)?.evaluations ?? 0;
  const latest = occurrence ? undefined : trace.edges.find(edge => edge.edge_id === id);
  return <InspectorFrame kind="edge-execution" title={id} initial={initial} context={<><span className="inspector-chip">{edge?.on === 'error' ? 'Fail edge' : 'Complete edge'}</span>{edge?.conditional ? <span>Conditional</span> : null}{!occurrence && count > 1 ? <button className="occurrence-switch" onClick={onReselect}>{row?.ordinal != null || row?.ui_ordinal != null ? `Evaluation #${String(row.ordinal ?? row.ui_ordinal)}` : 'Choose evaluation'} · ×{count}</button> : null}</>} tabs={edge?.conditional ? ['Summary', 'Input / Output', 'Evaluations', 'Definition'] : ['Summary', 'Input / Output', 'Definition']} onClose={onClose} onBack={onBack} backLabel={backLabel} footer={<span>{occurrence ? 'For selected occurrence' : 'Full recorded evaluation'}</span>}>
    {tab => tab === 'Definition' ? <DefinitionDetails workflow={workflow} kind="edge" identity={id} /> : tab === 'Input / Output' ? <EdgeValues iid={iid} occurrence={occurrence ?? row?.source_occurrence_id} row={row} conditional={Boolean(edge?.conditional)} frontier={trace.frontier} /> : tab === 'Evaluations' ? <><PageFeedback page={query} empty={occurrence ? 'No condition evaluation was recorded for this occurrence.' : 'No condition evaluations were recorded.'} />{query.rows.map((value, index) => <RecordChoice key={executionRecordKey(value, 'edge')} type="edge" title={`Event #${value.sequence}`} caption={formatTime(value.occurred_at_us as number, true)} badge={<span className={`status-badge status-${value.condition_result ? 'completed' : 'skipped'}`}>{value.condition_result ? 'Selected' : 'Not selected'}</span>} durationLabel={duration(value.duration_ns == null ? null : value.duration_ns / 1000)} active={Boolean(row && executionRecordKey(row, 'edge') === executionRecordKey(value, 'edge'))} onClick={() => { if (navigation.pickRecord) navigation.pickRecord('edge', value, 'Evaluations'); else setChosen(value); if (!occurrence && !query.index) stackPositions.write(iid, 'edge', id, count, index); }} />)}<InspectorPages page={query} label="Evaluations" /></> : <><div className="edge-route-heading"><strong>{edge?.source}</strong><span aria-hidden="true">→</span><strong>{edge?.target}</strong></div>{row ? <EvaluationSummary row={row} /> : edge?.conditional ? <PageFeedback page={query} empty="No condition evaluation was recorded." /> : <><p className="inspector-empty">This edge has no condition, so there is no separate condition evaluation.</p>{latest ? <div className={`evaluation-result ${latest.condition_result ? 'is-taken' : 'is-not-taken'}`}><strong>Latest route decision: {latest.condition_result ? 'Taken' : 'Not taken'}</strong></div> : null}</>}{occurrence && !row ? <dl className="trace-facts"><dt>Source occurrence</dt><dd><CopyId value={occurrence} /></dd></dl> : null}</>}
  </InspectorFrame>;
}
function DirectCallInspector({ row, trace, workflow, onBack, onClose, initial }: { row: TraceRow; initial?: InspectorTab; trace: ExecutionTrace; workflow: WorkflowGraphData; onBack: () => void; onClose: () => void }) {
  const navigation = useExecutionNavigation();
  const [wait, setWait] = useState<TraceRow | null>(null);
  return wait ? <WaitInspector row={wait} onBack={() => setWait(null)} onClose={onClose} backLabel={row.operator_id?.replace(/^system_command:/, '') ?? 'Call'} /> : <CallInspector row={row} initial={initial} frontier={trace.frontier} workflow={workflow} onBack={onBack} onClose={onClose} onWait={value => navigation.pickRecord ? navigation.pickRecord('wait', { ...value, node_id: row.node_id }) : setWait(value)} />;
}
export function ExecutionInspector({ trace, workflow, selection, record, onClose, onSelect, onReselect, onBack }: { trace: ExecutionTrace; workflow: WorkflowGraphData; selection: NonNullable<Selection>; record?: TraceRow; onClose: () => void; onSelect?: (value: Selection) => void; onReselect?: () => void; onBack?: () => void }) {
  const iid = trace.invocation.invocation_id;
  if (record?.wait_id) return <WaitInspector initial={selection.record?.section} row={record} onBack={onBack ?? onClose} onClose={onClose} backLabel="System Command Call" />;
  if (record?.call_id) return <DirectCallInspector row={record} trace={trace} workflow={workflow} initial={selection.record?.section} onBack={onBack ?? onClose} onClose={onClose} />;
  if (selection.kind === 'node') return <NodeInspector iid={iid} id={selection.id} record={record} trace={trace} workflow={workflow} onClose={onClose} onReselect={onReselect} initial={selection.record?.section} />;
  if (selection.kind === 'edge') return <EdgeInspector initial={selection.record?.section} iid={iid} id={selection.id} record={record} trace={trace} workflow={workflow} onClose={onClose} onReselect={onReselect} />;
  const row = trace.invocation;
  return <InspectorFrame kind="invocation" title={row.workflow_id ?? iid} context={<><span>v{workflow.workflow_version}</span><code>{workflow.definition_hash.slice(0, 8)}</code><StatusBadge row={row} /></>} tabs={['Summary', 'Input / Output', 'Execution', 'Definition']} onClose={onClose} footer={<span>Full recorded invocation · Sequence {String(row.last_sequence)}</span>}>
    {tab => tab === 'Summary' ? <><dl className="trace-facts"><dt>Invocation</dt><dd><CopyId value={iid} /></dd><dt>Session</dt><dd><CopyId value={row.session_id} /></dd><dt>Revision</dt><dd><CopyId value={workflow.workflow_revision_id} /></dd><dt>Entry node</dt><dd>{row.entry_node_id}</dd>{row.parent_invocation_id ? <><dt>Parent invocation</dt><dd><CopyId value={row.parent_invocation_id} /></dd></> : null}</dl><RowDetails row={row} />{row.values.includes('error') ? <LazyValue invocationId={iid} kind="invocation" identity={iid} field="error" label="Error" /> : null}</> : tab === 'Input / Output' ? <RowValues row={row} kind="invocation" /> : tab === 'Definition' ? <DefinitionDetails workflow={workflow} kind="workflow" identity={workflow.workflow_revision_id} /> : <InspectorSection title="Node occurrences" count={trace.nodes.reduce((sum, node) => sum + node.occurrences, 0)}>{trace.nodes.map(node => <RecordChoice key={node.node_id} title={node.node_id} caption={`${node.occurrences} ${node.occurrences === 1 ? 'occurrence' : 'occurrences'}`} badge={<StatusBadge row={node} />} onClick={() => onSelect?.({ kind: 'node', id: node.node_id })} />)}</InspectorSection>}
  </InspectorFrame>;
}
