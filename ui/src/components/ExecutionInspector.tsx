import { useEffect, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getExecutionRecord } from '../api';
import type { ExecutionTrace, Selection, TraceRow, WorkflowGraphData } from '../types';
import { systemCommandId, valueMessage } from '../inspectorModel';
import { duration, formatTime, LazyValue, MissingValue, RowDetails, RowValues, StatusBadge } from './TraceDetails';
import { CopyId, InspectorFrame, type InspectorTab } from './InspectorFrame';
import { DefinitionDetails, GroupDefinitionInspector } from './DefinitionDetails';
import { useExecutionSnapshot } from './ExecutionSnapshot';
import { useExecutionNavigation } from './ExecutionNavigation';
import { InvocationActions, NodeActions } from './ExecutionActionContent';
import { isExternalWaitRecord } from '../executionSelection';
import { beforeCalls, CallChoice, CallInspector, InspectorPages, InspectorSection, PageFeedback,
  RecordChoice, RouteChoice, StageChoice, StageInspector, useInspectorPage, CommandWaitDetails } from './ExecutionInspectorParts';

type Child = { kind: 'call' | 'stage'; row: TraceRow; initial?: InspectorTab } | { kind: 'edge'; id: string; occurrence: string };
function NodeValues({ row, externalWait, pending, onExecution, onActions }: {
  row: TraceRow; externalWait: boolean; pending: boolean; onExecution: () => void; onActions: () => void;
}) {
  const calls = useInspectorPage(row.invocation_id, 'call', { occurrence_id: row.occurrence_id! }, 0, externalWait && !pending);
  if (row.status === 'skipped') return <MissingValue title="Input / Output" message={valueMessage(row, 'input')} />;
  return <>
    {!externalWait ? row.values.includes('input') ? <LazyValue invocationId={row.invocation_id} kind="node" identity={row.occurrence_id!} field="input" valueKey={row.value_keys?.input} label="Mapped input" description="Input Mapping result, before individual Calls are validated." /> : <MissingValue title="Mapped input" message={row.error_phase === 'input_mapping' ? valueMessage(row, 'input') : 'No separate Input Mapping result was recorded. Each Call owns its actual input.'} /> : null}
    {externalWait ? pending ? <div className="inspector-notice"><p>The pending Wait request and response form are in Actions.</p><button className="inspector-link" onClick={onActions}>Open Actions →</button></div> : <InspectorSection title="Wait request"><PageFeedback page={calls} empty="No Wait request was recorded." />{calls.rows.map(call => <LazyValue key={call.call_id} invocationId={row.invocation_id} kind="wait" identity={call.call_id!} field="request" label="Request" />)}<InspectorPages page={calls} label="Wait requests" /></InspectorSection> : null}
    {row.values.includes('output') ? <LazyValue invocationId={row.invocation_id} kind="node" identity={row.occurrence_id!} field="output" valueKey={row.value_keys?.output} label={externalWait ? 'Response' : 'Output'} description={externalWait ? 'Accepted response returned by this Node.' : 'Final Node result, after aggregation when configured.'} /> : <MissingValue title={externalWait ? 'Response' : 'Output'} message={valueMessage(row, 'output')} />}
    {!externalWait ? <button className="inspector-section-link" onClick={onExecution}><span>View Calls in Execution</span><span aria-hidden="true">→</span></button> : null}
  </>;
}
function NodeExecution({ row, frontier, workflow, onCall, onStage, onEdge }: {
  row: TraceRow; frontier: number; workflow: WorkflowGraphData; onCall: (row: TraceRow) => void; onStage: (row: TraceRow) => void; onEdge: (id: string) => void;
}) {
  const skipped = row.status === 'skipped';
  const stages = useInspectorPage(row.invocation_id, 'stage', { occurrence_id: row.occurrence_id! }, frontier, !skipped);
  const calls = useInspectorPage(row.invocation_id, 'call', { occurrence_id: row.occurrence_id! }, frontier, !skipped);
  if (skipped) return <p className="inspector-empty">This occurrence was skipped. No executable or execution stages ran.</p>;
  const executable = workflow.definition.nodes.find(node => node.id === row.node_id)?.executable;
  const command = executable?.kind === 'system_command';
  const externalWait = command && ['wait', 'system_command:wait'].includes(executable.id);
  const routes = workflow.definition.edges.filter(edge => edge.source === row.node_id);
  return <div className="execution-pipeline">
    <InspectorSection title="Preparation"><PageFeedback page={stages} />{stages.rows.filter(beforeCalls).map(stage => <StageChoice key={String(stage.event_id)} row={stage} onPick={onStage} />)}{!stages.query.isPending && !stages.query.error && !stages.rows.some(beforeCalls) ? <p className="inspector-empty">No separate Input Mapping or Capability Resolution stage was recorded.</p> : null}</InspectorSection>
    <InspectorSection title={externalWait ? 'Wait' : command ? 'System command Calls' : 'Executable Calls'} count={row.call_count}><PageFeedback page={calls} empty="No Calls were recorded for this occurrence." />{calls.rows.map(call => call.operator_id === 'system_command:wait' ? <CommandWaitDetails key={call.call_id} row={call} /> : <CallChoice key={call.call_id} row={call} onPick={value => onCall({ ...value, node_id: row.node_id })} />)}<InspectorPages page={calls} label="Calls" /></InspectorSection>
    {stages.rows.some(stage => !beforeCalls(stage)) ? <InspectorSection title="Result processing">{stages.rows.filter(stage => !beforeCalls(stage)).map(stage => <StageChoice key={String(stage.event_id)} row={stage} onPick={onStage} />)}</InspectorSection> : null}
    <InspectorPages page={stages} label="Stages" />
    {routes.length ? <InspectorSection title="Outgoing edges" count={routes.length}>{routes.map(edge => <RouteChoice key={edge.id} edge={edge} onPick={onEdge} />)}</InspectorSection> : null}
  </div>;
}
function NodeInspector({ iid, id, record, trace, workflow, onClose, onReselect, initial, waitId }: { waitId?: string; initial?: InspectorTab; iid: string; id: string; record?: TraceRow; trace: ExecutionTrace; workflow: WorkflowGraphData; onClose: () => void; onReselect?: () => void }) {
  const navigation = useExecutionNavigation();
  const query = useInspectorPage(iid, 'node', { node_id: id }, trace.frontier, !record);
  const row = record ?? query.rows[0];
  const pendingWaits=navigation.pendingResponses?.filter(wait=>wait.invocation_id===iid && wait.occurrence_id===row?.occurrence_id) ?? [];
  const canAct=!navigation.replaying && navigation.canRespond && pendingWaits.length > 0;
  const [children, setChildren] = useState<Child[]>([]);
  const [returnTab, setReturnTab] = useState<InspectorTab>();
  useEffect(() => { setChildren([]); setReturnTab(undefined); }, [initial, waitId]);
  const executable = workflow.definition.nodes.find(node => node.id === id)?.executable;
  const externalWait = executable?.kind === 'system_command' && systemCommandId(executable.id) === 'system_command:wait';
  const child = children.at(-1);
  const count = trace.nodes.find(item => item.node_id === id)?.occurrences ?? 0;
  const push = (child: Child) => setChildren(current => [...current, child]);
  const pickCall = (value: TraceRow, initial?: InspectorTab) => navigation.pickRecord ? navigation.pickRecord('call', value, initial) : push({ kind: 'call', row: value, initial });
  const back = () => { setReturnTab('Execution'); setChildren(current => current.slice(0, -1)); };
  const parent = children.at(-2);
  const backLabel = parent?.kind === 'call' ? parent.row.operator_id?.replace(/^system_command:/, '') ?? 'Call' : parent?.kind === 'stage' ? 'Execution stage' : 'Node occurrence';
  const selectEdge = (edge: string) => { if (row) push({ kind: 'edge', id: edge, occurrence: row.occurrence_id! }); };
  if (child?.kind === 'call') return <CallInspector key={child.row.call_id} row={child.row} workflow={workflow} frontier={trace.frontier} initial={child.initial} onBack={back} onClose={onClose} backLabel={backLabel} />;
  if (child?.kind === 'stage') return <StageInspector key={String(child.row.event_id)} row={child.row} nodeId={id} workflow={workflow} onBack={back} onClose={onClose} onEdge={selectEdge} />;
  if (child?.kind === 'edge') return <EdgeInspector key={`${child.id}:${child.occurrence}`} iid={iid} id={child.id} occurrence={child.occurrence} trace={trace} workflow={workflow} onBack={back} backLabel={backLabel} onClose={onClose} />;
  const tabs: InspectorTab[] = row ? ['Summary', 'Input / Output', 'Execution', 'Definition'] : ['Summary', 'Definition'];
  if (canAct) tabs.unshift('Actions');
  return <InspectorFrame kind="node-execution" title={id} initial={returnTab ?? initial} context={<>
    {row ? <StatusBadge row={row} /> : null}
    {row && trace.fork && ['completed','failed','cancelled','skipped'].includes(trace.fork.inherited_occurrences[row.occurrence_id!] ?? '') ? <span className="inspector-chip" title={`Source Invocation ${trace.fork.source_invocation_id} · through Event #${trace.fork.source_sequence}`}>Inherited execution</span> : null}
    {count > 1 ? <button className="occurrence-switch" onClick={onReselect}>{row?.ordinal != null || row?.ui_ordinal != null ? `Occurrence #${String(row.ordinal ?? row.ui_ordinal)}` : 'Choose occurrence'} · ×{count}</button> : null}
  </>} tabs={tabs} onClose={onClose}>
    {(tab, selectTab) => {
      if (tab === 'Definition') return <DefinitionDetails workflow={workflow} kind="node" identity={id} />;
      if (!row) return <PageFeedback page={query} empty="No occurrence has been recorded for this Node." />;
      switch (tab) {
        case 'Actions': return <NodeActions key={waitId ?? row.occurrence_id} waits={pendingWaits} waitId={waitId} />;
        case 'Summary': return <>
          <RowDetails row={row} />
          {row.values.includes('error') ? <LazyValue invocationId={iid} kind="node" identity={row.occurrence_id!} field="error" valueKey={row.value_keys?.error} label="Error" /> : null}
        </>;
        case 'Input / Output': return <NodeValues row={{ ...row, node_id: id }} externalWait={externalWait} pending={Boolean(canAct)} onExecution={() => selectTab('Execution')} onActions={() => selectTab('Actions')} />;
        case 'Execution': return <NodeExecution row={{ ...row, node_id: id }} frontier={trace.frontier} workflow={workflow} onCall={pickCall} onStage={stage => push({ kind: 'stage', row: stage })} onEdge={selectEdge} />;
        default: return null;
      }
    }}
  </InspectorFrame>;
}
function EvaluationSummary({ row }: { row: TraceRow }) {
  return <><div className={`evaluation-result ${row.condition_result ? 'is-taken' : 'is-not-taken'}`}><span className="evaluation-result-icon" aria-hidden="true">{row.condition_result ? '✓' : '×'}</span><div><strong>{row.condition_result ? 'Selected' : 'Not selected'}</strong><small>{row.inherited ? 'Inherited condition result' : 'Recorded condition result'}</small></div></div><dl className="trace-facts"><dt>Source occurrence ID</dt><dd><CopyId value={row.source_occurrence_id} /></dd><dt>Source outcome</dt><dd>{row.source_status === 'complete' ? 'Completed' : row.source_status === 'error' ? 'Failed' : String(row.source_status ?? '—')}</dd><dt>Recorded</dt><dd>{formatTime(row.occurred_at_us as number, true)}</dd><dt>Processing time</dt><dd>{duration(row.duration_ns == null ? null : row.duration_ns / 1000)}</dd><dt>{row.inherited ? 'Source Event sequence' : 'Event sequence'}</dt><dd>#{row.sequence}</dd></dl></>;
}
function SourceOccurrence({ iid, occurrence, sourceId }: { iid: string; occurrence: string; sourceId?: string }) {
  const navigation = useExecutionNavigation();
  const snapshot = useExecutionSnapshot();
  const [locating, setLocating] = useState(false);
  const source = useQuery({ queryKey: ['execution-record', iid, 'node', occurrence, snapshot.epoch], meta: snapshot.meta,
    queryFn: ({ signal }) => getExecutionRecord(iid, 'node', occurrence, signal), enabled: navigation.active && locating,
    staleTime: Infinity, gcTime: 30_000, refetchOnWindowFocus: false });
  useEffect(() => {
    if (locating && source.data && navigation.active) {
      setLocating(false);
      navigation.pickRecord?.('node', source.data, source.data.status === 'failed' ? 'Summary' : 'Input / Output');
    }
  }, [locating, source.data, navigation.active, navigation.pickRecord]);
  return <><button className="inspector-section-link" disabled={locating && source.isFetching} onClick={() => { if (source.error) void source.refetch(); setLocating(true); }}><span>{locating && source.isFetching ? 'Opening source…' : `Inspect source Node${sourceId ? ` · ${sourceId}` : ''}`}</span><span aria-hidden="true">→</span></button>{source.error ? <p className="inspector-empty" role="alert">{source.error.message}</p> : null}</>;
}
function EdgeInspector({ iid, id, record, occurrence, trace, workflow, onClose, onReselect, onBack, backLabel, initial }: { initial?: InspectorTab; iid: string; id: string; record?: TraceRow; occurrence?: string; trace: ExecutionTrace; workflow: WorkflowGraphData; onClose: () => void; onReselect?: () => void; onBack?: () => void; backLabel?: string }) {
  const navigation = useExecutionNavigation();
  const edge = workflow.definition.edges.find(edge => edge.id === id);
  const query = useInspectorPage(iid, 'edge', { edge_id: id, ...(occurrence ? { occurrence_id: occurrence } : {}) }, trace.frontier, Boolean(edge?.conditional) && !record);
  const row = record ?? query.rows[0];
  useEffect(() => { if (navigation.active && !record && !occurrence && !onBack && row) navigation.pickRecord?.('edge', row); }, [navigation.active, record, occurrence, onBack, row]);
  const count = trace.edges.find(item => item.edge_id === id)?.evaluations ?? 0;
  const latest = occurrence ? undefined : trace.edges.find(edge => edge.edge_id === id);
  return <InspectorFrame kind="edge-execution" title={id} initial={initial} context={<><span className="inspector-chip">{edge?.on === 'error' ? 'Fail edge' : 'Complete edge'}</span>{edge?.conditional ? <span>Conditional</span> : null}{!occurrence && count > 1 ? <button className="occurrence-switch" onClick={onReselect}>{row?.ordinal != null || row?.ui_ordinal != null ? `Evaluation #${String(row.ordinal ?? row.ui_ordinal)}` : 'Choose evaluation'} · ×{count}</button> : null}</>} tabs={['Summary', 'Definition']} onClose={onClose} onBack={onBack} backLabel={backLabel}>
    {tab => tab === 'Definition' ? <DefinitionDetails workflow={workflow} kind="edge" identity={id} /> : <><div className="edge-route-heading"><strong>{edge?.source}</strong><span aria-hidden="true">→</span><strong>{edge?.target}</strong></div>{row ? <EvaluationSummary row={row} /> : edge?.conditional ? <PageFeedback page={query} empty="No condition evaluation was recorded." /> : <><p className="inspector-empty">This edge has no condition, so there is no separate condition evaluation.</p>{latest ? <div className={`evaluation-result ${latest.condition_result ? 'is-taken' : 'is-not-taken'}`}><strong>Latest route decision: {latest.condition_result ? 'Taken' : 'Not taken'}</strong></div> : null}</>}{occurrence && !row ? <dl className="trace-facts"><dt>Source occurrence ID</dt><dd><CopyId value={occurrence} /></dd></dl> : null}{occurrence ?? row?.source_occurrence_id ? <SourceOccurrence iid={iid} occurrence={(occurrence ?? row?.source_occurrence_id)!} sourceId={edge?.source} /> : null}</>}
  </InspectorFrame>;
}
export function ExecutionInspector({ trace, workflow, selection, record, onClose, onSelect, onReselect, onBack }: { trace: ExecutionTrace; workflow: WorkflowGraphData; selection: NonNullable<Selection>; record?: TraceRow; onClose: () => void; onSelect?: (value: Selection) => void; onReselect?: () => void; onBack?: () => void }) {
  const iid = trace.invocation.invocation_id;
  const navigation = useExecutionNavigation();
  const canAct = !navigation.replaying && (workflow.runnable || Boolean(navigation.pendingResponses?.length));
  const redirectWait = Boolean(record && selection.record && isExternalWaitRecord(selection.record.kind, record));
  useEffect(() => {
    if (redirectWait && record && selection.record && navigation.active) navigation.pickRecord?.(selection.record.kind, record, selection.record.section);
  }, [redirectWait, record, selection.record, navigation.active, navigation.pickRecord]);
  if (redirectWait) return null;
  if(selection.kind==='group')return <GroupDefinitionInspector workflow={workflow} identity={selection.id} onClose={onClose}/>;
  if (record?.wait_id) {
    const executable = workflow.definition.nodes.find(node => node.id === record.node_id)?.executable;
    return <CallInspector row={{ invocation_id: iid, node_id: record.node_id, occurrence_id: record.occurrence_id, call_id: record.wait_id, operator_id: systemCommandId(executable?.id), values: [] }} workflow={workflow} frontier={trace.frontier} initial={selection.record?.section ?? 'Execution'} onBack={onBack ?? onClose} onClose={onClose} />;
  }
  if (record?.call_id) return <CallInspector row={record} frontier={trace.frontier} workflow={workflow} initial={selection.record?.section} onBack={onBack ?? onClose} onClose={onClose} />;
  if (selection.kind === 'node') return <NodeInspector iid={iid} id={selection.id} record={record} trace={trace} workflow={workflow} onClose={onClose} onReselect={onReselect} initial={selection.record?.section} waitId={selection.record?.waitId} />;
  if (selection.kind === 'edge') return <EdgeInspector initial={selection.record?.section} iid={iid} id={selection.id} record={record} trace={trace} workflow={workflow} onClose={onClose} onReselect={onReselect} onBack={record ? onBack : undefined} />;
  const row = trace.invocation;
  return <InspectorFrame kind="invocation" initial={selection.section} title={row.workflow_id ?? iid} context={<><span>v{workflow.workflow_version}</span><code>{workflow.definition_hash.slice(0, 8)}</code><StatusBadge row={row} /></>} tabs={canAct ? ['Actions', 'Summary', 'Input / Output', 'Execution', 'Definition'] : ['Summary', 'Input / Output', 'Execution', 'Definition']} onClose={onClose}>
    {tab => {
      switch (tab) {
        case 'Actions': return <InvocationActions row={row} workflow={workflow} />;
        case 'Summary': return <>
          <dl className="trace-facts"><dt>Invocation ID</dt><dd><CopyId value={iid} /></dd><dt>Session ID</dt><dd><CopyId value={row.session_id} /></dd><dt>Revision</dt><dd><CopyId value={workflow.workflow_revision_id} /></dd><dt>Entry node</dt><dd>{row.entry_node_id}</dd>
            {row.parent_invocation_id ? <><dt>Parent Invocation ID</dt><dd><CopyId value={row.parent_invocation_id} /></dd></> : null}
          </dl>
          <RowDetails row={row} />
          {row.values.includes('error') ? <LazyValue invocationId={iid} kind="invocation" identity={iid} field="error" valueKey={row.value_keys?.error} label="Error" /> : null}
          {row.status === 'cancelled' ? <LazyValue invocationId={iid} kind="invocation" identity={iid} field="cancel_reason" label="Cancellation reason" /> : null}
        </>;
        case 'Input / Output': return <RowValues row={row} kind="invocation" />;
        case 'Definition': return <DefinitionDetails workflow={workflow} kind="workflow" identity={workflow.workflow_revision_id} />;
        case 'Execution': return <InspectorSection title="Nodes" count={trace.nodes.length}>
          {trace.nodes.length ? trace.nodes.map(node => <RecordChoice key={node.node_id} title={node.node_id} caption={node.occurrences > 1 ? `Current occurrence · ${node.occurrences} total` : '1 occurrence'} badge={<StatusBadge row={node} />} onClick={() => onSelect?.({ kind: 'node', id: node.node_id, record: { kind: 'node', id: node.occurrence_id!, section: 'Summary' } })} />) : <p className="inspector-empty">No Node occurrences have been recorded.</p>}
        </InspectorSection>;
      }
    }}
  </InspectorFrame>;
}
