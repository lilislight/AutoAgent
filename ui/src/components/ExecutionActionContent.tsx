import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getExecutionActionSpec, getExecutionValue } from '../api';
import type { PendingResponse, TraceRow, WorkflowGraphData } from '../types';
import { useActionDraft } from '../actionDrafts';
import { jsonExample, jsonIssues } from '../jsonSchema';
import { useExecutionNavigation } from './ExecutionNavigation';
import { SelectMenu } from './SelectMenu';
import { JsonEditor } from './JsonEditor';
import { LoadedValue } from './TraceDetails';
import { RunWorkflowForm } from './RunWorkflowForm';
import { ActionReference } from './ActionReference';

export function ResponseForm({ wait }: { wait: PendingResponse }) {
  const navigation = useExecutionNavigation();
  const [draft, setDraft] = useActionDraft<{ text?: string }>(`response:${wait.invocation_id}:${wait.wait_id}`, () => ({}));
  const [error, setError] = useState<string | null>(null);
  const spec = useQuery({ queryKey: ['execution-action-spec', navigation.invocationId, 'respond', wait.invocation_id, wait.wait_id], queryFn: ({ signal }) => getExecutionActionSpec(navigation.invocationId!, 'respond', { target_invocation_id: wait.invocation_id, wait_id: wait.wait_id }, signal), staleTime: 0, retry: false, refetchOnWindowFocus: false });
  const schema = spec.data?.input_schema;
  const text = draft.text ?? JSON.stringify(jsonExample(schema), null, 2);
  const pending = navigation.pendingResponses?.some(item => item.invocation_id === wait.invocation_id && item.wait_id === wait.wait_id);
  return <form className="run-form inline-action-form" onSubmit={event => { event.preventDefault(); setError(null); if (!spec.data || navigation.actionBusy || !pending) return; if (jsonIssues(text, schema).length) { setError('Correct the highlighted Response fields.'); return; } void navigation.sendAction?.('respond', { target_invocation_id: wait.invocation_id, wait_id: wait.wait_id, payload: JSON.parse(text) }); }}>
    <div className="run-fields"><div className="action-references"><ActionReference label="Invocation ID" value={wait.invocation_id} /><ActionReference label="Wait ID" value={wait.wait_id} /></div>
      <section className="response-request"><h3>Wait request</h3><LoadedValue invocationId={wait.invocation_id} kind="wait" identity={wait.wait_id} field="request" label="Wait request" /></section>
      <JsonEditor label="Response" schema={schema} value={text} onChange={text => { setDraft({ text }); setError(null); navigation.clearActionError?.(); }} errors={navigation.actionErrors} disabled={navigation.actionBusy || !pending} />
      {navigation.paused ? <p className="run-hint">The Root is paused. After responding, use Recover to resume execution.</p> : null}
      {spec.isFetching ? <p role="status">Checking response contract…</p> : null}{spec.error ? <p className="run-error">{spec.error.message}<button type="button" onClick={() => void spec.refetch()}>Retry</button></p> : null}
      {!pending ? <p role="status">This Wait no longer requires a response.</p> : null}{error || navigation.actionError ? <p className="run-error" role="alert">{error ?? navigation.actionError}</p> : null}
    </div><div className="run-footer"><button type="submit" className="run-primary" disabled={!pending || !spec.data || spec.isFetching || navigation.actionBusy}>{navigation.actionBusy ? 'Submitting…' : 'Respond'}</button></div>
  </form>;
}
export function PendingResponseList({ waits }: { waits: PendingResponse[] }) {
  const navigation = useExecutionNavigation();
  return <div className="pending-response-list">{waits.map(wait => <button type="button" className="response-choice" key={`${wait.invocation_id}:${wait.wait_id}`} onClick={() => navigation.respondToWait?.({ ...wait, node_id:wait.node_id ?? undefined, values: [] })}><strong>Response required</strong><span>{wait.node_id ?? wait.occurrence_id}</span><code>{wait.invocation_id.slice(0, 8)}</code></button>)}</div>;
}
export function NodeActions({ waits, waitId }: { waits: PendingResponse[]; waitId?: string }) {
  const [selected, setSelected] = useState<string | null>(waitId ?? null);
  const wait = waits.find(item => item.wait_id === selected) ?? waits[0];
  return <>{waits.length > 1 ? <div className="run-field"><span>Pending Wait</span><SelectMenu label="Pending Wait" value={wait?.wait_id ?? ''} onChange={setSelected} options={waits.map(item=>({value:item.wait_id,label:item.wait_id}))} /></div> : null}{wait ? <ResponseForm key={`${wait.invocation_id}:${wait.wait_id}`} wait={wait} /> : <p className="inspector-empty">No pending external responses.</p>}</>;
}
export function InvocationActions({ row, workflow }: { row: TraceRow; workflow: WorkflowGraphData }) {
  const navigation = useExecutionNavigation();
  const input = useQuery({ queryKey: ['run-again-input', row.invocation_id], queryFn: ({ signal }) => getExecutionValue(row.invocation_id, 'invocation', row.invocation_id, 'input', signal), enabled: workflow.runnable, staleTime: Infinity, gcTime: 30_000, retry: false });
  return <>{navigation.pendingResponses?.length ? <section className="inspector-section"><header><h3>Pending responses</h3><span>{navigation.pendingResponses.length}</span></header><PendingResponseList waits={navigation.pendingResponses} /></section> : null}
    {workflow.runnable ? <section className="inspector-section"><header><h3>Run again</h3></header>{input.isPending ? <p className="inspector-empty" role="status">Loading original Input…</p> : input.error ? <p className="run-error">{input.error.message}<button type="button" onClick={() => void input.refetch()}>Retry</button></p> : input.data?.available ? <RunWorkflowForm workflow={workflow} initial={{ entry: String(row.entry_node_id), input: input.data.value }} onAccepted={receipt => navigation.onRunAccepted?.(receipt)} scope={`run-again:${row.invocation_id}`} /> : <p className="inspector-empty">Original Input is unavailable.</p>}</section> : null}
  </>;
}
