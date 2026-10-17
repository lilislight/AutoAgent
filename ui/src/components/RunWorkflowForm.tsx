import { useEffect, useId, useRef, useState, type FormEvent } from 'react';
import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import { ApiError, getWorkflowRunSpec, listExecutionSessions, runWorkflow } from '../api';
import type { AcceptedExecution, WorkflowGraphData } from '../types';
import { runInput } from '../runInput';
import { useActionDraft } from '../actionDrafts';
import { jsonExample, jsonIssues, type FieldError } from '../jsonSchema';
import { SelectMenu } from './SelectMenu';
import { JsonEditor } from './JsonEditor';
import { ActionReference } from './ActionReference';
import { formatTime } from './TraceDetails';

export function useDebounced(value: string) {
  const [result, setResult] = useState(value);
  useEffect(() => { const timer = setTimeout(() => setResult(value), 250); return () => clearTimeout(timer); }, [value]);
  return result;
}
function SessionPicker({ value, onChange, disabled }: { value: string; onChange: (id: string) => void; disabled: boolean }) {
  const [search, setSearch] = useState(''), [open, setOpen] = useState(false);
  const query = useDebounced(search.trim().toLowerCase());
  const sessions = useInfiniteQuery({ queryKey: ['run-sessions', query], queryFn: ({ pageParam, signal }) => listExecutionSessions(pageParam, signal, undefined, { query, workflowIds: [], revisionIds: [], statuses: [], startedFrom: '', startedTo: '' }), initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor, enabled: open, staleTime: 3000, refetchOnWindowFocus: false });
  const rows = sessions.data?.pages.flatMap(page => page.items) ?? [];
  return <div className="run-field"><span>Existing Session</span><SelectMenu label="Existing Session" value={value} onChange={onChange} disabled={disabled} searchable placeholder="Search Session ID…" onSearch={setSearch} onOpenChange={setOpen}
    options={rows.map(row => ({ value: row.session_id, label: row.session_id, description: `Last activity · ${formatTime(row.updated_at_us)} · ${row.invocation_count} ${row.invocation_count === 1 ? 'invocation' : 'invocations'}` }))}
    loading={search.trim().toLowerCase() !== query || sessions.isFetching} error={sessions.error?.message} onRetry={() => void sessions.refetch()} hasMore={sessions.hasNextPage} onMore={() => { if (!sessions.isFetchingNextPage) void sessions.fetchNextPage(); }} /></div>;

}
export function RunWorkflowForm({ workflow, entry: selectedEntry, onEntry, onAccepted, initial, sessionMode: fixedMode, session: fixedSession, scope = 'workflow', hideSessionField = false }: {
  workflow: WorkflowGraphData; entry?: string; onEntry?: (id: string) => void; onAccepted: (receipt: AcceptedExecution) => void;
  initial?: { entry: string; input: unknown }; sessionMode?: 'new' | 'existing'; session?: string; scope?: string; hideSessionField?: boolean;
}) {
  const id = useId(), submitting = useRef(false);
  const [localEntry, setEntry] = useState(initial?.entry ?? (workflow.definition.entry_node_ids.length === 1 ? workflow.definition.entry_node_ids[0] : ''));
  const entry = selectedEntry ?? localEntry;
  const [sessionDraft, setSessionDraft] = useActionDraft(`${scope}:${workflow.workflow_revision_id}:session`, () => ({ mode: fixedMode ?? 'new' as 'new' | 'existing', id: fixedSession ?? '' }));
  const mode = fixedMode ?? sessionDraft.mode, session = fixedSession ?? sessionDraft.id;
  const [draft, setDraft] = useActionDraft<{ text?: string }>(`${scope}:${workflow.workflow_revision_id}:${entry}:input`, () => initial && entry === initial.entry ? { text: JSON.stringify(initial.input, null, 2) } : {});
  const [busy, setBusy] = useState(false), [error, setError] = useState<string | null>(null), [fieldErrors, setFieldErrors] = useState<FieldError[]>([]);
  const debounced = useDebounced(session.trim());
  const spec = useQuery({ queryKey: ['workflow-run-spec', workflow.workflow_id, workflow.workflow_revision_id, entry, mode === 'existing' ? debounced : null], queryFn: ({ signal }) => getWorkflowRunSpec(workflow.workflow_id, workflow.workflow_revision_id, entry, mode === 'existing' ? debounced : null, signal), staleTime: 0, retry: false, refetchOnWindowFocus: false });
  const selected = spec.data?.entries.find(row => row.node_id === entry);
  const schema = selected?.input_mode === 'contract' ? selected.input_schema : undefined;
  const text = draft.text ?? JSON.stringify(jsonExample(schema), null, 2);
  const checkingSession = mode === 'existing' && (!session.trim() || session.trim() !== debounced);
  const disabled = busy || !entry || !spec.data?.can_run || spec.isFetching || checkingSession;
  const submit = async (event: FormEvent) => {
    event.preventDefault(); if (disabled || submitting.current) return;
    setFieldErrors([]); setError(null);
    if (jsonIssues(text, schema).length) { setError('Correct the highlighted Input fields.'); return; }
    let body;
    try { body = runInput({ workflow: workflow.workflow_id, revision: workflow.workflow_revision_id, entry, text, sessionMode: mode, session }); }
    catch (failure) { setError((failure as Error).message); return; }
    submitting.current = true; setBusy(true);
    try { const receipt = await runWorkflow(body, new AbortController().signal); if (mode === 'new') setSessionDraft(previous => ({...previous,id:''})); onAccepted(receipt); }
    catch (failure) { setError((failure as Error).message); if (failure instanceof ApiError) setFieldErrors(failure.fields); void spec.refetch(); }
    finally { submitting.current = false; setBusy(false); }
  };
  return <form className="run-form inline-action-form" onSubmit={event => void submit(event)}><div className="run-fields">
    <div className="run-field"><span>Entry node</span><SelectMenu label="Entry node" value={entry} disabled={busy} placeholder="Choose an Entry node" options={workflow.definition.entry_node_ids.map(node => ({value:node,label:node}))} onChange={value => { setEntry(value); onEntry?.(value); setError(null); setFieldErrors([]); }} /></div>
    {!fixedMode ? <fieldset className="run-session-mode" disabled={busy}><legend>Session</legend>{(['new', 'existing'] as const).map(value => <label key={value}><input type="radio" name={`${id}-session`} checked={mode === value} onChange={() => setSessionDraft({ mode: value, id: '' })} />{value === 'new' ? 'New session' : 'Existing session'}</label>)}</fieldset> : null}
    {hideSessionField ? null : mode === 'new' ? <label className="run-field" htmlFor={`${id}-session`}><span>Session ID <small>Optional</small></span><input id={`${id}-session`} value={session} onChange={event => setSessionDraft({ ...sessionDraft, id: event.target.value })} disabled={busy} autoComplete="off" placeholder="Leave empty to generate an ID" maxLength={512} /></label> : fixedSession ? <ActionReference label="Session ID" value={fixedSession} /> : <SessionPicker value={session} onChange={id => { setSessionDraft({ ...sessionDraft, id }); setError(null); }} disabled={busy} />}
    <JsonEditor label="Input" value={text} onChange={value => { setDraft({ text: value }); setError(null); setFieldErrors([]); }} schema={schema} disabled={busy} errors={fieldErrors} />
    {selected?.input_mode === 'raw_json' ? <p className="run-hint">Supply the original Workflow input. Input Mapping validates its result during execution; an Operator schema cannot describe this input.</p> : null}
    {spec.isPending ? <p className="run-hint" role="status">Checking registered Revision…</p> : null}{spec.error ? <p className="run-error">{spec.error.message}<button type="button" onClick={() => void spec.refetch()}>Retry</button></p> : null}
    {!checkingSession && spec.data?.message ? <p className="run-error" role="status">{spec.data.message}</p> : null}{error ? <p className="run-error" role="alert">{error}</p> : null}
  </div><div className="run-footer"><span>{busy ? 'Submitting…' : 'Creates a new Root Invocation'}</span><button type="submit" className="run-primary" disabled={disabled}>{busy ? 'Starting…' : 'Run'}</button></div></form>;
}
