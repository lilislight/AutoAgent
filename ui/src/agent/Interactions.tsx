import { CardsSkeleton, BusyLabel } from '../components/LoadingState';
import { AnimatedRegion, AgentDisclosure } from './AgentDisclosure';
import { AgentIcon } from './AgentIcon';
import { ToolArgumentsPreview } from './ToolPayloadPreview';
import { interactionReply, submitInteraction, requestUnavailable, requestChanged, type Question, type ReplyDraft, type ReplyAction, type SubmissionResult } from './interactionSubmission';
import { retryAgentRead, agentReadRetryDelay } from './readFeedback';
import { useEffect, useId, useRef, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { agentClient, performAction } from './client';
import { requestSignature } from './replies';
import { requiresHumanResponse } from './approvalPresentation.js';
import type { ActionSpec, Json, PendingWait } from './types';

interface ApprovalRequest { id: string; call: { name: string; call_id: string }; arguments: Json; reason: string | null }

function QuestionFields({ questions, drafts, onChange, disabled }: { questions: Question[]; drafts: Record<string, ReplyDraft>; onChange: (id: string, value: ReplyDraft) => void; disabled: boolean }) {
  const group = useId();
  return <>{questions.map(question => {
    const draft = drafts[question.id] ?? { text: '', selected: [], skipped: false };
    return <fieldset key={question.id} disabled={disabled} className="agent-question-fields"><legend>{question.text}</legend>
      <label className="agent-skip"><input type="checkbox" checked={draft.skipped} onChange={event => onChange(question.id, { ...draft, skipped: event.target.checked })} /> Skip this question</label>
      {question.options?.map(option => <label key={option.id} className="agent-choice"><input disabled={draft.skipped} type={question.type === 'multiple_choice' ? 'checkbox' : 'radio'} name={`${group}-${question.id}`} checked={draft.selected.includes(option.id)} onChange={event => onChange(question.id, { ...draft, selected: question.type === 'multiple_choice' ? event.target.checked ? [...draft.selected, option.id] : draft.selected.filter(id => id !== option.id) : [option.id] })} /><span><strong>{option.label}</strong>{option.description ? <small>{option.description}</small> : null}</span></label>)}
      {question.type === 'text' || question.allow_custom_answer !== false ? <label className="agent-field">{question.type === 'text' ? 'Your answer' : 'Additional or custom answer'}<textarea value={draft.text} disabled={draft.skipped} rows={2} onChange={event => onChange(question.id, { ...draft, text: event.target.value })} /></label> : null}
    </fieldset>;
  })}</>;
}
function RequestForm({ spec, rootId, wait, onStage, onResult }: { spec: ActionSpec; rootId: string; wait: PendingWait;
  onStage: (stage: 'idle' | 'checking' | 'submitting') => void; onResult: (result: SubmissionResult) => void }) {
  const request = spec.request!;
  const question = request.kind === 'agent.ask_user';
  const [drafts, setDrafts] = useState<Record<string, ReplyDraft>>({});
  const [decisions, setDecisions] = useState<Record<string, boolean>>({});
  const [reasons, setReasons] = useState<Record<string, string>>({});
  const [action, setAction] = useState<ReplyAction | null>(null), [error, setError] = useState<string | null>(null);
  const busy = action !== null;
  const operation = useRef<AbortController | null>(null), locked = useRef(false);
  useEffect(() => () => operation.current?.abort(), []);
  let ready = true;
  try { interactionReply(request, drafts, decisions, reasons, 'submit'); } catch { ready = false; }
  const submit = async (chosen: ReplyAction = 'submit') => {
    if (locked.current) return;
    let payload: unknown;
    try { payload = interactionReply(request, drafts, decisions, reasons, chosen); }
    catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); return; }
    locked.current = true; setAction(chosen); setError(null); onStage('checking');
    const controller = new AbortController(); operation.current = controller;
    try {
      const result = await submitInteraction(spec, payload, {
        current: () => agentClient.spec(rootId, 'respond', { target_invocation_id: wait.invocation_id, wait_id: wait.wait_id }, controller.signal),
        respond: value => performAction({ invocation_id: rootId, action: 'respond', target_invocation_id: wait.invocation_id, wait_id: wait.wait_id, payload: value }, controller.signal),
        onSubmit: () => onStage('submitting'),
      });
      onResult(result);
    } catch (reason) { if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { if (!controller.signal.aborted) { locked.current = false; setAction(null); onStage('idle'); } }
  };
  return <section className="agent-interaction" aria-busy={busy} aria-label={question ? 'Agent question' : 'Tool approval'}><div className="agent-interaction-heading"><strong>{question ? 'Your input is needed' : 'Permission requested'}</strong><span className="agent-pill">{wait.invocation_id === rootId ? 'Main Agent' : `Agent ${wait.invocation_id.slice(0, 8)}`}</span></div>
    {question ? <QuestionFields questions={request.questions as unknown as Question[]} drafts={drafts} onChange={(id, value) => setDrafts(current => ({ ...current, [id]: value }))} disabled={busy} /> : <>
      {(request.requests as unknown as ApprovalRequest[]).map(item => <div className="agent-approval-item" key={item.id}><div><strong>{item.call.name}</strong><p className="agent-muted">{item.reason}</p><AgentDisclosure className="agent-approval-arguments" label="Review arguments"><ToolArgumentsPreview arguments={JSON.stringify(item.arguments)} name={item.call.name} /></AgentDisclosure><AnimatedRegion id={`reason-${wait.wait_id}-${item.id}`} open={decisions[item.id] === false}><label className="agent-field">Reason if denied (optional)<textarea rows={2} maxLength={2000} disabled={busy} value={reasons[item.id] ?? ''} onChange={event => setReasons(current => ({ ...current, [item.id]: event.target.value }))} /></label></AnimatedRegion></div><div className="agent-approval-options">{[true, false].map(approved => <label key={String(approved)}><input type="radio" name={`approval-${wait.wait_id}-${item.id}`} disabled={busy} checked={decisions[item.id] === approved} onChange={() => setDecisions(current => ({ ...current, [item.id]: approved }))} />{approved ? 'Allow once' : 'Deny'}</label>)}</div></div>)}
    </>}
    {error ? <p role="alert" className="agent-error-text">{error}</p> : null}
    <div className="agent-interaction-actions">{!question && (request.requests as Json[]).length > 0 ? <><button type="button" disabled={busy} onClick={() => void submit('deny')}><BusyLabel busy={action === 'deny'} label="Denying">Deny all</BusyLabel></button><button type="button" className={!ready ? 'agent-primary' : undefined} disabled={busy} onClick={() => void submit('approve')}><BusyLabel busy={action === 'approve'} label="Approving">Approve all once</BusyLabel></button></> : null}<button type="button" className="agent-primary" disabled={busy || !ready} onClick={() => void submit()}><BusyLabel busy={action === 'submit'} label={question ? 'Sending answers' : 'Submitting'}>{question ? 'Send answers' : (request.requests as Json[]).length ? 'Submit decisions' : 'Continue'}</BusyLabel></button></div>
  </section>;
}
function PendingInteraction({ rootId, wait, active, onResolved }: { rootId: string; wait: PendingWait; active: boolean; onResolved: () => void }) {
  const client = useQueryClient();
  const key = ['agent-request', rootId, wait.invocation_id, wait.wait_id];
  const [held, setHeld] = useState<ActionSpec | null>(null), [stage, setStage] = useState<'idle' | 'checking' | 'submitting'>('idle');
  const [notice, setNotice] = useState(''), [settled, setSettled] = useState<'accepted' | 'resolved' | null>(null);
  const query = useQuery({ queryKey: key, queryFn: ({ signal }) => agentClient.spec(rootId, 'respond', { target_invocation_id: wait.invocation_id, wait_id: wait.wait_id }, signal), enabled: active && !held && !settled, refetchInterval: active && !held && !settled ? 3000 : false, retry: retryAgentRead, retryDelay: agentReadRetryDelay });
  const refresh = () => { void query.refetch(); onResolved(); };
  const shown = held ?? query.data, request = shown?.request;
  const human = !!shown && requiresHumanResponse(shown);
  const wasHuman = useRef(false);
  useEffect(() => { if (human) wasHuman.current = true; }, [human]);
  const unavailable = !held && requestUnavailable(query.error);
  const changing = !held && requestChanged(query.error);
  useEffect(() => { if (!held && (query.data && !query.data.request || unavailable)) onResolved(); }, [query.data, held, unavailable, onResolved]);
  if (settled || shown && !request || unavailable) return wasHuman.current ? <div className="agent-interaction-receipt" role="status"><AgentIcon name="check" /><span>{settled === 'accepted' ? 'Response submitted' : 'Request resolved'}</span></div> : null;
  if (shown && !human) return null;
  if (changing && !shown) return <p className="agent-interaction-notice" role="status">Permissions are updating.<button type="button" onClick={refresh}>Refresh request</button></p>;
  if (query.error && !shown) return <div className="agent-error-card" role="alert">Could not load this request: {query.error.message}<button onClick={refresh}>Retry</button></div>;
  if (!shown || !request) return query.isLoading && wait.node_id !== 'wait_approval' ? <CardsSkeleton label="Loading pending request" count={1} /> : null;
  if (request.kind !== 'agent.ask_user' && request.kind !== 'agent.tool_approval') return <div className="agent-error-card">This request uses a custom form. Open its execution to respond.</div>;
  const result = (value: SubmissionResult) => {
    if (value.state === 'changed') { client.setQueryData(key, value.spec); setNotice('Permissions or questions changed. Review the updated request.'); }
    else if (value.state === 'refresh') { setNotice('Permissions changed. Refreshing the request for review.'); void query.refetch(); }
    else setSettled(value.state);
    onResolved();
  };
  return <>{notice ? <p className="agent-interaction-notice" role="status">{notice}</p> : null}
    {changing ? <p className="agent-interaction-notice" role="status">Permissions are updating.<button type="button" onClick={refresh}>Refresh request</button></p> : query.error && !held ? <p className="agent-error-text" role="alert">Could not refresh this request: {query.error.message}<button onClick={refresh}>Retry</button></p> : null}
    <span className="sr-only" role="status">{stage === 'checking' ? 'Checking the current request…' : stage === 'submitting' ? 'Submitting your response…' : ''}</span>
    <RequestForm key={requestSignature(shown)} spec={shown} rootId={rootId} wait={wait} onStage={value => { setStage(value); setHeld(value === 'idle' ? null : shown); }} onResult={result} />
  </>;
}
export function Interactions({ rootId, waits, active, onResolved }: { rootId: string; waits: PendingWait[]; active: boolean; onResolved: () => void }) {
  const [present, setPresent] = useState(waits);
  const identities = waits.map(wait => JSON.stringify([wait.invocation_id, wait.wait_id])).join('\n');
  useEffect(() => {
    setPresent(current => [...waits, ...current.filter(old => !waits.some(wait => wait.wait_id === old.wait_id && wait.invocation_id === old.invocation_id))]);
    const timer = setTimeout(() => setPresent(waits), 200);
    return () => clearTimeout(timer);
  }, [identities]);
  if (!present.length && !waits.length) return null;
  return <div className="agent-interactions" aria-label="Pending requests">{present.map(wait => {
    const pending = waits.some(value => value.wait_id === wait.wait_id && value.invocation_id === wait.invocation_id);
    return <AnimatedRegion key={wait.invocation_id + ':' + wait.wait_id} id={`request-${wait.invocation_id}-${wait.wait_id}`} open={pending} className="agent-interaction-presence"><PendingInteraction rootId={rootId} wait={wait} active={active && pending} onResolved={onResolved} /></AnimatedRegion>;
  })}</div>;
}
