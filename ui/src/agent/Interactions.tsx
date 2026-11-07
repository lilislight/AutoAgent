import { CardsSkeleton, BusyLabel } from '../components/LoadingState';
import { useEffect, useId, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { agentClient, performAction } from './client';
import { approvalReply, requestSignature } from './replies';
import type { ActionSpec, Json, PendingWait } from './types';

interface Question { id: string; type: 'text' | 'single_choice' | 'multiple_choice'; text: string; allow_custom_answer?: boolean; options: { id: string; label: string; description?: string }[] }
interface ApprovalRequest { id: string; call: { name: string; call_id: string }; arguments: Json; reason: string | null }
interface ReplyDraft { text: string; selected: string[]; skipped: boolean }

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
function RequestForm({ spec, rootId, wait, onRefresh, onResolved }: { onResolved: () => void; spec: ActionSpec; rootId: string; wait: PendingWait; onRefresh: () => void }) {
  const request = spec.request!;
  const question = request.kind === 'agent.ask_user';
  const [drafts, setDrafts] = useState<Record<string, ReplyDraft>>({});
  const [decisions, setDecisions] = useState<Record<string, boolean>>({});
  const [busy, setBusy] = useState(false), [error, setError] = useState<string | null>(null);
  const signature = requestSignature(spec);
  const operation = useRef<AbortController | null>(null);
  useEffect(() => () => operation.current?.abort(), []);
  const submit = async (all?: boolean) => {
    if (busy) return;
    setBusy(true); setError(null);
    const controller = new AbortController();
    operation.current = controller;
    try {
      const fresh = await agentClient.spec(rootId, 'respond', { target_invocation_id: wait.invocation_id, wait_id: wait.wait_id }, controller.signal);
      if (requestSignature(fresh) !== signature) { onRefresh(); throw new Error('The request changed. Review the current batch before replying.'); }
      let payload: unknown;
      if (question) {
        const questions = request.questions as unknown as Question[];
        payload = { answers: questions.map(item => {
          const value = drafts[item.id] ?? { text: '', selected: [], skipped: false };
          if (!value.skipped && !value.text.trim() && !value.selected.length) throw new Error('Answer or skip each question.');
          return { question_id: item.id, status: value.skipped ? 'skipped' : 'answered', text: value.skipped ? null : value.text.trim() || null, selected_option_ids: value.skipped ? [] : value.selected };
        }) };
      } else {
        const values = all === undefined ? decisions : Object.fromEntries((request.requests as unknown as ApprovalRequest[]).map(item => [item.id, all]));
        payload = approvalReply(request, values);
      }
      await performAction({ invocation_id: rootId, action: 'respond', target_invocation_id: wait.invocation_id, wait_id: wait.wait_id, payload }, controller.signal);
      onResolved();
    } catch (reason) { if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  };
  return <section className="agent-interaction" aria-label={question ? 'Agent question' : 'Tool approval'}><div className="agent-interaction-heading"><strong>{question ? 'Your input is needed' : 'Permission requested'}</strong><span className="agent-pill">{wait.invocation_id === rootId ? 'Main Agent' : `Agent ${wait.invocation_id.slice(0, 8)}`}</span></div>
    {question ? <QuestionFields questions={request.questions as unknown as Question[]} drafts={drafts} onChange={(id, value) => setDrafts(current => ({ ...current, [id]: value }))} disabled={busy} /> : <>
      <p>{(request.requests as Json[]).length ? 'Review these tool calls. Your choices apply to this batch only.' : 'Policy has already resolved this batch. Continue to apply the current decisions.'}</p>{Array.isArray(request.automatic) && request.automatic.length ? <p className="agent-muted">{request.automatic.length} calls resolved by policy.</p> : null}
      {(request.requests as unknown as ApprovalRequest[]).map(item => <div className="agent-approval-item" key={item.id}><div><strong>{item.call.name}</strong><p className="agent-muted">{item.reason}</p><details><summary>Review arguments</summary><pre>{JSON.stringify(item.arguments, null, 2)}</pre></details></div><div className="agent-approval-options">{[true, false].map(approved => <label key={String(approved)}><input type="radio" name={`approval-${wait.wait_id}-${item.id}`} disabled={busy} checked={decisions[item.id] === approved} onChange={() => setDecisions(current => ({ ...current, [item.id]: approved }))} />{approved ? 'Allow once' : 'Deny'}</label>)}</div></div>)}
    </>}
    {error ? <p role="alert" className="agent-error-text">{error}</p> : null}
    <div className="agent-interaction-actions">{!question && (request.requests as Json[]).length > 0 ? <><button disabled={busy} onClick={() => void submit(false)}>Deny all</button><button disabled={busy} onClick={() => void submit(true)}>Approve all once</button></> : null}<button className="agent-primary" disabled={busy} onClick={() => void submit()}><BusyLabel busy={busy} label="Submitting">{question ? 'Send answers' : (request.requests as Json[]).length ? 'Submit decisions' : 'Continue'}</BusyLabel></button></div>
  </section>;
}
function PendingInteraction({ rootId, wait, active, onResolved }: { rootId: string; wait: PendingWait; active: boolean; onResolved: () => void }) {
  const query = useQuery({ queryKey: ['agent-request', rootId, wait.invocation_id, wait.wait_id], queryFn: ({ signal }) => agentClient.spec(rootId, 'respond', { target_invocation_id: wait.invocation_id, wait_id: wait.wait_id }, signal), enabled: active, refetchInterval: active ? 3000 : false, retry: false });
  const refresh = () => { void query.refetch(); onResolved(); };
  const request = query.data?.request;
  if (query.error && !query.data) return <div className="agent-error-card" role="alert">Could not load this request: {query.error.message}<button onClick={refresh}>Refresh</button></div>;
  if (!query.data) return query.isLoading ? <CardsSkeleton label="Loading pending request" count={1} /> : null;
  if (!request) return <div className="agent-error-card">This request is no longer pending.<button onClick={refresh}>Refresh</button></div>;
  if (request.kind !== 'agent.ask_user' && request.kind !== 'agent.tool_approval') return <div className="agent-error-card">This request uses a custom form. Open its execution to respond.</div>;
  return <>{query.error ? <div className="agent-error-card" role="alert">Could not refresh this request: {query.error.message}<button onClick={refresh}>Refresh</button></div> : null}<RequestForm key={requestSignature(query.data)} spec={query.data} rootId={rootId} wait={wait} onRefresh={refresh} onResolved={onResolved} /></>;
}
export function Interactions({ rootId, waits, active, onResolved }: { rootId: string; waits: PendingWait[]; active: boolean; onResolved: () => void }) {
  return <div className="agent-interactions" aria-label="Pending requests">{waits.map(wait => <PendingInteraction key={wait.wait_id} rootId={rootId} wait={wait} active={active} onResolved={onResolved} />)}</div>;
}
