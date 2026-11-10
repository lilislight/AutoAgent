import { approvalReply, requestSignature } from './replies.js';
import type { ActionSpec, Json } from './types.js';

export interface Question { id: string; type: 'text' | 'single_choice' | 'multiple_choice'; text: string; allow_custom_answer?: boolean; options: { id: string; label: string; description?: string }[] }
export interface ReplyDraft { text: string; selected: string[]; skipped: boolean }
export type ReplyAction = 'approve' | 'deny' | 'submit';

/** Validate locally before looking up or submitting a durable request. */
export function interactionReply(request: Record<string, Json>, drafts: Record<string, ReplyDraft>, decisions: Record<string, boolean>, reasons: Record<string, string>, action: ReplyAction): unknown {
  if (request.kind === 'agent.ask_user') return { answers: (request.questions as unknown as Question[]).map(item => {
    const value = drafts[item.id] ?? { text: '', selected: [], skipped: false };
    if (!value.skipped && !value.text.trim() && !value.selected.length) throw new Error('Answer or skip each question.');
    return { question_id: item.id, status: value.skipped ? 'skipped' : 'answered', text: value.skipped ? null : value.text.trim() || null, selected_option_ids: value.skipped ? [] : value.selected };
  }) };
  const values = action === 'submit' ? decisions : Object.fromEntries((request.requests as unknown as { id: string }[]).map(item => [item.id, action === 'approve']));
  return approvalReply(request, values, reasons);
}

export type SubmissionResult = { state: 'accepted' | 'resolved' | 'refresh' } | { state: 'changed'; spec: ActionSpec };
export const requestUnavailable = (error: unknown) => !!error && typeof error === 'object' && 'code' in error && error.code === 'wait_unavailable';
export const requestChanged = (error: unknown) => !!error && typeof error === 'object' && 'code' in error && error.code === 'approval_changed';
export async function submitInteraction(spec: ActionSpec, payload: unknown, port: {
  current: () => Promise<ActionSpec>; respond: (payload: unknown) => Promise<unknown>; onSubmit: () => void;
}): Promise<SubmissionResult> {
  const current = async () => {
    try { return await port.current(); }
    catch (error) { if (requestUnavailable(error)) return null; if (requestChanged(error)) return 'refresh' as const; throw error; }
  };
  const fresh = await current();
  if (!fresh) return { state: 'resolved' };
  if (fresh === 'refresh') return { state: 'refresh' };
  if (!fresh.request) return { state: 'resolved' };
  if (requestSignature(fresh) !== requestSignature(spec)) return { state: 'changed', spec: fresh };
  port.onSubmit();
  try { await port.respond(payload); }
  catch (error) {
    if (requestUnavailable(error)) return { state: 'resolved' };
    if (!(error && typeof error === 'object' && ('status' in error && error.status === 409 || requestChanged(error)))) throw error;
    const latest = await current();
    if (latest === 'refresh') return { state: 'refresh' };
    if (!latest?.request) return { state: 'resolved' };
    if (requestSignature(latest) !== requestSignature(spec)) return { state: 'changed', spec: latest };
    throw error;
  }
  return { state: 'accepted' };
}
