import type { ActionSpec, Interaction } from './types.js';

/** A durable Wait does not necessarily require a human decision. */
export function requiresHumanResponse(spec: ActionSpec): boolean {
  if (!spec.request) return false;
  if (spec.review) return spec.review.requires_user_input && spec.review.reviewer === 'user';
  return spec.request.kind !== 'agent.tool_approval';
}

interface ApprovalDecision { request_id: string; approved: boolean; reason?: string | null }
interface ApprovalResolution { request_id: string; source: string; approval?: ApprovalDecision | null; result?: { is_error?: boolean; content?: { text?: string }[] } | null }
interface ApprovalResponse { source?: string; decisions?: ApprovalDecision[]; resolutions?: ApprovalResolution[] }

/** Match the individual resolution, since one batch may have multiple reviewers. */
export function toolApprovalRecord(interaction: Interaction | undefined, callId: string | undefined): { label: string; reason?: string } | null {
  if (interaction?.type !== 'approval' || interaction.state !== 'resolved' || !callId) return null;
  const requests = interaction.request.requests as unknown as { id: string; call: { call_id: string } }[] | undefined;
  const original = requests?.find(item => item.call.call_id === callId);
  if (!original) return null;
  const response = interaction.resolution?.response as unknown as ApprovalResponse | null;
  const resolution = response?.resolutions?.find(item => item.request_id === original.id);
  const decision = resolution?.approval ?? response?.decisions?.find(item => item.request_id === original.id);
  const source = resolution?.source ?? response?.source;
  if (!source) return null;
  const approved = decision?.approved ?? !resolution?.result?.is_error;
  const label = source === 'system' ? approved ? 'Automatically approved' : 'Automatically denied'
    : source === 'agent' ? approved ? 'Approved by Agent' : 'Denied by Agent'
    : source === 'policy' ? approved ? 'Allowed by policy' : 'Blocked by policy'
    : approved ? 'Approved by you' : 'Denied by you';
  const reason = decision?.reason;
  return { label, ...(reason && reason !== 'Automatically approved by approval mode.' ? { reason } : {}) };
}
