import type { ActionSpec, Json } from './types.js';
interface ApprovalRequest { id: string }
function canonical(value: Json): Json {
  if (Array.isArray(value)) return value.map(canonical);
  if (value !== null && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
  return value;
}
export function requestSignature(spec: ActionSpec) { return JSON.stringify(canonical(spec.request)); }
export function approvalReply(request: Record<string, Json>, decisions: Record<string, boolean>, reasons: Record<string, string> = {}) {
  const requests = request.requests as unknown as ApprovalRequest[];
  if (requests.some(item => typeof decisions[item.id] !== 'boolean')) throw new Error('Choose Allow or Deny for every tool.');
  return { decisions: requests.map(item => ({ request_id: item.id, approved: decisions[item.id], ...(!decisions[item.id] && reasons[item.id]?.trim() ? { reason: reasons[item.id].trim() } : {}) })), source: 'user', revision: request.revision ?? null, resolutions: [] };
}
