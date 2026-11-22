import { ApiError, request } from './http.js';

/** Apply display filtering only to requests made by the Workflow browser. */
export function workflowRequest<T>(path: string, signal: AbortSignal, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  headers.set('X-AutoAgent-UI', 'workflow');
  return request<T>(path, signal, { ...init, headers });
}

export function isHiddenWorkflowError(error: unknown): boolean {
  return error instanceof ApiError && error.code === 'agent_app_hidden';
}
