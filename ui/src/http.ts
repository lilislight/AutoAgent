interface Config { apiPrefix: string; uiBase: string; pageSize: number }
const element = document.getElementById("autoagent-config");
export const config: Config = element?.textContent
  ? JSON.parse(element.textContent) as Config
  : { apiPrefix: "/api/v2", uiBase: "/", pageSize: 50 };

export class ApiError extends Error {
  constructor(message: string, readonly status: number) { super(message); }
}
export async function request<T>(path: string, signal: AbortSignal, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  headers.set('Accept', 'application/json');
  const response = await fetch(config.apiPrefix + path, {
    ...init,
    signal: AbortSignal.any([signal, AbortSignal.timeout(20_000)]),
    headers,
  });
  if (!response.ok) {
    const body = await response.json().catch(() => null) as { detail?: unknown } | null;
    const detail = body?.detail;
    const message = typeof detail === "string" ? detail
      : detail && typeof detail === "object" && "message" in detail ? String(detail.message)
      : `Request failed (${response.status})`;
    throw new ApiError(message, response.status);
  }
  return await response.json() as T;
}

