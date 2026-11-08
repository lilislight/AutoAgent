const prefix = 'autoagent.agent-draft:';

export function readDraft(scope: string, session: string): string {
  try { return window.sessionStorage.getItem(prefix + JSON.stringify([scope, session])) ?? ''; }
  catch { return ''; }
}

export function saveDraft(scope: string, session: string, value: string): void {
  try {
    const key = prefix + JSON.stringify([scope, session]);
    if (value) window.sessionStorage.setItem(key, value);
    else window.sessionStorage.removeItem(key);
  } catch { /* Draft persistence is optional when browser storage is unavailable. */ }
}
