import type { InspectorTab, LocationState } from "./types.js";

function readSection(value: string | null): InspectorTab | undefined {
  return value === 'Summary' || value === 'Input / Output' || value === 'Execution' || value === 'Definition' || value === 'Actions' ? value : undefined;
}

export function readLocation(search: string): LocationState {
  const params = new URLSearchParams(search);
  const node = params.get("node");
  const edge = params.get("edge");
  const group = params.get('group');
  const execution = params.get("execution");
  const view = params.get("view") === "agents" ? "agents" : params.get("view") === "executions" ? "executions" : "workflows";
  const state: LocationState = {
    view, execution: view !== "workflows" ? execution : null,
    workflow: view === "workflows" ? params.get("workflow") : null,
    revision: view === "workflows" ? params.get("revision") : null,
    session: view !== "workflows" ? params.get("session") : null,
    selection: node ? { kind: "node", id: node } : edge ? { kind: "edge", id: edge } : group ? {kind:'group',id:group} : view === "executions" && params.has("invocation") ? { kind: "invocation", id: params.get("invocation")! } : view === "workflows" && params.has("definition") ? { kind: "workflow", id: params.get("definition")! } : null,
  };
  if (view === 'agents') state.selection = null;
  if (view === 'workflows' && params.get('compare')) state.compareBase = params.get('compare')!;
  if (view === 'executions') {
    const pane = params.get('pane');
    if (pane === 'graph' || pane === 'timeline' || pane === 'split') state.executionView = pane;
    if (state.selection && (params.has('record') || params.has('record_kind'))) {
      const kind = params.get('record_kind');
      const id = params.get('record');
      if (id && (kind === 'node' || kind === 'call' || kind === 'wait' || kind === 'edge') &&
          (kind === 'edge' ? state.selection.kind === 'edge' : state.selection.kind === 'node')) {
        const section = readSection(params.get('section'));
        state.selection.record = { kind, id };
        if (kind === 'node' && params.get('response_wait')) state.selection.record.waitId = params.get('response_wait')!;
        if (section) state.selection.record.section = section;
      } else state.selection = null;
    }
    if (state.selection && !state.selection.record) {
      const section = readSection(params.get('section'));
      if (section) state.selection.section = section;
    }
  }
  return state;
}

export function writeLocation(state: LocationState): string {
  const params = new URLSearchParams();
  if (state.view === "agents") {
    params.set('view', 'agents');
    if (state.session) params.set('session', state.session);
    if (state.execution) params.set('execution', state.execution);
    return `?${params.toString()}`;
  }
  if (state.view === "executions") { params.set("view", "executions"); if (state.execution) params.set("execution", state.execution); }
  if (state.view === "workflows" && state.workflow) params.set("workflow", state.workflow);
  if (state.view === "workflows" && state.revision) params.set("revision", state.revision);
  if (state.view === "workflows" && state.compareBase) params.set("compare", state.compareBase);
  if (state.view === "executions") {
    if (state.session) params.set("session", state.session);
    if (state.executionView) params.set("pane", state.executionView);
  }
  if (state.selection) params.set(state.selection.kind === "workflow" ? "definition" : state.selection.kind, state.selection.id);
  if (state.view === 'executions' && state.selection?.section && !state.selection.record) params.set('section', state.selection.section);
  if (state.view === 'executions' && state.selection?.record) {
    params.set('record_kind', state.selection.record.kind);
    params.set('record', state.selection.record.id);
    if (state.selection.record.section) params.set('section', state.selection.record.section);
    if (state.selection.record.kind === 'node' && state.selection.record.waitId) params.set('response_wait', state.selection.record.waitId);
  }
  const query = params.toString();
  return query ? `?${query}` : "";
}

/** Resolve page routes relative to the mounted UI, including proxy root paths. */
function pageBase(uiBase: string): string {
  const path = new URL(uiBase, 'http://autoagent.invalid').pathname;
  return path.endsWith('/') ? path : path + '/';
}

export function readPageLocation(pathname: string, search: string, uiBase = '/', defaultView: 'agents' | 'workflows' = 'workflows'): LocationState {
  const base = pageBase(uiBase);
  const path = pathname.startsWith(base) ? pathname.slice(base.length).replace(/\/$/, '') : '';
  if (path === 'agent' || path.startsWith('agent/c/')) {
    const params = new URLSearchParams(search);
    const encoded = path.startsWith('agent/c/') ? path.slice('agent/c/'.length) : '';
    let session: string | null = null;
    try { session = encoded ? decodeURIComponent(encoded) : null; } catch { /* Invalid links open a fresh conversation. */ }
    return { view: 'agents', session, execution: session ? params.get('invocation') : null, workflow: null, revision: null, selection: null,
      ...(params.get('workspace') ? { agentWorkspace: params.get('workspace')! } : {}) };
  }
  const explicitDeveloper = path === 'workflows' || path.startsWith('workflows/');
  const result = readLocation(search || (defaultView === 'agents' && !explicitDeveloper ? '?view=agents' : ''));
  if (result.view === 'agents' && !result.session) result.execution = null;
  return result;
}

export function writePageLocation(state: LocationState, uiBase = '/', defaultView: 'agents' | 'workflows' = 'workflows'): string {
  const base = pageBase(uiBase);
  if (state.view === 'agents') {
    const path = base + 'agent' + (state.session ? '/c/' + encodeURIComponent(state.session) : '');
    const params = new URLSearchParams();
    if (state.session && state.execution) params.set('invocation', state.execution);
    if (state.agentWorkspace) params.set('workspace', state.agentWorkspace);
    return path + (params.size ? '?' + params.toString() : '');
  }
  const query = writeLocation(state);
  // Preserve an explicit developer entry in an Agent-only project's default UI.
  return base + (query || (defaultView === 'agents' ? '?view=workflows' : ''));
}
