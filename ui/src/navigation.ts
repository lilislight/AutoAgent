import type { LocationState } from "./types.js";

export function readLocation(search: string): LocationState {
  const params = new URLSearchParams(search);
  const node = params.get("node");
  const edge = params.get("edge");
  const execution = params.get("execution");
  const view = params.get("view") === "executions" ? "executions" : "workflows";
  const state: LocationState = {
    view, execution: view === "executions" ? execution : null,
    workflow: view === "workflows" ? params.get("workflow") : null,
    revision: view === "workflows" ? params.get("revision") : null,
    session: view === "executions" ? params.get("session") : null,
    selection: node ? { kind: "node", id: node } : edge ? { kind: "edge", id: edge } : view === "executions" && params.has("invocation") ? { kind: "invocation", id: params.get("invocation")! } : view === "workflows" && params.has("definition") ? { kind: "workflow", id: params.get("definition")! } : null,
  };
  if (view === 'executions') {
    const pane = params.get('pane');
    if (pane === 'graph' || pane === 'timeline' || pane === 'split') state.executionView = pane;
    if (state.selection && (params.has('record') || params.has('record_kind'))) {
      const kind = params.get('record_kind');
      const id = params.get('record');
      if (id && (kind === 'node' || kind === 'call' || kind === 'wait' || kind === 'edge') &&
          (kind === 'edge' ? state.selection.kind === 'edge' : state.selection.kind === 'node')) {
        const section = params.get('section');
        state.selection.record = { kind, id };
        if (section === 'Summary' || section === 'Input / Output' || section === 'Execution' || section === 'Evaluations' || section === 'Definition') state.selection.record.section = section;
      } else state.selection = null;
    }
  }
  return state;
}

export function writeLocation(state: LocationState): string {
  const params = new URLSearchParams();
  if (state.view === "executions") { params.set("view", "executions"); if (state.execution) params.set("execution", state.execution); }
  if (state.view === "workflows" && state.workflow) params.set("workflow", state.workflow);
  if (state.view === "workflows" && state.revision) params.set("revision", state.revision);
  if (state.view === "executions") {
    if (state.session) params.set("session", state.session);
    if (state.executionView) params.set("pane", state.executionView);
  }
  if (state.selection) params.set(state.selection.kind === "workflow" ? "definition" : state.selection.kind, state.selection.id);
  if (state.view === 'executions' && state.selection?.record) {
    params.set('record_kind', state.selection.record.kind);
    params.set('record', state.selection.record.id);
    if (state.selection.record.section) params.set('section', state.selection.record.section);
  }
  const query = params.toString();
  return query ? `?${query}` : "";
}
