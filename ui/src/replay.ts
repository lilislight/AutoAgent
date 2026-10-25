import type { ExecutionTrace, ReplayWindow, ReplayOccurrence, TimelineMarker } from './types.js';

const ACTIVE = new Set(['ready', 'running', 'waiting']);
const rank = (status: string) => status === 'running' ? 0 : status === 'waiting' ? 1 : status === 'ready' ? 2 : 3;
export function replayMarker(window: ReplayWindow, position: { sequence?: number; at_us?: number }): TimelineMarker | null {
  const eligible = window.events.filter(event => position.sequence !== undefined ? event.sequence <= position.sequence : position.at_us !== undefined ? event.occurred_at_us <= position.at_us : event.sequence <= (window.boundary?.sequence ?? 0));
  return eligible.at(-1) ?? (window.previous ? null : window.events[0] ?? null);
}
export function replayFrame(window: ReplayWindow, sequence: number): ExecutionTrace | null {
  if (!window.baseline || !window.history_complete) return null;
  const state=new ReplayAccumulator(window);
  for(const event of window.events){if(event.sequence>sequence)break;state.apply(event);}
  return state.frame();
}

export class ReplayAccumulator {
  base: ExecutionTrace;
  // Shared immutable identity metadata lets local Replay checkpoints retain only
  // active occurrences and the latest terminal occurrence per logical Node.
  private nodeOrder?: ReadonlyMap<string, number>;
  private identities?: ReadonlyMap<string, { node_id: string; created_sequence: number }>;
  private callHistory?: ReadonlyMap<string, { sequence: number; count: number }[]>;
  private terminal = new Map<string, ReplayOccurrence>();
  private routeOrder?: ReadonlyMap<string, number>;
  private routeHistory?: ReadonlyMap<string, { sequence: number; route: ExecutionTrace['active_routes'][number] }[]>;
  occurrences: Map<string,ReplayOccurrence>;
  totals: Map<string,number>;
  callCounts: Map<string,number>;
  edges: Map<string,ExecutionTrace['edges'][number]>;
  routes: Map<string,ExecutionTrace['active_routes'][number]>;
  children: Set<string>;
  childCount: number;
  invocation: ExecutionTrace['invocation'];
  boundary: TimelineMarker | null;
  constructor(window: ReplayWindow) {
    this.base = window.baseline!.trace;
    const base = this.base;
    this.occurrences = new Map<string, ReplayOccurrence>(window.baseline!.occurrences.map(row => [row.occurrence_id, { ...row }]));
    this.totals = new Map(base.nodes.map(row => [row.node_id, row.occurrences]));
    const representedCalls = new Map(base.nodes.map(row=>[row.occurrence_id,row.call_count??0]));
    this.callCounts = new Map(window.baseline!.occurrences.map(row => [row.occurrence_id, row.call_count ?? representedCalls.get(row.occurrence_id) ?? 0]));
    this.edges = new Map(base.edges.map(row => [row.edge_id, { ...row }]));
    this.routes = new Map(base.active_routes.map(row => [`${row.edge_id}:${row.target_occurrence_id}`, { ...row }]));
    this.children = new Set(window.baseline!.child_ids);
    this.childCount = base.child_count;
    this.invocation = { ...base.invocation };
    this.boundary = base.last_event;
  }
  compact(events: ReplayWindow['events']): ReplayAccumulator {
    const copy = this.clone();
    const identities = new Map<string, { node_id: string; created_sequence: number }>();
    for (const row of copy.occurrences.values()) identities.set(row.occurrence_id, row);
    for (const event of events) for (const delta of event.changes) {
      const node = delta.node_id ?? delta.change.node_id;
      if (delta.entity_type === 'node' && node && !identities.has(delta.entity_id)) identities.set(delta.entity_id, { node_id: node, created_sequence: event.sequence });
    }
    copy.identities = identities;
    const callHistory = new Map<string, { sequence: number; count: number }[]>();
    for (const [id, count] of copy.callCounts) callHistory.set(id, [{ sequence: copy.boundary?.sequence ?? 0, count }]);
    for (const event of events) for (const start of event.call_starts) {
      const rows = callHistory.get(start.occurrence_id) ?? [];
      rows.push({ sequence: event.sequence, count: (rows.at(-1)?.count ?? 0) + start.count });
      callHistory.set(start.occurrence_id, rows);
    }
    copy.callHistory = callHistory;
    copy.callCounts.clear();
    const nodes = new Map<string, number>();
    for (const row of identities.values()) if (!nodes.has(row.node_id)) nodes.set(row.node_id, nodes.size);
    copy.nodeOrder = nodes;
    const history = new Map<string, { sequence: number; route: ExecutionTrace['active_routes'][number] }[]>();
    const order = new Map<string, number>();
    const addRoute = (sequence: number, route: ExecutionTrace['active_routes'][number]) => {
      const key = `${route.edge_id}:${route.target_occurrence_id}`;
      if (!order.has(key)) order.set(key, order.size);
      const rows = history.get(route.target_occurrence_id) ?? [];
      rows.push({ sequence, route }); history.set(route.target_occurrence_id, rows);
    };
    for (const route of copy.routes.values()) addRoute(copy.boundary?.sequence ?? 0, route);
    for (const event of events) for (const route of event.routes) addRoute(event.sequence, { ...route, status: 'ready' });
    copy.routeHistory = history; copy.routeOrder = order;
    for (const row of copy.occurrences.values()) if (!ACTIVE.has(row.status)) { copy.retainTerminal(row); copy.occurrences.delete(row.occurrence_id); }
    return copy;
  }
  private retainTerminal(row: ReplayOccurrence) {
    const old = this.terminal.get(row.node_id);
    if (!old || old.occurrence_id === row.occurrence_id || row.created_sequence > old.created_sequence || row.created_sequence === old.created_sequence && row.occurrence_id > old.occurrence_id) this.terminal.set(row.node_id, row);
  }
  clone(): ReplayAccumulator {
    const copy=Object.create(ReplayAccumulator.prototype) as ReplayAccumulator;
    Object.assign(copy,this);
    copy.occurrences=new Map(this.occurrences);
    copy.terminal=new Map(this.terminal);
    copy.totals=new Map(this.totals);copy.callCounts=new Map(this.callCounts);
    copy.edges=new Map(this.edges);
    copy.routes=new Map(this.routes);
    copy.children=new Set(this.children);copy.invocation={...this.invocation};
    return copy;
  }
  apply(event: ReplayWindow['events'][number]) {
    this.boundary = event;
    if (event.initial_edges) this.edges = new Map(event.initial_edges.map(row=>[row.edge_id,{...row}]));
    if (!this.callHistory) for (const started of event.call_starts) this.callCounts.set(started.occurrence_id, (this.callCounts.get(started.occurrence_id) ?? 0) + started.count);
    for (const evaluation of event.evaluations) {
      const previous = this.edges.get(evaluation.edge_id);
      this.edges.set(evaluation.edge_id, { edge_id: evaluation.edge_id, condition_result: evaluation.condition_result, evaluations: (previous?.evaluations ?? 0) + 1 });
    }
    let created: Set<string> | undefined;
    for (const delta of event.changes) {
      const value = delta.change;
      if (delta.entity_type === 'invocation') {
        if (value.status) { this.invocation.status = value.status; this.invocation.completed_at_us = ['completed','failed','cancelled'].includes(value.status) ? event.occurred_at_us : null; }
        if (value.control_state) this.invocation.control_state = value.control_state;
      } else if (delta.entity_type === 'node') {
        const identity = this.identities?.get(delta.entity_id);
        const old = this.occurrences.get(delta.entity_id);
        const node = delta.node_id ?? value.node_id ?? old?.node_id ?? identity?.node_id;
        if (!node) continue;
        const previous = old ?? (this.terminal.get(node)?.occurrence_id === delta.entity_id ? this.terminal.get(node) : undefined);
        if (!previous && !created?.has(delta.entity_id) && (!identity || identity.created_sequence === event.sequence)) { this.totals.set(node, (this.totals.get(node) ?? 0) + 1); (created ??= new Set()).add(delta.entity_id); }
        if (identity && !previous && identity.created_sequence < event.sequence && !value.status) continue;
        const row = { occurrence_id: delta.entity_id, node_id: node, status: value.status ?? previous?.status ?? 'ready', created_sequence: identity?.created_sequence ?? previous?.created_sequence ?? event.sequence };
        if (this.identities && !ACTIVE.has(row.status)) { this.retainTerminal(row); this.occurrences.delete(delta.entity_id); }
        else this.occurrences.set(delta.entity_id, row);
      } else if (value.edge_id && typeof value.selected === 'boolean') {
        this.edges.set(value.edge_id, { ...this.edges.get(value.edge_id), edge_id: value.edge_id, condition_result: value.selected, evaluations: this.edges.get(value.edge_id)?.evaluations ?? 0 });
      }
    }
    if (!this.routeHistory) for (const route of event.routes) this.routes.set(`${route.edge_id}:${route.target_occurrence_id}`, { ...route, status: 'ready' });
    for (const id of event.child_ids) if (!this.children.has(id)) { this.children.add(id); this.childCount++; }
  }
  frame(): ExecutionTrace {
  const representatives = new Map<string, ReplayOccurrence>();
  for (const row of [...this.terminal.values(), ...this.occurrences.values()]) {
    const old = representatives.get(row.node_id);
    if (!old || rank(row.status) < rank(old.status) || rank(row.status) === rank(old.status) && (row.created_sequence > old.created_sequence || row.created_sequence === old.created_sequence && row.occurrence_id > old.occurrence_id)) representatives.set(row.node_id, row);
  }
  const activeRoutes: ExecutionTrace['active_routes'] = [];
  if (this.routeHistory) {
    for (const row of this.occurrences.values()) if (ACTIVE.has(row.status)) {
      const routes = new Map<string, ExecutionTrace['active_routes'][number]>();
      for (const item of this.routeHistory.get(row.occurrence_id) ?? []) if (item.sequence <= (this.boundary?.sequence ?? 0)) routes.set(item.route.edge_id, { ...item.route, status: row.status });
      activeRoutes.push(...routes.values());
    }
    // Preserve route insertion order, including when an older occurrence resumes.
    activeRoutes.sort((a,b) => this.routeOrder!.get(`${a.edge_id}:${a.target_occurrence_id}`)! - this.routeOrder!.get(`${b.edge_id}:${b.target_occurrence_id}`)!);
  } else for (const route of this.routes.values()) {
    const status = this.occurrences.get(route.target_occurrence_id)?.status;
    if (status && ACTIVE.has(status)) activeRoutes.push({ ...route, status });
  }
  this.invocation.last_sequence = this.boundary?.sequence ?? 0;
  return { ...this.base, invocation:{...this.invocation}, nodes: [...representatives.values()].sort((a,b) => this.nodeOrder ? this.nodeOrder.get(a.node_id)! - this.nodeOrder.get(b.node_id)! : 0).map(row => ({ node_id: row.node_id, status: row.status, occurrence_id: row.occurrence_id, occurrences: this.totals.get(row.node_id) ?? 0, call_count: this.callsAt(row.occurrence_id) })), edges: [...this.edges.values()], active_routes: activeRoutes, child_count: this.childCount, last_event: this.boundary, observed_at_us: this.boundary?.occurred_at_us ?? this.base.observed_at_us };
  }
  private callsAt(id: string): number {
    if (!this.callHistory) return this.callCounts.get(id) ?? 0;
    const rows = this.callHistory.get(id) ?? [], sequence = this.boundary?.sequence ?? 0;
    let lo = 0, hi = rows.length;
    while (lo < hi) { const mid = (lo + hi) >>> 1; if (rows[mid].sequence <= sequence) lo = mid + 1; else hi = mid; }
    return rows[lo - 1]?.count ?? 0;
  }
}
