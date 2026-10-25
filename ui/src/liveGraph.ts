/** Current Graph state from metadata upserts; no Event payloads or history copies. */
import type { ExecutionTrace, TimelineRow, TraceDataset, TraceEdgeUpdate } from './types.js';
const active = (row: TimelineRow) => ['ready','running','waiting'].includes(row.status ?? '');
const rank = (row: TimelineRow) => row.status === 'running' ? 0 : row.status === 'waiting' ? 1 : row.status === 'ready' ? 2 : 3;
const newer = (a: TimelineRow, b: TimelineRow) => (a.ordinal ?? 0) > (b.ordinal ?? 0) || a.ordinal === b.ordinal && a.id > b.id;
export class LiveGraph {
  private totals = new Map<string,number>();
  private callCounts = new Map<string,number>();
  private current = new Map<string,Map<string,TimelineRow>>();
  private terminal = new Map<string,TimelineRow>();
  private edges = new Map<string,ExecutionTrace['edges'][number]>();
  constructor(data: TraceDataset) {
    for (const row of data.node) this.node(row);
    for (const row of data.call) this.call(row);
    // The manifest also preserves counts for imported/incomplete histories.
    for (const row of data.manifest.trace.nodes) this.callCounts.set(row.occurrence_id, row.call_count ?? this.callCounts.get(row.occurrence_id) ?? 0);
    for (const row of data.manifest.trace.edges) this.edges.set(row.edge_id,{...row});
  }
  node(row: TimelineRow, previous?: TimelineRow) {
    const id=row.node_id!;
    if (!previous) this.totals.set(id,(this.totals.get(id) ?? 0)+1);
    this.current.get(id)?.delete(row.id);
    if (active(row)) {
      const rows=this.current.get(id) ?? new Map<string,TimelineRow>();
      rows.set(row.id,row);this.current.set(id,rows);
    } else {
      const old=this.terminal.get(id);
      if (!old || old.id === row.id || newer(row,old)) this.terminal.set(id,row);
    }
  }
  call(row: TimelineRow) {
    this.callCounts.set(row.occurrence_id,(this.callCounts.get(row.occurrence_id) ?? 0)+1);
  }
  updateEdges(rows: TraceEdgeUpdate[]) {
    for(const row of rows) {
      const old=this.edges.get(row.edge_id);
      this.edges.set(row.edge_id,{edge_id:row.edge_id,condition_result:row.condition_result ?? old?.condition_result ?? false,evaluations:(old?.evaluations ?? 0)+row.evaluations_delta});
    }
  }
  frame(header: ExecutionTrace): ExecutionTrace {
    const nodes:ExecutionTrace['nodes']=[],routes:ExecutionTrace['active_routes']=[];
    for(const [id,total] of this.totals) {
      let representative=this.terminal.get(id);
      for(const row of this.current.get(id)?.values() ?? []) {
        if(!representative || rank(row)<rank(representative) || rank(row)===rank(representative) && newer(row,representative))representative=row;
        for(const route of row.activations ?? [])routes.push({...route,target_node_id:id,target_occurrence_id:row.id,status:row.status!});
      }
      if(representative)nodes.push({node_id:id,status:representative.status!,occurrence_id:representative.id,occurrences:total,call_count:this.callCounts.get(representative.id) ?? 0});
    }
    return {...header,nodes,edges:[...this.edges.values()],active_routes:routes};
  }
}
