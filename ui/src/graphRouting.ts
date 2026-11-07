// Orthogonal routing and shared-lane avoidance for Workflow graphs.
export const GRAPH_NODE_WIDTH = 232;
export const GRAPH_NODE_HEIGHT = 106;
export const GRAPH_ROUTE_CLEARANCE = 18;

export interface GraphPoint {
  x: number;
  y: number;
}

export interface GraphEdgeRoute {
  points: GraphPoint[];
  label: GraphPoint;
}

export interface RoutableNode {
  id: string;
}

export interface RoutableEdge {
  id: string;
  source: string;
  target: string;
  sourceOffset?:number;
  targetOffset?:number;
}

export interface GraphRect {
  left: number;
  top: number;
  right: number;
  bottom: number;
}

type Direction = "horizontal" | "vertical" | "start";

interface SearchState {
  point: number;
  direction: Direction;
  cost: number;
  estimate: number;
  previous: string | null;
}

export function sourcePort(position: GraphPoint,offset=GRAPH_NODE_HEIGHT/2): GraphPoint {
  return {
    x: position.x + GRAPH_NODE_WIDTH,
    y: position.y + offset,
  };
}

export function targetPort(position: GraphPoint,offset=GRAPH_NODE_HEIGHT/2): GraphPoint {
  return {
    x: position.x,
    y: position.y + offset,
  };
}

export function routeEdgesAroundNodes(
  nodes: RoutableNode[],
  edges: RoutableEdge[],
  positions: Record<string, GraphPoint>,
  edgeIds: Iterable<string>,
): Record<string, GraphEdgeRoute> {
  const requested = new Set(edgeIds);
  const obstacles = nodes.flatMap(node => positions[node.id] ? [expandedNodeRect(positions[node.id])] : []);
  const pairs = edges.flatMap(edge => {
    const source=positions[edge.source],target=positions[edge.target];
    return source && target ? [{id:edge.id,source:sourcePort(source,edge.sourceOffset),target:targetPort(target,edge.targetOffset),obstacles}] : [];
  });
  const routes=routePortPairs(pairs);
  return Object.fromEntries(Object.entries(routes).filter(([id])=>requested.has(id)).map(([id,points])=>[id,{points,label:routeMidpoint(points)}]));
}

export function routeMidpoint(points: GraphPoint[]): GraphPoint {
  const segments = points.slice(1).map((point, index) => {
    const previous = points[index];
    return {
      from: previous,
      to: point,
      length: manhattanDistance(previous, point),
    };
  });
  const total = segments.reduce((sum, segment) => sum + segment.length, 0);
  let remaining = total / 2;
  for (const segment of segments) {
    if (remaining <= segment.length) {
      const ratio = segment.length === 0 ? 0 : remaining / segment.length;
      return {
        x: segment.from.x + (segment.to.x - segment.from.x) * ratio,
        y: segment.from.y + (segment.to.y - segment.from.y) * ratio,
      };
    }
    remaining -= segment.length;
  }
  return points.at(-1) ?? { x: 0, y: 0 };
}

/** Wide hit areas are useful except where they would cover a neighbouring Edge. */
export function routeInteractionWidths(routes:Record<string,GraphPoint[]>):Record<string,number> {
  const widths:Record<string,number>=Object.fromEntries(Object.keys(routes).map(id=>[id,48]));
  const segments=Object.entries(routes).flatMap(([id,points])=>points.slice(1).map((b,i)=>({id,a:points[i],b})));
  for(let i=0;i<segments.length;i++)for(let j=0;j<i;j++){
    const a=segments[i],b=segments[j];if(a.id===b.id)continue;
    let gap=Infinity,shared=0;
    if(a.a.x===a.b.x && b.a.x===b.b.x){
      gap=Math.abs(a.a.x-b.a.x);shared=Math.min(Math.max(a.a.y,a.b.y),Math.max(b.a.y,b.b.y))-Math.max(Math.min(a.a.y,a.b.y),Math.min(b.a.y,b.b.y));
    }else if(a.a.y===a.b.y && b.a.y===b.b.y){
      gap=Math.abs(a.a.y-b.a.y);shared=Math.min(Math.max(a.a.x,a.b.x),Math.max(b.a.x,b.b.x))-Math.max(Math.min(a.a.x,a.b.x),Math.min(b.a.x,b.b.x));
    }
    if(gap>0 && gap<48 && shared>24){
      const width=Math.max(8,gap*.8);
      widths[a.id]=Math.min(widths[a.id],width);widths[b.id]=Math.min(widths[b.id],width);
    }
  }
  return widths;
}

export function roundedRoutePath(points: GraphPoint[], radius = 12): string {
  if (!points.length) return "";
  const commands = [`M ${points[0].x} ${points[0].y}`];
  for (let index = 1; index < points.length - 1; index++) {
    const before = points[index - 1];
    const corner = points[index];
    const after = points[index + 1];
    const incoming = Math.hypot(corner.x - before.x, corner.y - before.y);
    const outgoing = Math.hypot(after.x - corner.x, after.y - corner.y);
    if (!incoming || !outgoing) continue;
    const bend = Math.min(radius, incoming / 2, outgoing / 2);
    const start = { x: corner.x - (corner.x - before.x) * bend / incoming,
      y: corner.y - (corner.y - before.y) * bend / incoming };
    const end = { x: corner.x + (after.x - corner.x) * bend / outgoing,
      y: corner.y + (after.y - corner.y) * bend / outgoing };
    commands.push(`L ${start.x} ${start.y} Q ${corner.x} ${corner.y} ${end.x} ${end.y}`);
  }
  const last = points.at(-1)!;
  commands.push(`L ${last.x} ${last.y}`);
  return commands.join(" ");
}

export function segmentIntersectsRect(
  from: GraphPoint,
  to: GraphPoint,
  rect: GraphRect,
): boolean {
  if (from.x === to.x) {
    if (from.x <= rect.left || from.x >= rect.right) return false;
    const low = Math.min(from.y, to.y);
    const high = Math.max(from.y, to.y);
    return high > rect.top && low < rect.bottom;
  }
  if (from.y === to.y) {
    if (from.y <= rect.top || from.y >= rect.bottom) return false;
    const low = Math.min(from.x, to.x);
    const high = Math.max(from.x, to.x);
    return high > rect.left && low < rect.right;
  }
  return true;
}

export interface PortPair {
  id:string; source:GraphPoint; target:GraphPoint; obstacles:GraphRect[]; bounds?:GraphRect;
}
interface VisibilityGrid { points:GraphPoint[]; indexes:Map<string,number>; neighbors:number[][] }

/** Route one batch together: existing Edges have a cost, rather than being invisible. */
export function routePortPairs(pairs:PortPair[]):Record<string,GraphPoint[]> {
  const contexts=new Map<string,{obstacles:GraphRect[];bounds?:GraphRect;terminals:GraphPoint[];grid?:VisibilityGrid}>();
  const entries=pairs.map(pair=>{
    const start={x:pair.source.x+GRAPH_ROUTE_CLEARANCE,y:pair.source.y};
    const end={x:pair.target.x-GRAPH_ROUTE_CLEARANCE,y:pair.target.y};
    const key=JSON.stringify([pair.bounds,pair.obstacles]);
    let context=contexts.get(key);
    if(!context){context={obstacles:pair.obstacles,bounds:pair.bounds,terminals:[]};contexts.set(key,context);}
    context.terminals.push(start,end);
    return {pair,start,end,context};
  });
  const reservations=new RouteReservations(),routes:Record<string,GraphPoint[]>={};
  // Keep short forward connections direct; later return paths can take outer lanes.
  entries.sort((a,b)=>Number(a.end.x<a.start.x)-Number(b.end.x<b.start.x) ||
    manhattanDistance(a.start,a.end)-manhattanDistance(b.start,b.end));
  for(const {pair,start,end,context} of entries){
    context.grid??=visibilityGrid(context.terminals,context.obstacles,context.bounds);
    const {points,indexes,neighbors}=context.grid;
    const from=indexes.get(pointKey(start)),to=indexes.get(pointKey(end));
    const path=from!==undefined && to!==undefined?searchOrthogonalPath(points,neighbors,from,to,reservations):null;
    const routed=path??boundedFallback(start,end,pair.obstacles,pair.bounds);
    const result=simplifyOrthogonalPoints([pair.source,...routed,pair.target]);
    reservations.add(result);routes[pair.id]=result;
  }
  return routes;
}

function visibilityGrid(terminals:GraphPoint[],obstacles:GraphRect[],bounds?:GraphRect):VisibilityGrid {
  const lane=14;
  const xValues=uniqueSorted([...terminals.map(p=>p.x),...obstacles.flatMap(r=>[r.left-lane,r.left,r.right,r.right+lane]),...(bounds?[bounds.left,bounds.right]:[])]);
  const yValues=uniqueSorted([...terminals.map(p=>p.y),...obstacles.flatMap(r=>[r.top-lane,r.top,r.bottom,r.bottom+lane]),...(bounds?[bounds.top,bounds.bottom]:[])]);
  const points:GraphPoint[]=[],indexes=new Map<string,number>();
  for(const x of xValues)for(const y of yValues){
    const point={x,y};
    if(bounds && (x<bounds.left || x>bounds.right || y<bounds.top || y>bounds.bottom))continue;
    if(pointInsideAnyObstacle(point,obstacles))continue;
    indexes.set(pointKey(point),points.length);points.push(point);
  }
  return {points,indexes,neighbors:buildVisibilityNeighbors(points,obstacles)};
}

class RouteReservations {
  private horizontal=new Map<number,[number,number][]>();
  private vertical=new Map<number,[number,number][]>();
  add(points:GraphPoint[]){
    for(let i=1;i<points.length;i++){
      const a=points[i-1],b=points[i],horizontal=a.y===b.y;
      const map=horizontal?this.horizontal:this.vertical,key=horizontal?a.y:a.x;
      const low=horizontal?Math.min(a.x,b.x):Math.min(a.y,b.y),high=horizontal?Math.max(a.x,b.x):Math.max(a.y,b.y);
      const ranges=map.get(key)??[];ranges.push([low,high]);map.set(key,ranges);
    }
  }
  cost(a:GraphPoint,b:GraphPoint){
    const horizontal=a.y===b.y,map=horizontal?this.horizontal:this.vertical;
    const low=horizontal?Math.min(a.x,b.x):Math.min(a.y,b.y),high=horizontal?Math.max(a.x,b.x):Math.max(a.y,b.y);
    let shared=0;
    for(const [from,to] of map.get(horizontal?a.y:a.x)??[])shared+=Math.max(0,Math.min(high,to)-Math.max(low,from));
    return shared*12;
  }
}

function boundedFallback(start:GraphPoint,end:GraphPoint,obstacles:GraphRect[],bounds?:GraphRect):GraphPoint[]{
  const points=fallbackOuterRoute(start,end,obstacles);
  return bounds?points.map(p=>({x:Math.max(bounds.left,Math.min(bounds.right,p.x)),y:Math.max(bounds.top,Math.min(bounds.bottom,p.y))})):points;
}

function buildVisibilityNeighbors(
  points: GraphPoint[],
  obstacles: GraphRect[],
): number[][] {
  const neighbors = Array.from({ length: points.length }, () => [] as number[]);
  const rows = new Map<number, number[]>();
  const columns = new Map<number, number[]>();
  points.forEach((point, index) => {
    rows.set(point.y, [...(rows.get(point.y) ?? []), index]);
    columns.set(point.x, [...(columns.get(point.x) ?? []), index]);
  });
  for (const indexes of rows.values()) {
    indexes.sort((left, right) => points[left].x - points[right].x);
    connectVisibleNeighbors(indexes, points, obstacles, neighbors);
  }
  for (const indexes of columns.values()) {
    indexes.sort((left, right) => points[left].y - points[right].y);
    connectVisibleNeighbors(indexes, points, obstacles, neighbors);
  }
  return neighbors;
}

function connectVisibleNeighbors(
  indexes: number[],
  points: GraphPoint[],
  obstacles: GraphRect[],
  neighbors: number[][],
): void {
  for (let index = 1; index < indexes.length; index += 1) {
    const previous = indexes[index - 1];
    const current = indexes[index];
    if (
      obstacles.some((rect) =>
        segmentIntersectsRect(points[previous], points[current], rect),
      )
    ) {
      continue;
    }
    neighbors[previous].push(current);
    neighbors[current].push(previous);
  }
}

function searchOrthogonalPath(
  points: GraphPoint[],
  neighbors: number[][],
  start: number,
  end: number,
  reservations:RouteReservations,
): GraphPoint[] | null {
  const queue = new MinHeap<SearchState>((state) => state.estimate);
  const best = new Map<string, number>();
  const states = new Map<string, SearchState>();
  const initial: SearchState = {
    point: start,
    direction: "start",
    cost: 0,
    estimate: manhattanDistance(points[start], points[end]),
    previous: null,
  };
  queue.push(initial);
  best.set(stateKey(start, "start"), 0);
  states.set(stateKey(start, "start"), initial);
  let finalKey: string | null = null;
  while (queue.size > 0) {
    const current = queue.pop()!;
    const currentKey = stateKey(current.point, current.direction);
    if (current.cost !== best.get(currentKey)) continue;
    if (current.point === end) {
      finalKey = currentKey;
      break;
    }
    for (const neighbor of neighbors[current.point]) {
      const direction: Direction =
        points[current.point].x === points[neighbor].x
          ? "vertical"
          : "horizontal";
      const bendCost =
        current.direction === "start" || current.direction === direction
          ? 0
          : 24;
      const cost =
        current.cost +
        manhattanDistance(points[current.point], points[neighbor]) +
        bendCost + reservations.cost(points[current.point],points[neighbor]);
      const nextKey = stateKey(neighbor, direction);
      if (cost >= (best.get(nextKey) ?? Number.POSITIVE_INFINITY)) continue;
      const next: SearchState = {
        point: neighbor,
        direction,
        cost,
        estimate: cost + manhattanDistance(points[neighbor], points[end]),
        previous: currentKey,
      };
      best.set(nextKey, cost);
      states.set(nextKey, next);
      queue.push(next);
    }
  }
  if (!finalKey) return null;
  const path: GraphPoint[] = [];
  let cursor: string | null = finalKey;
  while (cursor) {
    const state = states.get(cursor);
    if (!state) break;
    path.push(points[state.point]);
    cursor = state.previous;
  }
  return path.reverse();
}

function fallbackOuterRoute(
  start: GraphPoint,
  end: GraphPoint,
  obstacles: GraphRect[],
): GraphPoint[] {
  const top =
    Math.min(start.y, end.y, ...obstacles.map((rect) => rect.top)) -
    GRAPH_ROUTE_CLEARANCE;
  const bottom =
    Math.max(start.y, end.y, ...obstacles.map((rect) => rect.bottom)) +
    GRAPH_ROUTE_CLEARANCE;
  const topDistance = Math.abs(start.y - top) + Math.abs(end.y - top);
  const bottomDistance =
    Math.abs(start.y - bottom) + Math.abs(end.y - bottom);
  const y = topDistance <= bottomDistance ? top : bottom;
  return simplifyOrthogonalPoints([
    start,
    { x: start.x, y },
    { x: end.x, y },
    end,
  ]);
}

function expandedNodeRect(position: GraphPoint): GraphRect {
  return {
    left: position.x - GRAPH_ROUTE_CLEARANCE,
    top: position.y - GRAPH_ROUTE_CLEARANCE,
    right: position.x + GRAPH_NODE_WIDTH + GRAPH_ROUTE_CLEARANCE,
    bottom: position.y + GRAPH_NODE_HEIGHT + GRAPH_ROUTE_CLEARANCE,
  };
}

function pointInsideAnyObstacle(
  point: GraphPoint,
  obstacles: GraphRect[],
): boolean {
  return obstacles.some(
    (rect) =>
      point.x > rect.left &&
      point.x < rect.right &&
      point.y > rect.top &&
      point.y < rect.bottom,
  );
}

function simplifyOrthogonalPoints(points: GraphPoint[]): GraphPoint[] {
  const deduplicated = points.filter(
    (point, index) =>
      index === 0 ||
      point.x !== points[index - 1].x ||
      point.y !== points[index - 1].y,
  );
  return deduplicated.filter((point, index) => {
    if (index === 0 || index === deduplicated.length - 1) return true;
    const previous = deduplicated[index - 1];
    const next = deduplicated[index + 1];
    return !(
      (previous.x === point.x && point.x === next.x) ||
      (previous.y === point.y && point.y === next.y)
    );
  });
}

function uniqueSorted(values: number[]): number[] {
  return [...new Set(values)].sort((left, right) => left - right);
}

function pointKey(point: GraphPoint): string {
  return `${point.x}:${point.y}`;
}

function stateKey(point: number, direction: Direction): string {
  return `${point}:${direction}`;
}

function manhattanDistance(left: GraphPoint, right: GraphPoint): number {
  return Math.abs(left.x - right.x) + Math.abs(left.y - right.y);
}

class MinHeap<T> {
  private readonly values: T[] = [];

  constructor(private readonly score: (value: T) => number) {}

  get size(): number {
    return this.values.length;
  }

  push(value: T): void {
    this.values.push(value);
    let index = this.values.length - 1;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (this.score(this.values[parent]) <= this.score(value)) break;
      this.values[index] = this.values[parent];
      index = parent;
    }
    this.values[index] = value;
  }

  pop(): T | undefined {
    if (this.values.length === 0) return undefined;
    const first = this.values[0];
    const last = this.values.pop()!;
    if (this.values.length === 0) return first;
    let index = 0;
    while (true) {
      const left = index * 2 + 1;
      const right = left + 1;
      if (left >= this.values.length) break;
      let child = left;
      if (
        right < this.values.length &&
        this.score(this.values[right]) < this.score(this.values[left])
      ) {
        child = right;
      }
      if (this.score(this.values[child]) >= this.score(last)) break;
      this.values[index] = this.values[child];
      index = child;
    }
    this.values[index] = last;
    return first;
  }
}
