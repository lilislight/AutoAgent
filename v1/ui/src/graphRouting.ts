export const GRAPH_NODE_WIDTH = 224;
export const GRAPH_NODE_HEIGHT = 104;
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
  from_node: string;
  to_node: string;
  order: number;
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

export function sourcePort(position: GraphPoint): GraphPoint {
  return {
    x: position.x + GRAPH_NODE_WIDTH,
    y: position.y + GRAPH_NODE_HEIGHT / 2,
  };
}

export function targetPort(position: GraphPoint): GraphPoint {
  return {
    x: position.x,
    y: position.y + GRAPH_NODE_HEIGHT / 2,
  };
}

export function routeEdgesAroundNodes(
  nodes: RoutableNode[],
  edges: RoutableEdge[],
  positions: Record<string, GraphPoint>,
  edgeIds: Iterable<string>,
): Record<string, GraphEdgeRoute> {
  const requested = new Set(edgeIds);
  const parallelLanes = edgeLaneOffsets(edges);
  const obstacles = nodes.flatMap((node) => {
    const position = positions[node.id];
    return position ? [expandedNodeRect(position)] : [];
  });
  return Object.fromEntries(
    edges.flatMap((edge) => {
      if (!requested.has(edge.id)) return [];
      const sourcePosition = positions[edge.from_node];
      const targetPosition = positions[edge.to_node];
      if (!sourcePosition || !targetPosition) return [];
      const points = routeSingleEdge(
        sourcePosition,
        targetPosition,
        obstacles,
        parallelLanes[edge.id] ?? 0,
      );
      return [[edge.id, { points, label: routeMidpoint(points) }]];
    }),
  );
}

export function affectedEdgeIdsAfterNodeMove(
  routes: Record<string, GraphEdgeRoute>,
  edges: RoutableEdge[],
  movedNodeId: string,
  positions: Record<string, GraphPoint>,
): Set<string> {
  const affected = new Set(
    edges
      .filter(
        (edge) =>
          edge.from_node === movedNodeId || edge.to_node === movedNodeId,
      )
      .map((edge) => edge.id),
  );
  const movedPosition = positions[movedNodeId];
  if (!movedPosition) return affected;
  const obstacle = expandedNodeRect(movedPosition);
  for (const edge of edges) {
    if (affected.has(edge.id)) continue;
    const points = routes[edge.id]?.points;
    if (
      points &&
      points.slice(1).some((point, index) =>
        segmentIntersectsRect(points[index], point, obstacle),
      )
    ) {
      affected.add(edge.id);
    }
  }
  return affected;
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

function routeSingleEdge(
  sourcePosition: GraphPoint,
  targetPosition: GraphPoint,
  obstacles: GraphRect[],
  laneOffset: number,
): GraphPoint[] {
  const source = sourcePort(sourcePosition);
  const target = targetPort(targetPosition);
  const sourceOutside = {
    x: source.x + GRAPH_ROUTE_CLEARANCE,
    y: source.y + laneOffset,
  };
  const targetOutside = {
    x: target.x - GRAPH_ROUTE_CLEARANCE,
    y: target.y + laneOffset,
  };
  const routed = routeBetween(sourceOutside, targetOutside, obstacles);
  const points = [
    source,
    { x: sourceOutside.x, y: source.y },
    sourceOutside,
    ...routed.slice(1, -1),
    targetOutside,
    { x: targetOutside.x, y: target.y },
    target,
  ];
  return simplifyOrthogonalPoints(points);
}

function routeBetween(
  start: GraphPoint,
  end: GraphPoint,
  obstacles: GraphRect[],
): GraphPoint[] {
  const xValues = uniqueSorted([
    start.x,
    end.x,
    ...obstacles.flatMap((rect) => [rect.left, rect.right]),
  ]);
  const yValues = uniqueSorted([
    start.y,
    end.y,
    ...obstacles.flatMap((rect) => [rect.top, rect.bottom]),
  ]);
  const points: GraphPoint[] = [];
  const pointIndex = new Map<string, number>();
  for (const x of xValues) {
    for (const y of yValues) {
      const point = { x, y };
      if (pointInsideAnyObstacle(point, obstacles)) continue;
      pointIndex.set(pointKey(point), points.length);
      points.push(point);
    }
  }
  const startIndex = pointIndex.get(pointKey(start));
  const endIndex = pointIndex.get(pointKey(end));
  if (startIndex === undefined || endIndex === undefined) {
    return fallbackOuterRoute(start, end, obstacles);
  }
  const neighbors = buildVisibilityNeighbors(points, obstacles);
  const path = searchOrthogonalPath(
    points,
    neighbors,
    startIndex,
    endIndex,
  );
  return path ?? fallbackOuterRoute(start, end, obstacles);
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
        bendCost;
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

function edgeLaneOffsets(edges: RoutableEdge[]): Record<string, number> {
  const groups = new Map<string, RoutableEdge[]>();
  for (const edge of edges) {
    const key = `${edge.from_node}\u0000${edge.to_node}`;
    groups.set(key, [...(groups.get(key) ?? []), edge]);
  }
  const result: Record<string, number> = {};
  for (const group of groups.values()) {
    group.sort(
      (left, right) =>
        left.order - right.order || left.id.localeCompare(right.id),
    );
    const center = (group.length - 1) / 2;
    group.forEach((edge, index) => {
      result[edge.id] = (index - center) * 12;
    });
  }
  return result;
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
