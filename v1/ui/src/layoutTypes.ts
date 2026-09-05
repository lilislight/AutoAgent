import type { GraphEdgeRoute, GraphPoint, GraphRect } from "./graphRouting.js";

export type NodePosition = GraphPoint;
export type EdgeRoute = GraphEdgeRoute;

export interface WorkflowLayout {
  positions: Record<string, NodePosition>;
  edgeRoutes: Record<string, EdgeRoute>;
  groupBounds: Record<string, GraphRect>;
}
