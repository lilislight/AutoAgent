import type { GraphEdgeRoute, GraphPoint } from "./graphRouting.js";

export type NodePosition = GraphPoint;
export type EdgeRoute = GraphEdgeRoute;

export interface WorkflowLayout {
  positions: Record<string, NodePosition>;
  edgeRoutes: Record<string, EdgeRoute>;
}
