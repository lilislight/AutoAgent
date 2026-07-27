import type { GraphEdgeRoute, GraphPoint } from "./graphRouting.js";

export type NodePosition = GraphPoint;
export type EdgeRoute = GraphEdgeRoute;

export interface WorkflowLayout {
  positions: Record<string, NodePosition>;
  edgeRoutes: Record<string, EdgeRoute>;
}

export interface LayoutWorkerRequest {
  requestId: number;
  graph: import("./types.js").WorkflowGraphView;
}

export type LayoutWorkerResponse =
  | {
      requestId: number;
      layout: WorkflowLayout;
      error?: never;
    }
  | {
      requestId: number;
      layout?: never;
      error: string;
    };
