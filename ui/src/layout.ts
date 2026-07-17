import type { WorkflowGraphView } from "./types";

export interface NodePosition {
  x: number;
  y: number;
}

export async function layoutWorkflow(
  graph: WorkflowGraphView,
): Promise<Record<string, NodePosition>> {
  // ELK is substantially larger than the tracing shell. Load it only when a
  // Workflow version has no saved layout or the user requests auto-layout.
  const { default: ELK } = await import("elkjs/lib/elk.bundled.js");
  const elk = new ELK();
  const result = await elk.layout({
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": "RIGHT",
      "elk.edgeRouting": "ORTHOGONAL",
      "elk.layered.spacing.nodeNodeBetweenLayers": "90",
      "elk.spacing.nodeNode": "48",
      "elk.padding": "[top=48,left=48,bottom=48,right=48]",
    },
    children: graph.nodes.map((node) => ({
      id: node.id,
      width: 224,
      height: 104,
    })),
    edges: graph.edges.map((edge) => ({
      id: edge.id,
      sources: [edge.from_node],
      targets: [edge.to_node],
    })),
  });
  return Object.fromEntries(
    (result.children ?? []).map((node) => [
      node.id,
      { x: node.x ?? 0, y: node.y ?? 0 },
    ]),
  );
}

export function loadSavedLayout(
  definitionHash: string,
): Record<string, NodePosition> | null {
  const raw = localStorage.getItem(layoutKey(definitionHash));
  if (!raw) return null;
  try {
    return JSON.parse(raw) as Record<string, NodePosition>;
  } catch {
    return null;
  }
}

export function saveLayout(
  definitionHash: string,
  positions: Record<string, NodePosition>,
): void {
  localStorage.setItem(layoutKey(definitionHash), JSON.stringify(positions));
}

function layoutKey(definitionHash: string): string {
  return `autoagent:layout:${definitionHash}`;
}
