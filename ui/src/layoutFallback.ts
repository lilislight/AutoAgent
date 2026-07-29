import ELKModule from "elkjs/lib/elk.bundled.js";

import {
  computeWorkflowLayout,
  type ElkLayoutEngine,
} from "./layoutEngine.js";
import type { WorkflowLayout } from "./layoutTypes.js";
import type { WorkflowGraphView } from "./types.js";

type ElkConstructor = new () => ElkLayoutEngine;

const ELK = ELKModule as unknown as ElkConstructor;

export function computeWorkflowLayoutOnMainThread(
  graph: WorkflowGraphView,
): Promise<WorkflowLayout> {
  return computeWorkflowLayout(graph, new ELK());
}
