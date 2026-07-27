import type {
  LayoutWorkerResponse,
  WorkflowLayout,
} from "./layoutTypes.js";
import type { WorkflowGraphView } from "./types.js";

export type {
  EdgeRoute,
  NodePosition,
  WorkflowLayout,
} from "./layoutTypes.js";

type PendingLayout = {
  resolve: (layout: WorkflowLayout) => void;
  reject: (error: Error) => void;
};

let layoutWorker: Worker | null = null;
let nextRequestId = 1;
const pendingLayouts = new Map<number, PendingLayout>();

export async function layoutWorkflow(
  graph: WorkflowGraphView,
): Promise<WorkflowLayout> {
  if (typeof Worker === "undefined") {
    return computeWithoutWorker(graph);
  }
  try {
    return await computeInWorker(graph);
  } catch {
    // Worker construction may be blocked by a restrictive CSP or unsupported
    // embedding environment. Layout remains available on the main thread.
    return computeWithoutWorker(graph);
  }
}

export function loadSavedLayout(
  definitionHash: string,
): WorkflowLayout | null {
  try {
    if (typeof localStorage === "undefined") return null;
    const raw = localStorage.getItem(layoutKey(definitionHash));
    if (!raw) return null;
    const value = JSON.parse(raw) as WorkflowLayout;
    if (!isWorkflowLayout(value)) return null;
    return value;
  } catch {
    return null;
  }
}

export function saveLayout(
  definitionHash: string,
  layout: WorkflowLayout,
): boolean {
  try {
    if (typeof localStorage === "undefined") return false;
    localStorage.setItem(layoutKey(definitionHash), JSON.stringify(layout));
    return true;
  } catch {
    // Layout persistence is optional. Quota, privacy mode, or an embedded
    // browser policy must never prevent the graph itself from rendering.
    return false;
  }
}

function computeInWorker(
  graph: WorkflowGraphView,
): Promise<WorkflowLayout> {
  const worker = getLayoutWorker();
  const requestId = nextRequestId++;
  return new Promise((resolve, reject) => {
    pendingLayouts.set(requestId, { resolve, reject });
    worker.postMessage({ requestId, graph });
  });
}

async function computeWithoutWorker(
  graph: WorkflowGraphView,
): Promise<WorkflowLayout> {
  const { computeWorkflowLayout } = await import("./layoutEngine.js");
  return computeWorkflowLayout(graph);
}

function getLayoutWorker(): Worker {
  if (layoutWorker) return layoutWorker;
  const worker = new Worker(
    new URL("./layout.worker.ts", import.meta.url),
    { type: "module", name: "autoagent-layout" },
  );
  worker.onmessage = (event: MessageEvent<LayoutWorkerResponse>) => {
    const response = event.data;
    const pending = pendingLayouts.get(response.requestId);
    if (!pending) return;
    pendingLayouts.delete(response.requestId);
    if ("error" in response) {
      pending.reject(new Error(response.error));
    } else {
      pending.resolve(response.layout);
    }
  };
  worker.onerror = () => {
    failLayoutWorker(new Error("Workflow layout Worker failed."));
  };
  layoutWorker = worker;
  return worker;
}

function failLayoutWorker(error: Error): void {
  layoutWorker?.terminate();
  layoutWorker = null;
  for (const pending of pendingLayouts.values()) {
    pending.reject(error);
  }
  pendingLayouts.clear();
}

function layoutKey(definitionHash: string): string {
  return `autoagent:layout:v7:${definitionHash}`;
}

function isWorkflowLayout(value: unknown): value is WorkflowLayout {
  if (!value || typeof value !== "object") return false;
  const candidate = value as Partial<WorkflowLayout>;
  return (
    isRecord(candidate.positions) &&
    isRecord(candidate.edgeRoutes) &&
    Object.values(candidate.positions).every(isPoint) &&
    Object.values(candidate.edgeRoutes).every(
      (route) =>
        Boolean(route) &&
        Array.isArray(route.points) &&
        route.points.length >= 2 &&
        route.points.every(isPoint) &&
        isPoint(route.label),
    )
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isPoint(value: unknown): value is { x: number; y: number } {
  if (!value || typeof value !== "object") return false;
  const point = value as { x?: unknown; y?: unknown };
  return (
    typeof point.x === "number" &&
    Number.isFinite(point.x) &&
    typeof point.y === "number" &&
    Number.isFinite(point.y)
  );
}
