import ELK from "elkjs/lib/elk-api.js";
import ELKWorker from "elkjs/lib/elk-worker.min.js?worker";

import {
  computeWorkflowLayout,
  type ElkLayoutEngine,
} from "./layoutEngine.js";
import type { WorkflowLayout } from "./layoutTypes.js";
import type { WorkflowGraphView } from "./types.js";

export type {
  EdgeRoute,
  NodePosition,
  WorkflowLayout,
} from "./layoutTypes.js";

const LAYOUT_TIMEOUT_MS = 15_000;

export async function layoutWorkflow(
  graph: WorkflowGraphView,
): Promise<WorkflowLayout> {
  if (typeof Worker === "undefined") {
    return computeWithoutWorker(graph);
  }
  const elk = new ELK({
    workerFactory: () => new ELKWorker(),
  });
  try {
    return await withTimeout(
      computeWorkflowLayout(graph, elk as ElkLayoutEngine),
      LAYOUT_TIMEOUT_MS,
    );
  } catch {
    // Worker construction may be blocked by a restrictive CSP or unsupported
    // embedding environment. Layout remains available on the main thread.
    return computeWithoutWorker(graph);
  } finally {
    elk.terminateWorker();
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

async function computeWithoutWorker(
  graph: WorkflowGraphView,
): Promise<WorkflowLayout> {
  const { computeWorkflowLayoutOnMainThread } = await import(
    "./layoutFallback.js"
  );
  return computeWorkflowLayoutOnMainThread(graph);
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timeoutId = window.setTimeout(() => {
      reject(new Error(`Workflow layout timed out after ${timeoutMs}ms.`));
    }, timeoutMs);
    promise.then(
      (value) => {
        window.clearTimeout(timeoutId);
        resolve(value);
      },
      (error: unknown) => {
        window.clearTimeout(timeoutId);
        reject(error);
      },
    );
  });
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
