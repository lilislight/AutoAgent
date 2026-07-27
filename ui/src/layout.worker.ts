/// <reference lib="webworker" />

import { computeWorkflowLayout } from "./layoutEngine.js";
import type {
  LayoutWorkerRequest,
  LayoutWorkerResponse,
} from "./layoutTypes.js";

const workerScope = self as DedicatedWorkerGlobalScope;

workerScope.onmessage = async (
  event: MessageEvent<LayoutWorkerRequest>,
): Promise<void> => {
  const { requestId, graph } = event.data;
  try {
    const layout = await computeWorkflowLayout(graph);
    const response: LayoutWorkerResponse = { requestId, layout };
    workerScope.postMessage(response);
  } catch (error) {
    const response: LayoutWorkerResponse = {
      requestId,
      error: error instanceof Error ? error.message : String(error),
    };
    workerScope.postMessage(response);
  }
};
