import ELK from "elkjs/lib/elk-api.js";
import ELKWorker from "elkjs/lib/elk-worker.min.js?worker";
import { computeLayout, type LayoutEngine, type Layout } from "./layout";
import type { GraphDefinition } from "./types";

const cache = new Map<string, Layout>();

export async function layoutWorkflow(hash: string, definition: GraphDefinition, signal: AbortSignal): Promise<Layout> {
  const cached = cache.get(hash);
  if (cached) {
    cache.delete(hash);
    cache.set(hash, cached);
    return cached;
  }
  const engine = new ELK({ workerFactory: () => new ELKWorker() });
  let timer: ReturnType<typeof setTimeout>;
  let abort: () => void = () => {};
  try {
    const boundary = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("Layout timeout")), 10_000);
      abort = () => reject(new DOMException("Aborted", "AbortError"));
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) abort();
    });
    const value = await Promise.race([computeLayout(definition, engine as unknown as LayoutEngine), boundary]);
    cache.set(hash, value);
    if (cache.size > 8) cache.delete(cache.keys().next().value!);
    return value;
  } finally {
    clearTimeout(timer!);
    signal.removeEventListener("abort", abort);
    engine.terminateWorker();
  }
}
