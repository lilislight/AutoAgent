import { createContext, useCallback, useContext, useEffect, useLayoutEffect, useRef, useState, type ReactNode } from "react";
import { useExecutionSnapshot } from './ExecutionSnapshot';
import { retainedViews } from '../workspaceRetention';

export const FLOW_LOADING_MINIMUM_MS = 300;
const FlowLoadingContext = createContext({ loading: false, markReady: () => {} });
export function useFlowReady() {
  const { markReady } = useContext(FlowLoadingContext);
  const latest = useRef(markReady);
  useLayoutEffect(() => { latest.current = markReady; }, [markReady]);
  return useCallback(() => latest.current(), []);
}

const ignoreReady = () => {};

/** Prepare the next workspace offscreen, then present its header and graph together. */
export function FlowLoadingBoundary<T extends { key: string }>({ flowKey, candidate, pending, failed, render, fallback, onPresented, minimumMs = FLOW_LOADING_MINIMUM_MS, retainLimit = 1, retainGroup, pinnedKey }: {
  flowKey: string | null; candidate: T | null; pending: boolean; failed: boolean;
  render: (value: T) => ReactNode; fallback: ReactNode;
  onPresented: (value: T | null) => void; minimumMs?: number;
  retainLimit?: number; retainGroup?: (value: T) => string | null; pinnedKey?: string;
}) {
  const [presented, setPresented] = useState<T | null>(null);
  const [retained, setRetained] = useState<T[]>([]);
  const [readyKey, setReadyKey] = useState<string | null>(null);
  const [complete, setComplete] = useState(false);
  const startedAt = useRef(performance.now());
  useLayoutEffect(() => {
    startedAt.current = performance.now();
    setReadyKey(presented?.key === flowKey || retained.some(value => value.key === flowKey) ? flowKey : null);
    setComplete(!flowKey);
    if (!flowKey) { setPresented(null); setRetained([]); }
  }, [flowKey]);
  const markReady = useCallback(() => setReadyKey(flowKey), [flowKey]);
  useEffect(() => {
    if (!candidate || pending || failed || readyKey !== flowKey || complete && presented === candidate) return;
    const timer = setTimeout(() => {
      setRetained(current => retainGroup ? retainedViews(candidate, [...(presented ? [presented] : []), ...current], retainGroup, retainLimit, pinnedKey) : []);
      setPresented(candidate);
      setComplete(true);
    }, Math.max(0, minimumMs - (performance.now() - startedAt.current)));
    return () => clearTimeout(timer);
  }, [candidate, pending, failed, readyKey, flowKey, minimumMs, retainLimit, retainGroup, pinnedKey, complete, presented]);
  useEffect(() => onPresented(presented), [presented, onPresented]);

  const loading = Boolean(flowKey && !failed && (!complete || pending || presented?.key !== flowKey));
  const current = flowKey ? presented : null;
  const layersByKey = new Map<string, T>();
  for (const value of [current, ...retained]) if (value) layersByKey.set(value.key, value);
  if (candidate && candidate.key !== current?.key) layersByKey.set(candidate.key, candidate);
  const layers = [...layersByKey.values()];
  const showFallback = !current || failed;
  return <>
    {layers.map(value => {
      const hidden = value.key !== current?.key || showFallback;
      return <div key={value.key} className={`flow-workspace-layer ${hidden ? "is-preparing" : ""}`} aria-hidden={hidden} inert={hidden || loading}>
        <FlowLoadingContext.Provider value={{ loading: !hidden && loading, markReady: value.key === flowKey ? markReady : ignoreReady }}>{render(value)}</FlowLoadingContext.Provider>
      </div>;
    })}
    {showFallback ? <div className="flow-workspace-layer"><FlowLoadingContext.Provider value={{ loading, markReady: ignoreReady }}>{fallback}</FlowLoadingContext.Provider></div> : null}
  </>;
}

/** Place inside the graph workspace, leaving its toolbar outside the loading area. */
export function FlowLoadingSurface({ children, loadingLabel }: { children: ReactNode; loadingLabel?: string }) {
  const { loading: preparing } = useContext(FlowLoadingContext);
  const loading = preparing || useExecutionSnapshot().refreshing;
  return <div className="definition-workspace flow-loading-surface" aria-busy={loading}>
    <div className={`flow-loading-content ${loading ? "is-loading" : ""}`} inert={loading}>{children}</div>
    {loading ? <div className="flow-loading-overlay" data-flow-loading role="status" aria-label="Loading flow" aria-live="polite">
      <span className="flow-loading-spinner" aria-hidden="true" />
      {loadingLabel?<span className="flow-loading-label">{loadingLabel}</span>:null}
    </div> : null}
  </div>;
}
