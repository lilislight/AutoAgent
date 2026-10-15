import { useSyncExternalStore } from 'react';
import { getTraceProgress, subscribeTraceProgress, traceProgressLabel } from '../traceProgress';
export function useTraceProgress(key: string) {
  const progress = useSyncExternalStore(subscribeTraceProgress, () => getTraceProgress(key));
  return traceProgressLabel(progress);
}
