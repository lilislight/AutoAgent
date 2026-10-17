import { useCallback, useSyncExternalStore } from 'react';
import type { ObservationClock } from '../liveObservation';
const idle = () => () => {};
export function useObservationClock(clock: ObservationClock | undefined, active: boolean, fallback: number) {
  const get = useCallback(() => clock ? clock.get() : fallback, [clock, fallback]);
  return useSyncExternalStore(active && clock ? clock.subscribe : idle, get, get);
}
