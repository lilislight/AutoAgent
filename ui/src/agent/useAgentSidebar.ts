import { useCallback, useEffect, useState, useSyncExternalStore } from 'react';

const mediaQuery = '(max-width: 760px), (max-width: 950px) and (pointer: coarse), (max-width: 1024px) and (max-height: 600px) and (pointer: coarse)';
function subscribe(listener: () => void) {
  const media = window.matchMedia(mediaQuery);
  media.addEventListener('change', listener);
  return () => media.removeEventListener('change', listener);
}
const narrowViewport = () => window.matchMedia(mediaQuery).matches;

export interface AgentSidebarControls {
  open: boolean;
  narrow: boolean;
  show: () => void;
  dismiss: () => void;
}

export function useAgentSidebar(active: boolean): AgentSidebarControls {
  const narrow = useSyncExternalStore(subscribe, narrowViewport, () => false);
  const [mobileOpen, setMobileOpen] = useState(false);
  useEffect(() => { setMobileOpen(false); }, [narrow, active]);
  const show = useCallback(() => setMobileOpen(true), []);
  const dismiss = useCallback(() => setMobileOpen(false), []);
  return { open: !narrow || mobileOpen, narrow, show, dismiss };
}
