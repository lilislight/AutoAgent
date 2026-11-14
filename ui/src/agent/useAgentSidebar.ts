import { useCallback, useEffect, useState, useSyncExternalStore } from 'react';

const mediaQuery = '(max-width: 900px)';
const preferenceKey = 'autoagent.agent-directory-open';

function subscribe(listener: () => void) {
  const media = window.matchMedia(mediaQuery);
  media.addEventListener('change', listener);
  return () => media.removeEventListener('change', listener);
}
const narrowViewport = () => window.matchMedia(mediaQuery).matches;

export interface AgentSidebarControls {
  open: boolean;
  narrow: boolean;
  toggle: () => void;
  dismiss: () => void;
}

export function useAgentSidebar(active: boolean): AgentSidebarControls {
  const narrow = useSyncExternalStore(subscribe, narrowViewport, () => false);
  const [desktopOpen, setDesktopOpen] = useState(() => {
    try { return localStorage.getItem(preferenceKey) !== 'closed'; }
    catch { return true; }
  });
  const [mobileOpen, setMobileOpen] = useState(false);
  useEffect(() => {
    try { localStorage.setItem(preferenceKey, desktopOpen ? 'open' : 'closed'); }
    catch { /* The toggle also works when browser storage is unavailable. */ }
  }, [desktopOpen]);
  useEffect(() => { setMobileOpen(false); }, [narrow, active]);
  const dismiss = useCallback(() => setMobileOpen(false), []);
  const toggle = useCallback(() => {
    if (narrow) setMobileOpen(value => !value);
    else setDesktopOpen(value => !value);
  }, [narrow]);
  return { open: narrow ? mobileOpen : desktopOpen, narrow, toggle, dismiss };
}
