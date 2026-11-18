import { useLayoutEffect } from 'react';

/** Safari's keyboard shrinks the visual viewport without reliably resizing the layout viewport. */
export function useAgentViewport(active: boolean) {
  useLayoutEffect(() => {
    const viewport = window.visualViewport;
    if (!active || !viewport) return;
    const root = document.documentElement;
    let frame = 0;
    const clear = () => {
      root.style.removeProperty('--agent-visible-height');
      root.style.removeProperty('--agent-visible-top');
      delete root.dataset.agentKeyboard;
    };
    const measure = () => {
      frame = 0;
      if (Math.abs(viewport.scale - 1) > .02) { clear(); return; }
      root.style.setProperty('--agent-visible-height', `${viewport.height}px`);
      root.style.setProperty('--agent-visible-top', `${Math.max(0, viewport.offsetTop)}px`);
      root.dataset.agentKeyboard = String(window.innerHeight - viewport.height > 120);
    };
    const schedule = () => { if (!frame) frame = requestAnimationFrame(measure); };
    measure();
    viewport.addEventListener('resize', schedule);
    viewport.addEventListener('scroll', schedule);
    window.addEventListener('resize', schedule);
    window.addEventListener('pageshow', schedule);
    return () => {
      cancelAnimationFrame(frame);
      viewport.removeEventListener('resize', schedule);
      viewport.removeEventListener('scroll', schedule);
      window.removeEventListener('resize', schedule);
      window.removeEventListener('pageshow', schedule);
      clear();
    };
  }, [active]);
}
