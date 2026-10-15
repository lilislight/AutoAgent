import { useLayoutEffect, useRef, type RefObject } from 'react';

/** Use the same transition for Graph, Timeline, Split and Split orientation. */
export function useViewMotion(container: RefObject<HTMLElement | null>, key: string, active: boolean) {
  const previous = useRef(key);
  const animation = useRef<Animation | null>(null);
  useLayoutEffect(() => {
    if (previous.current === key) return;
    previous.current = key;
    animation.current?.cancel();
    if (!active || !container.current || matchMedia('(prefers-reduced-motion: reduce)').matches) return;
    animation.current = container.current.animate([
      { opacity: .3, filter: 'blur(2px)', transform: 'translateY(5px)' },
      { opacity: 1, filter: 'blur(0)', transform: 'translateY(0)' },
    ], { duration: 240, easing: 'cubic-bezier(.2,.8,.2,1)' });
  }, [container, key, active]);
  useLayoutEffect(() => () => animation.current?.cancel(), []);
}
