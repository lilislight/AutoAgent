import { useLayoutEffect, useRef, type RefObject } from 'react';

/** Animate existing keyed elements without reparenting or resetting their state. */
export function useLayoutMotion(container: RefObject<HTMLElement | null>, selector: string, key: string, enter = false, active = true) {
  const previous = useRef(new Map<string, DOMRect>());
  const animations = useRef<Animation[]>([]);
  useLayoutEffect(() => {
    const elements = [...(container.current?.querySelectorAll<HTMLElement>(selector) ?? [])];
    animations.current.forEach(animation => animation.cancel());
    if (!active) { previous.current.clear(); animations.current = []; return; }
    const bounds = elements.map(element => [element, element.getBoundingClientRect()] as const);
    animations.current = [];
    const reduced = matchMedia('(prefers-reduced-motion: reduce)').matches;
    for (const [element, rect] of bounds) {
      const id = element.dataset.motionId!;
      const old = previous.current.get(id);
      if (!reduced && rect.width && rect.height) {
        const transform = old ? `translate(${old.x - rect.x}px, ${old.y - rect.y}px)` : 'translateX(42px)';
        if (old || enter) animations.current.push(element.animate([{ transform, opacity: old ? 1 : 0 }, { transform: 'translate(0, 0)', opacity: 1 }], { duration: 280, easing: 'cubic-bezier(.2,.8,.2,1)' }));
      }
    }
    previous.current = new Map(bounds.map(([element, rect]) => [element.dataset.motionId!, rect]));
  }, [container, selector, key, enter, active]);
  useLayoutEffect(() => () => animations.current.forEach(animation => animation.cancel()), []);
}
