import { useLayoutEffect, useRef, useState } from 'react';

export function useExecutionToolbar() {
  const ref = useRef<HTMLElement>(null);
  const [width, setWidth] = useState(1200);
  useLayoutEffect(() => {
    const header = ref.current;
    if (!header) return;
    const update = () => { if (header.clientWidth > 0) setWidth(header.clientWidth); };
    update();
    const observer = new ResizeObserver(update); observer.observe(header);
    return () => observer.disconnect();
  }, []);
  return { ref, compact: width < 1020, narrow: width < 640, tiny: width < 440 };
}
