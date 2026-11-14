interface Anchor { left: number; right: number; top: number; height: number }
interface Size { width: number; height: number }

/** Keep hover hints beside their trigger, with horizontal edge avoidance. */
export function placeTooltip(anchor: Anchor, size: Size, viewport: Size, preferred: 'side' | 'top' = 'side') {
  const gap = 8, padding = 8;
  if (preferred === 'top') {
    const maxWidth = Math.max(1, Math.min(420, viewport.width - padding * 2));
    const width = Math.min(size.width, maxWidth);
    const above = anchor.top - gap - padding;
    const below = viewport.height - anchor.top - anchor.height - gap - padding;
    const side = above >= size.height || above >= below ? 'top' : 'bottom';
    return {
      left: Math.max(padding, Math.min((anchor.left + anchor.right - width) / 2, viewport.width - width - padding)),
      top: Math.max(padding, Math.min(side === 'top' ? anchor.top - size.height - gap : anchor.top + anchor.height + gap, viewport.height - size.height - padding)),
      side, maxWidth,
    };
  }
  const rightSpace = Math.max(0, viewport.width - padding - anchor.right - gap);
  const leftSpace = Math.max(0, anchor.left - gap - padding);
  const side = rightSpace >= size.width ? 'right' : leftSpace >= size.width ? 'left' : rightSpace >= leftSpace ? 'right' : 'left';
  const space = side === 'right' ? rightSpace : leftSpace;
  const maxWidth = Math.max(1, Math.min(420, space || viewport.width - padding * 2));
  const width = Math.min(size.width, maxWidth);
  return {
    left: Math.max(padding, Math.min(side === 'right' ? anchor.right + gap : anchor.left - gap - width, viewport.width - width - padding)),
    top: Math.max(padding, Math.min(anchor.top + (anchor.height - size.height) / 2, viewport.height - size.height - padding)),
    side, maxWidth,
  };
}
