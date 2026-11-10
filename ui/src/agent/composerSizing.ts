/** Two visible text lines by default, bounded by eight lines and available height. */
export function composerHeight(scrollHeight: number, lineHeight: number, padding: number, viewportHeight: number) {
  const minimum = lineHeight * 2 + padding;
  const maximum = Math.max(minimum, Math.min(lineHeight * 8 + padding, viewportHeight * .3));
  return { height: Math.min(maximum, Math.max(minimum, scrollHeight)), overflowing: scrollHeight > maximum + 1 };
}
