export interface StackAnchor { x: number; y: number; width: number; height: number }
export interface StackSurface { left: number; top: number; width: number; height: number }
const clamp = (value: number, min: number, max: number) => Math.min(Math.max(value, min), max);

/** Keep the front card near the clicked object, moving inward only to avoid clipping. */
export function placeStack(anchor: StackAnchor | undefined, surface: StackSurface, total: number) {
  const source = anchor ?? { x: surface.left + surface.width / 2, y: surface.top + surface.height / 2, width: 160, height: 80 };
  const layers = Math.min(4, Math.max(0, total - 1));
  const width = Math.min(surface.width - 32, clamp(surface.width * .34, 240, 320));
  const cardHeight = Math.min(clamp((width - 28) * .6 + 16, 152, 180), Math.max(96, (surface.height - 138) / (1 + (layers + 1) * .16)));
  const spacing = clamp(cardHeight * .16, 16, 32);
  const frontY = 8 + layers * spacing;
  const sceneHeight = frontY + cardHeight + spacing + 18;
  const height = 50 + sceneHeight + 30;
  const x = source.x - surface.left, y = source.y - surface.top;
  const left = clamp(x - width / 2, 16, Math.max(16, surface.width - width - 16));
  const top = clamp(y - (50 + frontY + cardHeight / 2), 16, Math.max(16, surface.height - height - 16));
  return { left, top, width, height, cardHeight, spacing, frontY, sceneHeight,
    originX: x - (left + width / 2), originY: y - (top + 50 + frontY + cardHeight / 2),
    sourceScaleX: clamp(source.width / (width - 28), .08, 1), sourceScaleY: clamp(source.height / cardHeight, .06, 1) };
}

export interface StackMotion { position: number; target: number }
export function wheelMotion(state: StackMotion, pixels: number, maximum: number): StackMotion {
  const delta = -clamp(pixels, -240, 240);
  if (!delta) return state;
  const position = clamp(state.position + delta / 110, 0, maximum);
  return { position, target: position };
}

/** Animate explicit button/key navigation; wheel input stays at its exact position. */
export function advanceMotion(state: StackMotion, seconds: number, maximum: number): StackMotion {
  const target = clamp(state.target, 0, maximum);
  const position = target + (state.position - target) * Math.exp(-20 * seconds);
  return { position: Math.abs(position - target) < .002 ? target : position, target };
}

export const motionSettled = (state: StackMotion) => state.position === state.target;
