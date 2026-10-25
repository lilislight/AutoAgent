import { NODE_HEIGHT, NODE_WIDTH, type Layout } from './layout.js';

export interface GraphCamera { x: number; y: number; zoom: number }
export interface GraphViewport { x: number; y: number; zoom: number }
export interface GraphSize { width: number; height: number }
export function sameGraphCamera(left: GraphCamera, right: GraphCamera): boolean {
  return Math.abs(left.x-right.x)<.01 && Math.abs(left.y-right.y)<.01 && Math.abs(left.zoom-right.zoom)<.0001;
}

/** Share a world-space centre, so unequal panes still look at the same point. */
export function cameraFromViewport(viewport: GraphViewport, size: GraphSize): GraphCamera {
  return { x: (size.width / 2 - viewport.x) / viewport.zoom, y: (size.height / 2 - viewport.y) / viewport.zoom, zoom: viewport.zoom };
}
export function viewportFromCamera(camera: GraphCamera, size: GraphSize): GraphViewport {
  return { x: size.width / 2 - camera.x * camera.zoom, y: size.height / 2 - camera.y * camera.zoom, zoom: camera.zoom };
}
export function fitGraphCamera(layout: Layout, size: GraphSize): GraphCamera {
  const positions = Object.values(layout.positions);
  if (!positions.length) return { x: 0, y: 0, zoom: 1 };
  let left = Infinity, top = Infinity, right = -Infinity, bottom = -Infinity;
  for (const [id, point] of Object.entries(layout.positions)) {
    left = Math.min(left, point.x); top = Math.min(top, point.y);
    right = Math.max(right, point.x + (layout.sizes?.[id]?.width ?? NODE_WIDTH)); bottom = Math.max(bottom, point.y + (layout.sizes?.[id]?.height ?? NODE_HEIGHT));
  }
  return { x: (left + right) / 2, y: (top + bottom) / 2,
    zoom: Math.max(.15, Math.min(1, Math.max(1, size.width - 80) / (right - left), Math.max(1, size.height - 100) / (bottom - top))) };
}
