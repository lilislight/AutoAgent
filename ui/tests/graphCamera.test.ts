import test from 'node:test';
import assert from 'node:assert/strict';
import { cameraFromViewport, fitGraphCamera, sameGraphCamera, viewportFromCamera } from '../src/graphCamera.js';

test('linked Graphs preserve the same world centre and zoom across unequal pane widths and resizing', () => {
  const left = { width: 430, height: 640 }, right = { width: 730, height: 640 };
  const camera = cameraFromViewport({ x: -185, y: 95, zoom: .5 }, left);
  assert.deepEqual(camera, { x: 800, y: 450, zoom: .5 });
  const target = viewportFromCamera(camera, right);
  assert.deepEqual(cameraFromViewport(target, right), camera);
  assert.deepEqual(cameraFromViewport(viewportFromCamera(camera, { width: 580, height: 480 }), { width: 580, height: 480 }), camera);
});

test('shared fitting includes removed Nodes and never enlarges a single Node beyond its normal size', () => {
  const layout = { positions: { current: { x: 200, y: 300 }, removed: { x: -400, y: -100 } }, routes: {} };
  const camera = fitGraphCamera(layout, { width: 600, height: 500 });
  const viewport = viewportFromCamera(camera, { width: 600, height: 500 });
  assert.ok(-400 * camera.zoom + viewport.x >= 39);
  assert.ok((200 + 232) * camera.zoom + viewport.x <= 561);
  assert.equal(fitGraphCamera({ positions: { only: { x: 0, y: 0 } }, routes: {} }, { width: 1800, height: 1000 }).zoom, 1);
});

test('programmatic viewport echoes tolerate rendering rounding while Minimap and focus changes propagate', () => {
  const camera = { x: 460, y: 148, zoom: .647005 };
  assert.equal(sameGraphCamera(camera, { x: 460.0002, y: 148.0004, zoom: .64700502 }), true);
  assert.equal(sameGraphCamera(camera, { ...camera, x: 500 }), false);
  assert.equal(sameGraphCamera(camera, { ...camera, zoom: .8 }), false);
});
