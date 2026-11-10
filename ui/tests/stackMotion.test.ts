import { test } from "node:test";
import assert from "node:assert/strict";
import { advanceMotion, motionSettled, placeStack, wheelMotion, STACK_SCENE_TOP, type StackMotion } from "../src/stackMotion.js";
const near = (actual: number, expected: number) => assert.ok(Math.abs(actual - expected) < 1e-8);

test("Node and Edge stacks share a compact size in the same viewport and keep their own origins", () => {
  const surface = { left: 300, top: 100, width: 900, height: 800 };
  const small = placeStack({ x: 620, y: 550, width: 140, height: 64 }, surface, 3);
  const large = placeStack({ x: 820, y: 550, width: 232, height: 106 }, surface, 3);
  assert.notEqual(small.left, large.left);
  const edge = placeStack({ x: 750, y: 550, width: 22, height: 14 }, surface, 3);
  assert.equal(large.width, small.width);
  assert.equal(edge.width, small.width);
  assert.equal(large.cardHeight, small.cardHeight);
  assert.equal(edge.cardHeight, small.cardHeight);
  assert.ok(large.sourceScaleX > edge.sourceScaleX);
  for (const [layout, x] of [[small, 620], [large, 820]] as const) {
    near(surface.left + layout.left + layout.width / 2 + layout.originX, x);
    near(surface.top + layout.top + STACK_SCENE_TOP + layout.frontY + layout.cardHeight / 2 + layout.originY, 550);
  }
});

test("card size responds to the canvas size and remains compact on large screens", () => {
  const anchor = { x: 620, y: 550, width: 140, height: 64 };
  const small = placeStack(anchor, { left: 300, top: 100, width: 480, height: 600 }, 3);
  const large = placeStack(anchor, { left: 300, top: 100, width: 1200, height: 900 }, 3);
  const ultrawide = placeStack(anchor, { left: 300, top: 100, width: 2400, height: 1200 }, 3);
  assert.ok(large.width > small.width);
  assert.ok(large.cardHeight > small.cardHeight);
  assert.equal(ultrawide.width, large.width);
  assert.equal(ultrawide.cardHeight, large.cardHeight);
});

test("stacks at canvas edges remain visible while their animation origin stays at the click", () => {
  for (const height of [400, 600]) {
    const surface = { left: 300, top: 100, width: 800, height };
    for (const x of [305, 1095]) for (const y of [105, 100 + height - 5]) {
      const layout = placeStack({ x, y, width: 25, height: 15 }, surface, 24);
      assert.ok(layout.left >= 16 && layout.top >= 16);
      assert.ok(layout.left + layout.width <= surface.width - 16);
      assert.ok(layout.top + layout.height <= surface.height - 16);
      near(surface.left + layout.left + layout.width / 2 + layout.originX, x);
      near(surface.top + layout.top + STACK_SCENE_TOP + layout.frontY + layout.cardHeight / 2 + layout.originY, y);
    }
  }
});

test("wheel up reveals older executions and remains at the exact position after release", () => {
  let state: StackMotion = wheelMotion({ position: 0, target: 0 }, -100, 23);
  const released = state.position;
  assert.ok(released > 0 && released < 1);
  for (let i = 0; i < 240; i++) state = advanceMotion(state, 1 / 120, 23);
  assert.ok(motionSettled(state));
  assert.equal(state.position, released);
  assert.ok(wheelMotion(state, 100, 23).position < state.position);
});

test("wheel input cancels button animation and a reverse gesture immediately reverses position", () => {
  const initial = wheelMotion({ position: 6, target: 12 }, -100, 23);
  let slow = initial, fast = initial;
  for (let i = 0; i < 12; i++) slow = advanceMotion(slow, 1 / 60, 23);
  for (let i = 0; i < 24; i++) fast = advanceMotion(fast, 1 / 120, 23);
  assert.ok(Math.abs(slow.position - fast.position) < 1e-9);
  const reversed = wheelMotion(slow, 40, 23);
  assert.ok(reversed.position < slow.position);
  assert.ok(motionSettled(reversed));
});

test("strong wheel gestures stop at both ends without leaving residual momentum", () => {
  let oldest = wheelMotion({ position: 22, target: 22 }, -1000, 23);
  let newest = wheelMotion({ position: 1, target: 1 }, 1000, 23);
  for (let i = 0; i < 180; i++) {
    oldest = advanceMotion(oldest, 1 / 60, 23);
    newest = advanceMotion(newest, 1 / 60, 23);
    assert.ok(oldest.position <= 23 && newest.position >= 0);
  }
  assert.equal(oldest.position, 23); assert.equal(newest.position, 0);
  assert.ok(motionSettled(oldest) && motionSettled(newest));
});

test("explicit button navigation still animates to the selected card", () => {
  let state: StackMotion = { position: 2.5, target: 4 };
  state = advanceMotion(state, 1 / 60, 23);
  assert.ok(state.position > 2.5 && state.position < 4);
  for (let i = 0; i < 120; i++) state = advanceMotion(state, 1 / 60, 23);
  assert.equal(state.position, 4);
  assert.ok(motionSettled(state));
});
