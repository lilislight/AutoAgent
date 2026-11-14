import { test } from 'node:test';
import assert from 'node:assert/strict';
import { placeTooltip } from '../src/tooltipPlacement.js';

test('hover hints open to the right with a gap and vertical alignment', () => {
  const position = placeTooltip({ left: 50, right: 300, top: 200, height: 40 }, { width: 280, height: 100 }, { width: 1200, height: 800 });
  assert.equal(position.side, 'right');
  assert.equal(position.left, 308);
  assert.equal(position.top, 170);
});

test('right-edge hints move to the left rather than covering their trigger or leaving the viewport', () => {
  const position = placeTooltip({ left: 1150, right: 1182, top: 750, height: 32 }, { width: 280, height: 120 }, { width: 1200, height: 800 });
  assert.equal(position.side, 'left');
  assert.equal(position.left, 862);
  assert.ok(position.left + 280 < 1150);
  assert.equal(position.top + 120, 792);
});

test('narrow viewports constrain tooltip width to the available horizontal space', () => {
  const position = placeTooltip({ left: 180, right: 210, top: 0, height: 30 }, { width: 420, height: 200 }, { width: 400, height: 300 });
  assert.equal(position.side, 'right');
  assert.equal(position.maxWidth, 174);
  assert.ok(position.left >= 218);
  assert.ok(position.left + position.maxWidth <= 392);
  assert.equal(position.top, 8);
});

test('composer context hints open above their trigger and stay inside a narrow viewport', () => {
  const position = placeTooltip({ left: 340, right: 372, top: 700, height: 32 }, { width: 250, height: 110 }, { width: 390, height: 844 }, 'top');
  assert.equal(position.side, 'top');
  assert.equal(position.top, 582);
  assert.equal(position.left + 250, 382);
  const nearTop = placeTooltip({ left: 100, right: 132, top: 12, height: 32 }, { width: 250, height: 110 }, { width: 390, height: 844 }, 'top');
  assert.equal(nearTop.side, 'bottom');
  assert.equal(nearTop.top, 52);
});
