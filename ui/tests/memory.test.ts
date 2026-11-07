import { test } from 'node:test';
import assert from 'node:assert/strict';
import { memoryBudgetSource, memoryBytes, memorySeries, memoryStatus, type MemoryPoint, type MemorySnapshot } from '../src/memory.js';

test('budget origin distinguishes automatic capacity, user settings and unknown-capacity fallback', () => {
  assert.equal(memoryBudgetSource('explicit'), 'Configured');
  assert.match(memoryBudgetSource('cgroup'), /Cgroup capacity/);
  assert.match(memoryBudgetSource('physical'), /Physical capacity/);
  assert.match(memoryBudgetSource('fallback'), /detection unavailable/);
});

test('memory labels distinguish missing measurements, disabled, stale and failed telemetry', () => {
  assert.equal(memoryBytes(null), 'Unavailable');
  assert.equal(memoryBytes(0), '0.0 MiB');
  assert.equal(memoryBytes(1024 ** 3), '1.00 GiB');
  const snapshot = { enabled: false, stale: true, latest: { pressure: 'high' } } as MemorySnapshot;
  assert.equal(memoryStatus(snapshot), 'Off');
  assert.equal(memoryStatus({ ...snapshot, enabled: true }), 'Stale');
  assert.equal(memoryStatus({ ...snapshot, enabled: true, stale: false }), 'High');
  assert.equal(memoryStatus(snapshot, true), 'Unavailable');
});

test('RSS paths retain spikes, break on missing measurements and use real sample times', () => {
  const point = (timestamp_us: number, rss_bytes: number | null) => ({ timestamp_us, rss_bytes } as MemoryPoint);
  const path = memorySeries([point(0, 10), point(25, 100), point(50, null), point(100, 10)], 'rss_bytes', 0, 100, 100);
  assert.equal(path, 'M44.0,146.0 L194.0,20.0 M644.0,146.0');
  assert.equal(memorySeries([], 'rss_bytes', 0, 0, 1), '');
});
