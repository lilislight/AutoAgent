import { test } from 'node:test';
import assert from 'node:assert/strict';
import { localDateKey, localDateTimeKey, parseLocalDateTime } from '../src/localDateTime.js';

test('filter timestamps preserve local date and seconds without browser locale parsing', () => {
  const date = parseLocalDateTime('2026-10-12T09:15:42');
  assert.ok(date);
  assert.equal(localDateKey(date), '2026-10-12');
  assert.equal(date.getHours(), 9);
  assert.equal(date.getMinutes(), 15);
  assert.equal(date.getSeconds(), 42);
  assert.equal(localDateTimeKey(date), '2026-10-12T09:15:42');
  assert.equal(parseLocalDateTime('2026-10-12T09:15')?.getSeconds(), 0);
  assert.equal(localDateKey(parseLocalDateTime('0004-02-29T12:00:00')!), '0004-02-29');
});

test('invalid dates and incomplete or overflowing time fields cannot become a filter', () => {
  for (const input of ['', '2026-02-29T12:00:00', '2024-02-30T12:00:00', '2026-13-01T12:00:00', '2026-10-12T24:00:00', '2026-10-12T09:60:00', '2026-10-12T09:15:60', '2026-10-12T09:', '0000-01-01T12:00:00', '10/12/2026 09:15:00']) {
    assert.equal(parseLocalDateTime(input), null, input);
  }
  assert.ok(parseLocalDateTime('2024-02-29T12:00:00'));
});
