import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { fieldWindow, jsonMatchRanges, jsonPath, jsonPreview, jsonTextPreview, pathStartsWith, searchJson } from '../src/jsonInspection.js';
import { JsonSearchText, JsonValue } from '../src/components/JsonValue.js';

test('highlighting matches literal text repeatedly without treating the query as a regular expression', () => {
  assert.deepEqual(jsonMatchRanges('a.b A.B aXb', 'a.b'), [{ start: 0, end: 3 }, { start: 4, end: 7 }]);
  assert.deepEqual(jsonMatchRanges('x [*] y [*]', '[*]'), [{ start: 2, end: 5 }, { start: 8, end: 11 }]);
  assert.deepEqual(jsonMatchRanges('ordinary', '   '), []);
  assert.deepEqual(jsonMatchRanges('İx NEEDLE', 'needle'), [{ start: 3, end: 9 }]);
  assert.deepEqual(jsonMatchRanges('İx', 'i'), [{ start: 0, end: 1 }]);
});

test('long value previews expose a tail match while keeping the displayed content bounded', () => {
  const value = 'prefix '.repeat(1000) + 'TAIL MARKER' + ' suffix'.repeat(1000);
  const preview = jsonTextPreview(value, 'tail marker', 160);
  assert.match(preview, /TAIL MARKER/);
  assert.ok(preview.length <= 162);
  assert.ok(preview.startsWith('…') && preview.endsWith('…'));
  assert.equal(jsonPreview(value, 160, 'tail marker'), preview);
  assert.equal(jsonTextPreview('a'.repeat(501), '', 500), 'a'.repeat(500) + '…');
});

test('highlight markup preserves text and escapes HTML supplied by a JSON value', () => {
  const html = renderToStaticMarkup(createElement(JsonSearchText, { text: '<img src=x onerror=alert(1)> TARGET target', query: 'target' }));
  assert.equal((html.match(/<mark /g) ?? []).length, 2);
  assert.match(html, />TARGET<\/mark>/);
  assert.match(html, />target<\/mark>/);
  assert.match(html, /&lt;img/);
  assert.doesNotMatch(html, /<img/);
});

test('field paths preserve array indexes and escape unusual object keys', () => {
  assert.equal(jsonPath([]), '$');
  assert.equal(jsonPath(['messages', 3, 'content']), '$.messages[3].content');
  assert.equal(jsonPath(['a.b', '0', 'quote"key', '', 'line\nbreak']), '$["a.b"]["0"]["quote\\"key"][""]["line\\nbreak"]');
  assert.equal(pathStartsWith(['a', 1, 'value'], ['a', 1]), true);
  assert.equal(pathStartsWith(['a', '1'], ['a', 1]), false);
});

test('search finds keys and scalar values without reporting an entire parent object', async () => {
  const value = { settings: { target: false }, messages: [{ content: 'TARGET text' }], nothing: null };
  const found = await searchJson(value, 'target', new AbortController().signal);
  assert.deepEqual(found.matches.map(row => row.path), [['settings', 'target'], ['messages', 0, 'content']]);
  assert.deepEqual((await searchJson(value, 'false', new AbortController().signal)).matches.map(row => row.path), [['settings', 'target']]);
  assert.deepEqual((await searchJson(value, 'null', new AbortController().signal)).matches.map(row => row.path), [['nothing']]);
});

test('search limits matches accurately, including an exact-limit result', async () => {
  assert.equal((await searchJson(['hit','hit'], 'hit', new AbortController().signal, 2)).truncated, false);
  const found = await searchJson(['hit','hit','hit'], 'hit', new AbortController().signal, 2);
  assert.equal(found.matches.length, 2);
  assert.equal(found.truncated, true);
});

test('search can reach the end of a large array and can cancel obsolete work', async () => {
  const value = Array.from({ length: 50_000 }, (_, index) => index === 49_999 ? 'only-tail-marker' : 'ordinary value');
  const found = await searchJson(value, 'only-tail-marker', new AbortController().signal);
  assert.deepEqual(found.matches.map(row => row.path), [[49_999]]);
  assert.equal(fieldWindow(49_999), 49_950);
  const abort = new AbortController();
  const task = searchJson(value, 'not present', abort.signal);
  abort.abort();
  await assert.rejects(task, { name: 'AbortError' });
});

test('shared objects have both paths while a cyclic value cannot trap search', async () => {
  const shared = { content: 'needle' };
  assert.deepEqual((await searchJson({ a: shared, b: shared }, 'needle', new AbortController().signal)).matches.map(row => row.path), [['a','content'],['b','content']]);
  const cycle: Record<string, unknown> = { content: 'needle' }; cycle.self = cycle;
  assert.equal((await searchJson(cycle, 'needle', new AbortController().signal)).matches.length, 1);
});

test('initial JSON rendering keeps a wide array bounded and copy controls accessible', () => {
  const html = renderToStaticMarkup(createElement(JsonValue, { value: Array.from({ length: 10_000 }, (_, index) => index), label: 'output' }));
  assert.equal((html.match(/data-json-path=/g) ?? []).length, 51);
  assert.match(html, /Search JSON/);
  assert.match(html, /Copy JSON/);
  assert.match(html, /Download JSON/);
  assert.doesNotMatch(html, /data-json-path="\$\[9999\]"/);
});
