import assert from 'node:assert/strict';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import test from 'node:test';
import { previewToolResult, previewToolText, TOOL_PREVIEW_CHARACTERS, TOOL_PREVIEW_LINES } from '../src/agent/toolPreview.js';
import { ToolArgumentsPreview, ToolResultPreview } from '../src/agent/ToolPayloadPreview.js';

test('long single lines stop at the character cap without splitting emoji', () => {
  const text = 'x'.repeat(TOOL_PREVIEW_CHARACTERS - 1) + '🙂tail';
  const result = previewToolText(text);
  assert.equal(result.truncated, true);
  assert.equal(result.text.length, TOOL_PREVIEW_CHARACTERS - 1);
  assert.equal(previewToolText('x'.repeat(TOOL_PREVIEW_CHARACTERS)).truncated, false);
});

test('multiline previews stop at the line cap and mark omitted content', () => {
  const result = previewToolText('line\n'.repeat(TOOL_PREVIEW_LINES) + 'OMITTED');
  assert.equal(result.text, 'line\n'.repeat(TOOL_PREVIEW_LINES));
  assert.equal(result.truncated, true);
});

test('split text and structured results share a single preview budget', () => {
  const content = [{ type: 'text', text: 'x'.repeat(30_000) }, { type: 'text', text: 'y'.repeat(30_000) }];
  const result = previewToolResult(content, { omitted: 'tail' });
  assert.equal(result.content.map(item => item.text ?? '').join('').length, TOOL_PREVIEW_CHARACTERS);
  assert.equal(result.structuredText, '');
  assert.equal(result.truncated, true);
  assert.equal(content[1].text.length, 30_000);
});

test('small payloads retain their normal content and large payloads offer download', () => {
  const normal = renderToStaticMarkup(createElement(ToolArgumentsPreview, { arguments: '{"path":"file.txt"}', name: 'read_file' }));
  assert.match(normal, /file.txt/);
  assert.doesNotMatch(normal, /Download full content/);
  const large = renderToStaticMarkup(createElement(ToolResultPreview, { content: [], structured: { text: 'x'.repeat(60_000) + 'HIDDEN_SUFFIX' }, name: 'read_file' }));
  assert.match(large, /Download full content/);
  assert.match(large, /50,000 characters or 500 lines/);
  assert.doesNotMatch(large, /HIDDEN_SUFFIX/);
});
