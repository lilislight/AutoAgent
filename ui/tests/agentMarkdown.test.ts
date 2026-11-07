import assert from 'node:assert/strict';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import test from 'node:test';
import { Markdown } from '../src/agent/Markdown.js';

const render = (text: string) => renderToStaticMarkup(createElement(Markdown, { text }));

test('headings do not absorb the following ordered list or paragraph', () => {
  const html = render('# 项目方案\n\n## 1. 核心功能\n1. **快速收集**：记录任务。\n2. 日程与提醒。\n\n## 2. 推荐技术栈\n小型项目推荐 **Vue**。\n对比其他选项。');
  assert.match(html, /<h1>项目方案<\/h1>/);
  assert.match(html, /<h2>1\. 核心功能<\/h2>\n<ol>/);
  assert.match(html, /<li><strong>快速收集<\/strong>：记录任务。<\/li>/);
  assert.match(html, /<h2>2\. 推荐技术栈<\/h2>\n<p>小型项目推荐/);
});

test('GFM tables render as scrollable semantic tables', () => {
  const html = render('| 字段 | 类型 |\n|---|---|\n| id | BIGINT |\n| status | `pending` |');
  assert.match(html, /class="agent-markdown-table"/);
  assert.match(html, /<thead><tr><th>字段<\/th><th>类型<\/th><\/tr><\/thead>/);
  assert.match(html, /<td><code>pending<\/code><\/td>/);
  assert.equal((html.match(/<tbody><tr>|<\/tr><tr>/g) ?? []).length, 2);
});

test('nested lists, task items, quotes and fenced code keep their structure', () => {
  const html = render('- Parent\n  - Child with *emphasis* and ~~removed~~\n- [x] Done\n\n> Quoted\n\n~~~python\nprint("hello")\n~~~');
  assert.match(html, /<li>Parent\n<ul>/);
  assert.match(html, /<em>emphasis<\/em> and <del>removed<\/del>/);
  assert.match(html, /type="checkbox" disabled="" checked=""/);
  assert.match(html, /<blockquote>\n<p>Quoted<\/p>/);
  assert.match(html, /<pre><code class="language-python">print\(&quot;hello&quot;\)/);
});

test('streaming responses tolerate unfinished markdown', () => {
  assert.match(render('## Working\n\n```json\n{"ready":'), /<h2>Working<\/h2>[\s\S]*<pre><code class="language-json">/);
  assert.match(render('Text with **unfinished'), /Text with \*\*unfinished/);
});

test('raw HTML and unsafe URLs cannot become executable markup', () => {
  const html = render('<script>alert(1)</script>\n\n<img src=x onerror=alert(1)>\n\n[unsafe](javascript:alert%281%29) ![bad](data:image/svg+xml;base64,QQ==) [safe](https://example.com/path)');
  assert.doesNotMatch(html, /<script|<img|javascript:|onerror=|data:image\/svg/);
  assert.match(html, /<span>unsafe<\/span>/);
  assert.match(html, /href="https:\/\/example.com\/path" rel="noopener noreferrer" target="_blank"/);
});
