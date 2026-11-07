import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { BusyLabel, CardsSkeleton, CodeSkeleton, DirectorySkeleton, FactsSkeleton, InlineLoading, OptionsSkeleton, RecordsSkeleton, TimelineSkeleton } from '../src/components/LoadingState.js';

test('each directory skeleton keeps the layout of its own row type', () => {
  const render = (kind: 'workflow' | 'session' | 'invocation' | 'conversation') => renderToStaticMarkup(createElement(DirectorySkeleton, { kind, count: 2, label: 'Loading rows' }));
  assert.match(render('workflow'), /directory-group-label/);
  assert.doesNotMatch(render('conversation'), /directory-group-label/);
  assert.match(render('session'), /session-item/);
  assert.match(render('session'), /<small>/);
  assert.match(render('invocation'), /invocation-row-heading/);
  assert.match(render('invocation'), /invocation-row-meta/);
  for (const kind of ['workflow', 'session', 'invocation', 'conversation'] as const) {
    assert.doesNotMatch(render(kind), /<button|<input|<select/);
    assert.equal((render(kind).match(/directory-skeleton-row/g) ?? []).length, 2);
  }
});

test('structured loaders expose one status and hide placeholder data', () => {
  for (const Component of [CardsSkeleton, CodeSkeleton, FactsSkeleton, OptionsSkeleton, RecordsSkeleton, TimelineSkeleton]) {
    const html = renderToStaticMarkup(createElement(Component, { label: 'Loading content' }));
    assert.equal((html.match(/role="status"/g) ?? []).length, 1);
    assert.match(html, /aria-label="Loading content"/);
    assert.match(html, /aria-hidden="true"/);
    assert.doesNotMatch(html, /loading-spinner/);
  }
});

test('facts preserve known labels while their values load', () => {
  const html = renderToStaticMarkup(createElement(FactsSkeleton, { label: 'Loading facts', labels: ['Started', 'Completed'] }));
  assert.match(html, /<dt>Started<\/dt>/);
  assert.match(html, /<dt>Completed<\/dt>/);
  assert.equal((html.match(/<dd>/g) ?? []).length, 2);
});

test('busy buttons reserve both labels and expose only the current one', () => {
  const render = (busy: boolean) => renderToStaticMarkup(createElement(BusyLabel, { busy, label: 'Saving', children: 'Save settings' }));
  assert.match(render(false), /busy-label-idle" aria-hidden="false"/);
  assert.match(render(false), /busy-label-progress" aria-hidden="true"/);
  assert.match(render(true), /busy-label-idle" aria-hidden="true"/);
  assert.match(render(true), /busy-label-progress" aria-hidden="false"/);
  assert.equal((render(true).match(/loading-spinner/g) ?? []).length, 1);
  const inline = renderToStaticMarkup(createElement(InlineLoading, { label: 'Checking contract' }));
  assert.equal((inline.match(/role="status"/g) ?? []).length, 1);
});
