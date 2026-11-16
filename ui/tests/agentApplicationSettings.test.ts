import test from 'node:test';
import assert from 'node:assert/strict';
import { selectedRemote, withRemoteSelection, type ResourceConfig } from '../src/agent/settings/types.js';
import { buildCondition, conditionSummary, draftCondition, fieldOperators, parsePolicy, retainRuleMetadata, schemaPaths } from '../src/agent/settings/policyState.js';
import type { Condition, Policy } from '../src/agent/settings/types.js';

test('unchecking a discovered MCP tool preserves automatic selection of future tools', () => {
  const source: ResourceConfig = { kind: 'mcp', name: 'Local', enabled: true, include: null, exclude: [] };
  const changed = withRemoteSelection(source, 'write', false);
  assert.equal(selectedRemote(changed, 'write'), false);
  assert.equal(selectedRemote(changed, 'read'), true);
  assert.equal(selectedRemote(changed, 'new-tool'), true);
  assert.equal(selectedRemote(withRemoteSelection(changed, 'write', true), 'write'), true);
  assert.deepEqual(source.exclude, []);
});

test('an explicit MCP tool selection stays bounded when enabling another tool', () => {
  const source: ResourceConfig = { kind: 'mcp', name: 'Local', enabled: true, include: ['read'], exclude: ['write'] };
  const changed = withRemoteSelection(source, 'write', true);
  assert.equal(selectedRemote(changed, 'write'), true);
  assert.equal(selectedRemote(changed, 'unknown'), false);
  assert.deepEqual(withRemoteSelection(changed, 'read', false).include, ['write']);
});

test('parameter suggestions resolve command unions and quote special field names', () => {
  const schema = { properties: { command: { oneOf: [{ $ref: '#/$defs/Exec' }, { $ref: '#/$defs/Shell' }] }, 'a/b': { properties: { 'x~y': {} } } }, $defs: { Exec: { properties: { program: {}, args: {} } }, Shell: { properties: { script: {} } } } };
  assert.deepEqual(schemaPaths(schema), ['args.command', 'args.command.program', 'args.command.args', 'args.command.script', 'args["a/b"]', 'args["a/b"]["x~y"]']);
});

test('recursive Tool schemas cannot expand the policy editor indefinitely', () => {
  const paths = schemaPaths({ properties: { node: { $ref: '#/$defs/Node' } }, $defs: { Node: { properties: { next: { $ref: '#/$defs/Node' } } } } });
  assert.ok(paths.length > 1);
  assert.ok(paths.length < 200);
});

test('known field types offer appropriate operators while custom fields remain extensible', () => {
  assert.ok(fieldOperators({ type: 'string' }).includes('regex'));
  assert.ok(!fieldOperators({ type: 'string' }).includes('all_items'));
  assert.ok(fieldOperators({ type: 'array' }).includes('all_items'));
  assert.ok(!fieldOperators({ type: 'boolean' }).includes('gt'));
  assert.ok(fieldOperators(undefined).includes('regex'));
});

test('malformed advanced policy JSON cannot enter or crash the rule builder', () => {
  for (const value of [null, [], { default: 'allow', rules: [null] }, { default: 'ask', rules: [{ decision: 'allow', match: 'all', conditions: [null] }] }]) {
    assert.throws(() => parsePolicy(JSON.stringify(value)));
  }
  const policy = { default: 'ask', rules: [{ decision: 'allow', when: { field: 'args.path', op: 'glob', value: '*.md' } }] };
  assert.deepEqual(parsePolicy(JSON.stringify(policy)), policy);
  assert.deepEqual(parsePolicy('{}'), { default: 'allow', rules: [] });
});

test('nested conditions roundtrip between builder and JSON without losing arrays or negation', () => {
  const when: Condition = { all: [
    { field: 'operation.attributes.path', op: 'regex', value: '^src/', flags: 'i' },
    { not: { field: 'args.overwrite', op: 'equals', value: true } },
    { field: 'args.edits', op: 'any_item', value: { any: [
      { field: 'new_text', op: 'contains', value: 'secret' }, { field: '$', op: 'equals', value: null },
    ] } },
  ] };
  assert.deepEqual(buildCondition(draftCondition(when)), when);
  assert.deepEqual(parsePolicy(JSON.stringify({ rules: [{ when, decision: 'ask' }] })).rules[0].when, when);
});

test('empty groups and mixed condition structures cannot be silently discarded', () => {
  for (const when of [{ all: [] }, { any: [] }, { all: [], any: [] }, { not: null }, { field: 'args.x', op: 'unknown', value: 1 }]) {
    assert.throws(() => parsePolicy(JSON.stringify({ rules: [{ when, decision: 'deny' }] })));
  }
});

test('saving a converted policy retains hidden metadata on unchanged rules', () => {
  const original: Policy = { default: 'allow', rules: [
    { id: 'external', reason: 'Outside Workspace', decision: 'ask', when: { field: 'operation.attributes.scope', op: 'equals', value: 'external' } },
  ] };
  const draft: Policy = { default: 'deny', rules: [
    { ...original.rules[0], id: undefined, reason: undefined },
    { ...original.rules[0], id: undefined, reason: undefined, decision: 'deny' },
  ] };
  const saved = retainRuleMetadata(draft, original);
  assert.equal(saved.rules[0].id, 'external');
  assert.equal(saved.rules[0].reason, 'Outside Workspace');
  assert.equal(saved.rules[1].id, undefined);
});

test('rule summaries communicate workspace checks, negation and missing fields', () => {
  const summarize = (when: Condition) => conditionSummary(draftCondition(when));
  assert.equal(summarize({ field: 'operation.attributes.scope', op: 'equals', value: 'external' }), 'Outside the workspace');
  assert.equal(summarize({ field: 'args.path', op: 'prefix', value: 'vendor/' }), 'args.path starts with "vendor/"');
  assert.equal(summarize({ not: { field: 'args.path', op: 'exists', value: false } }), 'Not: args.path is missing');
});
