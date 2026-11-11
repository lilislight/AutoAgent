import test from 'node:test';
import assert from 'node:assert/strict';
import { selectedRemote, withRemoteSelection, type ResourceConfig } from '../src/agent/settings/types.js';
import { parsePolicy, schemaPaths } from '../src/agent/settings/policyState.js';

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

test('parameter suggestions resolve command unions and escape JSON Pointer fields', () => {
  const schema = { properties: { command: { oneOf: [{ $ref: '#/$defs/Exec' }, { $ref: '#/$defs/Shell' }] }, 'a/b': { properties: { 'x~y': {} } } }, $defs: { Exec: { properties: { program: {}, args: {} } }, Shell: { properties: { script: {} } } } };
  assert.deepEqual(schemaPaths(schema), ['/command', '/command/program', '/command/args', '/command/script', '/a~1b', '/a~1b/x~0y']);
});

test('recursive Tool schemas cannot expand the policy editor indefinitely', () => {
  const paths = schemaPaths({ properties: { node: { $ref: '#/$defs/Node' } }, $defs: { Node: { properties: { next: { $ref: '#/$defs/Node' } } } } });
  assert.ok(paths.length > 1);
  assert.ok(paths.length < 200);
});

test('malformed advanced policy JSON cannot enter or crash the rule builder', () => {
  for (const value of [null, [], { default: 'allow', rules: [null] }, { default: 'ask', rules: [{ decision: 'allow', match: 'all', conditions: [null] }] }]) {
    assert.throws(() => parsePolicy(JSON.stringify(value)));
  }
  const policy = { default: 'ask', rules: [{ decision: 'allow', match: 'all', conditions: [{ source: 'arguments', path: '/path', operator: 'glob', value: '*.md' }] }] };
  assert.deepEqual(parsePolicy(JSON.stringify(policy)), policy);
});
