import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import type { ResourceActions } from '../src/agent/settings/ResourcePanel.js';
import type { Group, Inventory, Resource } from '../src/agent/settings/types.js';

async function loadPanel() {
  const previous = Object.getOwnPropertyDescriptor(globalThis, 'document');
  Object.defineProperty(globalThis, 'document', { configurable: true, value: { getElementById: () => null } });
  try { return await import('../src/agent/settings/ResourcePanel.js'); }
  finally {
    if (previous) Object.defineProperty(globalThis, 'document', previous);
    else Reflect.deleteProperty(globalThis, 'document');
  }
}
const { ResourcePanel } = await loadPanel();

const actions: ResourceActions = { pending: null, save: async () => {}, run() {}, editPolicy() {}, saveSourcePolicy: async () => {} };
function group(id: string, origin: Resource['origin'], name: string, tool: string): Group {
  const resource: Resource = { management_id: `${id}/${tool}`, group_id: id, group_name: name, resource_id: tool,
    kind: 'tool', name: tool, origin, description: '', available: true, enabled: true, permissions: null,
    default_decision: 'ask', parameter_policy: true, permission_source: 'registration', permissions_editable: true };
  return { id, name, origin, resources: [resource], enabled: true, permissions: null, permissions_editable: true };
}
function render(groups: Group[]) {
  const inventory: Inventory = { items: [], resources: groups.flatMap(group => group.resources), groups,
    workspace_id: 'workspace', revision: 1, edit_code_permissions: true, policy_schema: {} };
  return renderToStaticMarkup(createElement(ResourcePanel, { inventory, actions }));
}

test('every Built-in group and Tool offers only Enable even with older editable metadata', () => {
  const html = render([
    group('builtin/files', 'builtin', 'Files', 'read_file'),
    group('builtin/commands', 'builtin', 'Commands', 'execute_command'),
    group('builtin/skill-tools', 'builtin', 'Skill Tools', 'skill_load'),
    group('builtin/collaboration', 'builtin', 'Agents', 'spawn_agent'),
  ]);
  assert.equal(html.match(/role="switch"/g)?.length, 8);
  assert.match(html, /Enable read_file/);
  assert.match(html, /Enable execute_command/);
  assert.doesNotMatch(html, /<select|Rules for| permission"/);
});

test('Project and MCP groups retain their rule editors, permission choices and Enable switches', () => {
  const html = render([group('project/test', 'project', 'Project Tools', 'publish'), group('mcp/test', 'mcp', 'MCP Tools', 'lookup')]);
  assert.match(html, /Rules for publish/);
  assert.match(html, /Rules for lookup/);
  assert.match(html, /Rules for Project Tools/);
  assert.match(html, /Rules for MCP Tools/);
  assert.equal(html.match(/<select/g)?.length, 4);
  assert.equal(html.match(/<option /g)?.length, 12);
  assert.equal(html.match(/value="ask" selected=""/g)?.length, 2);
  assert.equal(html.match(/value="allow" selected=""/g)?.length, 2);
  assert.doesNotMatch(html, /Code policy|Follow code/);
  assert.equal(html.match(/role="switch"/g)?.length, 4);
});
