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

const actions: ResourceActions = { pending: null, save: async () => {}, run() {}, editPolicy() {} };
function group(id: string, origin: Resource['origin'], name: string, tool: string): Group {
  const resource: Resource = { management_id: `${id}/${tool}`, group_id: id, group_name: name, resource_id: tool,
    kind: 'tool', name: tool, origin, description: '', available: true, enabled: true, custom_policy: null, permissions_editable: true };
  return { id, name, origin, resources: [resource], enabled: true, permissions: null, permissions_editable: true };
}
function render(groups: Group[], onlyGroup?: string) {
  const inventory: Inventory = { items: [], resources: groups.flatMap(group => group.resources), groups,
    workspace_id: 'workspace', revision: 1, edit_code_permissions: true, policy_schema: {} };
  return renderToStaticMarkup(createElement(ResourcePanel, { inventory, actions, onlyGroup }));
}

test('Built-in Tools expose permission editing while group actions only toggle availability', () => {
  const html = render([
    group('builtin/files', 'builtin', 'Files', 'read_file'),
    group('builtin/commands', 'builtin', 'Commands', 'execute_command'),
    group('builtin/skill-tools', 'builtin', 'Skill Tools', 'skill_load'),
    group('builtin/collaboration', 'builtin', 'Agents', 'spawn_agent'),
  ]);
  assert.equal(html.match(/role="switch"/g)?.length, 8);
  assert.match(html, /Enable read_file/);
  assert.match(html, /Enable execute_command/);
  assert.match(html, /Rules for read_file/);
  assert.match(html, /Rules for execute_command/);
  assert.doesNotMatch(html, /Rules for Files|Rules for Commands/);
  assert.doesNotMatch(html, /<select/);
});

test('MCP tools appear only inside their own server', () => {
  const groups = [group('project/test', 'project', 'Project Tools', 'publish'), group('mcp/test', 'mcp', 'MCP Tools', 'lookup')];
  const tools = render(groups);
  assert.match(tools, /Rules for publish/);
  assert.doesNotMatch(tools, /Rules for lookup|Rules for Project Tools/);
  const mcp = render(groups, 'mcp/test');
  assert.match(mcp, /Rules for lookup/);
  assert.doesNotMatch(mcp, /Rules for publish|Rules for MCP Tools/);
});
