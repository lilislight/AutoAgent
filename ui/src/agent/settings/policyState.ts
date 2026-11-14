import type { Condition, Operator, Policy } from './types.js';

export const operators: Operator[] = ['equals', 'not_equals', 'in', 'not_in', 'exists', 'prefix', 'suffix', 'contains', 'glob', 'regex', 'lt', 'lte', 'gt', 'gte', 'any_item', 'all_items'];
const object = (item: unknown): item is Record<string, unknown> => !!item && typeof item === 'object' && !Array.isArray(item);
const decisions = ['allow', 'ask', 'deny'];
export function parsePolicy(text: string): Policy {
  const value: unknown = JSON.parse(text);
  let nodes = 0;
  const valid = (item: unknown, depth = 0): boolean => {
    if (!object(item) || depth > 32 || ++nodes > 1024) return false;
    const keys = Object.keys(item);
    if ('all' in item || 'any' in item) {
      const children = item.all ?? item.any;
      return keys.length === 1 && Array.isArray(children) && children.length > 0 && children.length <= 256 && children.every(child => valid(child, depth + 1));
    }
    if ('not' in item) return keys.length === 1 && valid(item.not, depth + 1);
    if (keys.some(key => !['field', 'op', 'value', 'flags'].includes(key)) || typeof item.field !== 'string' || !item.field ||
        !operators.includes(item.op as Operator) || !('value' in item) || (item.flags !== undefined && (typeof item.flags !== 'string' || (item.op !== 'regex' && item.flags !== '')))) return false;
    return item.op === 'any_item' || item.op === 'all_items' ? valid(item.value, depth + 1) : true;
  };
  if (!object(value) || Object.keys(value).some(key => !['default', 'rules'].includes(key)) ||
      (value.default !== undefined && !decisions.includes(String(value.default))) ||
      (value.rules !== undefined && (!Array.isArray(value.rules) || value.rules.length > 256 || value.rules.some(rule =>
        !object(rule) || Object.keys(rule).some(key => !['id', 'when', 'decision', 'reason'].includes(key)) || !decisions.includes(String(rule.decision)) ||
        (rule.id != null && (typeof rule.id !== 'string' || !rule.id.trim())) || (rule.reason != null && typeof rule.reason !== 'string') || !valid(rule.when))))) {
    throw new Error('Use default and rules with when conditions and an allow, ask or deny decision.');
  }
  return { default: 'allow', rules: [], ...value } as Policy;
}

export function schemaFields(schema: Record<string, unknown> | undefined, prefix = 'args'): Record<string, Record<string, unknown>> {
  const paths = new Set<string>();
  const fields: Record<string, Record<string, unknown>> = {};
  const visit = (node: Record<string, unknown> | undefined, current: string, depth: number) => {
    if (!node || depth > 10 || paths.size >= 200) return;
    if (typeof node.$ref === 'string' && node.$ref.startsWith('#/')) {
      let resolved: unknown = schema;
      for (const token of node.$ref.slice(2).split('/')) resolved = (resolved as Record<string, unknown> | undefined)?.[token.replaceAll('~1', '/').replaceAll('~0', '~')];
      visit(resolved as Record<string, unknown> | undefined, current, depth + 1);
    }
    for (const [name, child] of Object.entries((node.properties ?? {}) as Record<string, Record<string, unknown>>)) {
      if (paths.size >= 200) break;
      const path = /^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ? `${current}.${name}` : `${current}[${JSON.stringify(name)}]`;
      paths.add(path); fields[path] = child; visit(child, path, depth + 1);
    }
    if (object(node.items)) { const path = current + '[0]'; paths.add(path); fields[path] = node.items; visit(node.items, path, depth + 1); }
    for (const key of ['oneOf', 'anyOf', 'allOf']) if (Array.isArray(node[key])) for (const child of node[key]) visit(child, current, depth + 1);
  };
  visit(schema, prefix, 0);
  return fields;
}

export const schemaPaths = (schema: Record<string, unknown> | undefined, prefix = 'args') => Object.keys(schemaFields(schema, prefix));
export function fieldOperators(schema: Record<string, unknown> | undefined): Operator[] {
  const common: Operator[] = ['equals', 'not_equals', 'in', 'not_in', 'exists'];
  if (!schema || typeof schema.type !== 'string') return operators;
  if (schema.type === 'string') return [...common, 'prefix', 'suffix', 'contains', 'glob', 'regex'];
  if (schema.type === 'array') return [...common, 'prefix', 'suffix', 'contains', 'any_item', 'all_items'];
  if (schema.type === 'number' || schema.type === 'integer') return [...common, 'lt', 'lte', 'gt', 'gte'];
  return common;
}

export interface DraftCondition { kind: 'field' | 'all' | 'any' | 'not'; field: string; op: Operator; valueText: string; flags: string; children: DraftCondition[] }
export const newCondition = (field = 'args.path'): DraftCondition => ({ kind: 'field', field, op: 'equals', valueText: 'null', flags: '', children: [] });
export function draftCondition(value: Condition): DraftCondition {
  if ('all' in value || 'any' in value || 'not' in value) {
    const kind = 'all' in value ? 'all' : 'any' in value ? 'any' : 'not';
    const children = 'all' in value ? value.all : 'any' in value ? value.any : [value.not];
    return { ...newCondition(), kind, children: children.map(draftCondition) };
  }
  const items = value.op === 'any_item' || value.op === 'all_items';
  return { ...newCondition(), field: value.field, op: value.op, flags: value.flags ?? '',
    valueText: JSON.stringify(value.value) ?? 'null', children: items ? [draftCondition(value.value as Condition)] : [] };
}
export function buildCondition(value: DraftCondition): Condition {
  if (value.kind === 'all' || value.kind === 'any') return { [value.kind]: value.children.map(buildCondition) } as Condition;
  if (value.kind === 'not') return { not: buildCondition(value.children[0]) };
  const items = value.op === 'any_item' || value.op === 'all_items';
  let parsed: unknown;
  try { parsed = JSON.parse(value.valueText); } catch { parsed = value.valueText; }
  return { field: value.field, op: value.op, value: items ? buildCondition(value.children[0]) : parsed,
    ...(value.op === 'regex' && value.flags ? { flags: value.flags } : {}) };
}
