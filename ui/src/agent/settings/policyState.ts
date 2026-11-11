import type { Policy } from './types.js';

export function parsePolicy(text: string): Policy {
  const value: unknown = JSON.parse(text);
  const object = (item: unknown): item is Record<string, unknown> => !!item && typeof item === 'object' && !Array.isArray(item);
  const decisions = ['allow', 'ask', 'deny'];
  const operators = ['equals', 'not_equals', 'in', 'not_in', 'glob', 'exists', 'prefix', 'contains', 'lt', 'lte', 'gt', 'gte', 'regex'];
  if (!object(value) || !decisions.includes(String(value.default)) || !Array.isArray(value.rules) || value.rules.some(rule =>
    !object(rule) || !decisions.includes(String(rule.decision)) || !['all', 'any'].includes(String(rule.match)) ||
    (rule.reason != null && typeof rule.reason !== 'string') || !Array.isArray(rule.conditions) || rule.conditions.some(item =>
      !object(item) || !['arguments', 'operation', 'call'].includes(String(item.source)) || typeof item.path !== 'string' ||
      (item.path !== '' && !item.path.startsWith('/')) || !operators.includes(String(item.operator))))) {
    throw new Error('Use a policy with a default decision and rules containing valid conditions, match and decision fields.');
  }
  return value as unknown as Policy;
}

export function schemaPaths(schema: Record<string, unknown> | undefined): string[] {
  const paths = new Set<string>();
  const visit = (node: Record<string, unknown> | undefined, prefix: string, depth: number) => {
    if (!node || depth > 10 || paths.size >= 200) return;
    if (typeof node.$ref === 'string' && node.$ref.startsWith('#/')) {
      let resolved: unknown = schema;
      for (const token of node.$ref.slice(2).split('/')) resolved = (resolved as Record<string, unknown> | undefined)?.[token.replaceAll('~1', '/').replaceAll('~0', '~')];
      visit(resolved as Record<string, unknown> | undefined, prefix, depth + 1);
    }
    for (const [name, child] of Object.entries((node.properties ?? {}) as Record<string, Record<string, unknown>>)) {
      if (paths.size >= 200) break;
      const path = prefix + '/' + name.replaceAll('~', '~0').replaceAll('/', '~1');
      paths.add(path); visit(child, path, depth + 1);
    }
    for (const key of ['oneOf', 'anyOf', 'allOf']) if (Array.isArray(node[key])) for (const child of node[key]) visit(child, prefix, depth + 1);
  };
  visit(schema, '', 0);
  return [...paths];
}
