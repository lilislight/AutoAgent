import { Ajv, type ErrorObject } from 'ajv';
import { findNodeAtLocation, getNodePath, getLocation, parseTree, printParseErrorCode, type Node, type ParseError } from 'jsonc-parser';

export interface FieldError { path: (string | number)[]; code: string; message: string }
export interface JsonIssue extends FieldError { from: number; to: number }
type Schema = Record<string, any>;
const ajv = new Ajv({ allErrors: true, strict: false, validateFormats: false });
const NULL_SCHEMA = { type: 'null' };
const validators = new WeakMap<object, ReturnType<typeof ajv.compile>>();
export function resolveSchema(value: unknown, root: unknown): Schema {
  if (!value || typeof value !== 'object') return {};
  let schema = value as Schema;
  const seen = new Set();
  while (schema.$ref?.startsWith('#/') && !seen.has(schema.$ref)) {
    seen.add(schema.$ref);
    const target = schema.$ref.slice(2).split('/').reduce((item: any, key: string) => item?.[key.replace(/~1/g, '/').replace(/~0/g, '~')], root);
    if (!target) break;
    schema = { ...schema, ...target }; if (target.$ref === undefined) delete schema.$ref;
  }
  if (schema.allOf) schema = schema.allOf.reduce((merged: Schema, part: unknown) => {
    const item = resolveSchema(part, root);
    return { ...merged, ...item, properties: { ...merged.properties, ...item.properties }, required: [...(merged.required ?? []), ...(item.required ?? [])] };
  }, { ...schema, allOf: undefined });
  return schema;
}
export function schemaAt(root: unknown, path: (string | number)[]): Schema {
  let schema = resolveSchema(root, root);
  for (const key of path) {
    const variants = schema.oneOf ?? schema.anyOf;
    if (variants) schema = resolveSchema(variants.find((item: unknown) => {
      const candidate = resolveSchema(item, root); return typeof key === 'number' ? candidate.items : candidate.properties?.[key];
    }) ?? variants[0], root);
    schema = resolveSchema(typeof key === 'number' ? schema.prefixItems?.[key] ?? schema.items : schema.properties?.[key] ?? schema.additionalProperties, root);
  }
  return schema;
}
export function jsonExample(root: unknown, value: unknown = root): unknown {
  if (root === null) return null;
  const make = (value: unknown, depth: number): unknown => {
    if (depth > 7) return null;
    const schema = resolveSchema(value, root);
    if (schema.examples?.length) return schema.examples[0];
    if ('example' in schema) return schema.example;
    if ('default' in schema) return schema.default;
    if ('const' in schema) return schema.const;
    if (schema.enum?.length) return schema.enum[0];
    if (schema.oneOf || schema.anyOf) return make((schema.oneOf ?? schema.anyOf).find((item: Schema) => item.type !== 'null') ?? (schema.oneOf ?? schema.anyOf)[0], depth + 1);
    const type = Array.isArray(schema.type) ? schema.type.find((item: string) => item !== 'null') : schema.type;
    if (type === 'null') return null;
    if (type === 'object' || schema.properties) return Object.fromEntries(Object.entries(schema.properties ?? {}).filter(([, item]) => !(item as Schema).readOnly).map(([key, item]) => [key, make(item, depth + 1)]));
    if (type === 'array') return Array.from({ length: Math.min(3, Math.max(1, schema.minItems ?? 1)) }, (_, index) => make(schema.prefixItems?.[index] ?? schema.items, depth + 1));
    if (type === 'boolean') return false;
    if (type === 'integer' || type === 'number') return Math.max(schema.minimum ?? 0, schema.exclusiveMinimum != null ? schema.exclusiveMinimum + 1 : -Infinity);
    if (type === 'string') return schema.format === 'date-time' ? '2026-01-01T00:00:00Z' : schema.format === 'date' ? '2026-01-01' : 'string'.padEnd(Math.min(256, schema.minLength ?? 0), 'x').slice(0, schema.maxLength ?? Infinity);
    return {};
  };
  return make(value, 0);
}
function errorPath(error: ErrorObject): (string | number)[] {
  const path = error.instancePath ? error.instancePath.slice(1).split('/').map(part => part.replace(/~1/g, '/').replace(/~0/g, '~')) : [];
  if (error.keyword === 'required') path.push(error.params.missingProperty);
  if (error.keyword === 'additionalProperties') path.push(error.params.additionalProperty);
  return path;
}
export function locateIssue(tree: Node | undefined, text: string, error: FieldError): JsonIssue {
  const path: (string | number)[] = [];
  for (const segment of error.path) {
    const parent = tree && findNodeAtLocation(tree, path);
    path.push(parent?.type === 'array' && /^\d+$/.test(String(segment)) ? Number(segment) : segment);
  }
  let node = tree && findNodeAtLocation(tree, path);
  while (!node && path.length) { path.pop(); node = tree && findNodeAtLocation(tree, path); }
  // Missing properties point at the parent object rather than another same-named field.
  const from = node?.offset ?? 0;
  return { ...error, from, to: Math.min(text.length, from + Math.max(1, node?.length ?? 1)) };
}
export function jsonIssues(text: string, schema?: unknown, external: FieldError[] = []): JsonIssue[] {
  const errors: ParseError[] = [];
  const tree = parseTree(text, errors, { allowTrailingComma: false, disallowComments: true });
  if (errors.length) return errors.map(error => ({ path: [], code: 'syntax', message: printParseErrorCode(error.error), from: error.offset, to: Math.min(text.length, error.offset + Math.max(1, error.length)) }));
  let value: unknown;
  try { value = JSON.parse(text); } catch { return [{ path: [], code: 'syntax', message: 'Expected a JSON value.', from: 0, to: text.length }]; }
  const issues: FieldError[] = [];
  if (schema !== undefined) {
    try {
      const contract = schema === null ? NULL_SCHEMA : schema;
      let validate = typeof contract === 'object' && contract ? validators.get(contract) : undefined;
      if (!validate) { validate = ajv.compile(contract as object); if (typeof contract === 'object' && contract) { ajv.removeSchema(contract); validators.set(contract, validate); } }
      if (!validate(value)) issues.push(...(validate.errors ?? []).map(error => ({ path: errorPath(error), code: error.keyword, message: error.message ?? 'Invalid value.' })));
    } catch { /* Unsupported schema features remain authoritative on the Server. */ }
  }
  return [...issues, ...external].slice(0, 30).map(error => locateIssue(tree, text, error));
}
export function jsonCompletion(text: string, position: number, schema: unknown) {
  const location = getLocation(text, position);
  const path = location.isAtPropertyKey ? location.path.slice(0, -1) : location.path;
  const target = schemaAt(schema, path);
  const tree = parseTree(text);
  const object = tree && findNodeAtLocation(tree, path);
  const existing = new Set(object?.type === 'object' ? object.children?.map(child => child.children?.[0]?.value) : []);
  return { location, target, properties: Object.entries(target.properties ?? {}).filter(([key]) => !existing.has(key)).map(([key, raw]) => ({ key, schema: resolveSchema(raw, schema), required: target.required?.includes(key) })) };
}
export function jsonHover(text: string, position: number, schema: unknown) {
  const tree = parseTree(text); if (!tree) return null;
  const find = (node: Node): Node => node.children?.find(child => child.offset <= position && child.offset + child.length >= position) ? find(node.children.find(child => child.offset <= position && child.offset + child.length >= position)!) : node;
  const hovered = find(tree);
  let node = hovered;
  if (node.parent?.type === 'property' && node.parent.children?.[0] === node) node = node.parent.children[1] ?? node;
  const path = getNodePath(node); const target = schemaAt(schema, path);
  return { from: hovered.offset, to: hovered.offset + hovered.length, schema: target, path };
}
