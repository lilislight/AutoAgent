import { test } from 'node:test';
import assert from 'node:assert/strict';
import { jsonExample, jsonIssues, jsonCompletion, jsonHover } from '../src/jsonSchema.js';
const schema = { type: 'object', properties: { items: { type: 'array', items: { $ref: '#/$defs/Item' } }, name: { type: 'string', default: 'demo' } }, required: ['items'], $defs: { Item: { type: 'object', properties: { count: { type: 'integer', minimum: 1, description: 'Number of jobs' } }, required: ['count'] } } };
test('Examples resolve nested references, defaults, arrays and null inputs', () => {
  assert.deepEqual(jsonExample(schema), { items: [{ count: 1 }], name: 'demo' });
  assert.equal(jsonExample(null), null);
  assert.equal(jsonIssues('{}', false).length, 1);
  assert.equal(jsonIssues('null', null).length, 0);
  assert.deepEqual(jsonExample(undefined), {});
});
test('Field errors locate the exact array item instead of another same-named key', () => {
  const text = '{"items":[{"count":1},{"count":"bad"}]}';
  const issues = jsonIssues(text, schema);
  assert.equal(issues.length, 1);
  assert.equal(text.slice(issues[0].from, issues[0].to), '"bad"');
  const missing = jsonIssues('{"items":[{}]}', schema)[0];
  assert.equal('{"items":[{}]}'.slice(missing.from, missing.to), '{}');
});
test('Syntax and Server field errors preserve accurate source ranges', () => {
  assert.equal(jsonIssues('{"a":}', undefined)[0].code, 'syntax');
  const text = '{"items":[{"count":1}]}';
  const errors = jsonIssues(text, schema, [{ path: ['items', 0, 'count'], code: 'custom', message: 'Rejected' }]);
  assert.equal(text.slice(errors[0].from, errors[0].to), '1');
});
test('Completion and hover use the nested contract and omit existing properties', () => {
  const partial = '{"items":[{ }]}';
  const completion = jsonCompletion(partial, partial.indexOf(' }'), schema);
  assert.equal(completion.properties[0].key, 'count');
  assert.equal(completion.properties[0].required, true);
  const text = '{"items":[{"count":1}]}';
  const hover = jsonHover(text, text.indexOf('count') + 1, schema)!;
  assert.equal(hover.schema.description, 'Number of jobs');
  assert.equal(text.slice(hover.from, hover.to), '"count"');
  assert.equal(jsonIssues(text, schema).length, 0);
});
