import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { ExecutionTrace } from '../src/types.js';
import { executionGraphStates } from '../src/executionGraphState.js';

function trace(): ExecutionTrace {
  return { observed_at_us: 100, last_event: null, invocation: { invocation_id: 'root', workflow_id: 'loop', workflow_revision_id: 'rev', root_invocation_id: 'root', status: 'running', control_state: 'active', values: [] }, nodes: [
    { node_id: 'step', occurrence_id: 'second', status: 'running', occurrences: 2 },
    { node_id: 'done', occurrence_id: 'done', status: 'completed', occurrences: 1 },
    { node_id: 'skip', occurrence_id: 'skip', status: 'skipped', occurrences: 1 },
  ], edges: [{ edge_id: 'back', condition_result: false, evaluations: 4 }, { edge_id: 'entry', condition_result: true, evaluations: 1 }],
  active_routes: [{ edge_id: 'back', source_occurrence_id: 'first', target_occurrence_id: 'second', target_node_id: 'step', status: 'running' }], child_count: 0, frontier: 1, view: 'durable' };
}

test('current consumed routes override older Loop decisions while other inputs stay static', () => {
  const view = executionGraphStates(trace());
  assert.equal(view.edges.get('back'), 'active');
  assert.equal(view.edges.get('entry'), 'taken');
  assert.equal(view.nodes.get('skip'), 'skipped');
  assert.equal(view.unrecordedNode, 'not-started');
});

test('paused control changes active displays without recoloring completed or skipped Nodes', () => {
  const data = trace(); data.invocation.control_state = 'paused';
  const view = executionGraphStates(data);
  assert.equal(view.nodes.get('step'), 'paused');
  assert.equal(view.nodes.get('done'), 'completed');
  assert.equal(view.nodes.get('skip'), 'skipped');
  assert.equal(view.edges.get('back'), 'paused');
});

test('concurrent activations prioritize running over waiting without inventing activity', () => {
  const data = trace();
  data.active_routes.unshift({ ...data.active_routes[0], target_occurrence_id: 'other', status: 'waiting' });
  assert.equal(executionGraphStates(data).edges.get('back'), 'active');
  data.active_routes = [data.active_routes[0]];
  assert.equal(executionGraphStates(data).edges.get('back'), 'waiting');
  data.active_routes = [];
  data.invocation.status = 'completed';
  const view = executionGraphStates(data);
  assert.equal(view.edges.get('back'), 'not-taken');
  assert.equal(view.unrecordedNode, 'not-run');
});
