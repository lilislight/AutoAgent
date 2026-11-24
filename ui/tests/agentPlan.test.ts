import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { PlanDetails, SessionPlan } from '../src/agent/SessionPlan.js';
import { planChanged } from '../src/agent/eventRefresh.js';
import type { AgentEvent, TaskPlan } from '../src/agent/types.js';

const plan: TaskPlan = { plan: [{ step: 'Inspect', status: 'completed' as const }, { step: 'Ship', status: 'in_progress' }], explanation: 'Verify the remaining changes.' };

test('plan details communicate status and escape text with no editing or clearing actions', () => {
  const html = renderToStaticMarkup(createElement(PlanDetails, { plan: { ...plan,
    plan: [...plan.plan, { step: '<script>unsafe()</script>', status: 'pending' }] } }));
  assert.match(html, /1 of 3 completed/);
  assert.match(html, /aria-label="In progress"/);
  assert.match(html, /&lt;script&gt;/);
  assert.doesNotMatch(html, /<script>|<button|<input|Edit plan|Clear plan/);
  const completed = renderToStaticMarkup(createElement(PlanDetails, { plan: { ...plan,
    plan: plan.plan.map(item => ({ ...item, status: 'completed' as const })) } }));
  assert.match(completed, />Completed</);
  assert.match(completed, /Inspect/);
  assert.match(completed, /Ship/);
});

test('completed plans retain a viewing trigger while an absent plan has no control', () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  client.setQueryData(['agent-plan', 'done'], { plan: { ...plan, plan: plan.plan.map(item => ({ ...item, status: 'completed' as const })) } });
  client.setQueryData(['agent-plan', 'empty'], { plan: null });
  const render = (session: string, existing = true) => renderToStaticMarkup(createElement(QueryClientProvider, { client },
    createElement(SessionPlan, { session, active: false, existing, mobile: false })));
  assert.match(render('done'), /View plan, 2 of 2 steps completed/);
  assert.equal(render('empty'), '');
  assert.equal(render('done', false), '');
  client.clear();
});

test('only durable plan tool results and lifecycle boundaries refresh current plan state', () => {
  const event = (state: string, persist = true, name = 'update_plan') => ({ kind: 'agent.tools', persist, payload: { tools: [{ name, state }] } }) as AgentEvent;
  assert.equal(planChanged([event('completed')]), true);
  assert.equal(planChanged([event('completed', false)]), false);
  assert.equal(planChanged([event('running')]), false);
  assert.equal(planChanged([event('completed', true, 'read_file')]), false);
  assert.equal(planChanged([{ kind: 'agent.completed' } as AgentEvent]), true);
});
