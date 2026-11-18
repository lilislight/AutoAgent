"""End-to-end collaboration boundaries in the Core-only harness example."""

import asyncio
import time
import unittest

from autoagent import Node, Recovery, Workflow
from autoagent.core.app import AutoAgentApp
from examples.core_agent_harness_demo import (
    ApprovalRequest, ApprovalResponse, Decision, Draft, MockServices,
    RunSpec, Task, ToolCall, parent_workflow, run_scenario,
)


class CoreAgentHarnessDemoTests(unittest.IsolatedAsyncioTestCase):
    async def test_parallel_workers_revise_and_execute_only_after_approval(self):
        """Two Children revise via Signals and execute only the exact approved operations."""
        result = await run_scenario('approved')
        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.summary['status'], 'completed')
        self.assertEqual({t['runtime_status'] for t in result.summary['tasks']}, {'completed'})
        self.assertEqual(len(result.observations['effects']), 2)
        self.assertEqual(result.observations['llm_max_parallel'], 2)
        timeline = result.observations['timeline']
        for task_id in ('research', 'publish'):
            llm = [e for e in timeline if e['kind'] == 'child.llm.started' and e['task_id'] == task_id]
            self.assertEqual([e['attempt'] for e in llm], [1, 2])
            self.assertIn('Add evidence', llm[1]['feedback'])
            decisions = [e for e in timeline if e['kind'] == 'child.decision' and e['task_id'] == task_id]
            self.assertEqual([e['action'] for e in decisions], ['revise', 'approve'])
            approval = next(e for e in timeline if e['kind'] == 'external.approval_decision' and e['task_id'] == task_id)
            tool = next(e for e in timeline if e['kind'] == 'child.tool.called' and e['task_id'] == task_id)
            self.assertLess(approval['sequence'], tool['sequence'])
            self.assertEqual(approval['operation_key'], tool['operation_key'])
        external_signal = [e for e in timeline if e['kind'] == 'external.signal_did_not_resume_approval']
        self.assertEqual(len(external_signal), 1)
        self.assertTrue(external_signal[0]['duplicate_receipt'])
        self.assertEqual(len([e for e in timeline if e['kind'] == 'parent.external_message']), 1)
        # Canonical Runtime Events, rather than mock logs alone, confirm real waits/messages.
        counts = result.observations['runtime_events']
        self.assertGreater(counts['node_occurrence.waiting'], 0)
        self.assertGreater(counts['signal.accepted'], 0)

    async def test_rejection_prevents_the_effect_and_settles_both_children(self):
        """Denying publishing completes that Child without calling its tool."""
        result = await run_scenario('rejected')
        tasks = {t['task_id']: t for t in result.summary['tasks']}
        self.assertEqual(tasks['publish']['outcome'], 'rejected')
        self.assertEqual(tasks['publish']['runtime_status'], 'completed')
        self.assertEqual(tasks['research']['outcome'], 'executed')
        self.assertEqual(len(result.observations['effects']), 1)
        calls = [e['task_id'] for e in result.observations['timeline'] if e['kind'] == 'child.tool.called']
        self.assertEqual(calls, ['research'])

    async def test_child_failure_before_message_does_not_strand_the_parent(self):
        """Select observes an early failed Child while the other Child finishes normally."""
        result = await run_scenario('llm_failure')
        tasks = {t['task_id']: t for t in result.summary['tasks']}
        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.summary['status'], 'degraded')
        self.assertEqual(tasks['research']['runtime_status'], 'failed')
        self.assertIn('before sending a request', tasks['research']['error'])
        self.assertEqual(tasks['publish']['outcome'], 'executed')
        research_requests = [e for e in result.observations['timeline']
                             if e['kind'] == 'parent.review' and e['task_id'] == 'research']
        self.assertEqual(research_requests, [])

    async def test_child_tool_failure_is_observed_after_approval(self):
        """An approved tool failure becomes a Child failure in the Parent's final summary."""
        result = await run_scenario('tool_failure')
        tasks = {t['task_id']: t for t in result.summary['tasks']}
        self.assertEqual(result.summary['status'], 'degraded')
        self.assertEqual(tasks['publish']['runtime_status'], 'failed')
        self.assertIn('after approval', tasks['publish']['error'])
        self.assertEqual(tasks['research']['outcome'], 'executed')
        self.assertEqual(len(result.observations['effects']), 1)

    async def test_unrelated_human_approval_cannot_reach_a_tool(self):
        """A schema-valid approval for another operation fails before any tool executes."""
        services = MockServices()
        app = AutoAgentApp(runtime_event_sink=services)
        try:
            submitted = await app.asubmit_invoke(parent_workflow(services),
                                               RunSpec(run_id='wrong-approval').model_dump())
            deadline = time.monotonic() + 10
            resumed = False
            while time.monotonic() < deadline:
                status = await app.astatus(submitted.ref)
                if status.status in {'completed', 'failed', 'cancelled'}:
                    break
                external = next((w for w in status.waits if w.kind == 'external'), None)
                if external and not resumed:
                    request = ApprovalRequest.model_validate(external.request)
                    response = ApprovalResponse(operation_key=request.operation_key + '/unrelated',
                                                approved=True, reason='Wrong operation')
                    await app.asubmit_resume(submitted.ref, external.id, response.model_dump())
                    resumed = True
                await asyncio.sleep(0.005)
            else:
                self.fail('Mismatched approval did not settle')
            self.assertTrue(resumed)
            self.assertEqual(status.status, 'failed')
            self.assertIn('Approval does not match', status.error.message)
            self.assertEqual(services.snapshot()['effects'], {})
            self.assertFalse(any(e['kind'] == 'child.tool.called' for e in services.snapshot()['timeline']))
        finally:
            await app.aclose()

    async def test_mock_remote_tool_deduplicates_the_logical_operation(self):
        """The fake remote service returns one effect for repeated use of the same key."""
        services = MockServices()
        task = Task(run_id='idempotency', task_id='publish', instruction='Publish', tool='publish')
        draft = Draft(task_id=task.task_id, attempt=2, proposal='Reviewed', operation_key='idempotency/publish/tool/2')
        call = ToolCall(task=task, draft=draft,
                        decision=Decision(operation_key=draft.operation_key, action='approve', feedback='Approved'))
        services.approved.add(draft.operation_key)
        # Calls enter the real Core Operator adapter, even though the service is fake.
        app = AutoAgentApp()
        workflow = Workflow('deduplicate_tool', nodes=[Node('tool', services.tool,
                                                           recovery_mode=Recovery(mode='replay_safe'))])
        try:
            first = await app.ainvoke(workflow, call.model_dump())
            second = await app.ainvoke(workflow, call.model_dump())
            self.assertEqual(first.status, 'completed')
            self.assertEqual(second.status, 'completed')
            self.assertEqual(first.output, second.output)
            self.assertEqual(len(services.snapshot()['effects']), 1)
        finally:
            await app.aclose()


if __name__ == '__main__':
    unittest.main()
