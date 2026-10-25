"""Business Evaluation through real Hosts, fixed evidence and independent Trial history."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

from typing_extensions import TypedDict

from autoagent import AwaitSignal, Edge, Node, SignalEndpoint, Spawn, Wait, Workflow
from autoagent.evaluation import (CheckResult, EvalCase, EvalSuite, EventCount, OutputEquals,
    RunOptions, StatusIs, compare_runs, load_report, run_suite)
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.host.persistence import PersistenceConfig
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class Value(TypedDict):
    text: str


def echo(value: Value) -> Value:
    return value


def fail(value: Value) -> Value:
    raise ValueError('Business rejection: ' + value['text'])


async def echo_scenario(context):
    return await context.invoke('echo', context.case.input, entry_node_id='echo')


async def no_op(context):
    return None


class EvaluationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.counter = 0
        self.settings = HostSettings(persistence=PersistenceConfig(mode='buffered', batch_delay_ms=10000))
        self.databases = []

    async def asyncTearDown(self):
        for database in self.databases:
            await database.close()
        self.temporary.cleanup()

    def factory(self, workflows=None):
        @asynccontextmanager
        async def create(environment):
            url = environment.database_url
            if isinstance(self, PostgreSQLEvaluationTests):
                database = PostgreSQLDatabase()
                url = await database.create()
                self.databases.append(database)
            host = AutoAgentHost(workflows=workflows or [Workflow('echo', nodes=[Node('echo', echo)])],
                settings=replace(self.settings, database_url=url))
            async with host:
                yield host
        return create

    async def run_eval(self, suite, *, options=None, factory=None, case_ids=()):
        self.counter += 1
        return await run_suite(suite, host_factory=factory or self.factory(),
            directory=self.root / f'run-{self.counter}', options=options, case_ids=case_ids)

    def suite(self, *, scenario=echo_scenario, evaluators=(StatusIs(), OutputEquals()), cases=None):
        return EvalSuite('business', cases or (EvalCase('one', {'text': 'a'}, {'text': 'a'}),),
            scenario, evaluators)

    async def test_buffered_history_is_durable_before_checks_and_report_round_trips(self):
        """Buffered execution evidence is flushed, graded and retained without copying output into reports."""
        result = await self.run_eval(self.suite())
        self.assertTrue(result['ok'], result)
        trial = result['results'][0]
        self.assertEqual(trial['verdict'], 'passed')
        self.assertEqual(trial['observations'][0]['view'], 'durable')
        saved = load_report(self.root / 'run-1')
        self.assertEqual(saved['results'], result['results'])
        self.assertNotIn('output', trial)
        self.assertEqual(trial['observations'][0]['members'][0]['position']['sequence_scope'], 'session')

    async def test_wait_snapshots_do_not_change_after_response_or_cleanup(self):
        """Waiting and completed observations preserve their own status, Context and output cuts."""
        async def scenario(context):
            waiting = await context.invoke('approval', context.case.input, entry_node_id='wait', until='boundary')
            request = await waiting.wait_request(waiting.member()['waits'][0])
            context.check(CheckResult('request', request == context.case.input))
            completed = await context.respond(waiting, context.case.expected)
            context.check(CheckResult('request-after-response',
                await waiting.wait_request(waiting.member()['waits'][0]) == context.case.input))
            context.check(CheckResult('before-status', waiting.member()['status'] == 'waiting'))
            context.check(CheckResult('before-output', await waiting.value() is None))
            return completed
        result = await self.run_eval(self.suite(scenario=scenario),
            factory=self.factory([Workflow('approval', nodes=[Node('wait', Wait(Value, Value))])]))
        self.assertTrue(result['ok'], result)
        cuts = result['results'][0]['observations']
        self.assertLess(cuts[0]['members'][0]['position']['sequence'], cuts[-1]['members'][0]['position']['sequence'])

    async def test_parent_wait_response_and_children_have_independent_sequence_cuts(self):
        """A Scenario can approve a child and grade the whole family with per-Session evidence."""
        child = Workflow('child', nodes=[Node('approve', Wait(Value, Value))])
        parent = Workflow('parent', nodes=[Node('spawn', Spawn(child, 'approve'))])
        async def scenario(context):
            waiting = await context.invoke('parent', context.case.input, entry_node_id='spawn', until='boundary')
            completed = await context.respond(waiting, {'text': 'approved'}, node_id='approve')
            return completed
        suite = self.suite(scenario=scenario, evaluators=(StatusIs(), EventCount('invocation.completed', minimum=2, maximum=2, family=True)))
        result = await self.run_eval(suite, factory=self.factory([parent]))
        self.assertTrue(result['ok'], result)
        members = result['results'][0]['observations'][-1]['members']
        self.assertEqual(len({member['session_id'] for member in members}), 2)
        self.assertTrue(all(member['position'] for member in members))

    async def test_multiple_workflows_reuse_case_alias_but_not_another_trial_session(self):
        """A Case spans Workflows in one Session; repeated and parallel Cases never share that identity."""
        async def scenario(context):
            first = await context.invoke('echo', context.case.input, entry_node_id='echo')
            second = await context.invoke('other', context.case.input, entry_node_id='echo')
            context.check(CheckResult('same-session', first.member()['session_id'] == second.member()['session_id']))
            self.assertEqual(await first.value(), context.case.input)
            return second
        suite = self.suite(scenario=scenario, cases=(EvalCase('a', {'text': 'a'}, {'text': 'a'}), EvalCase('b', {'text': 'b'}, {'text': 'b'})))
        result = await self.run_eval(suite, options=RunOptions(concurrency=2, repeats=2), factory=self.factory([
            Workflow('echo', nodes=[Node('echo', echo)]), Workflow('other', nodes=[Node('echo', echo)])]))
        self.assertTrue(result['ok'], result)
        sessions = [item['roots'][0]['session_id'] for item in result['results']]
        self.assertEqual(len(set(sessions)), 4)

    async def test_expected_business_failure_is_not_a_runner_error(self):
        """A Workflow failure can satisfy a business expectation without raising an Evaluation error."""
        suite = self.suite(evaluators=(StatusIs('failed'),))
        result = await self.run_eval(suite, factory=self.factory([Workflow('echo', nodes=[Node('echo', fail)])]))
        self.assertTrue(result['ok'], result)
        self.assertEqual(result['results'][0]['execution'], 'completed')

    async def test_recovery_keeps_identity_and_old_failure_evidence(self):
        """Recovery reuses the Invocation while an earlier failure observation remains unchanged."""
        attempts = []
        def flaky(value: Value) -> Value:
            attempts.append(True)
            if len(attempts) == 1:
                raise ValueError('temporary rejection')
            return value
        async def scenario(context):
            failed = await context.invoke('echo', context.case.input, entry_node_id='echo')
            error = await failed.value('error')
            completed = await context.recover(failed)
            context.check(CheckResult('same-invocation', failed.invocation_id == completed.invocation_id))
            context.check(CheckResult('old-failure', failed.member()['status'] == 'failed' and
                await failed.value('error') == error))
            return completed
        result = await self.run_eval(self.suite(scenario=scenario), factory=self.factory([
            Workflow('echo', nodes=[Node('echo', flaky)])]))
        self.assertTrue(result['ok'], result)

    async def test_signal_and_pause_continue_use_normal_host_controls(self):
        """A waiting Scenario pauses, recovers and supplies a declared Signal without a second Runtime."""
        async def scenario(context):
            waiting = await context.invoke('echo', context.case.input, entry_node_id='receive', until='boundary')
            paused = await context.pause(waiting)
            context.check(CheckResult('paused', paused.member()['control_state'] == 'paused'))
            ready = await context.recover(paused, until='boundary')
            return await context.signal(ready, 'message', context.case.expected, source_id='test', sequence=1)
        result = await self.run_eval(self.suite(scenario=scenario, evaluators=(StatusIs(),)), factory=self.factory([
            Workflow('echo', nodes=[Node('receive', AwaitSignal('message'))],
                signal_endpoints=[SignalEndpoint('message', Value)])]))
        self.assertTrue(result['ok'], result)

    async def test_null_output_is_present_and_distinct_from_missing(self):
        """An explicit null output satisfies a null expectation instead of being treated as unavailable."""
        def empty(value: Value) -> None:
            return None
        result = await self.run_eval(self.suite(cases=(EvalCase('null', {'text': 'a'}, None),)),
            factory=self.factory([Workflow('echo', nodes=[Node('echo', empty)])]))
        self.assertTrue(result['ok'], result)

    async def test_ambiguous_wait_selection_is_an_explicit_error(self):
        """Multiple approvals require a selector rather than silently responding to the first Wait."""
        workflow = Workflow('parallel', nodes=[Node('start', echo), Node('first', Wait(Value, Value)),
            Node('second', Wait(Value, Value))], edges=[Edge('start', 'first'), Edge('start', 'second')])
        async def scenario(context):
            first = await context.invoke('parallel', context.case.input, entry_node_id='start', until='boundary')
            return await context.respond(first, context.case.expected)
        result = await self.run_eval(self.suite(scenario=scenario), factory=self.factory([workflow]))
        self.assertEqual(result['results'][0]['verdict'], 'error')
        self.assertIn('found 2', result['results'][0]['errors'][0]['message'])

    async def test_evaluator_exception_does_not_hide_other_failures(self):
        """Independent check failures survive a broken Evaluator and are not mislabeled Workflow errors."""
        def broken(context):
            raise RuntimeError('judge unavailable')
        def wrong(context):
            return CheckResult('wrong', False, 'Business changed')
        result = await self.run_eval(self.suite(evaluators=(broken, wrong, StatusIs())))
        trial = result['results'][0]
        self.assertEqual(trial['execution'], 'completed')
        self.assertEqual(trial['verdict'], 'error')
        self.assertEqual([check['passed'] for check in trial['checks']], [False, True])
        self.assertEqual(trial['errors'][0]['phase'], 'evaluation')

    async def test_ungraded_cases_never_pass(self):
        """Executing a Scenario or recording observations alone cannot produce a green report."""
        result = await self.run_eval(self.suite(evaluators=()))
        self.assertFalse(result['ok'])
        self.assertEqual(result['results'][0]['verdict'], 'ungraded')
        def unknown(context):
            return CheckResult('not-assessed', None)
        result = await self.run_eval(self.suite(evaluators=(StatusIs(), unknown)))
        self.assertEqual(result['results'][0]['verdict'], 'ungraded')

    async def test_event_budget_exhaustion_cannot_prove_absence(self):
        """A truncated all-Event scan raises an explicit assessment error rather than passing absence checks."""
        async def scan(context):
            count = 0
            async for _ in context.evidence.events():
                count += 1
            return CheckResult('all-events', count > 0)
        result = await self.run_eval(self.suite(evaluators=(scan,)), options=RunOptions(evidence_event_limit=1))
        self.assertEqual(result['results'][0]['verdict'], 'error')
        self.assertEqual(result['results'][0]['errors'][0]['type'], 'EvidenceIncomplete')

    async def test_timeout_cancels_owned_parent_and_waiting_child(self):
        """A timed-out terminal wait cancels the complete owned family and saves its failure report."""
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        parent = Workflow('echo', nodes=[Node('echo', Spawn(child, 'wait'))])
        result = await self.run_eval(self.suite(), options=RunOptions(timeout_seconds=5), factory=self.factory([parent]))
        trial = result['results'][0]
        self.assertEqual(trial['execution'], 'timeout', result)
        self.assertTrue(trial['cleanup']['ok'], result)
        self.assertTrue(trial['roots'])
        self.assertEqual(trial['verdict'], 'error')

    async def test_fixture_resources_and_teardown_errors_are_preserved(self):
        """Fixtures expose per-Trial business evidence and cleanup failure prevents a false success."""
        @asynccontextmanager
        async def fixture(context):
            context.resources['transactions'] = []
            yield
            raise RuntimeError('fixture cleanup failed')
        def no_transactions(context):
            return CheckResult('no-side-effect', context.resources['transactions'] == [])
        suite = replace(self.suite(evaluators=(no_transactions,)), fixture=fixture)
        result = await self.run_eval(suite)
        self.assertEqual(result['results'][0]['checks'][0]['passed'], True)
        self.assertFalse(result['ok'])
        self.assertFalse(result['results'][0]['cleanup']['ok'])

    async def test_compare_detects_regression_and_changed_expectations(self):
        """Version comparison identifies behavioral regression but refuses changed ground truth."""
        baseline = await self.run_eval(self.suite())
        candidate = await self.run_eval(self.suite(), factory=self.factory([Workflow('echo', nodes=[Node('echo', fail)])]))
        comparison = compare_runs(baseline, candidate)
        self.assertEqual(comparison['cases'][0]['outcome'], 'regressed')
        changed = await self.run_eval(self.suite(cases=(EvalCase('one', {'text': 'a'}, {'text': 'different'}),)))
        self.assertEqual(compare_runs(baseline, changed)['cases'][0]['outcome'], 'not_comparable')

    async def test_interruption_saves_partial_results_and_propagates_cancellation(self):
        """Cancelling a Run preserves interrupted Trial records and still propagates cancellation."""
        started = asyncio.Event()
        async def scenario(context):
            await context.invoke('approval', context.case.input, entry_node_id='wait', until='boundary')
            started.set()
            await asyncio.Event().wait()
        task = asyncio.create_task(self.run_eval(self.suite(scenario=scenario), factory=self.factory([
            Workflow('approval', nodes=[Node('wait', Wait(Value, Value))])])) )
        await asyncio.wait_for(started.wait(), 5)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        report = load_report(self.root / 'run-1')
        self.assertEqual(report['status'], 'interrupted')
        self.assertEqual(report['results'][0]['verdict'], 'interrupted')


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL is not configured.')
class PostgreSQLEvaluationTests(EvaluationTests):
    pass


class DefinitionTests(unittest.TestCase):
    def test_checker_closure_configuration_and_wrapped_fixture_identity(self):
        """Changed closure thresholds affect comparison identity; normal async fixtures have usable identities."""
        from autoagent.evaluation.models import callable_identity
        def checker(threshold):
            def evaluate(context):
                return CheckResult('threshold', threshold > 0)
            return evaluate
        self.assertNotEqual(callable_identity(checker(1)), callable_identity(checker(2)))
        @asynccontextmanager
        async def fixture(context):
            yield
        self.assertTrue(callable_identity(fixture)['identity_complete'])

    def test_output_comparison_distinguishes_booleans_and_reports_field_paths(self):
        """JSON true never equals numeric one, and mismatch reports identify the changed field."""
        from autoagent.evaluation.evaluators import _difference
        self.assertEqual(_difference({'a': [True]}, {'a': [1]})[0], '/a/0')
        self.assertIsNone(_difference({'a': [1]}, {'a': [1.0]}))

    def test_invalid_data_limits_and_duplicate_ids_fail_before_execution(self):
        """Ambiguous identities, invalid JSON and unbounded options fail at definition time."""
        for create in (lambda: EvalCase('', {}), lambda: EvalCase('nan', float('nan')),
                       lambda: RunOptions(concurrency=True), lambda: RunOptions(timeout_seconds=float('inf')),
                       lambda: EvalSuite('suite', (EvalCase('a', {}), EvalCase('a', {})), no_op)):
            with self.subTest(create=create), self.assertRaises((ValueError, TypeError)):
                create()

    def test_report_loader_rejects_result_path_traversal(self):
        """A report cannot load Trial files from outside its Run directory."""
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'report.json'
            path.write_text(json.dumps(dict(schema_version=1, kind='autoagent.evaluation.run',
                results=[dict(result_file='../private.json')])) )
            with self.assertRaisesRegex(Exception, 'escapes'):
                load_report(path)
