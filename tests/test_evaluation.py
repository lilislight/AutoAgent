"""Business Evaluation through real Hosts, fixed evidence and independent Trial history."""

import asyncio
from copy import deepcopy
from contextlib import asynccontextmanager
from dataclasses import replace
from enum import Enum
import json
from pathlib import Path
import tempfile
import unittest

from typing_extensions import TypedDict

from autoagent import AwaitSignal, Edge, Node, Recovery, SignalEndpoint, Spawn, Wait, Workflow
from autoagent.evaluation import (CheckResult, EvalCase, EvalSuite, NodeExecuted, OutputEquals,
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


class EvaluationHarness(unittest.IsolatedAsyncioTestCase):
    postgres = False
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
            if self.postgres:
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
            scenario, evaluators, conditions={})

class EvaluationTests(EvaluationHarness):
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
            interaction = (await waiting.interactions())[0]
            request = await interaction.request()
            context.check(CheckResult('request', request == context.case.input))
            completed = await context.respond(waiting, context.case.expected)
            context.check(CheckResult('request-after-response',
                await interaction.request() == context.case.input))
            context.check(CheckResult('before-status', waiting.status == 'waiting'))
            context.check(CheckResult('before-output', await waiting.output() is None))
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
        suite = self.suite(scenario=scenario, evaluators=(StatusIs(), NodeExecuted('spawn')))
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
            context.check(CheckResult('same-session', first.session_id == second.session_id))
            self.assertEqual(await first.output(), context.case.input)
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
            error = await failed.error()
            completed = await context.recover(failed)
            context.check(CheckResult('same-invocation', failed.invocation_id == completed.invocation_id))
            context.check(CheckResult('old-failure', failed.status == 'failed' and
                await failed.error() == error))
            return completed
        result = await self.run_eval(self.suite(scenario=scenario), factory=self.factory([
            Workflow('echo', nodes=[Node('echo', flaky, recovery_mode=Recovery('replay_safe'))])]))
        self.assertTrue(result['ok'], result)

    async def test_signal_and_pause_continue_use_normal_host_controls(self):
        """A waiting Scenario pauses, recovers and supplies a declared Signal without a second Runtime."""
        async def scenario(context):
            waiting = await context.invoke('echo', context.case.input, entry_node_id='receive', until='boundary')
            paused = await context.pause(waiting)
            context.check(CheckResult('paused', paused.control_state == 'paused'))
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
        self.assertIn('ambiguous', result['results'][0]['errors'][0]['message'])

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
            occurrences = await context.execution.node('echo').all()
            return CheckResult('complete-business-history', bool(occurrences))
        result = await self.run_eval(self.suite(evaluators=(scan,)), options=RunOptions(read_limit=1))
        self.assertEqual(result['results'][0]['verdict'], 'error')
        self.assertEqual(result['results'][0]['errors'][0]['type'], 'ViewUnavailable')

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
    postgres = True


class DefinitionTests(unittest.TestCase):
    def test_rule_helper_class_configuration_changes_are_part_of_its_identity(self):
        """A rule helper's class attributes and method globals cannot change behind an unchanged source hash."""
        from autoagent.evaluation.models import callable_identity
        class Policy:
            threshold = 1
            def allowed(self, value):
                return value <= self.threshold and value <= BUSINESS_LIMIT
        def evaluate(context):
            return CheckResult('policy', Policy().allowed(1))
        before = callable_identity(evaluate)
        Policy.threshold = 2
        self.assertNotEqual(before, callable_identity(evaluate))
        self.assertTrue(before['identity_complete'])

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
            path.write_text(json.dumps(dict(schema_version=2, kind='autoagent.evaluation.run',
                results=[dict(result_file='../private.json')])) )
            with self.assertRaisesRegex(Exception, 'escapes'):
                load_report(path)


BUSINESS_LIMIT = 1


def business_limit(context):
    return CheckResult('business-limit', len(context.case.input['text']) <= BUSINESS_LIMIT)


class Mode(str, Enum):
    read = 'read'


class EnumRequest(TypedDict):
    mode: Mode


def enum_work(value: EnumRequest) -> Value:
    return {'text': value['mode'].value}


class BusinessRegressionTests(EvaluationHarness):
    """Additional real business scenarios; inherited foundation contracts stay reusable."""

    async def test_mutating_expected_cannot_turn_a_wrong_workflow_into_a_pass(self):
        """Nested immutable ground truth rejects Scenario edits while working copies remain usable."""
        async def scenario(context):
            context.case.expected['text'] = 'wrong'
            return await context.invoke('echo', {'text':'wrong'}, entry_node_id='echo')
        result = await self.run_eval(self.suite(scenario=scenario))
        self.assertEqual(result['results'][0]['verdict'],'error')
        self.assertIn('immutable',result['results'][0]['errors'][0]['message'])
        case = EvalCase('nested', {'items':[{'value':1}]}, {})
        with self.assertRaises(Exception): case.input['items'][0]['value'] = 2
        data = case.copy_input()
        data['items'][0]['value'] = 2
        self.assertEqual(case.input['items'][0]['value'],1)

    async def test_global_business_rule_change_is_not_a_workflow_improvement(self):
        """Relaxing a rule's global threshold changes its contract instead of crediting the target code."""
        global BUSINESS_LIMIT
        suite = self.suite(evaluators=(business_limit,), cases=(EvalCase('one',{'text':'long'}),))
        try:
            BUSINESS_LIMIT = 1
            baseline = await self.run_eval(suite)
            BUSINESS_LIMIT = 20
            candidate = await self.run_eval(suite)
        finally:
            BUSINESS_LIMIT = 1
        self.assertEqual(baseline['results'][0]['verdict'],'failed')
        self.assertEqual(candidate['results'][0]['verdict'],'passed')
        self.assertEqual(compare_runs(baseline,candidate)['cases'][0]['outcome'],'not_comparable')

    async def test_rule_configuration_cannot_change_after_a_run_starts(self):
        """Changing a global criterion during the Scenario invalidates the Trial instead of passing."""
        global BUSINESS_LIMIT
        async def scenario(context):
            global BUSINESS_LIMIT
            view = await context.invoke('echo', context.case.input, entry_node_id='echo')
            BUSINESS_LIMIT = 100
            return view
        try:
            BUSINESS_LIMIT = 1
            result = await self.run_eval(self.suite(scenario=scenario, evaluators=(business_limit,)))
        finally:
            BUSINESS_LIMIT = 1
        trial = result['results'][0]
        self.assertEqual(trial['verdict'], 'error', result)
        self.assertIn('changed during the Run', trial['errors'][0]['message'])

    async def test_changed_or_undeclared_conditions_do_not_produce_accepted_improvement(self):
        """External conditions and unknown identities stop accepted comparisons without blocking ordinary runs."""
        baseline = await self.run_eval(replace(self.suite(),conditions={'ledger':'v1'}))
        candidate = await self.run_eval(replace(self.suite(),conditions={'ledger':'v2'}))
        comparison = compare_runs(baseline,candidate)
        self.assertFalse(comparison['ok'])
        self.assertEqual(comparison['cases'][0]['outcome'],'not_comparable')
        undeclared = await self.run_eval(replace(self.suite(),conditions=None))
        self.assertTrue(undeclared['ok'])
        self.assertEqual(compare_runs(undeclared,undeclared)['cases'][0]['outcome'],'inconclusive')

    async def test_report_comparison_rejects_inconsistent_pass_and_ungraded_rule(self):
        """A portable report cannot claim pass with a failed rule or call an ungraded rule unchanged-passed."""
        baseline = await self.run_eval(self.suite())
        altered = deepcopy(baseline)
        altered['results'][0]['checks'][0]['passed'] = False
        with self.assertRaisesRegex(Exception, 'contradicts'):
            compare_runs(baseline, altered)
        altered['results'][0]['checks'][0]['passed'] = None
        altered['results'][0]['verdict'] = 'ungraded'
        comparison = compare_runs(baseline, altered)
        self.assertEqual(comparison['cases'][0]['outcome'], 'inconclusive')
        self.assertEqual(comparison['cases'][0]['rules'][1]['outcome'], 'inconclusive')

    async def test_json_enum_input_uses_the_same_host_contract_as_cli(self):
        """A JSON enum string is decoded normally before the Workflow runs."""
        async def scenario(context):
            return await context.invoke('enum',context.case.input,entry_node_id='entry')
        suite = self.suite(scenario=scenario, cases=(EvalCase('enum',{'mode':'read'},{'text':'read'}),))
        result = await self.run_eval(suite,factory=self.factory([Workflow('enum',nodes=[Node('entry',enum_work)])]))
        self.assertTrue(result['ok'],result)

    async def test_cancelled_approval_can_recover_without_repeating_completed_stage(self):
        """Cancel/recover retains business identity and completed work while opening a fresh actionable approval."""
        workflow = Workflow('approval', nodes=[Node('prepare', echo),
            Node('approve', Wait(Value, Value), recovery_mode=Recovery('replay_safe'))],
            edges=[Edge('prepare', 'approve')])
        async def scenario(context):
            waiting = await context.invoke('approval', context.case.input, entry_node_id='prepare', until='boundary')
            cancelled = await context.cancel(waiting)
            context.check(CheckResult('cancelled', cancelled.status == 'cancelled'))
            recovered = await context.recover(cancelled, until='boundary')
            context.check(CheckResult('same-business-execution', recovered.invocation_id == waiting.invocation_id))
            stage = await recovered.node('prepare').one()
            context.check(CheckResult('completed-stage-preserved', await stage.output() == context.case.input))
            final = await context.respond(recovered, context.case.expected, node_id='approve')
            context.check(CheckResult('past-cancellation-stays-fixed', cancelled.status == 'cancelled'))
            return final
        result = await self.run_eval(self.suite(scenario=scenario), factory=self.factory([workflow]))
        self.assertTrue(result['ok'], result)

    async def test_approval_can_be_answered_while_another_node_is_running(self):
        """Target approval readiness permits reply without waiting for the complete Root to become idle."""
        release = asyncio.Event()
        async def background(value: Value) -> Value:
            await release.wait()
            return value
        workflow = Workflow('parallel',nodes=[Node('start',echo),Node('approval',Wait(Value,Value)),Node('background',background)],
            edges=[Edge('start','approval'),Edge('start','background')])
        async def scenario(context):
            running = await context.invoke('parallel',context.case.input,entry_node_id='start',until='none')
            ready = await context.wait_for(running,interaction_node='approval',timeout=3)
            resumed = await context.respond(ready,context.case.expected,node_id='approval',until='none',timeout=3)
            release.set()
            return await context.observe(resumed)
        result = await self.run_eval(self.suite(scenario=scenario,evaluators=(StatusIs(),)),factory=self.factory([workflow]))
        self.assertTrue(result['ok'],result)

    async def test_no_workflow_or_duplicate_rule_names_never_produce_green_results(self):
        """Business Evaluation requires a Workflow and stable distinct rule identities."""
        async def scenario(context):
            context.check(CheckResult('pure-assertion',True))
        result = await self.run_eval(self.suite(scenario=scenario,evaluators=()))
        self.assertFalse(result['ok'])
        result = await self.run_eval(self.suite(evaluators=(StatusIs(),StatusIs())))
        self.assertEqual(result['results'][0]['verdict'],'error')
        self.assertIn('Duplicate business rule',result['results'][0]['errors'][0]['message'])


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL is not configured.')
class PostgreSQLBusinessRegressionTests(BusinessRegressionTests):
    postgres = True


class WorkflowBusinessSuiteTests(EvaluationHarness):
    async def test_order_rules_cover_intermediate_amounts_routing_and_actual_payments(self):
        """Approved and denied order cases assess the whole Workflow and per-Trial external ledger state."""
        from examples.evaluation_demo.orders import order_workflow, orders
        def factory(environment):
            return self.factory([order_workflow(environment.resources['ledger'])])(environment)
        suite = replace(orders,host_factory=factory)
        result = await self.run_eval(suite,options=RunOptions(repeats=2,concurrency=2))
        self.assertTrue(result['ok'],result)
        self.assertEqual(result['summary']['passed'],4)
        self.assertTrue(all(any(check['evidence'] for check in trial['checks']) for trial in result['results']))

    async def test_same_final_result_does_not_hide_an_intermediate_business_violation(self):
        """A candidate with a correct receipt but wrong approval amount is a business regression."""
        from examples.evaluation_demo.orders import order_workflow, orders
        def before(environment):
            return self.factory([order_workflow(environment.resources['ledger'])])(environment)
        def after(environment):
            return self.factory([order_workflow(environment.resources['ledger'],wrong_approval_total=True,normalize_receipt=True)])(environment)
        suite = replace(orders,cases=(orders.cases[1],),host_factory=before)
        baseline = await self.run_eval(suite)
        candidate = await self.run_eval(replace(suite,host_factory=after))
        self.assertTrue(baseline['ok'],baseline)
        self.assertEqual(candidate['results'][0]['verdict'],'failed',candidate)
        checks = {item['name']:item for item in candidate['results'][0]['checks']}
        self.assertTrue(checks['output']['passed'])
        self.assertFalse(checks['approval-amount']['passed'])
        comparison = compare_runs(baseline,candidate)
        self.assertEqual(comparison['cases'][0]['outcome'],'regressed',comparison)
        self.assertIn('approval-amount',[rule['name'] for rule in comparison['cases'][0]['rules'] if rule['outcome']=='new_failure'])

    async def test_internal_workflow_refactoring_preserves_business_acceptance(self):
        """Adding an internal stage remains comparable when approval, routing and actual payments stay valid."""
        from examples.evaluation_demo.orders import Total, order_workflow, orders
        def unchanged(value: Total) -> Total:
            return value
        def before(environment):
            return self.factory([order_workflow(environment.resources['ledger'])])(environment)
        def after(environment):
            workflow = order_workflow(environment.resources['ledger'])
            workflow.nodes.append(Node('internal-validation', unchanged))
            workflow.edges = [edge for edge in workflow.edges if edge.id != 'request-approval']
            workflow.edges.extend([Edge('calculate', 'internal-validation'), Edge('internal-validation', 'approval')])
            return self.factory([workflow])(environment)
        suite = replace(orders, cases=(orders.cases[0],), host_factory=before)
        baseline = await self.run_eval(suite)
        candidate = await self.run_eval(replace(suite, host_factory=after))
        self.assertTrue(baseline['ok'], baseline)
        self.assertTrue(candidate['ok'], candidate)
        self.assertEqual(compare_runs(baseline, candidate)['cases'][0]['outcome'], 'unchanged_passed')

    async def test_duplicate_business_submission_reuses_the_ledger_idempotency_key(self):
        """Two Workflow invocations repeat a business request while the ledger records only one payment."""
        from examples.evaluation_demo.orders import order_scenario, order_workflow, orders
        def factory(environment):
            return self.factory([order_workflow(environment.resources['ledger'])])(environment)
        async def scenario(context):
            await order_scenario(context)
            # A second complete business request uses the same Session and service key.
            waiting = await context.invoke('order',context.case.input,entry_node_id='calculate',until='boundary')
            request = await (await waiting.interactions(node_id='approval'))[0].request()
            final = await context.respond(waiting,dict(request,approved=True),node_id='approval')
            context.check(CheckResult('one-payment-for-duplicate-key',len(context.resources['ledger'].payments)==1 and len(context.resources['ledger'].requests)==2))
            return final
        suite = replace(orders,cases=(orders.cases[0],),scenario=scenario,host_factory=factory)
        result = await self.run_eval(suite)
        # The custom multi-round scenario evaluates two public receipts through its own rule.
        self.assertTrue(any(check['name']=='one-payment-for-duplicate-key' and check['passed'] for check in result['results'][0]['checks']),result)
        self.assertTrue(result['ok'],result)


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL is not configured.')
class PostgreSQLWorkflowBusinessSuiteTests(WorkflowBusinessSuiteTests):
    postgres = True


class CaseContractTests(EvaluationHarness):
    async def test_scenario_cannot_replace_its_case_to_change_expected_results(self):
        """Replacing the public Case cannot silently grade changed ground truth under the original fingerprint."""
        async def scenario(context):
            context.case = EvalCase('one', {'text': 'wrong'}, {'text': 'wrong'})
            return await context.invoke('echo', context.case.input, entry_node_id='echo')
        result = await self.run_eval(self.suite(scenario=scenario))
        self.assertFalse(result['ok'])
        self.assertEqual(result['results'][0]['verdict'], 'error')


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL is not configured.')
class PostgreSQLCaseContractTests(CaseContractTests):
    postgres = True
