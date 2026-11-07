"""Per-unit native waits, durable deadlines and receipt-preserving retries."""

import asyncio
import json
import tempfile
import threading
import unittest
from collections import Counter
from contextlib import closing, asynccontextmanager
from pathlib import Path
from unittest.mock import patch

from autoagent import (
    AutoAgentApp, Await, AwaitAny, AwaitAnyRequest, AwaitSignal, BackoffPolicy,
    Capability, Edge, InputMappingContext, Map, Node, Operator, ReceiveSignal,
    ReceiveSignalRequest, RetryPolicy, RuntimeHandle, RuntimeTransitionError,
    Select, SelectRequest, SelfHandle, SendSignal, SendSignalRequest, SignalEndpoint, SignalRequestCase,
    Spawn, TimeoutPolicy, Timer, TimerRequest, TimerRequestCase, Wait, Workflow,
    WorkflowCompiler,
)
from autoagent.core import RuntimeGraphCheckpoint
from autoagent.core.operators import OperatorCallContext
from tests.test_node_policies import Value, ZERO, identity, items
from tests.test_system_commands import Collector
from tests.test_durable_waits import terminal
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class CommandMapTests(unittest.TestCase):
    def test_partial_wait_registration_event_prefixes_admit_remaining_units(self):
        """A crash during registration cannot strand a partially admitted Map batch."""
        from autoagent.core import RuntimeEvent
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        workflow = Workflow('register-prefix', nodes=[Node('ask', Wait(Value, Value), input_mapping=items,
            map=Map(max_parallelism=1))])
        sink = Collector()
        with closing(AutoAgentApp(runtime_event_sink=sink)) as app:
            self.assertEqual(app.invoke(workflow, {'value': 3}, session_id='root').status, 'waiting')
        events = tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)
        start = next(i for i, e in enumerate(events) if e.event_name == 'operator_call.started')
        for cut in range(start, len(events)):
            restored_sink = Collector()
            with self.subTest(cut=events[cut].event_name), closing(AutoAgentApp(runtime_event_sink=restored_sink)) as app:
                app.register_workflow(workflow)
                ref = load_graph(app, _checkpoint_from_prefix(events[:cut + 1])).invocations[0]
                result = app.recover(ref)
                self.assertEqual(result.status, 'waiting', result.error)
                self.assertEqual(len(result.waits), 3)
                for wait in result.waits:
                    result = app.resume(ref, wait.id, {'value': wait.request['value'] + 10})
                self.assertEqual(result.output, [{'value': 10}, {'value': 11}, {'value': 12}])
                starts = Counter(e.payload.unit_index for e in (*events[:cut + 1], *restored_sink.events)
                    if e.event_name == 'operator_call.started')
                self.assertEqual(starts, {0: 1, 1: 1, 2: 1})

    def test_dynamic_send_and_receive_map_use_each_typed_request(self):
        """Input Mapping can address per-unit signals and consume each message exactly once."""
        def sends(ctx: InputMappingContext) -> list[SendSignalRequest]:
            return [SendSignalRequest(handle=ctx.self_handle, endpoint='message', payload={'value': i}) for i in range(3)]
        def reads(ctx: InputMappingContext) -> list[ReceiveSignalRequest]:
            return [ReceiveSignalRequest(endpoint='message') for _ in range(3)]
        workflow = Workflow('mapped-mailbox', nodes=[Node('send', SendSignal(), input_mapping=sends, map=Map(max_parallelism=1)),
            Node('read', ReceiveSignal(), input_mapping=reads, map=Map(max_parallelism=3))],
            edges=[Edge('send', 'read')], signal_endpoints=[SignalEndpoint('message', Value)])
        with closing(AutoAgentApp()) as app:
            result = app.invoke(workflow, None)
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual([batch.messages[0].payload['value'] for batch in result.output], [0, 1, 2])

    def test_await_any_map_selects_within_each_group_and_joins_all_groups(self):
        """Each AwaitAny unit chooses one Child while the Map returns one result per group."""
        def groups(ctx: InputMappingContext) -> list[AwaitAnyRequest]:
            handles = [RuntimeHandle.model_validate(h) for h in next(iter(ctx.incoming.values()))]
            return [AwaitAnyRequest(handles=handles[:2]), AwaitAnyRequest(handles=handles[2:])]
        child = Workflow('child', nodes=[Node('work', identity)])
        workflow = Workflow('grouped', nodes=[Node('spawn', Spawn(child, 'work'), input_mapping=items, map=Map()),
            Node('any', AwaitAny(), input_mapping=groups, map=Map(max_parallelism=1))], edges=[Edge('spawn', 'any')])
        with closing(AutoAgentApp()) as app:
            result = app.invoke(workflow, {'value': 4})
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(len(result.output), 2)
            self.assertIn(result.output[0].output['value'], {0, 1})
            self.assertIn(result.output[1].output['value'], {2, 3})

    def test_external_waits_register_all_and_resume_out_of_order_after_reload(self):
        """Map=1 registers all waits, keeps answered units and aggregates in input order."""
        sink = Collector()
        workflow = Workflow('questions', nodes=[Node('ask', Wait(Value, Value), map=Map(max_parallelism=1), input_mapping=items)])
        with closing(AutoAgentApp(runtime_event_sink=sink)) as app:
            result = app.invoke(workflow, {'value': 3})
            self.assertEqual(result.status, 'waiting')
            self.assertEqual(len(result.waits), 3)
            waits = {w.request['value']: w.id for w in result.waits}
            result = app.resume(result.ref, waits[1], {'value': 11})
            self.assertEqual(len(result.waits), 2)
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            checkpoint = RuntimeGraphCheckpoint.from_record(json.loads(json.dumps(checkpoint.to_record())))
            app.load_checkpoint(checkpoint)
            result = app.recover(result.ref)
            self.assertEqual({w.id for w in result.waits}, {waits[0], waits[2]})
            with self.assertRaises(RuntimeTransitionError):
                app.resume(result.ref, waits[1], {'value': 99})
            result = app.resume(result.ref, waits[2], {'value': 12})
            result = app.resume(result.ref, waits[0], {'value': 10})
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(result.output, [{'value': 10}, {'value': 11}, {'value': 12}])
            self.assertEqual(sum(e.event_name == 'operator_call.started' for e in sink.events), 3)

    def test_empty_wait_map_finishes_without_wait_or_call(self):
        """An empty wait Map completes immediately with an empty result."""
        sink = Collector()
        with closing(AutoAgentApp(runtime_event_sink=sink)) as app:
            result = app.invoke(Workflow('empty', nodes=[Node('ask', Wait(Value, Value), map=Map(), input_mapping=items)]), {'value': 0})
        self.assertEqual((result.status, result.output, result.waits), ('completed', [], ()))
        self.assertFalse(any(e.event_name == 'operator_call.started' for e in sink.events))

    def test_dynamic_signal_wait_map_consumes_fifo_once(self):
        """Each Map unit independently registers and consumes one shared-endpoint message."""
        def requests(ctx: InputMappingContext) -> list[ReceiveSignalRequest]:
            return [ReceiveSignalRequest(endpoint='message') for _ in range(3)]
        workflow = Workflow('signals', nodes=[Node('get', AwaitSignal(), input_mapping=requests, map=Map(max_parallelism=1))],
            signal_endpoints=[SignalEndpoint('message', Value)])
        with closing(AutoAgentApp()) as app:
            result = app.invoke(workflow, None)
            self.assertEqual(len(result.waits), 3)
            for i in range(3):
                app.signal(result.ref, 'message', {'value': i}, source_id='client', sequence=i + 1)
            result = terminal(app, result.ref)
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual([r.messages[0].payload['value'] for r in result.output], [0, 1, 2])
            self.assertEqual(len({r.messages[0].message_id for r in result.output}), 3)

    def test_select_map_keeps_independent_groups_and_absolute_deadlines(self):
        """Signal and Timer winners settle separate Select units and survive checkpointing."""
        now = [100]
        def requests(ctx: InputMappingContext) -> list[SelectRequest]:
            return [SelectRequest(cases={'message': SignalRequestCase(endpoint='message'),
                'timer': TimerRequestCase(delay_us=10_000)}),
                SelectRequest(cases={'timer': TimerRequestCase(delay_us=20_000)})]
        workflow = Workflow('selects', nodes=[Node('choose', Select(), input_mapping=requests, map=Map(max_parallelism=1))],
            signal_endpoints=[SignalEndpoint('message', Value)])
        with closing(AutoAgentApp(clock_us=lambda: now[0])) as app:
            result = app.invoke(workflow, None)
            self.assertEqual(len(result.waits), 2)
            app.signal(result.ref, 'message', {'value': 7}, source_id='client', sequence=1)
            result = app.join(result.ref)
            self.assertEqual(len(result.waits), 1)
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            now[0] = 20_101
            app.load_checkpoint(checkpoint)
            result = app.recover(result.ref)
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual([r.case for r in result.output], ['message', 'timer'])
            self.assertEqual(result.output[1].value['deadline_at_us'], 20_100)

    def test_timer_map_registers_all_units_without_serial_delay(self):
        """Each Timer Map unit freezes its deadline at registration rather than previous completion."""
        now = [100]
        def requests(ctx: InputMappingContext) -> list[TimerRequest]:
            return [TimerRequest(delay_us=10_000), TimerRequest(delay_us=20_000)]
        with closing(AutoAgentApp(clock_us=lambda: now[0])) as app:
            result = app.invoke(Workflow('timers', nodes=[Node('timer', Timer(), input_mapping=requests, map=Map(max_parallelism=1))]), None)
            self.assertEqual(len(result.waits), 2)
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            now[0] = 20_101
            app.load_checkpoint(checkpoint)
            result = app.recover(result.ref)
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual([r.deadline_at_us for r in result.output], [10_100, 20_100])

    def test_await_map_admission_is_independent_of_child_lifetime(self):
        """Await Map=1 admits every Child and exposes all independent external waits."""
        child = Workflow('child', nodes=[Node('ask', Wait(Value, Value))])
        workflow = Workflow('parent', nodes=[Node('await', Await(child, 'ask'), input_mapping=items, map=Map(max_parallelism=1))])
        with closing(AutoAgentApp()) as app:
            result = app.invoke(workflow, {'value': 3})
            self.assertEqual(len(result.waits), 3)
            self.assertEqual({w.request['value'] for w in result.waits}, {0, 1, 2})

    def test_capability_map_resolves_mapped_units_once_and_reuses_on_retry(self):
        """A resolver sees typed unit values and unit indices, selecting independently before retry."""
        resolutions, attempts = [], Counter()
        def work(value: Value) -> Value:
            attempts[value['value']] += 1
            if value['value'] == 1 and attempts[1] == 1:
                raise OSError('temporary')
            return value
        def other(value: Value) -> Value:
            return {'value': value['value'] + 100}
        one, two = Operator(work, id='one'), Operator(other, id='two')
        def resolver(capability, value):
            resolutions.append((OperatorCallContext.current().unit_index, value['value']))
            return two if value['value'] == 2 else one
        sink = Collector()
        with closing(AutoAgentApp(capability_resolvers={'custom': resolver}, runtime_event_sink=sink)) as app:
            app.register_operator(one, capability_id='custom')
            app.register_operator(two, capability_id='custom')
            result = app.invoke(Workflow('mapped', nodes=[Node('call', Capability('custom', one.contract),
                input_mapping=items, map=Map(max_parallelism=2), retry=RetryPolicy(backoff=ZERO))]), {'value': 3})
        self.assertEqual(result.status, 'completed', result.error)
        self.assertEqual(result.output, [{'value': 0}, {'value': 1}, {'value': 102}])
        self.assertCountEqual(resolutions, [(0, 0), (1, 1), (2, 2)])
        self.assertEqual(attempts, {0: 1, 1: 2})
        bindings = [e.payload for e in sink.events if e.event_name == 'capability.resolved']
        self.assertEqual(len(bindings), 3)


class CommandDeadlineTests(unittest.TestCase):
    def test_backoff_frees_map_dispatch_slot_and_cancel_stops_retry(self):
        """A deferred retry neither blocks later units nor executes after cancellation."""
        second, seen = threading.Event(), []
        def sends(ctx: InputMappingContext) -> list[SendSignalRequest]:
            return [SendSignalRequest(handle=ctx.self_handle, endpoint='message', payload={'value': i}) for i in range(2)]
        workflow = Workflow('backoff', nodes=[Node('send', SendSignal(), input_mapping=sends, map=Map(max_parallelism=1),
            retry=RetryPolicy(1, BackoffPolicy(initial_seconds=10, max_seconds=10, jitter='none')))],
            signal_endpoints=[SignalEndpoint('message', Value)])
        with closing(AutoAgentApp()) as app:
            original = app._workflow_executor._send_signal_runtime
            async def flaky(*args):
                value = args[-1].payload['value']
                seen.append(value)
                if value == 0:
                    raise OSError('not available yet')
                receipt = await original(*args)
                second.set()
                return receipt
            with patch.object(app._workflow_executor, '_send_signal_runtime', flaky):
                ref = app.submit_invoke(workflow, None).ref
                self.assertTrue(second.wait(2), 'Map retry held its dispatch permit')
                self.assertEqual(app.cancel(ref).status, 'cancelled')
            self.assertEqual(seen, [0, 1])

    def test_signal_timeout_reconciles_acceptance_and_prevents_late_effect(self):
        """A timed-out acknowledgement reuses accepted effects and cannot create a late new effect."""
        def payload(ctx: InputMappingContext) -> Value:
            return {'value': 7}
        workflow = Workflow('timed-send', nodes=[Node('send', SendSignal(SelfHandle(), 'message'), input_mapping=payload,
            timeout=TimeoutPolicy(.02))], signal_endpoints=[SignalEndpoint('message', Value)])
        for accepted_first in (False, True):
            with self.subTest(accepted_first=accepted_first), closing(AutoAgentApp(runtime_event_sink=(sink := Collector()))) as app:
                original, calls = app._workflow_executor._send_signal_runtime, []
                async def slow(*args):
                    calls.append(1)
                    if len(calls) == 1:
                        if accepted_first:
                            await original(*args)
                        await asyncio.sleep(.2)
                    return await original(*args)
                with patch.object(app._workflow_executor, '_send_signal_runtime', slow):
                    result = app.invoke(workflow, None)
                self.assertEqual(result.status, 'completed' if accepted_first else 'failed', result.error)
                self.assertEqual(sum(e.event_name == 'signal.accepted' for e in sink.events), 1 if accepted_first else 0)
                if not accepted_first:
                    self.assertEqual(result.error.type, 'NodeTimeoutError')

    def test_resume_and_cancel_retry_reconcile_the_same_effect(self):
        """Retried control commands preserve their operation identity after a lost acknowledgement."""
        from tests.test_resume_command import workflow as resume_workflow
        from tests.test_control_commands import cancel_workflow
        for build, command, event in ((resume_workflow, 'resume', 'wait.resumed'),
                                      (cancel_workflow, 'cancel', 'invocation.cancellation_requested')):
            with self.subTest(command=command), closing(AutoAgentApp(runtime_event_sink=(sink := Collector()))) as app:
                workflow = build()
                next(node for node in workflow.nodes if node.id == command).retry = RetryPolicy(1, ZERO)
                attribute = '_' + command + '_runtime'
                original = getattr(app._workflow_executor, attribute)
                calls = []
                async def lost_ack(*args):
                    receipt = await original(*args)
                    calls.append(args[2])
                    if len(calls) == 1:
                        raise OSError('lost control acknowledgement')
                    return receipt
                with patch.object(app._workflow_executor, attribute, lost_ack):
                    result = app.invoke(workflow, {'value': 1})
                self.assertEqual(result.status, 'completed', result.error)
                self.assertEqual(len(calls), 2)
                self.assertEqual(len(set(calls)), 1)
                self.assertEqual(sum(e.event_name == event for e in sink.events), 1)

    def test_partial_wait_map_retries_expired_units_without_reasking_successes(self):
        """Only unanswered expired units get new waits, including after a later partial reply."""
        now = [100]
        workflow = Workflow('batch-reask', nodes=[Node('ask', Wait(Value, Value), input_mapping=items, map=Map(max_parallelism=1),
            timeout=TimeoutPolicy(1), retry=RetryPolicy(1, ZERO, retry_on=('WaitTimeoutError',)))])
        with closing(AutoAgentApp(clock_us=lambda: now[0])) as app:
            first = app.invoke(workflow, {'value': 3})
            first = app.resume(first.ref, first.waits[0].id, {'value': 10})
            checkpoint = app.unload_session(first.ref, capture_checkpoint=True)
            now[0] = 1_000_101
            app.load_checkpoint(checkpoint)
            second = app.recover(first.ref)
            self.assertEqual(len(second.waits), 2)
            second = app.resume(second.ref, second.waits[1].id, {'value': 12})
            self.assertEqual(len(second.waits), 1)
            final = app.resume(second.ref, second.waits[0].id, {'value': 11})
            self.assertEqual(final.output, [{'value': 10}, {'value': 11}, {'value': 12}])

    def test_receive_post_commit_failure_reuses_atomic_result(self):
        """A failed Receive acknowledgement cannot fail its committed Call or consume again."""
        def payload(ctx: InputMappingContext) -> Value:
            return {'value': 7}
        workflow = Workflow('atomic-read', nodes=[Node('send', SendSignal(SelfHandle(), 'message'), input_mapping=payload),
            Node('read', ReceiveSignal('message'), retry=RetryPolicy(1, ZERO))],
            edges=[Edge('send', 'read')], signal_endpoints=[SignalEndpoint('message', Value)])
        sink = Collector()
        with closing(AutoAgentApp(runtime_event_sink=sink)) as app:
            original = app._workflow_executor._receive_signal_runtime
            async def lost_ack(*args):
                await original(*args)
                raise OSError('lost receive acknowledgement')
            with patch.object(app._workflow_executor, '_receive_signal_runtime', lost_ack):
                result = app.invoke(workflow, None)
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(result.output.messages[0].payload, {'value': 7})
        self.assertEqual(sum(e.event_name == 'signal.received' for e in sink.events), 1)

    def test_send_retry_event_prefixes_recover_same_effect(self):
        """Every post-accept event prefix restores Retry and its receipt without duplicate delivery."""
        from autoagent.core import RuntimeEvent
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        def payload(ctx: InputMappingContext) -> Value:
            return {'value': 7}
        workflow = Workflow('retry-prefix', nodes=[Node('send', SendSignal(SelfHandle(), 'message'), input_mapping=payload,
            retry=RetryPolicy(1, ZERO)), Node('read', ReceiveSignal('message'))], edges=[Edge('send', 'read')],
            signal_endpoints=[SignalEndpoint('message', Value)])
        sink = Collector()
        with closing(AutoAgentApp(runtime_event_sink=sink)) as app:
            original = app._workflow_executor._send_signal_runtime
            seen = []
            async def flaky(*args):
                result = await original(*args)
                seen.append(1)
                if len(seen) == 1:
                    raise OSError('lost ack')
                return result
            with patch.object(app._workflow_executor, '_send_signal_runtime', flaky):
                self.assertEqual(app.invoke(workflow, None, session_id='root').status, 'completed')
        events = tuple(RuntimeEvent.from_record(json.loads(json.dumps(e.to_record()))) for e in sink.events)
        start = next(i for i, e in enumerate(events) if e.event_name == 'signal.accepted')
        for cut in range(start, len(events)):
            with self.subTest(cut=events[cut].event_name), closing(AutoAgentApp()) as app:
                app.register_workflow(workflow)
                ref = load_graph(app, _checkpoint_from_prefix(events[:cut + 1])).invocations[0]
                result = app.recover(ref)
                self.assertEqual(result.status, 'completed', result.error)
                self.assertEqual(len(result.output.messages), 1)
                self.assertEqual(result.output.messages[0].payload, {'value': 7})

    def test_durable_wait_timeout_survives_reload_without_default_reask(self):
        """A recorded absolute timeout fails once despite generic Exception retries and reload."""
        now = [100]
        sink = Collector()
        workflow = Workflow('deadline', nodes=[Node('ask', Wait(Value, Value), timeout=TimeoutPolicy(1),
            retry=RetryPolicy(3, ZERO))])
        with closing(AutoAgentApp(clock_us=lambda: now[0], runtime_event_sink=sink)) as app:
            result = app.invoke(workflow, {'value': 1})
            self.assertEqual(app.suspension_info(result.ref).next_deadline_at_us, 1_000_100)
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            now[0] = 1_000_101
            app.load_checkpoint(checkpoint)
            result = app.recover(result.ref)
            self.assertEqual(result.status, 'failed', result)
            self.assertEqual(result.error.type, 'WaitTimeoutError')
        self.assertEqual(sum(e.event_name == 'operator_call.started' for e in sink.events), 1)

    def test_explicit_wait_timeout_retry_creates_new_wait_identity(self):
        """Opt-in timeout retry creates a fresh response target while retiring the expired wait."""
        now = [100]
        workflow = Workflow('reask', nodes=[Node('ask', Wait(Value, Value), timeout=TimeoutPolicy(1),
            retry=RetryPolicy(1, ZERO, retry_on=('WaitTimeoutError',)))])
        with closing(AutoAgentApp(clock_us=lambda: now[0])) as app:
            first = app.invoke(workflow, {'value': 1})
            checkpoint = app.unload_session(first.ref, capture_checkpoint=True)
            now[0] = 1_000_101
            app.load_checkpoint(checkpoint)
            second = app.recover(first.ref)
            self.assertEqual(second.status, 'waiting', second)
            self.assertNotEqual(first.waits[0].id, second.waits[0].id)
            with self.assertRaises(RuntimeTransitionError):
                app.resume(first.ref, first.waits[0].id, {'value': 99})
            final = app.resume(second.ref, second.waits[0].id, {'value': 2})
            self.assertEqual((final.status, final.output), ('completed', {'value': 2}))

    def test_internal_signal_deadline_fails_without_consuming_later_messages(self):
        """An expired internal wait fails after reload with its original error category."""
        now = [100]
        workflow = Workflow('signal-deadline', nodes=[Node('get', AwaitSignal('message'), timeout=TimeoutPolicy(1))],
            signal_endpoints=[SignalEndpoint('message', Value)])
        with closing(AutoAgentApp(clock_us=lambda: now[0])) as app:
            result = app.invoke(workflow, None)
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            now[0] = 1_000_101
            app.load_checkpoint(checkpoint)
            final = app.recover(result.ref)
            self.assertEqual((final.status, final.error.type), ('failed', 'WaitTimeoutError'))

    def test_timer_rejects_retry_and_execution_timeout(self):
        """A Timer's business deadline cannot be replaced by execution or retry policies."""
        for policy in ({'retry': RetryPolicy()}, {'timeout': TimeoutPolicy(1)}):
            self.assertFalse(WorkflowCompiler().compile(Workflow('invalid', nodes=[Node('timer', Timer(), **policy)])).ok)

    def test_send_retry_reconciles_accepted_receipt_without_second_message(self):
        """A transient post-accept failure retries the same operation and reuses its receipt."""
        sink = Collector()
        def payload(ctx: InputMappingContext) -> Value:
            return {'value': 7}
        workflow = Workflow('sender', nodes=[Node('send', SendSignal(SelfHandle(), 'message'), input_mapping=payload,
            retry=RetryPolicy(1, ZERO)), Node('read', ReceiveSignal('message', limit=3))],
            edges=[Edge('send', 'read')], signal_endpoints=[SignalEndpoint('message', Value)])
        with closing(AutoAgentApp(runtime_event_sink=sink)) as app:
            original = app._workflow_executor._send_signal_runtime
            called = []
            async def flaky(*args):
                receipt = await original(*args)
                called.append(args[2])
                if len(called) == 1:
                    raise OSError('accepted, acknowledgement lost')
                return receipt
            with patch.object(app._workflow_executor, '_send_signal_runtime', flaky):
                result = app.invoke(workflow, None)
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(len(result.output.messages), 1)
            self.assertEqual(len(set(called)), 1)
        self.assertEqual(sum(e.event_name == 'signal.accepted' for e in sink.events), 1)
        self.assertEqual(sum(e.event_name == 'command.retry_started' for e in sink.events), 1)


class HostMapContracts:
    async def database_url(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return 'sqlite+aiosqlite:///' + str(Path(temporary.name) / 'runtime.db')

    async def test_external_wait_deadline_wakes_evicted_graph(self):
        """Host indexes external wait deadlines so an evicted graph still expires automatically."""
        from autoagent.host import AutoAgentHost, HostSettings
        from tests.test_host_residency import eventually
        workflow = Workflow('deadline', nodes=[Node('ask', Wait(Value, Value), timeout=TimeoutPolicy(1))])
        async with AutoAgentHost(workflows=[workflow], environment={}, settings=HostSettings(
            database_url=await self.database_url(), residency_interval_seconds=.02,
            min_residency_seconds=0, cold_after_seconds=0, max_idle_seconds=0, short_timer_seconds=0)) as host:
            ref = await host.execute('deadline', {'value': 1})
            waiting = await host.join(ref)
            self.assertEqual(waiting.status, 'waiting')
            await eventually(lambda: ref.session_id not in host._runtime.resident)
            async def finished():
                result = await host.get_execution(ref)
                return result if result.status == 'failed' else None
            result = await eventually(finished)
            self.assertEqual(result.error.type, 'WaitTimeoutError')
            self.assertIsNone(host._runtime.residency.fault)

    async def test_host_llm_map_with_managed_router_and_custom_or_default_selection(self):
        """Host routing accepts scalar mapped requests in both selection modes and records each LLM call."""
        from autoagent.agent import LLM_CALL, LLMRequest, agent_workflow, llm_call_operator
        from autoagent.host import AutoAgentHost, HostSettings
        from tests.test_agent_capabilities import DemoLLM, request
        def requests(ctx: InputMappingContext) -> list[LLMRequest]:
            return [request(model='first', streaming=False), request(model='second', streaming=True)]
        workflow = Workflow('mapped-llm', nodes=[Node('call', LLM_CALL, input_mapping=requests, map=Map())])
        for custom in (False, True):
            with self.subTest(custom=custom):
                provider, resolved = DemoLLM(), []
                @asynccontextmanager
                async def lifespan(setup):
                    operator = llm_call_operator(provider, id='application.llm')
                    setup.register_operator(operator, capability_id=LLM_CALL.id, default=True)
                    if custom:
                        async def resolver(capability, value):
                            resolved.append(value.model)
                            return operator
                        setup.register_capability_resolver(LLM_CALL.id, resolver)
                    yield
                async with AutoAgentHost(workflows=[agent_workflow(), workflow], lifespan=lifespan,
                    environment={}, settings=HostSettings(database_url=await self.database_url())) as host:
                    result = await host.join(await host.execute('mapped-llm', None))
                    self.assertEqual(result.status, 'completed', result.error)
                    self.assertEqual(len(result.output), 2)
                    self.assertCountEqual([(mode, req.model) for mode, req in provider.calls], [('invoke', 'first'), ('stream', 'second')])
                    self.assertCountEqual(resolved, ['first', 'second'] if custom else [])


class SQLiteHostMapTests(HostMapContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL URL not configured')
class PostgreSQLHostMapTests(HostMapContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
