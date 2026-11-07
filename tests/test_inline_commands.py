"""Nested non-waiting Commands retain effects independently from Operator retries."""

from contextlib import closing
import json
import time
import unittest

from pydantic import BaseModel, ConfigDict

from autoagent.core import (
    AutoAgentApp, CommandInvoker, Node, Workflow, RetryPolicy, Recovery, BackoffPolicy,
    Spawn, Status, Wait, SendSignal, SendSignalRequest, ReceiveSignal, ReceiveSignalRequest,
    SignalEndpoint, Map, Select, SelectRequest, SignalRequestCase, TimerRequestCase,
    InputMappingContext, Edge,
    RuntimeEvent, RecoverySource,
    ChildRequestCase, RuntimeObservation, RuntimeWait, RuntimeHandle,
    HasSignal, HasSignalRequest,
)


class Value(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: int


class InlineCommandTests(unittest.TestCase):
    def test_has_signal_retry_reuses_observation_without_consuming(self):
        """An inline readiness check keeps its recorded boolean across Retry despite later delivery."""
        observations = []
        async def work(value: Value) -> Value:
            commands = CommandInvoker.current()
            observations.append((await commands.execute(HasSignal(), HasSignalRequest(endpoints=['inbox']), key='check')).has_messages)
            own, _ = commands.identity()
            await commands.execute(SendSignal(), SendSignalRequest(handle=own, endpoint='inbox',
                payload=value.model_dump()), key='send')
            if len(observations) == 1:
                raise OSError('retry after observation')
            batch = await commands.execute(ReceiveSignal(), ReceiveSignalRequest(endpoint='inbox'), key='receive')
            return Value(value=len(batch.messages))
        workflow = Workflow('presence-retry', nodes=[Node('work', work,
            retry=RetryPolicy(max_attempts=1, retry_on=('OSError',)))],
            signal_endpoints=[SignalEndpoint('inbox', Value)])
        with closing(AutoAgentApp()) as app:
            result = app.invoke(workflow, Value(value=1))
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(observations, [False, False])
            self.assertEqual(result.output['value'], 1)

    def test_terminal_recovery_retains_consumed_batch_across_occurrence_rename(self):
        """Explicit replay-safe recovery reuses accepted effects after a failed Node receives a new occurrence ID."""
        events, attempts = [], []
        class Sink:
            async def append(self, event):
                events.append(event)
        async def work(value: Value) -> Value:
            commands = CommandInvoker.current()
            own, _ = commands.identity()
            await commands.execute(SendSignal(), SendSignalRequest(handle=own, endpoint="inbox", payload=value.model_dump()), key="send")
            batch = await commands.execute(ReceiveSignal(), ReceiveSignalRequest(endpoint="inbox"), key="receive")
            attempts.append(batch.messages[0].message_id)
            if len(attempts) == 1:
                raise OSError("after consuming")
            return Value(value=len(batch.messages))
        workflow = Workflow("terminal", nodes=[Node("work", work, recovery_mode=Recovery("replay_safe"))],
            signal_endpoints=[SignalEndpoint("inbox", Value)])
        with closing(AutoAgentApp(runtime_event_sink=Sink())) as app:
            failed = app.invoke(workflow, Value(value=1))
            self.assertEqual(failed.status, "failed", failed.error)
            result = app.recover(failed.ref, source=RecoverySource.from_events(tuple(events)))
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(attempts[0], attempts[1])
            self.assertEqual(sum(event.event_name == "signal.accepted" for event in events), 1)

    def test_graph_observation_cycles_restore_and_end_at_finite_timer(self):
        """Parent and Child may observe each other without recursive readiness evaluation or broken checkpoints."""
        async def child(value: Value) -> SelectRequest:
            commands = CommandInvoker.current()
            _, owner = commands.identity()
            return SelectRequest(cases={"parent": ChildRequestCase(handle=owner, scope="graph"),
                "timer": TimerRequestCase(deadline_at_us=commands.clock_us() + 300_000)})
        async def parent(value: Value) -> SelectRequest:
            commands = CommandInvoker.current()
            handle = await commands.execute(Spawn("observer", "prepare"), value, key="spawn")
            return SelectRequest(cases={"child": ChildRequestCase(handle=handle, scope="graph"),
                "timer": TimerRequestCase(deadline_at_us=commands.clock_us() + 1_000_000)})
        def select_input(context: InputMappingContext) -> SelectRequest:
            return SelectRequest.model_validate_json(json.dumps(next(iter(context.incoming.values()))))
        observer = Workflow("observer", nodes=[Node("prepare", child),
            Node("wait", Select(), input_mapping=select_input)], edges=[Edge("prepare", "wait")])
        root = Workflow("cycle", nodes=[Node("prepare", parent),
            Node("wait", Select(), input_mapping=select_input)], edges=[Edge("prepare", "wait")])
        with closing(AutoAgentApp()) as app:
            app.register_workflow(observer)
            result = app.invoke(root, Value(value=1))
            self.assertEqual(result.status, "waiting", result.error)
            deadline = time.monotonic() + 2
            while not app.suspension_info(result.ref).can_unload and time.monotonic() < deadline:
                time.sleep(0.005)
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            app.load_checkpoint(checkpoint)
            result = app.recover(result.ref)
            deadline = time.monotonic() + 3
            while result.status == "waiting" and time.monotonic() < deadline:
                time.sleep(0.01)
                result = app.join(result.ref)
            self.assertEqual(result.status, "completed", result.error)

    def test_target_only_observation_ignores_descendant_wait_versions(self):
        """Agent neighbor observation versions cannot change just because the observing Child starts waiting."""
        parent = RuntimeHandle(session_id="p", invocation_id="pi", workflow_id="w", workflow_revision_id="r")
        child = RuntimeHandle(session_id="c", invocation_id="ci", workflow_id="w", workflow_revision_id="r")
        before = RuntimeObservation(handle=parent, status="waiting", waits=[RuntimeWait(handle=parent, wait_id="own")])
        after = before.model_copy(update={"waits": [*before.waits, RuntimeWait(handle=child, wait_id="child")]})
        self.assertEqual(before.target_only(), after.target_only())

    def test_effect_prefixes_recover_without_duplicate_spawns_or_messages(self):
        """Every interrupted nested effect reuses its ownership and Signal receipts after checkpoint restore."""
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        class Sink:
            def __init__(self):
                self.events = []
            async def append(self, event):
                self.events.append(event)

        async def echo(value: Value) -> Value:
            return value
        async def work(value: Value) -> Value:
            commands = CommandInvoker.current()
            for key in ("one", "two"):
                await commands.execute(Spawn("worker", "work"), value, key=key)
            own, _ = commands.identity()
            await commands.execute(SendSignal(), SendSignalRequest(handle=own, endpoint="inbox", payload=value.model_dump()), key="send")
            batch = await commands.execute(ReceiveSignal(), ReceiveSignalRequest(endpoint="inbox"), key="receive")
            return Value(value=len(batch.messages))
        child = Workflow("worker", nodes=[Node("work", echo)])
        workflow = Workflow("effects", nodes=[Node("work", work, recovery_mode=Recovery("replay_safe")),
            Node("wait", Wait(Value, Value))], edges=[Edge("work", "wait")],
            signal_endpoints=[SignalEndpoint("inbox", Value)])
        sink = Sink()
        with closing(AutoAgentApp(runtime_event_sink=sink)) as app:
            app.register_workflow(child)
            result = app.invoke(workflow, Value(value=7), session_id="root")
            self.assertEqual(result.status, "waiting", result.error)
            graph = app.unload_session(result.ref, capture_checkpoint=True)
        events = tuple(RuntimeEvent.from_record(event.to_record()) for event in sink.events)
        boundaries = {"command.invoked", "command.returned", "signal.accepted", "signal.received", "child_invocation.planned"}
        for cut, event in enumerate(events):
            if event.event_name not in boundaries:
                continue
            with self.subTest(cut=cut, event=event.event_name), closing(AutoAgentApp(runtime_event_sink=(tail := Sink()))) as app:
                app.register_workflow(child)
                app.register_workflow(workflow)
                ref = load_graph(app, _checkpoint_from_prefix(events[:cut+1])).invocations[0]
                recovered = app.recover(ref)
                self.assertEqual(recovered.status, "waiting", recovered.error)
                all_events = (*events[:cut+1], *tail.events)
                self.assertEqual(sum(item.event_name == "child_invocation.planned" for item in all_events), 2)
                self.assertEqual(sum(item.event_name == "signal.accepted" for item in all_events), 1)
                self.assertEqual(recovered.waits[0].request, {"value": 1})
        with closing(AutoAgentApp()) as app:
            app.register_workflow(child)
            app.register_workflow(workflow)
            plan = app.prepare_fork(graph, source=RecoverySource.from_events(events))
            root = plan.graph.sessions[0].state.invocation
            self.assertEqual(len(root.child_plans), 2)
            self.assertEqual(len({unit.session_id for child_plan in root.child_plans.values() for unit in child_plan.units}), 2)
            self.assertEqual(len(root.scheduler.occurrences["work@root"].execution.inline_commands), 4)

    def test_send_receive_and_retry_reuse_accepted_effects(self):
        """A retried Operator reuses its recorded send and consumed batch rather than duplicating them."""
        attempts = []
        async def work(value: Value) -> Value:
            commands = CommandInvoker.current()
            own, _ = commands.identity()
            await commands.execute(SendSignal(), SendSignalRequest(handle=own, endpoint="inbox", payload=value.model_dump()), key="send")
            messages = await commands.execute(ReceiveSignal(), ReceiveSignalRequest(endpoint="inbox"), key="receive")
            attempts.append(messages.messages[0].message_id)
            if len(attempts) == 1:
                raise OSError("retry after accepted effects")
            return Value(value=len(messages.messages))
        workflow = Workflow("inline", nodes=[Node("work", work, retry=RetryPolicy(max_attempts=1,
            backoff=BackoffPolicy(initial_seconds=0)))], signal_endpoints=[SignalEndpoint("inbox", Value)])
        with closing(AutoAgentApp()) as app:
            result = app.invoke(workflow, Value(value=1))
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(result.output, {"value": 1})
            self.assertEqual(attempts[0], attempts[1])

    def test_inline_spawn_uses_registered_workflow_and_native_child_ownership(self):
        """Spawn inside an Operator creates one owned Child and returns a stable Handle."""
        async def child(value: Value) -> Value:
            return value
        async def work(value: Value) -> Value:
            commands = CommandInvoker.current()
            handle = await commands.execute(Spawn("worker", "work"), value, key="spawn")
            again = await commands.execute(Spawn("worker", "work"), value, key="spawn")
            self.assertEqual(handle, again)
            self.assertEqual(commands.visible_handles()[0], ("child", handle))
            observed = await commands.execute(Status(), handle, key="status")
            return Value(value=1 if observed.handle == handle else 0)
        with closing(AutoAgentApp()) as app:
            app.register_workflow(Workflow("worker", nodes=[Node("work", child)]))
            result = app.invoke(Workflow("inline_spawn", nodes=[Node("work", work)]), Value(value=4))
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(result.output, {"value": 1})

    def test_nested_key_conflicts_and_waiting_commands_are_rejected(self):
        """One stable key cannot change its input and nested calls cannot hide durable waits."""
        async def work(value: Value) -> Value:
            commands = CommandInvoker.current()
            own, _ = commands.identity()
            await commands.execute(SendSignal(), SendSignalRequest(handle=own, endpoint="inbox", payload=value.model_dump()), key="send")
            with self.assertRaisesRegex(Exception, "different input"):
                await commands.execute(SendSignal(), SendSignalRequest(handle=own, endpoint="inbox", payload={"value": 99}), key="send")
            with self.assertRaises(TypeError):
                await commands.execute(Wait(Value, Value), value, key="wait")
            return value
        with closing(AutoAgentApp()) as app:
            result = app.invoke(Workflow("conflict", nodes=[Node("work", work)], signal_endpoints=[SignalEndpoint("inbox", Value)]), Value(value=1))
            self.assertEqual(result.status, "completed", result.error)

    def test_select_readiness_keeps_message_for_later_receive(self):
        """A non-consuming Signal case wakes without returning or deleting message bodies."""
        def wait_input(context: InputMappingContext) -> SelectRequest:
            return SelectRequest(cases={"message": SignalRequestCase(endpoint="inbox", consume=False),
                "timer": TimerRequestCase(delay_us=1_000_000)})
        def received_input(context: InputMappingContext) -> Value:
            result = next(iter(context.incoming.values()))
            self.assertEqual(result, {"case": "message", "value": {"available": True, "endpoint": "inbox"}})
            return Value(value=0)
        async def receive(value: Value) -> Value:
            messages = await CommandInvoker.current().execute(ReceiveSignal(), ReceiveSignalRequest(endpoint="inbox"), key="receive")
            return Value(value=len(messages.messages))
        # Exercise readiness independently, then inspect its retained mailbox.
        with closing(AutoAgentApp()) as app:
            workflow = Workflow("ready", nodes=[Node("wait", Select(), input_mapping=wait_input),
                Node("receive", receive, input_mapping=received_input)], edges=[Edge("wait", "receive")],
                signal_endpoints=[SignalEndpoint("inbox", Value)])
            result = app.invoke(workflow, None)
            app.signal(result.ref, "inbox", {"value": 7}, source_id="test", sequence=1)
            result = app.join(result.ref)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(result.output, {"value": 1})

