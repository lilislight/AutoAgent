"""Node retry, loop admission and pre-mapping error inputs across recovery."""

import asyncio
import json
import threading
import time
import unittest
from collections.abc import AsyncIterator
from contextlib import closing, aclosing
from unittest.mock import patch

from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp, BackoffPolicy, Capability, ConditionContext, Edge,
    InputMappingContext, Map, Node, Operator, OutputBindingContext,
    Recovery, ResourcePolicy, RetryPolicy, Stream, StreamContext,
    TimeoutPolicy, Wait, Workflow, WorkflowCompiler, WorkflowDefinitionSnapshot,
)
from autoagent.core.operators import OperatorCallContext
from autoagent.core.runtime import (
    NodeStarted, OperatorCallFailed, OperatorCallStarted, RuntimeEvent,
    SessionCheckpoint, StateReducer,
)
from tests.graph_fixtures import load_graph
from tests.test_phase2_runtime_state import RecordingSink


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


def items(context: InputMappingContext) -> list[Value]:
    return [{"value": i} for i in range(context.invocation_input["value"])]


def always(context: ConditionContext) -> bool:
    return True


def never(context: ConditionContext) -> bool:
    return False


class Reducer:
    def initial(self, context: StreamContext) -> Value:
        return {"value": 0}
    def add(self, context: StreamContext, state: Value, chunk: Value) -> Value:
        return chunk
    def finish(self, context: StreamContext, state: Value) -> Value:
        return state


ZERO = BackoffPolicy(initial_seconds=0, max_seconds=0, jitter="none")


class NodePolicyTests(unittest.TestCase):
    def test_validation_and_semantic_revision(self):
        """Policy values are validated and contribute to the Workflow Revision."""
        self.assertEqual(RetryPolicy().max_attempts, 1)
        for value in (True, -1, 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                RetryPolicy(max_attempts=value)
        for value in (True, 0, -1, 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ResourcePolicy(value)
        for value in (True, -1, float("inf"), float("nan"), 10**1000):
            with self.subTest(value=value), self.assertRaises(ValueError):
                BackoffPolicy(initial_seconds=value)
        compiler = WorkflowCompiler()
        workflow = Workflow("definition", nodes=[Node("work", identity)])
        baseline = compiler.compile_or_raise(workflow)
        workflow.nodes[0].retry = RetryPolicy()
        retry = compiler.compile_or_raise(workflow)
        workflow.nodes[0].resource = ResourcePolicy(2)
        limit = compiler.compile_or_raise(workflow)
        self.assertEqual(len({x.workflow_revision_id for x in (baseline, retry, limit)}), 3)
        snapshot = WorkflowDefinitionSnapshot.from_workflow_ir(limit)
        self.assertEqual(snapshot.definition["nodes"][0]["retry"]["max_attempts"], 1)
        self.assertEqual(WorkflowDefinitionSnapshot.from_record(snapshot.to_record()), snapshot)
        workflow.nodes[0].retry = "invalid"
        self.assertFalse(compiler.compile(workflow).ok)
        self.assertFalse(compiler.compile(Workflow("wait", nodes=[Node("wait", Wait(Value, Value), retry=RetryPolicy())])).ok)

    def test_backoff_growth_caps_and_jitter(self):
        """Backoff progression and both jitter modes obey the configured cap."""
        for mode, expected in (("fixed", [1, 1, 1, 1]), ("linear", [1, 2, 3, 3]),
                               ("exponential", [1, 2, 3, 3])):
            backoff = BackoffPolicy(mode=mode, initial_seconds=1, max_seconds=3, jitter="none")
            self.assertEqual([backoff.delay(i) for i in range(1, 5)], expected)
        for jitter, low in (("full", 0), ("equal", 1)):
            with patch("random.uniform", side_effect=lambda a, b: (a + b) / 2) as uniform:
                BackoffPolicy(initial_seconds=2, max_seconds=2, jitter=jitter).delay(1)
                uniform.assert_called_once_with(low, 2)
        self.assertEqual(BackoffPolicy(jitter="none").delay(100000), 30)

    def test_retries_exclude_initial_call_and_restore_input(self):
        """Two configured retries permit three Calls with identical immutable inputs."""
        seen, ordinals = [], []
        def flaky(value: Value) -> Value:
            seen.append(value["value"])
            ordinals.append(OperatorCallContext.current().retry_attempt)
            value["value"] = 999
            if len(seen) < 3:
                raise OSError("temporary")
            return {"value": 8}
        sink = RecordingSink()
        with closing(AutoAgentApp(runtime_event_sink=sink)) as app:
            result = app.invoke(Workflow("retry", nodes=[Node("work", flaky,
                retry=RetryPolicy(2, ZERO), resource=ResourcePolicy(1))]), {"value": 7})
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(seen, [7, 7, 7])
        self.assertEqual(ordinals, [0, 1, 2])
        state = StateReducer().reduce(sink.events)
        self.assertEqual(dict(state.invocation.scheduler.node_executions), {"work": 1})
        self.assertEqual(len(state.invocation.scheduler.operator_calls), 3)
        for event in sink.events:
            self.assertEqual(RuntimeEvent.from_record(json.loads(json.dumps(event.to_record()))), event)
        self.assertEqual(SessionCheckpoint.from_record(SessionCheckpoint.from_state(state).to_record()).state, state)

    def test_absent_zero_default_and_exhausted_retry(self):
        """An absent or zero policy runs once; default policy adds exactly one retry."""
        for policy, expected in ((None, 1), (RetryPolicy(0, ZERO), 1), (RetryPolicy(backoff=ZERO), 2)):
            calls = []
            def fail(value: Value) -> Value:
                calls.append(1)
                raise OSError("down")
            with closing(AutoAgentApp()) as app:
                result = app.invoke(Workflow("fail", nodes=[Node("work", fail, retry=policy)]), {"value": 1})
            self.assertEqual(result.status, "failed")
            self.assertEqual(len(calls), expected)

    def test_exception_filter_and_contract_failures(self):
        """Retry filters match inheritance while invalid outputs do not retry."""
        for invalid_output in (False, True):
            calls = []
            def fail(value: Value) -> Value:
                calls.append(1)
                if invalid_output:
                    return {"value": "wrong"}
                raise ValueError("permanent")
            with closing(AutoAgentApp()) as app:
                result = app.invoke(Workflow("filter", nodes=[Node("work", fail,
                    retry=RetryPolicy(3, ZERO, retry_on=("OSError",)))]), {"value": 1})
            self.assertEqual(result.status, "failed")
            self.assertEqual(len(calls), 1)

    def test_map_retries_failed_units_without_counting_calls_as_node_starts(self):
        """Hundreds of Map Calls consume one Node start and only failed units retry."""
        counts = {}
        def flaky(value: Value) -> Value:
            index = value["value"]
            counts[index] = counts.get(index, 0) + 1
            if index == 2 and counts[index] == 1:
                raise OSError("temporary")
            return value
        with closing(AutoAgentApp()) as app:
            result = app.invoke(Workflow("map", nodes=[Node("work", flaky, input_mapping=items,
                map=Map(max_parallelism=4), retry=RetryPolicy(backoff=ZERO), resource=ResourcePolicy(1))]), {"value": 300})
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(len(result.output), 300)
        self.assertEqual(sum(counts.values()), 301)
        self.assertEqual(counts[2], 2)

    def test_node_loop_limit_survives_retention_and_resets_for_new_invocation(self):
        """Pruning completed loop occurrences cannot refund a Node's execution quota."""
        calls = []
        def work(value: Value) -> Value:
            calls.append(1)
            return value
        workflow = Workflow("loop", nodes=[Node("entry", identity), Node("work", work, resource=ResourcePolicy(4)), Node("exit", identity)],
            edges=[Edge("entry", "work"), Edge("work", "work", always, id="back"), Edge("work", "exit", never, id="exit")])
        with closing(AutoAgentApp()) as app:
            for _ in range(2):
                result = app.invoke(workflow, {"value": 1}, session_id="loop-session")
                self.assertEqual(result.error.type, "NodeExecutionLimitExceeded")
        self.assertEqual(len(calls), 8)

    def test_many_distinct_nodes_do_not_share_an_invocation_quota(self):
        """A Workflow exceeding the removed global 1000-Node guard can complete."""
        workflow = Workflow("large", nodes=[Node(str(i), identity, resource=ResourcePolicy(1)) for i in range(1001)],
            edges=[Edge(str(i), str(i + 1)) for i in range(1000)])
        with closing(AutoAgentApp()) as app:
            result = app.invoke(workflow, {"value": 1})
        self.assertEqual(result.status, "completed", result.error)

    def test_error_edge_preserves_pre_mapping_inputs_even_on_mapping_failure(self):
        """Error branches receive source inputs before mutation or failed Input Mapping."""
        for fail_mapping in (False, True):
            received = []
            def mapping(context: InputMappingContext) -> Value:
                context.invocation_input["value"] = 88
                context.incoming["before"]["value"] = 99
                if fail_mapping:
                    raise ValueError("mapping failed")
                return {"value": 77}
            def fail(value: Value) -> Value:
                raise OSError("provider failed")
            def recover(context: InputMappingContext) -> Value:
                error = context.incoming["failed"]
                received.append(error)
                return error["input"]["incoming"]["before"]
            workflow = Workflow("error", nodes=[Node("start", identity), Node("fail", fail, input_mapping=mapping,
                retry=RetryPolicy(backoff=ZERO)), Node("backup", identity, input_mapping=recover)],
                edges=[Edge("start", "fail", id="before"), Edge("fail", "backup", on="error", id="failed")])
            with closing(AutoAgentApp()) as app:
                result = app.invoke(workflow, {"value": 3})
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(result.output, {"value": 3})
            payload = received[0]
            self.assertEqual(payload["node_id"], "fail")
            self.assertEqual(payload["input"], {"invocation_input": {"value": 3}, "incoming": {"before": {"value": 3}}})
            self.assertEqual(payload["error"]["phase"], "input_mapping" if fail_mapping else "operator")

    def test_map_error_edge_identifies_the_failed_unit_and_keeps_pre_mapping_input(self):
        """Map failure metadata names its unit without replacing the Node's original input."""
        received = []
        counts = {}
        def unit(value: Value) -> Value:
            index = value["value"]
            counts[index] = counts.get(index, 0) + 1
            if index == 2 or (index == 0 and counts[index] == 1):
                raise OSError("unit failed")
            return value
        def recover(context: InputMappingContext) -> Value:
            failure = context.incoming["failed"]
            received.append(failure)
            return failure["input"]["invocation_input"]
        workflow = Workflow("map-error", nodes=[Node("map", unit, input_mapping=items,
            map=Map(max_parallelism=1), retry=RetryPolicy(1, ZERO)), Node("backup", identity, input_mapping=recover)],
            edges=[Edge("map", "backup", on="error", id="failed")])
        with closing(AutoAgentApp()) as app:
            result = app.invoke(workflow, {"value": 3})
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(received[0]["error"]["unit_index"], 2)
        self.assertEqual(received[0]["input"], {"invocation_input": {"value": 3}, "incoming": {}})

    def test_binding_failure_keeps_original_input(self):
        """Releasing mapped and Call values cannot erase an output-binding failure's input."""
        def bind(context: OutputBindingContext) -> None:
            raise ValueError("binding failed")
        def recover(context: InputMappingContext) -> Value:
            return context.incoming["failed"]["input"]["invocation_input"]
        with closing(AutoAgentApp()) as app:
            result = app.invoke(Workflow("binding", nodes=[Node("work", identity, output_binding=bind),
                Node("backup", identity, input_mapping=recover)], edges=[Edge("work", "backup", on="error", id="failed")]), {"value": 4})
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(result.output, {"value": 4})

    def test_recovery_resumes_scheduled_retry_without_resetting_budget(self):
        """A checkpoint during Backoff keeps its ordinal, deadline and Node quota."""
        calls = []
        def flaky(value: Value) -> Value:
            calls.append(OperatorCallContext.current().retry_attempt)
            if len(calls) == 1:
                raise OSError("temporary")
            return value
        workflow = Workflow("recover", nodes=[Node("work", flaky, retry=RetryPolicy(1, ZERO), resource=ResourcePolicy(1))])
        sink = RecordingSink()
        with closing(AutoAgentApp(runtime_event_sink=sink)) as app:
            self.assertEqual(app.invoke(workflow, {"value": 1}).status, "completed")
        boundary = next(e for e in sink.events if isinstance(e.payload, OperatorCallFailed))
        checkpoint = SessionCheckpoint.from_state(StateReducer().reduce([e for e in sink.events if e.sequence <= boundary.sequence]))
        self.assertIsNotNone(boundary.payload.retry_at_us)
        with closing(AutoAgentApp()) as app:
            app.register_workflow(workflow)
            loaded = load_graph(app, SessionCheckpoint.from_record(json.loads(json.dumps(checkpoint.to_record()))))
            result = app.recover(loaded.invocations[0])
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(calls, [0, 1, 1])

    def test_recovery_of_final_failure_cannot_restart_runtime_retries(self):
        """A checkpoint at the final failed Call cannot renew its retry allowance."""
        calls = []
        def fail(value: Value) -> Value:
            calls.append(1)
            raise OSError("down")
        workflow = Workflow("exhausted", nodes=[Node("work", fail, retry=RetryPolicy(1, ZERO))])
        sink = RecordingSink()
        with closing(AutoAgentApp(runtime_event_sink=sink)) as app:
            self.assertEqual(app.invoke(workflow, {"value": 1}).status, "failed")
        boundary = [e for e in sink.events if isinstance(e.payload, OperatorCallFailed)][-1]
        checkpoint = SessionCheckpoint.from_state(StateReducer().reduce([e for e in sink.events if e.sequence <= boundary.sequence]))
        with closing(AutoAgentApp()) as app:
            app.register_workflow(workflow)
            result = app.recover(load_graph(app, checkpoint).invocations[0])
        self.assertEqual(result.status, "failed")
        self.assertEqual(len(calls), 2)

    def test_uncertain_retry_still_obeys_crash_recovery_policy(self):
        """An earlier retryable failure cannot hide an uncertain running retry from Recovery."""
        calls = []
        def flaky(value: Value) -> Value:
            calls.append(1)
            if len(calls) == 1:
                raise OSError("temporary")
            return value
        workflow = Workflow("uncertain", nodes=[Node("work", flaky, retry=RetryPolicy(1, ZERO),
            recovery_mode=Recovery("never"))])
        sink = RecordingSink()
        with closing(AutoAgentApp(runtime_event_sink=sink)) as app:
            self.assertEqual(app.invoke(workflow, {"value": 1}).status, "completed")
        boundary = [e for e in sink.events if isinstance(e.payload, OperatorCallStarted)][-1]
        checkpoint = SessionCheckpoint.from_state(StateReducer().reduce([e for e in sink.events if e.sequence <= boundary.sequence]))
        with closing(AutoAgentApp()) as app:
            app.register_workflow(workflow)
            result = app.recover(load_graph(app, checkpoint).invocations[0])
        self.assertEqual(result.error.type, "RecoveryNotAllowed")
        self.assertEqual(len(calls), 2)

    def test_loop_limit_survives_checkpoint_reload(self):
        """Reloading an ongoing loop preserves starts pruned from the active occurrence list."""
        workflow = Workflow("loop-restore", nodes=[Node("entry", identity),
            Node("work", identity, resource=ResourcePolicy(3)), Node("exit", identity)],
            edges=[Edge("entry", "work"), Edge("work", "work", always, id="back"), Edge("work", "exit", never, id="exit")])
        sink = RecordingSink()
        with closing(AutoAgentApp(runtime_event_sink=sink)) as app:
            self.assertEqual(app.invoke(workflow, {"value": 1}).error.type, "NodeExecutionLimitExceeded")
        boundary = [e for e in sink.events if e.event_name == "node_occurrence.completed"][-1]
        state = StateReducer().reduce([e for e in sink.events if e.sequence <= boundary.sequence])
        self.assertEqual(state.invocation.scheduler.node_executions["work"], 3)
        with closing(AutoAgentApp()) as app:
            app.register_workflow(workflow)
            result = app.recover(load_graph(app, SessionCheckpoint.from_state(state)).invocations[0])
        self.assertEqual(result.error.type, "NodeExecutionLimitExceeded")

    def test_resolvers_are_scoped_to_capability_and_do_not_run_on_retry(self):
        """A custom strategy does not affect other Capabilities or reselect on failure."""
        chosen_calls, resolutions = [], []
        def flaky(value: Value) -> Value:
            chosen_calls.append(1)
            if len(chosen_calls) == 1:
                raise OSError("temporary")
            return value
        chosen = Operator(flaky, id="chosen")
        unused = Operator(identity, id="unused")
        fallback = Operator(identity, id="default")
        def resolver(capability, value):
            resolutions.append(capability.id)
            return chosen
        with closing(AutoAgentApp(capability_resolvers={"custom": resolver})) as app:
            app.register_operator(chosen, capability_id="custom")
            app.register_operator(unused, capability_id="custom", default=True)
            app.register_operator(fallback, capability_id="ordinary", default=True)
            workflow = Workflow("resolve", nodes=[Node("first", Capability("custom", chosen.contract), retry=RetryPolicy(backoff=ZERO)),
                Node("second", Capability("ordinary", fallback.contract))], edges=[Edge("first", "second")])
            result = app.invoke(workflow, {"value": 1})
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(resolutions, ["custom"])
        self.assertEqual(len(chosen_calls), 2)


class NodePolicyAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_retry_gets_a_new_deadline(self):
        """Async deadline failure retries with a fresh per-Call timeout budget."""
        calls = []
        async def work(value: Value) -> Value:
            calls.append(1)
            if len(calls) == 1:
                await asyncio.Future()
            return value
        async with aclosing(AutoAgentApp()) as app:
            result = await app.ainvoke(Workflow("timeout", nodes=[Node("work", work,
                timeout=TimeoutPolicy(.02), retry=RetryPolicy(backoff=ZERO))]), {"value": 1})
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(len(calls), 2)

    async def test_backoff_releases_operator_capacity_and_cancel_stops_retry(self):
        """Another Invocation can run during Backoff and cancellation prevents later Calls."""
        failed = threading.Event()
        calls = []
        async def work(value: Value) -> Value:
            calls.append(1)
            failed.set()
            raise OSError("down")
        async with aclosing(AutoAgentApp(max_operator_concurrency=1)) as app:
            ref = (await app.asubmit_invoke(Workflow("backoff", nodes=[Node("work", work,
                retry=RetryPolicy(2, BackoffPolicy(initial_seconds=10, max_seconds=10, jitter="none")))]), {"value": 1})).ref
            await asyncio.to_thread(failed.wait, 2)
            result = await asyncio.wait_for(app.ainvoke(Workflow("other", nodes=[Node("work", identity)]), {"value": 2}), 2)
            self.assertEqual(result.status, "completed")
            await app.acancel(ref)
            self.assertEqual((await app.ajoin(ref)).status, "cancelled")
            self.assertEqual(len(calls), 1)

    async def test_published_stream_is_not_automatically_repeated(self):
        """Retry never duplicates a stream after its first externally emitted chunk."""
        calls = []
        async def work(value: Value) -> AsyncIterator[Value]:
            calls.append(1)
            yield value
            raise OSError("stream failed")
        async with aclosing(AutoAgentApp()) as app:
            result = await app.ainvoke(Workflow("stream", nodes=[Node("work", work,
                stream=Stream(Reducer()), retry=RetryPolicy(3, ZERO))]), {"value": 1})
        self.assertEqual(result.status, "failed")
        self.assertEqual(len(calls), 1)

    async def test_stream_source_failure_before_first_chunk_can_retry(self):
        """A transient generator failure before publication can restart the source."""
        calls = []
        async def work(value: Value) -> AsyncIterator[Value]:
            calls.append(1)
            if len(calls) == 1:
                raise OSError("connection failed")
            yield value
        async with aclosing(AutoAgentApp()) as app:
            result = await app.ainvoke(Workflow("stream-connect", nodes=[Node("work", work,
                stream=Stream(Reducer()), retry=RetryPolicy(1, ZERO))]), {"value": 1})
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(len(calls), 2)

    async def test_stream_contract_or_reducer_failure_before_publication_does_not_retry(self):
        """Invalid chunks and reducer initialization errors are not transient source failures."""
        for bad_reducer in (False, True):
            calls = []
            async def work(value: Value) -> AsyncIterator[Value]:
                calls.append(1)
                yield {"value": "wrong"}
            class BrokenReducer(Reducer):
                def initial(self, context: StreamContext) -> Value:
                    raise OSError("reducer bug")
            async with aclosing(AutoAgentApp()) as app:
                result = await app.ainvoke(Workflow("stream-invalid", nodes=[Node("work", work,
                    stream=Stream(BrokenReducer() if bad_reducer else Reducer()), retry=RetryPolicy(3, ZERO))]), {"value": 1})
            self.assertEqual(result.status, "failed")
            self.assertLessEqual(len(calls), 1)

    async def test_sync_timeout_does_not_overlap_a_retry_with_running_python(self):
        """A timed-out synchronous handler still owns capacity and cannot be retried concurrently."""
        entered, release = threading.Event(), threading.Event()
        calls = []
        def work(value: Value) -> Value:
            calls.append(1)
            entered.set()
            release.wait(3)
            return value
        try:
            async with aclosing(AutoAgentApp()) as app:
                result = await app.ainvoke(Workflow("sync-timeout", nodes=[Node("work", work,
                    timeout=TimeoutPolicy(.02), retry=RetryPolicy(3, ZERO))]), {"value": 1})
                self.assertTrue(entered.is_set())
                self.assertEqual(result.error.type, "NodeTimeoutError")
                self.assertEqual(len(calls), 1)
        finally:
            release.set()
