"""Node-defined deadlines and retained ownership of timed-out physical work."""

import asyncio
import threading
import unittest

from typing_extensions import TypedDict

from autoagent import (AutoAgentApp, InputMappingContext, Map, Node, Stream, StreamContext,
                       TimeoutPolicy, Wait, Workflow, WorkflowCompiler, WorkflowDefinitionSnapshot)
from tests.test_operator_execution_bridge import wait_set


class Value(TypedDict):
    value: int


class Reducer:
    def initial(self, context: StreamContext) -> Value:
        return {"value": 0}
    def add(self, context: StreamContext, state: Value, chunk: Value) -> Value:
        return {"value": state["value"] + chunk["value"]}
    def finish(self, context: StreamContext, state: Value) -> Value:
        return state


class NodeTimeoutDefinitionTests(unittest.TestCase):
    def test_policy_validation_and_revision_capture(self):
        """Only finite positive deadlines compile, and changing one changes the Revision."""
        for seconds in (True, 0, -1, float("nan"), float("inf"), "1", 10**1000):
            with self.subTest(seconds=seconds), self.assertRaises(ValueError):
                TimeoutPolicy(seconds)
        def identity(value: Value) -> Value:
            return value
        compiler = WorkflowCompiler()
        first = compiler.compile_or_raise(Workflow("deadline", nodes=[Node("work", identity, timeout=TimeoutPolicy(1))]))
        second = compiler.compile_or_raise(Workflow("deadline", nodes=[Node("work", identity, timeout=TimeoutPolicy(2))]))
        self.assertNotEqual(first.workflow_revision_id, second.workflow_revision_id)
        snapshot = WorkflowDefinitionSnapshot.from_workflow_ir(first)
        self.assertEqual(snapshot.definition["nodes"][0]["timeout"]["seconds"], 1)
        self.assertEqual(WorkflowDefinitionSnapshot.from_record(snapshot.to_record()), snapshot)
        invalid = compiler.compile(Workflow("wait", nodes=[Node("wait", Wait(Value, Value), timeout=TimeoutPolicy(1))]))
        self.assertIsNone(invalid.workflow_ir)
        invalid = compiler.compile(Workflow("wrong", nodes=[Node("work", identity, timeout=1)]))
        self.assertIsNone(invalid.workflow_ir)


class NodeTimeoutExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_timeout_is_a_failed_call_and_node(self):
        """Deadline expiration reports NodeTimeoutError without becoming caller cancellation."""
        events = []
        class Sink:
            async def append(self, event):
                events.append(event)
        async def hang(value: Value) -> Value:
            await asyncio.Future()
        app = AutoAgentApp(runtime_event_sink=Sink())
        try:
            result = await app.ainvoke(Workflow("deadline", nodes=[Node("work", hang, timeout=TimeoutPolicy(.02))]), {"value": 1})
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.error.type, "NodeTimeoutError")
            failures = [event for event in events if event.event_name == "operator_call.failed"]
            self.assertEqual(len(failures), 1)
            self.assertEqual(failures[0].payload.error.type, "NodeTimeoutError")
        finally:
            await app.aclose()

    async def test_user_timeout_error_is_not_relabelled_as_node_deadline(self):
        """An upstream TimeoutError remains distinct when the local timer has not expired."""
        async def fail() -> None:
            raise TimeoutError("upstream deadline")
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(Workflow("upstream", nodes=[Node("work", fail, timeout=TimeoutPolicy(1))]), None)
            self.assertEqual(result.error.type, "TimeoutError")
            self.assertEqual(result.error.message, "upstream deadline")
        finally:
            await app.aclose()

    async def test_swallowed_deadline_cancellation_does_not_publish_success(self):
        """A handler cannot turn an expired Node deadline into successful output."""
        async def suppress() -> Value:
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                return {"value": 1}
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(Workflow("suppressed", nodes=[Node("work", suppress, timeout=TimeoutPolicy(.02))]), None)
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.error.type, "NodeTimeoutError")
        finally:
            await app.aclose()

    async def test_stream_deadline_closes_its_source(self):
        """A stream that keeps producing still expires and runs generator cleanup."""
        from collections.abc import AsyncIterator
        closed = threading.Event()
        async def stream(value: Value) -> AsyncIterator[Value]:
            try:
                while True:
                    yield value
                    await asyncio.sleep(.005)
            finally:
                closed.set()
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(Workflow("stream", nodes=[Node("work", stream, stream=Stream(Reducer()), timeout=TimeoutPolicy(.03))]), {"value": 1})
            self.assertEqual(result.status, "failed", result.error)
            self.assertEqual(result.error.type, "NodeTimeoutError")
            self.assertTrue(closed.is_set())
        finally:
            await app.aclose()

    async def test_map_units_receive_individual_execution_budgets(self):
        """Sequential Map units can collectively exceed the per-Call timeout."""
        def inputs(context: InputMappingContext) -> list[Value]:
            return [{"value": n} for n in range(3)]
        async def unit(value: Value) -> Value:
            await asyncio.sleep(.02)
            return value
        app = AutoAgentApp(max_operator_concurrency=1)
        try:
            result = await app.ainvoke(Workflow("map", nodes=[Node("work", unit, input_mapping=inputs,
                map=Map(max_parallelism=1), timeout=TimeoutPolicy(.05))]), {"value": 0})
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(result.output, [{"value": n} for n in range(3)])
        finally:
            await app.aclose()

    async def test_sync_deadline_retains_capacity_and_queueing_does_not_consume_budget(self):
        """Timed-out threads retain their permit while queued Calls keep a fresh deadline."""
        entered, release, next_entered = [threading.Event() for _ in range(3)]
        def block() -> None:
            entered.set()
            release.wait(3)
        async def following() -> None:
            next_entered.set()
        app = AutoAgentApp(max_operator_concurrency=1)
        try:
            first = await app.asubmit_invoke(Workflow("first", nodes=[Node("work", block, timeout=TimeoutPolicy(.02))]), None)
            await wait_set(entered)
            self.assertEqual((await app.ajoin(first.ref)).error.type, "NodeTimeoutError")
            second = await app.asubmit_invoke(Workflow("second", nodes=[Node("work", following, timeout=TimeoutPolicy(.02))]), None)
            await asyncio.sleep(.05)
            self.assertFalse(next_entered.is_set())
            release.set()
            result = await asyncio.wait_for(app.ajoin(second.ref), 2)
            self.assertEqual(result.status, "completed", result.error)
        finally:
            release.set()
            await app.aclose()

    async def test_external_cancellation_stays_cancellation(self):
        """A caller cancelling before the deadline receives a cancelled Invocation."""
        entered = threading.Event()
        async def hang() -> None:
            entered.set()
            await asyncio.Future()
        app = AutoAgentApp()
        try:
            first = await app.asubmit_invoke(Workflow("cancel", nodes=[Node("work", hang, timeout=TimeoutPolicy(10))]), None)
            await wait_set(entered)
            await app.acancel(first.ref)
            self.assertEqual((await app.ajoin(first.ref)).status, "cancelled")
        finally:
            await app.aclose()
