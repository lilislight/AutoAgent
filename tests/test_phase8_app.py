from __future__ import annotations

import asyncio
import threading
import time
import unittest
from typing_extensions import TypedDict

from autoagent import (
    AggregationContext,
    AutoAgentApp,
    ConditionContext,
    Edge,
    InputMappingContext,
    Map,
    Node,
    OutputBindingContext,
    Recovery,
    InvocationUpdate,
    RuntimeTransitionError,
    UserEvent,
    UserEventMapping,
    Wait,
    Workflow,
)
from autoagent.core import (
    InMemoryEventJournal,
    InvocationRecoveryRequested,
    InvocationResult,
    NodeOccurrenceStarted,
)
from autoagent.core.runtime import StateTransition


class Value(TypedDict):
    value: int


class Batch(TypedDict):
    items: list[Value]


class Request(TypedDict):
    question: str


class Response(TypedDict):
    answer: str


class HandleSummary(TypedDict):
    count: int


def identity(value: Value) -> Value:
    return value


def increment(value: Value) -> Value:
    return {"value": value["value"] + 1}


def map_items(context: InputMappingContext) -> list[Value]:
    return context.invocation_input["items"]  # type: ignore[index,return-value]


def aggregate_child_values(context: AggregationContext) -> Value:
    return {"value": sum(item["value"] for item in context.outputs)}  # type: ignore[index]


def aggregate_child_invocations(context: AggregationContext) -> HandleSummary:
    return {"count": len(context.outputs)}


def map_completion_event(context: OutputBindingContext) -> Value:
    return context.output  # type: ignore[return-value]


def map_response_event(context: OutputBindingContext) -> Response:
    return context.output  # type: ignore[return-value]


def continue_loop(context: ConditionContext) -> bool:
    return context.output["value"] < 3  # type: ignore[index]


def exit_loop(context: ConditionContext) -> bool:
    return context.output["value"] >= 3  # type: ignore[index]


def wait_request(_context: InputMappingContext) -> Request:
    return {"question": "approve?"}


def wait_loop_request(_context: InputMappingContext) -> Request:
    return {"question": "next value?"}


def below_two(context: ConditionContext) -> bool:
    return context.output["value"] < 2  # type: ignore[index]


def at_least_two(context: ConditionContext) -> bool:
    return context.output["value"] >= 2  # type: ignore[index]


class AppTests(unittest.TestCase):
    def setUp(self) -> None:
        self.journal = InMemoryEventJournal()
        self.app = AutoAgentApp(
            max_operator_concurrency=8,
            runtime_journal=self.journal,
        )

    def tearDown(self) -> None:
        self.app.close()

    def test_sync_serial_invocation_returns_only_the_execution_boundary(self) -> None:
        """Keep automatic Trace and checkpoint data out of invoke results."""
        workflow = Workflow(
            "serial-app",
            nodes=[Node("one", increment), Node("two", increment)],
            edges=[Edge("one", "two")],
        )
        result = self.app.invoke(workflow, {"value": 1})
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.output, {"value": 3})
        self.assertFalse(hasattr(result, "trace_events"))
        self.assertFalse(hasattr(result, "checkpoint"))
        self.assertIsNotNone(self.journal.state(result.session_id).invocation)

    def test_async_api_map_and_parallel_calls(self) -> None:
        """Verify async api map and parallel calls."""
        async def run() -> None:
            workflow = Workflow(
                "map-app",
                nodes=[
                    Node(
                        "map",
                        increment,
                        input_mapping=map_items,
                        map=Map(max_parallelism=2),
                    )
                ],
            )
            result = await self.app.ainvoke(
                workflow,
                {"items": [{"value": 1}, {"value": 2}, {"value": 3}]},
            )
            self.assertEqual(
                result.output,
                [{"value": 2}, {"value": 3}, {"value": 4}],
            )

        asyncio.run(run())

    def test_astream_is_strictly_caller_driven_and_ends_with_result(self) -> None:
        """Verify astream applies backpressure and yields one final boundary result."""

        async def run() -> None:
            operator_started = asyncio.Event()

            async def observed(value: Value) -> Value:
                operator_started.set()
                return value

            workflow = Workflow(
                "attached-stream",
                nodes=[
                    Node(
                        "node",
                        observed,
                        user_events=(
                            UserEventMapping("node.completed", map_completion_event),
                        ),
                    )
                ],
            )
            stream = self.app.astream(workflow, {"value": 1})
            first = await anext(stream)
            self.assertIsInstance(first, InvocationUpdate)
            self.assertIsInstance(first.event, UserEvent)
            self.assertEqual(first.event.kind, "node.completed")
            self.assertTrue(operator_started.is_set())
            self.assertEqual(
                self.journal.state(first.event.session_id).invocation.status,
                "running",
            )

            items = [first]
            async for item in stream:
                items.append(item)

            self.assertTrue(operator_started.is_set())
            self.assertIsInstance(items[-1], InvocationResult)
            self.assertEqual(items[-1].status, "completed")
            self.assertEqual(items[-1].output, {"value": 1})
            self.assertFalse(hasattr(items[-1], "user_events"))

        asyncio.run(run())

    def test_stream_is_strictly_caller_driven_and_ends_with_result(self) -> None:
        """Verify synchronous stream preserves attached-stream backpressure."""

        operator_started = threading.Event()

        def observed(value: Value) -> Value:
            operator_started.set()
            return value

        stream = self.app.stream(
            Workflow(
                "sync-attached-stream",
                nodes=[
                    Node(
                        "node",
                        observed,
                        user_events=(
                            UserEventMapping("node.completed", map_completion_event),
                        ),
                    )
                ],
            ),
            {"value": 1},
        )
        first = next(stream)
        self.assertIsInstance(first, InvocationUpdate)
        self.assertIsInstance(first.event, UserEvent)
        self.assertEqual(first.event.kind, "node.completed")
        self.assertTrue(operator_started.is_set())

        items = [first, *stream]
        self.assertTrue(operator_started.is_set())
        self.assertIsInstance(items[-1], InvocationResult)
        self.assertEqual(items[-1].status, "completed")
        self.assertEqual(items[-1].output, {"value": 1})

    def test_parent_stream_excludes_child_user_events(self) -> None:
        """Yield only User Events emitted by the exact streamed Invocation."""

        child = Workflow(
            "isolated-user-event-child",
            nodes=[
                Node(
                    "work",
                    identity,
                    user_events=(
                        UserEventMapping("child.completed", map_completion_event),
                    ),
                )
            ],
        )
        parent = Workflow(
            "isolated-user-event-parent",
            nodes=[
                Node(
                    "child",
                    child,
                    user_events=(
                        UserEventMapping("parent.completed", map_completion_event),
                    ),
                )
            ],
        )
        items = list(self.app.stream(parent, {"value": 1}))
        updates = [
            item.event for item in items if isinstance(item, InvocationUpdate)
        ]
        self.assertEqual([event.kind for event in updates], ["parent.completed"])
        result = items[-1]
        self.assertEqual(result.status, "completed")
        child_ref = self.app.child_invocations(result.ref)[0]
        self.assertEqual(
            self.app._user_event_journal.events(child_ref.invocation_id), ()
        )

    def test_stream_context_manager_cancels_when_closed_early(self) -> None:
        """Verify closing a synchronous attached stream converges cancellation."""

        stream = self.app.stream(
            Workflow(
                "sync-stream-close",
                nodes=[
                    Node(
                        "node",
                        identity,
                        user_events=(
                            UserEventMapping("node.completed", map_completion_event),
                        ),
                    )
                ],
            ),
            {"value": 1},
        )
        session_id = None
        with stream:
            for item in stream:
                if isinstance(item, InvocationUpdate):
                    session_id = item.event.session_id
                    break
        assert session_id is not None
        state = self.journal.state(session_id)
        self.assertEqual(state.invocation.status, "cancelled")

    def test_astream_serializes_parallel_event_publishers(self) -> None:
        """Verify parallel branches share one ordered attached-stream rendezvous."""

        async def run() -> None:
            workflow = Workflow(
                "parallel-stream",
                nodes=[
                    Node("start", identity),
                    Node("left", identity, user_events=(UserEventMapping("left", map_completion_event),)),
                    Node("right", identity, user_events=(UserEventMapping("right", map_completion_event),)),
                ],
                edges=[Edge("start", "left"), Edge("start", "right")],
            )

            async def consume() -> list[object]:
                return [
                    item
                    async for item in self.app.astream(workflow, {"value": 1})
                ]

            items = await asyncio.wait_for(consume(), timeout=2)
            self.assertIsInstance(items[-1], InvocationResult)
            self.assertEqual(items[-1].status, "completed")
            sequences = [item.event.sequence for item in items if isinstance(item, InvocationUpdate)]
            self.assertEqual(sequences, list(range(1, len(sequences) + 1)))

        asyncio.run(run())

    def test_astream_returns_waiting_as_its_final_boundary(self) -> None:
        """Verify astream ends normally when an Invocation reaches Wait."""

        async def run() -> None:
            items = [
                item
                async for item in self.app.astream(
                    Workflow("stream-wait", nodes=[Node("wait", Wait(Request, Response))]),
                    {"question": "continue?"},
                )
            ]
            boundary = items[-1]
            self.assertIsInstance(boundary, InvocationResult)
            self.assertEqual(boundary.status, "waiting")
            self.assertEqual(len(boundary.waits), 1)

        asyncio.run(run())

    def test_slow_astream_consumer_does_not_block_other_invocations(self) -> None:
        """Verify attached-stream backpressure is isolated to its Invocation."""

        async def run() -> None:
            stream = self.app.astream(
                Workflow(
                    "slow-stream",
                    nodes=[Node("node", identity, user_events=(UserEventMapping("done", map_completion_event),))],
                ),
                {"value": 1},
            )
            first = await anext(stream)
            self.assertIsInstance(first, InvocationUpdate)

            independent = await self.app.ainvoke(
                Workflow("independent", nodes=[Node("node", identity)]),
                {"value": 2},
            )
            self.assertEqual(independent.status, "completed")
            self.assertEqual(independent.output, {"value": 2})
            await stream.aclose()

        asyncio.run(run())

    def test_map_failure_settles_every_physical_call_before_invocation_terminal(self) -> None:
        """Verify map failure settles every physical call before invocation terminal."""
        async def map_unit(value: Value) -> Value:
            if value["value"] == 0:
                await asyncio.sleep(0.02)
                raise RuntimeError("unit failed")
            await asyncio.sleep(1)
            return value

        workflow = Workflow(
            "map-failure-convergence",
            nodes=[
                Node(
                    "map",
                    map_unit,
                    input_mapping=map_items,
                    map=Map(max_parallelism=3),
                )
            ],
        )
        result = self.app.invoke(
            workflow,
            {"items": [{"value": 0}, {"value": 1}, {"value": 2}]},
        )
        self.assertEqual(result.status, "failed")
        state = self.journal.state(result.session_id)
        calls = tuple(state.invocation.scheduler.operator_calls.values())
        self.assertEqual(len(calls), 3)
        self.assertFalse(any(call.status == "running" for call in calls))
        self.assertTrue(all(call.completed_at_ns is not None for call in calls))

    def test_async_submit_join_resume_and_cancel_are_symmetric(self) -> None:
        """Verify async submit, join, resume, and cancel are symmetric."""
        async def run() -> None:
            workflow = Workflow(
                "async-wait-app",
                nodes=[
                    Node(
                        "approval",
                        Wait(Request, Response),
                        input_mapping=wait_request,
                    )
                ],
            )
            submitted = await self.app.asubmit_invoke(workflow, {})
            waiting = await self.app.ajoin(submitted.ref, 2)
            self.assertEqual(waiting.status, "waiting")
            completed = await self.app.aresume(
                waiting.ref,
                waiting.waits[0].id,
                {"answer": "yes"},
            )
            self.assertEqual(completed.status, "completed")

            second = await self.app.asubmit_invoke(workflow, {})
            waiting_again = await self.app.ajoin(second.ref, 2)
            cancelled = await self.app.acancel(waiting_again.ref, "stop")
            self.assertEqual(cancelled.status, "cancelled")

        asyncio.run(run())

    def test_stream_resume_yields_updates_and_final_result(self) -> None:
        """Verify stream_resume observes one resumed segment through its boundary."""

        def after(response: Response) -> Response:
            return response

        workflow = Workflow(
            "stream-resume-app",
            nodes=[
                Node(
                    "approval",
                    Wait(Request, Response),
                    input_mapping=wait_request,
                ),
                Node(
                    "after",
                    after,
                    user_events=(
                        UserEventMapping("after.completed", map_response_event),
                    ),
                ),
            ],
            edges=[Edge("approval", "after")],
        )
        waiting = self.app.invoke(workflow, {})
        items = list(
            self.app.stream_resume(
                waiting.ref,
                waiting.waits[0].id,
                {"answer": "yes"},
            )
        )
        self.assertTrue(
            any(
                isinstance(item, InvocationUpdate)
                and item.event.kind == "after.completed"
                for item in items
            )
        )
        self.assertIsInstance(items[-1], InvocationResult)
        self.assertEqual(items[-1].status, "completed")
        self.assertEqual(items[-1].output, {"answer": "yes"})

    def test_astream_resume_yields_updates_and_final_result(self) -> None:
        """Verify astream_resume provides the asynchronous resumed segment."""

        async def run() -> None:
            def after(response: Response) -> Response:
                return response

            workflow = Workflow(
                "astream-resume-app",
                nodes=[
                        Node("approval", Wait(Request, Response), input_mapping=wait_request),
                        Node(
                            "after",
                            after,
                            user_events=(
                                UserEventMapping("after.completed", map_response_event),
                            ),
                        ),
                    ],
                    edges=[Edge("approval", "after")],
            )
            waiting = await self.app.ainvoke(workflow, {})
            items = [
                item
                async for item in self.app.astream_resume(
                    waiting.ref,
                    waiting.waits[0].id,
                    {"answer": "yes"},
                )
            ]
            self.assertTrue(
                any(
                    isinstance(item, InvocationUpdate)
                    and item.event.kind == "after.completed"
                    for item in items
                )
            )
            self.assertIsInstance(items[-1], InvocationResult)
            self.assertEqual(items[-1].status, "completed")

        asyncio.run(run())

    def test_join_timeout_does_not_cancel_the_invocation(self) -> None:
        """Verify join raises TimeoutError while submitted work keeps running."""

        started = threading.Event()
        release = threading.Event()

        def blocked(value: Value) -> Value:
            started.set()
            release.wait(2)
            return value

        submitted = self.app.submit_invoke(
            Workflow("join-timeout", nodes=[Node("blocked", blocked)]),
            {"value": 1},
        )
        try:
            self.assertTrue(started.wait(1))
            with self.assertRaisesRegex(
                TimeoutError,
                "Invocation did not reach a stable boundary in time",
            ):
                self.app.join(submitted.ref, timeout=0.01)
        finally:
            release.set()
        self.assertEqual(
            self.app.join(submitted.ref, timeout=1).status,
            "completed",
        )

    def test_fast_branch_advances_without_waiting_for_slow_sibling(self) -> None:
        """Verify fast branch advances without waiting for slow sibling."""
        slow_started = threading.Event()
        release_slow = threading.Event()
        downstream_started = threading.Event()

        def slow(value: Value) -> Value:
            slow_started.set()
            release_slow.wait(2)
            return value

        def downstream(value: Value) -> Value:
            downstream_started.set()
            return value

        workflow = Workflow(
            "incremental-app",
            nodes=[
                Node("start", identity),
                Node("slow", slow),
                Node("fast", identity),
                Node("downstream", downstream),
            ],
            edges=[
                Edge("start", "slow"),
                Edge("start", "fast"),
                Edge("fast", "downstream"),
            ],
        )
        submitted = self.app.submit_invoke(workflow, {"value": 1})
        self.assertEqual(submitted.status, "running")
        self.assertTrue(slow_started.wait(1))
        self.assertTrue(
            downstream_started.wait(1),
            "fast branch did not advance while slow sibling was running",
        )
        release_slow.set()
        result = self.app.join(submitted.ref, timeout=2)
        self.assertEqual(result.status, "completed")

    def test_wait_can_resume_while_sibling_operator_is_still_running(self) -> None:
        """Verify wait can resume while sibling operator is still running."""
        slow_started = threading.Event()
        release_slow = threading.Event()
        resumed_path = threading.Event()

        def slow(value: Value) -> Value:
            slow_started.set()
            release_slow.wait(2)
            return value

        def after(_value: Response) -> Response:
            resumed_path.set()
            return _value

        workflow = Workflow(
            "parallel-wait-app",
            nodes=[
                Node("start", identity),
                Node("slow", slow),
                Node("approval", Wait(Request, Response), input_mapping=wait_request),
                Node("after", after),
            ],
            edges=[
                Edge("start", "slow"),
                Edge("start", "approval"),
                Edge("approval", "after"),
            ],
        )
        submitted = self.app.submit_invoke(workflow, {"value": 1})
        self.assertTrue(slow_started.wait(1))
        wait_id = None
        for _ in range(100):
            state = self.journal.state(submitted.session_id)
            waits = tuple(state.invocation.scheduler.waits.values())
            if waits:
                wait_id = waits[0].id
                break
            time.sleep(0.005)
        self.assertIsNotNone(wait_id)

        resume_thread = threading.Thread(
            target=lambda: self.app.resume(
                submitted.ref, wait_id, {"answer": "yes"}  # type: ignore[arg-type]
            )
        )
        resume_thread.start()
        self.assertTrue(
            resumed_path.wait(1),
            "resumed Wait did not advance while sibling was running",
        )
        release_slow.set()
        resume_thread.join(2)
        self.assertFalse(resume_thread.is_alive())
        self.assertEqual(self.app.join(submitted.ref, 2).status, "completed")

    def test_loop_executes_scoped_occurrences_until_exit(self) -> None:
        """Verify loop executes scoped occurrences until exit."""
        workflow = Workflow(
            "loop-app",
            nodes=[
                Node("start", identity),
                Node("header", identity),
                Node("body", increment),
                Node("finish", identity),
            ],
            edges=[
                Edge("start", "header"),
                Edge("header", "body"),
                Edge("body", "header", continue_loop, id="back"),
                Edge("body", "finish", exit_loop, id="exit"),
            ],
        )
        result = self.app.invoke(workflow, {"value": 0})
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.output, {"value": 3})
        state = self.journal.state(result.session_id)
        headers = [
            item
            for item in state.invocation.scheduler.occurrences.values()
            if item.node_id == "header"
        ]
        self.assertEqual(len(headers), 3)

    def test_wait_resume_and_second_invocation_in_same_session(self) -> None:
        """Verify wait resume and second invocation in same session."""
        workflow = Workflow(
            "wait-app",
            nodes=[
                Node(
                    "approval",
                    Wait(Request, Response),
                    input_mapping=wait_request,
                )
            ],
        )
        waiting = self.app.invoke(workflow, {"value": 1}, session_id="session")
        self.assertEqual(waiting.status, "waiting")
        self.assertEqual(len(waiting.waits), 1)
        completed = self.app.resume(
            waiting.ref,
            waiting.waits[0].id,
            {"answer": "yes"},
        )
        self.assertEqual(completed.status, "completed")
        self.assertEqual(completed.output, {"answer": "yes"})

        again = self.app.invoke(workflow, {"value": 2}, session_id="session")
        self.assertEqual(again.status, "waiting")
        self.assertNotEqual(again.invocation_id, waiting.invocation_id)

    def test_wait_inside_loop_resumes_the_exact_scoped_occurrence(self) -> None:
        """Verify wait inside loop resumes the exact scoped occurrence."""
        workflow = Workflow(
            "wait-loop-app",
            nodes=[
                Node("start", identity),
                Node("header", identity),
                Node(
                    "wait",
                    Wait(Request, Value),
                    input_mapping=wait_loop_request,
                ),
                Node("body", identity),
                Node("finish", identity),
            ],
            edges=[
                Edge("start", "header"),
                Edge("header", "wait"),
                Edge("wait", "body"),
                Edge("body", "header", below_two, id="back"),
                Edge("body", "finish", at_least_two, id="exit"),
            ],
        )
        first = self.app.invoke(workflow, {"value": 0})
        self.assertEqual(first.status, "waiting")
        second = self.app.resume(first.ref, first.waits[0].id, {"value": 1})
        self.assertEqual(second.status, "waiting")
        waiting = second.waits
        self.assertEqual(len(waiting), 1)
        final = self.app.resume(first.ref, waiting[0].id, {"value": 2})
        self.assertEqual(final.status, "completed")
        self.assertEqual(final.output, {"value": 2})

    def test_active_session_rejects_another_invocation_and_cancel_is_terminal(self) -> None:
        """Verify active session rejects another invocation and cancel is terminal."""
        workflow = Workflow(
            "active",
            nodes=[Node("approval", Wait(Request, Response), input_mapping=wait_request)],
        )
        waiting = self.app.invoke(workflow, {}, session_id="active-session")
        with self.assertRaisesRegex(RuntimeTransitionError, "SESSION_INVOCATION_ACTIVE"):
            self.app.invoke(workflow, {}, session_id="active-session")
        cancelled = self.app.cancel(waiting.ref, "stop")
        self.assertEqual(cancelled.status, "cancelled")

    def test_child_workflow_await_and_spawn_modes(self) -> None:
        """Verify child workflow await and spawn modes."""
        child = Workflow("child", nodes=[Node("increment", increment)])
        awaited = Workflow("parent-await", nodes=[Node("child", child)])
        awaited_result = self.app.invoke(awaited, {"value": 4})
        self.assertEqual(awaited_result.output, {"value": 5})

        spawned = Workflow(
            "parent-spawn",
            nodes=[Node("child", child, execution_mode="spawn")],
        )
        spawned_result = self.app.invoke(spawned, {"value": 4})
        self.assertEqual(spawned_result.status, "completed")
        self.assertEqual(spawned_result.output.workflow_id, "child")
        self.assertTrue(spawned_result.output.invocation_id)
        self.assertTrue(spawned_result.output.session_id)
        child_result = self.app.join(spawned_result.output, timeout=1)
        self.assertEqual(child_result.status, "completed")
        self.assertEqual(child_result.output, {"value": 5})
        self.assertEqual(
            self.app.status(spawned_result.output).invocation_id,
            spawned_result.output.invocation_id,
        )

    def test_map_await_child_workflows_preserves_order_and_parallel_limit(self) -> None:
        """Verify mapped awaited Children run concurrently and return input order."""

        active = 0
        peak = 0

        async def delayed(value: Value) -> Value:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep((4 - value["value"]) * 0.01)
                return {"value": value["value"] + 10}
            finally:
                active -= 1

        child = Workflow("mapped-await-child", nodes=[Node("work", delayed)])
        parent = Workflow(
            "mapped-await-parent",
            nodes=[
                Node(
                    "children",
                    child,
                    input_mapping=map_items,
                    map=Map(max_parallelism=2),
                )
            ],
        )
        result = self.app.invoke(
            parent,
            {"items": [{"value": 1}, {"value": 2}, {"value": 3}]},
        )
        self.assertEqual(
            result.output,
            [{"value": 11}, {"value": 12}, {"value": 13}],
        )
        self.assertEqual(peak, 2)
        self.assertEqual(len(self.app.child_invocations(result.ref)), 3)

    def test_map_await_child_workflows_supports_aggregation_and_empty_input(self) -> None:
        """Verify Child Map aggregation and empty Map use normal Node outputs."""

        child = Workflow("aggregate-child", nodes=[Node("work", increment)])
        aggregated = Workflow(
            "aggregate-parent",
            nodes=[
                Node(
                    "children",
                    child,
                    input_mapping=map_items,
                    map=Map(aggregate=aggregate_child_values, max_parallelism=3),
                )
            ],
        )
        result = self.app.invoke(
            aggregated,
            {"items": [{"value": 1}, {"value": 2}, {"value": 3}]},
        )
        self.assertEqual(result.output, {"value": 9})

        empty = Workflow(
            "empty-child-map",
            nodes=[
                Node(
                    "children",
                    child,
                    input_mapping=map_items,
                    map=Map(max_parallelism=2),
                )
            ],
        )
        empty_result = self.app.invoke(empty, {"items": []})
        self.assertEqual(empty_result.output, [])
        self.assertEqual(self.app.child_invocations(empty_result.ref), ())

    def test_map_spawn_child_workflows_returns_ordered_handles_and_limits_children(self) -> None:
        """Verify mapped Spawn returns stable handles while limiting Child execution."""

        active = 0
        peak = 0

        async def delayed(value: Value) -> Value:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.03)
                return {"value": value["value"] + 1}
            finally:
                active -= 1

        child = Workflow("mapped-spawn-child", nodes=[Node("work", delayed)])
        parent = Workflow(
            "mapped-spawn-parent",
            nodes=[
                Node(
                    "children",
                    child,
                    input_mapping=map_items,
                    map=Map(max_parallelism=2),
                    execution_mode="spawn",
                )
            ],
        )
        result = self.app.invoke(
            parent,
            {"items": [{"value": 1}, {"value": 2}, {"value": 3}]},
        )
        handles = result.output
        self.assertEqual(len(handles), 3)
        self.assertEqual(
            tuple(handles),
            self.app.child_invocations(result.ref),
        )
        outputs = [self.app.join(handle, timeout=1).output for handle in handles]
        self.assertEqual(outputs, [{"value": 2}, {"value": 3}, {"value": 4}])
        self.assertEqual(peak, 2)

    def test_map_spawn_child_workflows_supports_aggregation_and_empty_input(self) -> None:
        """Verify Spawn Map can aggregate handles and creates nothing for empty input."""

        child = Workflow("spawn-aggregate-child", nodes=[Node("work", increment)])
        parent = Workflow(
            "spawn-aggregate-parent",
            nodes=[
                Node(
                    "children",
                    child,
                    input_mapping=map_items,
                    map=Map(aggregate=aggregate_child_invocations, max_parallelism=2),
                    execution_mode="spawn",
                )
            ],
        )
        result = self.app.invoke(
            parent,
            {"items": [{"value": 1}, {"value": 2}]},
        )
        self.assertEqual(result.output, {"count": 2})
        handles = self.app.child_invocations(result.ref)
        self.assertEqual(len(handles), 2)
        for handle in handles:
            self.assertEqual(self.app.join(handle, timeout=1).status, "completed")

        empty = self.app.invoke(parent, {"items": []})
        self.assertEqual(empty.output, {"count": 0})
        self.assertEqual(self.app.child_invocations(empty.ref), ())

    def test_map_await_child_failure_cancels_and_settles_siblings(self) -> None:
        """Verify one failed awaited Child settles every sibling before parent failure."""

        async def child_work(value: Value) -> Value:
            if value["value"] == 0:
                await asyncio.sleep(0.01)
                raise RuntimeError("child failed")
            await asyncio.sleep(10)
            return value

        child = Workflow("mapped-failure-child", nodes=[Node("work", child_work)])
        parent = Workflow(
            "mapped-failure-parent",
            nodes=[
                Node(
                    "children",
                    child,
                    input_mapping=map_items,
                    map=Map(max_parallelism=3),
                )
            ],
        )
        started_at = time.monotonic()
        result = self.app.invoke(
            parent,
            {"items": [{"value": 1}, {"value": 0}, {"value": 2}]},
        )
        self.assertLess(time.monotonic() - started_at, 2)
        self.assertEqual(result.status, "failed")
        handles = self.app.child_invocations(result.ref)
        self.assertEqual(len(handles), 3)
        self.assertTrue(
            all(
                self.app.status(handle).status in {"failed", "cancelled"}
                for handle in handles
            )
        )
        parent_state = self.journal.state(result.session_id).invocation
        assert parent_state is not None
        plan = next(iter(parent_state.child_plans.values()))
        self.assertTrue(all(unit.phase == "terminal" for unit in plan.units))

    def test_cancelling_parent_await_map_converges_every_child(self) -> None:
        """Verify cancelling an awaited Child Map cancels all active Children."""

        started = threading.Event()
        active = 0

        async def child_work(value: Value) -> Value:
            nonlocal active
            active += 1
            if active == 2:
                started.set()
            try:
                await asyncio.sleep(10)
                return value
            finally:
                active -= 1

        child = Workflow("mapped-cancel-child", nodes=[Node("work", child_work)])
        parent = Workflow(
            "mapped-cancel-parent",
            nodes=[
                Node(
                    "children",
                    child,
                    input_mapping=map_items,
                    map=Map(max_parallelism=2),
                )
            ],
        )
        submitted = self.app.submit_invoke(
            parent,
            {"items": [{"value": 1}, {"value": 2}, {"value": 3}]},
        )
        self.assertTrue(started.wait(1))
        cancelled = self.app.cancel(submitted.ref, "stop mapped children")
        self.assertEqual(cancelled.status, "cancelled")
        handles = self.app.child_invocations(submitted.ref)
        # Planning materializes stable handles for every Map unit before work starts.
        self.assertEqual(len(handles), 3)
        self.assertTrue(
            all(self.app.status(handle).status == "cancelled" for handle in handles)
        )

    def test_spawn_handle_can_cancel_a_running_child_invocation(self) -> None:
        """Verify spawn handle can cancel a running child invocation."""
        started = threading.Event()

        async def long_child(value: Value) -> Value:
            started.set()
            await asyncio.sleep(10)
            return value

        child = Workflow("long-child", nodes=[Node("work", long_child)])
        parent = Workflow(
            "spawn-cancel",
            nodes=[Node("child", child, execution_mode="spawn")],
        )
        spawned = self.app.invoke(parent, {"value": 3})
        self.assertTrue(started.wait(1))
        cancelled = self.app.cancel(spawned.output, "parent stopped child")
        self.assertEqual(cancelled.status, "cancelled")
        self.assertEqual(cancelled.invocation_id, spawned.output.invocation_id)
        child_state = self.journal.state(spawned.output.session_id)
        self.assertFalse(
            any(
                call.status == "running"
                for call in child_state.invocation.scheduler.operator_calls.values()
            )
        )

    def test_cancelling_parent_await_converges_child_invocation(self) -> None:
        """Verify cancelling parent await converges child invocation."""
        started = threading.Event()

        async def long_child(value: Value) -> Value:
            started.set()
            await asyncio.sleep(10)
            return value

        child = Workflow("awaited-long-child", nodes=[Node("work", long_child)])
        parent = Workflow("await-child-cancel", nodes=[Node("child", child)])
        submitted = self.app.submit_invoke(parent, {"value": 1})
        self.assertTrue(started.wait(1))
        handle = self.app.child_invocations(submitted.ref)[0]
        parent_result = self.app.cancel(submitted.ref, "stop parent")
        self.assertEqual(parent_result.status, "cancelled")
        self.assertEqual(self.app.status(handle).status, "cancelled")

    def test_awaited_child_waits_for_resume_instead_of_failing_parent(self) -> None:
        """Verify awaited child waits for resume instead of failing parent."""
        child = Workflow(
            "waiting-child",
            nodes=[Node("approval", Wait(Value, Value))],
        )
        parent = Workflow("parent-waits-child", nodes=[Node("child", child)])
        submitted = self.app.submit_invoke(parent, {"value": 1})
        deadline = time.monotonic() + 1
        handles = ()
        while time.monotonic() < deadline:
            handles = self.app.child_invocations(submitted.ref)
            if handles and self.app.status(handles[0]).status == "waiting":
                break
            time.sleep(0.001)
        self.assertEqual(len(handles), 1)
        child_waiting = self.app.status(handles[0])
        self.assertEqual(child_waiting.status, "waiting")
        self.app.resume(
            handles[0],
            child_waiting.waits[0].id,
            {"value": 8},
        )
        completed = self.app.join(submitted.ref, 1)
        self.assertEqual(completed.status, "completed")
        self.assertEqual(completed.output, {"value": 8})

    def test_same_workflow_id_keeps_multiple_revisions(self) -> None:
        """Verify same workflow id keeps multiple revisions."""
        first = self.app.register_workflow(
            Workflow("same", nodes=[Node("one", identity)])
        )
        second = self.app.register_workflow(
            Workflow("same", nodes=[Node("two", increment)])
        )
        self.assertNotEqual(first.workflow_revision_id, second.workflow_revision_id)
        self.assertEqual(
            self.app.workflow_definition_snapshot(first.workflow_revision_id).definition_hash,
            first.definition_hash,
        )
        self.assertEqual(
            self.app.workflow_definition_snapshot("same").definition_hash,
            second.definition_hash,
        )
        old_result = self.app.invoke(first.workflow_revision_id, {"value": 1})
        latest_result = self.app.invoke("same", {"value": 1})
        self.assertEqual(old_result.output, {"value": 1})
        self.assertEqual(latest_result.output, {"value": 2})

    def test_recovery_resolves_the_historical_revision_not_latest(self) -> None:
        """Verify a loaded checkpoint resolves its exact historical revision."""
        started = threading.Event()
        calls = 0

        async def old_handler(value: Value) -> Value:
            nonlocal calls
            calls += 1
            started.set()
            if calls == 1:
                await asyncio.sleep(10)
            return value

        old = Workflow(
            "revision-recovery",
            nodes=[
                Node(
                    "old",
                    old_handler,
                    recovery_mode=Recovery("replay_safe"),
                )
            ],
        )
        new = Workflow(
            "revision-recovery",
            nodes=[Node("new", increment)],
        )
        source = AutoAgentApp()
        source.submit_invoke(old, {"value": 5}, session_id="revision-session")
        self.assertTrue(started.wait(1))
        source.register_workflow(new)
        checkpoint = source.close().sessions[0]

        recovered_journal = InMemoryEventJournal()
        recovered_app = AutoAgentApp(runtime_journal=recovered_journal)
        try:
            old_ir = recovered_app.register_workflow(old)
            new_ir = recovered_app.register_workflow(new)
            self.assertNotEqual(
                old_ir.workflow_revision_id, new_ir.workflow_revision_id
            )
            loaded = recovered_app.load_checkpoint(checkpoint)
            recovered = recovered_app.recover(loaded.invocations[0])
            self.assertEqual(recovered.status, "completed")
            self.assertEqual(recovered.output, {"value": 5})
            self.assertEqual(
                recovered_journal.state(
                    recovered.session_id
                ).invocation.workflow_revision_id,
                old_ir.workflow_revision_id,
            )
        finally:
            recovered_app.close()

    def test_live_recovery_is_rejected_without_duplicate_execution(self) -> None:
        """Verify live recovery is rejected without duplicate execution."""
        started = threading.Event()
        release = threading.Event()
        calls = 0

        def blocking(value: Value) -> Value:
            nonlocal calls
            calls += 1
            started.set()
            release.wait(2)
            return value

        submitted = self.app.submit_invoke(
            Workflow("live-recovery", nodes=[Node("work", blocking)]),
            {"value": 1},
        )
        self.assertTrue(started.wait(1))
        with self.assertRaisesRegex(RuntimeTransitionError, "INVOCATION_STILL_LIVE"):
            self.app.recover(submitted.ref)
        release.set()
        completed = self.app.join(submitted.ref, 1)
        self.assertEqual(completed.status, "completed")
        self.assertEqual(calls, 1)

    def test_new_app_recovers_running_invocation_from_checkpoint(self) -> None:
        """Verify another App resumes replay-safe in-flight work from a checkpoint."""
        started = threading.Event()
        calls = 0

        async def blocking(value: Value) -> Value:
            nonlocal calls
            calls += 1
            started.set()
            if calls == 1:
                await asyncio.sleep(10)
            return value

        workflow = Workflow(
            "event-recovery",
            nodes=[Node("node", blocking, recovery_mode=Recovery("replay_safe"))],
        )
        source = AutoAgentApp()
        source.submit_invoke(workflow, {"value": 9}, session_id="recovery-session")
        self.assertTrue(started.wait(1))
        checkpoint = source.close().sessions[0]

        recovered_journal = InMemoryEventJournal()
        recovered_app = AutoAgentApp(runtime_journal=recovered_journal)
        try:
            recovered_app.register_workflow(workflow)
            loaded = recovered_app.load_checkpoint(checkpoint)
            recovered = recovered_app.recover(loaded.invocations[0])
            self.assertEqual(recovered.status, "completed")
            self.assertEqual(recovered.output, {"value": 9})
            state = recovered_journal.state(recovered.session_id)
            self.assertEqual(
                sum(
                    call.status == "completed"
                    for call in state.invocation.scheduler.operator_calls.values()
                ),
                1,
            )
            self.assertEqual(calls, 2)
        finally:
            recovered_app.close()

    def test_crash_recovery_rejects_running_node_without_replay_permission(self) -> None:
        """Verify crash recovery rejects running node without replay permission."""
        started = threading.Event()
        calls = 0

        async def side_effect(value: Value) -> Value:
            nonlocal calls
            calls += 1
            started.set()
            await asyncio.sleep(10)
            return value

        workflow = Workflow("unsafe-recovery", nodes=[Node("node", side_effect)])
        source = AutoAgentApp()
        source.submit_invoke(workflow, {"value": 4}, session_id="unsafe-session")
        self.assertTrue(started.wait(1))
        checkpoint = source.close().sessions[0]
        recovered_app = AutoAgentApp()
        try:
            recovered_app.register_workflow(workflow)
            loaded = recovered_app.load_checkpoint(checkpoint)
            recovered = recovered_app.recover(loaded.invocations[0])
            self.assertEqual(recovered.status, "failed")
            self.assertEqual(recovered.error.type, "RecoveryNotAllowed")
            self.assertEqual(calls, 1)
        finally:
            recovered_app.close()

    def test_crash_recovery_attempt_budget_is_enforced(self) -> None:
        """Verify crash recovery attempt budget is enforced."""
        started = threading.Event()
        calls = 0

        async def replay_safe(value: Value) -> Value:
            nonlocal calls
            calls += 1
            started.set()
            await asyncio.sleep(10)
            return value

        workflow = Workflow(
            "recovery-budget",
            nodes=[
                Node(
                    "node",
                    replay_safe,
                    recovery_mode=Recovery("replay_safe", max_attempts=1),
                )
            ],
        )
        source = AutoAgentApp()
        source.submit_invoke(workflow, {"value": 7}, session_id="budget-session")
        self.assertTrue(started.wait(1))
        checkpoint = source.close().sessions[0]
        state = checkpoint.state
        invocation = state.invocation
        assert invocation is not None
        occurrence_id = next(
            occurrence.id
            for occurrence in invocation.scheduler.occurrences.values()
            if occurrence.status == "running"
        )
        prefix_journal = InMemoryEventJournal()
        prefix_journal.install_states({checkpoint.session_id: checkpoint.state})
        prefix_journal.append(
            StateTransition(
                session_id=checkpoint.session_id,
                invocation_id=invocation.id,
                occurred_at_ns=state.session.updated_at_ns + 1,
                payload=InvocationRecoveryRequested(),
            ).to_runtime_event(state.sequence + 1)
        )
        recovered_once = prefix_journal.state(checkpoint.session_id)
        prefix_journal.append(
            StateTransition(
                session_id=checkpoint.session_id,
                invocation_id=invocation.id,
                occurred_at_ns=recovered_once.session.updated_at_ns + 1,
                payload=NodeOccurrenceStarted(occurrence_id),
            ).to_runtime_event(recovered_once.sequence + 1)
        )
        exhausted_checkpoint = prefix_journal.capture_checkpoint(
            checkpoint.session_id
        )
        recovered_app = AutoAgentApp()
        try:
            recovered_app.register_workflow(workflow)
            loaded = recovered_app.load_checkpoint(exhausted_checkpoint)
            result = recovered_app.recover(loaded.invocations[0])
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.error.type, "RecoveryAttemptsExceeded")
            self.assertEqual(calls, 1)
        finally:
            recovered_app.close()


if __name__ == "__main__":
    unittest.main()
