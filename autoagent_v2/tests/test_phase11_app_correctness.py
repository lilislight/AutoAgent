from __future__ import annotations

import asyncio
import threading
import unittest
from collections.abc import AsyncIterator
from dataclasses import replace

from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp,
    AppCheckpoint,
    Capability,
    ConditionContext,
    ContextOperation,
    ContextPatch,
    Edge,
    InputMappingContext,
    InvocationRef,
    InvocationResult,
    InvocationUpdate,
    Map,
    Node,
    Operator,
    OutputBindingContext,
    Recovery,
    RuntimeTransitionError,
    Stream,
    StreamContext,
    TraceEvent,
    Wait,
    Workflow,
)
from autoagent.core import (
    InMemoryEventJournal,
    NodeOccurrenceCompleted,
    RuntimeCheckpointBundle,
    RuntimeEvent,
    StateReducer,
)


class Value(TypedDict):
    value: int


class ErrorValue(TypedDict):
    message: str


def identity(value: Value) -> Value:
    return value


def map_items(context: InputMappingContext) -> list[Value]:
    return context.invocation_input["items"]  # type: ignore[index,return-value]


def forged_identity(_value: Value) -> Value:
    return {"value": 999}


def fail(_value: Value) -> Value:
    raise RuntimeError("failed")


def error_mapping(context: InputMappingContext) -> ErrorValue:
    error = next(iter(context.incoming.values()))
    return {"message": error["message"]}  # type: ignore[index]


def accept_error(value: ErrorValue) -> ErrorValue:
    return value


class AppCorrectnessTests(unittest.TestCase):
    def test_user_operator_cancelled_error_becomes_terminal_failure(self) -> None:
        """Verify a user-raised CancelledError cannot leave a running occurrence."""

        async def self_cancel(_value: Value) -> Value:
            raise asyncio.CancelledError

        app = AutoAgentApp()
        try:
            items = list(
                app.stream(
                    Workflow(
                        "operator-self-cancel",
                        nodes=[Node("work", self_cancel)],
                    ),
                    {"value": 1},
                    session_id="operator-self-cancel-session",
                )
            )
            result = items[-1]
            self.assertIsInstance(result, InvocationResult)
            self.assertEqual(result.status, "failed")
            self.assertIsNotNone(result.error)
            self.assertEqual(result.error.type, "CancelledError")
            state = result.checkpoint.state(result.session_id)
            self.assertTrue(
                all(
                    occurrence.status != "running"
                    for occurrence in state.invocation.scheduler.occurrences.values()
                )
            )
            kinds = {
                item.event.kind
                for item in items
                if isinstance(item, InvocationUpdate)
            }
            self.assertIn("operator_call.failed", kinds)
            self.assertIn("node_occurrence.failed", kinds)
            self.assertIn("invocation.failed", kinds)
        finally:
            app.close()

    def test_user_hook_cancelled_error_becomes_terminal_failure(self) -> None:
        """Verify a user Hook cannot use CancelledError as control flow."""

        async def self_cancel(_context: InputMappingContext) -> Value:
            raise asyncio.CancelledError

        app = AutoAgentApp()
        try:
            result = app.invoke(
                Workflow(
                    "hook-self-cancel",
                    nodes=[Node("work", identity, input_mapping=self_cancel)],
                ),
                {"value": 1},
            )
            self.assertEqual(result.status, "failed")
            self.assertIsNotNone(result.error)
            self.assertEqual(result.error.type, "CancelledError")
            state = result.checkpoint.state(result.session_id)
            occurrence = next(iter(state.invocation.scheduler.occurrences.values()))
            self.assertEqual(occurrence.status, "failed")
        finally:
            app.close()

    def test_non_streaming_invoke_captures_only_its_return_boundary(self) -> None:
        """Verify non-streaming execution does not build discarded checkpoints."""

        class CountingJournal(InMemoryEventJournal):
            captures = 0

            def capture_checkpoint(self, root_session_id, *, captured_at_ns=None):
                self.captures += 1
                return super().capture_checkpoint(
                    root_session_id, captured_at_ns=captured_at_ns
                )

        journal = CountingJournal()
        app = AutoAgentApp(runtime_journal=journal)
        try:
            result = app.invoke(
                Workflow(
                    "single-result-checkpoint",
                    nodes=[Node("first", identity), Node("second", identity)],
                    edges=[Edge("first", "second")],
                ),
                {"value": 1},
            )
            self.assertEqual(result.status, "completed")
            self.assertEqual(journal.captures, 1)
        finally:
            app.close()

    def test_stream_checkpoint_after_node_completion_restores_remaining_work(self) -> None:
        """Verify a mid-stream checkpoint resumes after completed work without replay."""

        calls: list[str] = []

        def first(value: Value) -> Value:
            calls.append("first")
            return {"value": value["value"] + 1}

        def second(value: Value) -> Value:
            calls.append("second")
            return {"value": value["value"] + 1}

        workflow = Workflow(
            "stream-checkpoint-resume",
            nodes=[Node("first", first), Node("second", second)],
            edges=[Edge("first", "second")],
        )
        source = AutoAgentApp()
        stream = source.stream(
            workflow,
            {"value": 1},
            session_id="stream-checkpoint-session",
        )
        checkpoint = None
        try:
            for item in stream:
                if not isinstance(item, InvocationUpdate) or not isinstance(
                    item.event, TraceEvent
                ):
                    continue
                if item.event.kind == "operator_call.started":
                    self.assertIsNone(item.checkpoint)
                if item.event.kind == "node_occurrence.started":
                    self.assertIsNotNone(item.checkpoint)
                if item.event.kind != "node_occurrence.completed":
                    continue
                assert item.checkpoint is not None
                occurrence_id = item.event.subject_ids["occurrence_id"]
                state = item.checkpoint.state(item.event.session_id)
                occurrence = state.invocation.scheduler.occurrences[occurrence_id]
                if occurrence.node_id == "first":
                    checkpoint = item.checkpoint
                    break
        finally:
            stream.close()
            source.close()

        self.assertIsNotNone(checkpoint)
        self.assertEqual(calls, ["first"])
        recovered = AutoAgentApp()
        try:
            recovered.register_workflow(workflow)
            loaded = recovered.load_checkpoint(checkpoint)
            result = recovered.recover(loaded.roots[0])
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"value": 3})
            self.assertEqual(calls, ["first", "second"])
        finally:
            recovered.close()

    def test_astream_checkpoint_matches_each_safe_trace_boundary(self) -> None:
        """Verify async stream updates atomically pair safe Trace boundaries with State."""

        async def run() -> None:
            app = AutoAgentApp()
            try:
                items = [
                    item
                    async for item in app.astream(
                        Workflow(
                            "astream-checkpoint-boundaries",
                            nodes=[Node("node", identity)],
                        ),
                        {"value": 1},
                        session_id="astream-checkpoint-session",
                    )
                ]
                updates = [
                    item for item in items if isinstance(item, InvocationUpdate)
                ]
                checkpoint_updates = [
                    item for item in updates if item.checkpoint is not None
                ]
                self.assertEqual(
                    {item.event.kind for item in checkpoint_updates},
                    {
                        "scheduler.initialized",
                        "node_occurrence.started",
                        "node_occurrence.completed",
                        "invocation.completed",
                    },
                )
                self.assertEqual(
                    len({item.checkpoint.id for item in checkpoint_updates}),
                    len(checkpoint_updates),
                )
                for item in checkpoint_updates:
                    self.assertIsInstance(item.event, TraceEvent)
                    state = item.checkpoint.state("astream-checkpoint-session")
                    self.assertEqual(state.state_version, item.event.state_version)
                result = items[-1]
                self.assertIsInstance(result, InvocationResult)
                self.assertEqual(result.status, "completed")
                self.assertEqual(
                    result.checkpoint.state("astream-checkpoint-session").invocation.output,
                    {"value": 1},
                )
            finally:
                await app.aclose()

        asyncio.run(run())

    def test_stream_node_start_checkpoint_recovers_interrupted_operator(self) -> None:
        """Verify the latest pre-call stream checkpoint can replay interrupted work."""

        started = threading.Event()
        calls = 0

        async def replayable(value: Value) -> Value:
            nonlocal calls
            calls += 1
            started.set()
            if calls == 1:
                await asyncio.Event().wait()
            return value

        workflow = Workflow(
            "stream-running-checkpoint",
            nodes=[
                Node(
                    "work",
                    replayable,
                    recovery_mode=Recovery("replay_safe"),
                )
            ],
        )
        source = AutoAgentApp()
        stream = source.stream(
            workflow,
            {"value": 7},
            session_id="stream-running-checkpoint-session",
        )
        checkpoint = None
        waiter_errors: list[BaseException] = []
        waiter: threading.Thread | None = None
        try:
            for item in stream:
                if (
                    isinstance(item, InvocationUpdate)
                    and isinstance(item.event, TraceEvent)
                    and item.event.kind == "node_occurrence.started"
                ):
                    checkpoint = item.checkpoint
                    break
            self.assertIsNotNone(checkpoint)
            assert checkpoint is not None
            state = checkpoint.state("stream-running-checkpoint-session")
            self.assertEqual(state.invocation.status, "running")
            self.assertTrue(
                any(
                    occurrence.status == "running"
                    for occurrence in state.invocation.scheduler.occurrences.values()
                )
            )

            call_started = next(stream)
            self.assertEqual(call_started.event.kind, "operator_call.started")
            self.assertIsNone(call_started.checkpoint)

            def request_next() -> None:
                try:
                    next(stream)
                except StopIteration:
                    return
                except BaseException as error:
                    waiter_errors.append(error)

            waiter = threading.Thread(target=request_next)
            waiter.start()
            self.assertTrue(started.wait(1))
        finally:
            stream.close()
            if waiter is not None:
                waiter.join(1)
            source.close()

        self.assertEqual(waiter_errors, [])
        self.assertEqual(calls, 1)
        assert checkpoint is not None
        recovered = AutoAgentApp()
        try:
            recovered.register_workflow(workflow)
            loaded = recovered.load_checkpoint(checkpoint)
            result = recovered.recover(loaded.roots[0])
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"value": 7})
            self.assertEqual(calls, 2)
        finally:
            recovered.close()

    def test_map_partial_calls_recover_from_the_whole_node_boundary(self) -> None:
        """Verify partial Map calls are replayed from the Node start checkpoint."""

        calls: list[int] = []
        block_following = True

        async def record(value: Value) -> Value:
            nonlocal block_following
            calls.append(value["value"])
            if block_following and value["value"] > 0:
                await asyncio.Event().wait()
            return value

        workflow = Workflow(
            "stream-map-atomic-recovery",
            nodes=[
                Node(
                    "map",
                    record,
                    input_mapping=map_items,
                    map=Map(max_parallelism=1),
                    recovery_mode=Recovery("replay_safe"),
                )
            ],
        )
        source = AutoAgentApp()
        stream = source.stream(
            workflow,
            {"items": [{"value": 0}, {"value": 1}, {"value": 2}]},
            session_id="stream-map-atomic-session",
        )
        checkpoint = None
        try:
            for item in stream:
                if isinstance(item, InvocationUpdate) and item.checkpoint is not None:
                    checkpoint = item.checkpoint
                if (
                    isinstance(item, InvocationUpdate)
                    and item.event.kind == "operator_call.completed"
                ):
                    self.assertIsNone(item.checkpoint)
                    break
        finally:
            stream.close()
            source.close()
            block_following = False

        self.assertIsNotNone(checkpoint)
        source_calls = tuple(calls)
        self.assertTrue(source_calls)
        self.assertEqual(source_calls[0], 0)
        self.assertLess(len(source_calls), 3)
        recovered = AutoAgentApp()
        try:
            recovered.register_workflow(workflow)
            loaded = recovered.load_checkpoint(checkpoint)
            result = recovered.recover(loaded.roots[0])
            self.assertEqual(result.status, "completed")
            self.assertEqual(
                result.output,
                [{"value": 0}, {"value": 1}, {"value": 2}],
            )
            self.assertEqual(calls, [*source_calls, 0, 1, 2])
        finally:
            recovered.close()

    def test_stream_chunks_do_not_replace_the_node_recovery_boundary(self) -> None:
        """Verify observed chunks still recover by replaying their whole Operator."""

        calls = 0

        async def chunks(value: Value) -> AsyncIterator[Value]:
            nonlocal calls
            calls += 1
            yield {"value": value["value"] + 1}
            yield {"value": value["value"] + 2}

        class LastValueReducer:
            def initial(self, context: StreamContext) -> Value:
                return context.input  # type: ignore[return-value]

            def add(
                self, _context: StreamContext, _state: Value, chunk: Value
            ) -> Value:
                return chunk

            def finish(self, _context: StreamContext, state: Value) -> Value:
                return state

        workflow = Workflow(
            "stream-chunk-atomic-recovery",
            nodes=[
                Node(
                    "stream",
                    chunks,
                    stream=Stream(LastValueReducer()),
                    recovery_mode=Recovery("replay_safe"),
                )
            ],
        )
        source = AutoAgentApp()
        stream = source.stream(
            workflow,
            {"value": 3},
            session_id="stream-chunk-atomic-session",
        )
        checkpoint = None
        try:
            for item in stream:
                if isinstance(item, InvocationUpdate) and item.checkpoint is not None:
                    checkpoint = item.checkpoint
                if (
                    isinstance(item, InvocationUpdate)
                    and item.event.kind == "stream.chunk"
                ):
                    self.assertIsNone(item.checkpoint)
                    break
        finally:
            stream.close()
            source.close()

        self.assertIsNotNone(checkpoint)
        self.assertEqual(calls, 1)
        recovered = AutoAgentApp()
        try:
            recovered.register_workflow(workflow)
            loaded = recovered.load_checkpoint(checkpoint)
            result = recovered.recover(loaded.roots[0])
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"value": 5})
            self.assertEqual(calls, 2)
        finally:
            recovered.close()

    def test_parallel_runtime_transitions_reach_sink_once_and_in_order(self) -> None:
        """Verify one Session serializes canonical Event export across parallel Nodes."""

        class SlowSink:
            def __init__(self) -> None:
                self.events: list[RuntimeEvent] = []

            async def append(self, event: RuntimeEvent) -> None:
                await asyncio.sleep(0.001)
                self.events.append(event)

        async def delayed(value: Value) -> Value:
            await asyncio.sleep(0)
            return value

        sink = SlowSink()
        app = AutoAgentApp(runtime_event_sink=sink)
        workflow = Workflow(
            "parallel-event-export",
            nodes=[
                Node("start", identity),
                *(Node(f"branch-{index}", delayed) for index in range(4)),
            ],
            edges=[Edge("start", f"branch-{index}") for index in range(4)],
        )
        try:
            result = app.invoke(
                workflow,
                {"value": 1},
                session_id="parallel-event-session",
            )
            self.assertEqual(result.status, "completed")
            sequences = [event.sequence for event in sink.events]
            self.assertEqual(sequences, list(range(1, len(sequences) + 1)))
            self.assertEqual(len({event.id for event in sink.events}), len(sink.events))
            self.assertEqual(
                StateReducer().reduce(tuple(sink.events)),
                result.checkpoint.state(result.session_id),
            )
        finally:
            app.close()

    def test_child_admission_updates_share_the_parent_stream_and_checkpoint_root(self) -> None:
        """Verify early Child admission is observed and captured under its parent Root."""

        child = Workflow("stream-child", nodes=[Node("work", identity)])
        parent = Workflow("stream-parent", nodes=[Node("child", child)])
        app = AutoAgentApp()
        try:
            items = list(
                app.stream(
                    parent,
                    {"value": 1},
                    session_id="stream-parent-session",
                )
            )
            updates = [
                item
                for item in items
                if isinstance(item, InvocationUpdate)
                and isinstance(item.event, TraceEvent)
            ]
            child_sessions = {
                item.event.session_id
                for item in updates
                if item.event.kind == "session.opened"
                and item.event.session_id != "stream-parent-session"
            }
            self.assertEqual(len(child_sessions), 1)
            child_session_id = next(iter(child_sessions))
            child_scheduler = next(
                item
                for item in updates
                if item.event.kind == "scheduler.initialized"
                and item.event.session_id == child_session_id
            )
            self.assertIsNotNone(child_scheduler.checkpoint)
            self.assertEqual(
                child_scheduler.checkpoint.root_session_id,
                "stream-parent-session",
            )
            self.assertEqual(
                set(child_scheduler.checkpoint.states),
                {"stream-parent-session", child_session_id},
            )
        finally:
            app.close()

    def test_nested_child_checkpoint_versions_match_the_emitting_state(self) -> None:
        """Verify every nested Child safe update snapshots its own exact version."""

        leaf = Workflow("nested-version-leaf", nodes=[Node("leaf", identity)])
        child = Workflow("nested-version-child", nodes=[Node("child", leaf)])
        parent = Workflow("nested-version-parent", nodes=[Node("parent", child)])
        app = AutoAgentApp()
        try:
            items = list(
                app.stream(
                    parent,
                    {"value": 1},
                    session_id="nested-version-root",
                )
            )
            child_sessions: set[str] = set()
            for item in items:
                if not isinstance(item, InvocationUpdate) or item.checkpoint is None:
                    continue
                self.assertIn(item.event.session_id, item.checkpoint.states)
                state = item.checkpoint.state(item.event.session_id)
                self.assertEqual(state.state_version, item.event.state_version)
                if item.event.session_id != "nested-version-root":
                    child_sessions.add(item.event.session_id)
            self.assertEqual(len(child_sessions), 2)
            result = items[-1]
            self.assertIsInstance(result, InvocationResult)
            self.assertEqual(result.status, "completed")
        finally:
            app.close()

    def test_stream_child_map_skips_partial_admission_states_in_checkpoints(self) -> None:
        """Verify concurrent Child admission never makes a streamed checkpoint invalid."""

        child = Workflow("stream-map-child", nodes=[Node("work", identity)])
        parent = Workflow(
            "stream-map-parent",
            nodes=[
                Node(
                    "children",
                    child,
                    input_mapping=map_items,
                    map=Map(max_parallelism=8),
                )
            ],
        )
        app = AutoAgentApp(max_operator_concurrency=8)
        try:
            items = list(
                app.stream(
                    parent,
                    {"items": [{"value": index} for index in range(8)]},
                    session_id="stream-map-parent-session",
                )
            )
            result = items[-1]
            self.assertIsInstance(result, InvocationResult)
            self.assertEqual(result.status, "completed")
            self.assertEqual(
                result.output,
                [{"value": index} for index in range(8)],
            )
            checkpoints = [
                item.checkpoint
                for item in items
                if isinstance(item, InvocationUpdate) and item.checkpoint is not None
            ]
            self.assertTrue(checkpoints)
            for checkpoint in checkpoints:
                self.assertEqual(
                    RuntimeCheckpointBundle.from_record(checkpoint.to_record()),
                    checkpoint,
                )
                self.assertTrue(
                    all(state.invocation is not None for state in checkpoint.states.values())
                )
        finally:
            app.close()

    def test_every_streamed_child_phase_checkpoint_recovers_parent(self) -> None:
        """Verify recovery catches parent Child phases up in their required order."""

        child = Workflow("recover-child-phases", nodes=[Node("work", identity)])
        parent = Workflow("recover-parent-phases", nodes=[Node("child", child)])
        source = AutoAgentApp()
        checkpoints: dict[str, RuntimeCheckpointBundle] = {}
        try:
            for item in source.stream(
                parent,
                {"value": 1},
                session_id="recover-parent-session",
            ):
                if not isinstance(item, InvocationUpdate) or item.checkpoint is None:
                    continue
                root_state = item.checkpoint.state("recover-parent-session")
                plans = tuple(root_state.invocation.child_plans.values())
                if not plans:
                    continue
                unit = plans[0].units[0]
                child_present = unit.session_id in item.checkpoint.states
                key = (
                    "planned-with-child"
                    if unit.phase == "planned" and child_present
                    else "planned-without-child"
                    if unit.phase == "planned"
                    else unit.phase
                )
                checkpoints.setdefault(key, item.checkpoint)
        finally:
            source.close()

        self.assertTrue(
            {"planned-without-child", "planned-with-child", "opened", "accepted"}
            <= set(checkpoints)
        )
        for label, checkpoint in checkpoints.items():
            with self.subTest(boundary=label):
                recovered = AutoAgentApp()
                try:
                    recovered.register_workflow(parent)
                    loaded = recovered.load_checkpoint(checkpoint)
                    result = recovered.recover(loaded.roots[0])
                    self.assertEqual(result.status, "completed")
                    root_state = result.checkpoint.state(result.session_id)
                    plan = next(iter(root_state.invocation.child_plans.values()))
                    self.assertEqual(plan.units[0].phase, "terminal")
                    child_state = result.checkpoint.state(plan.units[0].session_id)
                    self.assertEqual(child_state.invocation.status, "completed")
                finally:
                    recovered.close()

    def test_spawn_child_partial_phase_checkpoints_finish_parent_plan(self) -> None:
        """Verify recovered spawn Children advance opened plans through terminal."""

        child = Workflow("recover-spawn-phase-child", nodes=[Node("work", identity)])
        parent = Workflow(
            "recover-spawn-phase-parent",
            nodes=[Node("child", child, execution_mode="spawn")],
        )
        source = AutoAgentApp()
        checkpoints: dict[str, RuntimeCheckpointBundle] = {}
        try:
            for item in source.stream(
                parent,
                {"value": 1},
                session_id="recover-spawn-phase-root",
            ):
                if not isinstance(item, InvocationUpdate) or item.checkpoint is None:
                    continue
                root_state = item.checkpoint.state("recover-spawn-phase-root")
                plans = tuple(root_state.invocation.child_plans.values())
                if not plans:
                    continue
                unit = plans[0].units[0]
                child_present = unit.session_id in item.checkpoint.states
                key = (
                    "planned-with-child"
                    if unit.phase == "planned" and child_present
                    else unit.phase
                )
                if key in {"planned-with-child", "opened"}:
                    checkpoints.setdefault(key, item.checkpoint)
        finally:
            source.close()

        self.assertEqual(set(checkpoints), {"planned-with-child", "opened"})
        for label, checkpoint in checkpoints.items():
            with self.subTest(boundary=label):
                recovered = AutoAgentApp()
                try:
                    recovered.register_workflow(parent)
                    loaded = recovered.load_checkpoint(checkpoint)
                    root_result = recovered.recover(loaded.roots[0])
                    self.assertEqual(root_result.status, "completed")
                    handle = recovered.child_handles(root_result.ref)[0]
                    child_result = recovered.wait_child(handle, timeout=1)
                    self.assertEqual(child_result.status, "completed")
                    root_state = child_result.checkpoint.state(
                        root_result.session_id
                    )
                    plan = next(iter(root_state.invocation.child_plans.values()))
                    self.assertEqual(plan.units[0].phase, "terminal")
                finally:
                    recovered.close()

    def test_resume_waits_for_attached_stream_result_delivery(self) -> None:
        """Verify Resume cannot overtake an attached stream's waiting Result."""

        workflow = Workflow(
            "stream-wait-result-boundary",
            nodes=[Node("wait", Wait(Value, Value)), Node("done", identity)],
            edges=[Edge("wait", "done")],
        )
        app = AutoAgentApp()
        stream = app.stream(
            workflow,
            {"value": 1},
            session_id="stream-wait-result-session",
        )
        try:
            ref = None
            wait_id = None
            for item in stream:
                if (
                    isinstance(item, InvocationUpdate)
                    and isinstance(item.event, TraceEvent)
                    and item.event.kind == "invocation.waiting"
                ):
                    assert item.checkpoint is not None
                    state = item.checkpoint.state(item.event.session_id)
                    ref = item.event.invocation_id
                    wait_id = next(iter(state.invocation.scheduler.waits))
                    break
            assert ref is not None and wait_id is not None
            exact_ref = InvocationRef("stream-wait-result-session", ref)
            with self.assertRaisesRegex(
                RuntimeTransitionError, "INVOCATION_RESULT_PENDING"
            ):
                app.resume(exact_ref, wait_id, {"value": 2})

            boundary = next(stream)
            self.assertIsInstance(boundary, InvocationResult)
            self.assertEqual(boundary.status, "waiting")
            resumed = app.resume(exact_ref, wait_id, {"value": 2})
            self.assertEqual(resumed.status, "completed")
            self.assertEqual(resumed.output, {"value": 2})
        finally:
            stream.close()
            app.close()

    def test_terminal_invocation_cannot_be_replaced_before_result_delivery(self) -> None:
        """Verify a concurrent invoke cannot make an undelivered result stale."""

        async def run() -> None:
            class BlockingSink:
                def __init__(self) -> None:
                    self.completed = threading.Event()
                    self.blocked = False
                    self.release_event: asyncio.Event | None = None

                async def append(self, event: RuntimeEvent) -> None:
                    if event.payload.kind == "invocation.completed" and not self.blocked:
                        self.blocked = True
                        self.release_event = asyncio.Event()
                        self.completed.set()
                        await self.release_event.wait()

                async def release(self) -> None:
                    if self.release_event is not None:
                        self.release_event.set()

            sink = BlockingSink()
            app = AutoAgentApp(runtime_event_sink=sink)
            workflow = Workflow("result-delivery-lease", nodes=[Node("node", identity)])
            first_task = asyncio.create_task(
                app.ainvoke(
                    workflow,
                    {"value": 1},
                    session_id="result-delivery-session",
                )
            )
            try:
                await asyncio.sleep(0)
                self.assertTrue(sink.completed.wait(1))
                with self.assertRaisesRegex(
                    RuntimeTransitionError, "SESSION_RESULT_PENDING"
                ):
                    await app.ainvoke(
                        workflow,
                        {"value": 2},
                        session_id="result-delivery-session",
                    )
                await app._await(app._submit(sink.release()))
                first = await asyncio.wait_for(first_task, 1)
                self.assertEqual(first.output, {"value": 1})
                second = await app.ainvoke(
                    workflow,
                    {"value": 2},
                    session_id="result-delivery-session",
                )
                self.assertEqual(second.output, {"value": 2})
            finally:
                if not app._closed:
                    await app._await(app._submit(sink.release()))
                await asyncio.gather(first_task, return_exceptions=True)
                await app.aclose()

        asyncio.run(run())

    def test_wait_result_lease_prevents_same_session_replacement(self) -> None:
        """Verify an attached waiter receives its terminal result before replacement."""

        async def run() -> None:
            started = threading.Event()
            finish_event: asyncio.Event | None = None

            class BlockingSink:
                def __init__(self) -> None:
                    self.completed = threading.Event()
                    self.release_event: asyncio.Event | None = None

                async def append(self, event: RuntimeEvent) -> None:
                    if event.payload.kind == "invocation.completed":
                        self.release_event = asyncio.Event()
                        self.completed.set()
                        await self.release_event.wait()

                async def release(self) -> None:
                    if self.release_event is not None:
                        self.release_event.set()

            async def delayed(value: Value) -> Value:
                nonlocal finish_event
                finish_event = asyncio.Event()
                started.set()
                await finish_event.wait()
                return value

            async def finish() -> None:
                if finish_event is not None:
                    finish_event.set()

            sink = BlockingSink()
            app = AutoAgentApp(runtime_event_sink=sink)
            workflow = Workflow("wait-result-lease", nodes=[Node("node", delayed)])
            try:
                submission = await app.asubmit_invoke(
                    workflow,
                    {"value": 1},
                    session_id="wait-result-session",
                )
                self.assertTrue(started.wait(1))
                waiter = asyncio.create_task(app.await_result(submission.ref))
                await asyncio.sleep(0)
                await app._await(app._submit(finish()))
                self.assertTrue(sink.completed.wait(1))
                with self.assertRaisesRegex(
                    RuntimeTransitionError, "SESSION_RESULT_PENDING"
                ):
                    await app.ainvoke(
                        workflow,
                        {"value": 2},
                        session_id="wait-result-session",
                    )
                await app._await(app._submit(sink.release()))
                first = await asyncio.wait_for(waiter, 1)
                self.assertEqual(first.output, {"value": 1})
            finally:
                if not app._closed:
                    await app._await(app._submit(finish()))
                    await app._await(app._submit(sink.release()))
                await app.aclose()

        asyncio.run(run())

    def test_close_and_active_astream_finish_without_lifecycle_error(self) -> None:
        """Verify App close cleanly terminates a consumer blocked on the next update."""

        async def run() -> None:
            app = AutoAgentApp()
            first_update = asyncio.Event()

            async def consume() -> None:
                async for _item in app.astream(
                    Workflow("close-active-stream", nodes=[Node("node", identity)]),
                    {"value": 1},
                    session_id="close-active-stream-session",
                ):
                    first_update.set()

            consumer = asyncio.create_task(consume())
            await asyncio.wait_for(first_update.wait(), 1)
            checkpoint = await app.aclose()
            await asyncio.wait_for(consumer, 1)
            self.assertIsInstance(checkpoint, AppCheckpoint)

        asyncio.run(run())

    def test_empty_app_checkpoint_round_trips_through_load(self) -> None:
        """Verify an idle App close checkpoint is a valid no-op restore input."""

        source = AutoAgentApp()
        checkpoint = source.close()
        self.assertEqual(checkpoint, AppCheckpoint())
        target = AutoAgentApp()
        try:
            self.assertEqual(
                target.load_checkpoint(checkpoint).roots,
                (),
            )
        finally:
            target.close()

    def test_sync_stream_is_exhausted_after_its_terminal_error(self) -> None:
        """Verify a caught stream error cannot leave the iterator blocking forever."""

        app = AutoAgentApp()
        stream = app.stream(
            Workflow(
                "stream-admission-error",
                nodes=[Node("left", identity), Node("right", identity)],
            ),
            {"value": 1},
        )
        try:
            with self.assertRaises(RuntimeTransitionError):
                next(stream)
            with self.assertRaises(StopIteration):
                next(stream)
        finally:
            stream.close()
            app.close()

    def test_checkpoint_load_rejects_child_shared_by_another_root(self) -> None:
        """Verify sequential loads cannot assign one Child State to two Roots."""

        child = Workflow("owned-checkpoint-child", nodes=[Node("node", identity)])
        parent = Workflow(
            "owned-checkpoint-parent",
            nodes=[Node("child", child, execution_mode="spawn")],
        )
        source = AutoAgentApp()
        try:
            result = source.invoke(parent, {"value": 1}, session_id="root-one")
            source.wait_child(result.output, timeout=1)
            first = source.child_status(result.output).checkpoint
        finally:
            source.close()

        child_session_id = result.output["session_id"]
        root_one = first.states["root-one"]
        invocation_two = replace(root_one.invocation, id="invocation-two")
        session_two = replace(
            root_one.session,
            id="root-two",
            latest_invocation_id="invocation-two",
        )
        root_two = replace(
            root_one,
            session=session_two,
            invocation=invocation_two,
        )
        second = RuntimeCheckpointBundle.from_states(
            "root-two",
            {
                "root-two": root_two,
                child_session_id: first.states[child_session_id],
            },
        )
        target = AutoAgentApp()
        try:
            target.register_workflow(parent)
            target.load_checkpoint(first)
            before = tuple(target._journal.session_ids())
            with self.assertRaisesRegex(
                RuntimeTransitionError, "CHECKPOINT_GRAPH_CONFLICT"
            ):
                target.load_checkpoint(second)
            self.assertEqual(tuple(target._journal.session_ids()), before)
            self.assertEqual(len(target.close().roots), 1)
        finally:
            target.close()

    def test_public_invoke_cannot_replace_a_session_owned_by_child_plan(self) -> None:
        """Verify reusing a Child Session cannot invalidate its parent checkpoint graph."""

        child = Workflow("owned-child", nodes=[Node("work", identity)])
        parent = Workflow(
            "owned-parent",
            nodes=[Node("child", child, execution_mode="spawn")],
        )
        app = AutoAgentApp()
        try:
            parent_result = app.invoke(parent, {"value": 1})
            handle = parent_result.output
            app.wait_child(handle, timeout=1)
            with self.assertRaisesRegex(
                RuntimeTransitionError, "SESSION_OWNED_BY_CHILD"
            ):
                app.invoke(
                    child,
                    {"value": 2},
                    session_id=handle["session_id"],
                )
            checkpoint = app.close()
            self.assertEqual(len(checkpoint.roots), 1)
            self.assertIn(handle["session_id"], checkpoint.roots[0].states)
        finally:
            app.close()

    def test_attached_stream_reserves_session_before_invocation_open(self) -> None:
        """Verify another invoke fails promptly while a stream pauses at Session open."""

        async def run() -> None:
            app = AutoAgentApp()
            stream = app.astream(
                Workflow("reserved-stream", nodes=[Node("node", identity)]),
                {"value": 1},
                session_id="reserved-stream-session",
            )
            try:
                first = await anext(stream)
                self.assertEqual(first.event.kind, "session.opened")
                with self.assertRaisesRegex(
                    RuntimeTransitionError, "INVOCATION_STREAM_ATTACHED"
                ):
                    await asyncio.wait_for(
                        app.ainvoke(
                            Workflow(
                                "conflicting-invoke",
                                nodes=[Node("node", identity)],
                            ),
                            {"value": 2},
                            session_id="reserved-stream-session",
                        ),
                        timeout=0.5,
                    )
            finally:
                await stream.aclose()
                await app.aclose()

        asyncio.run(run())

    def test_invalid_entry_does_not_open_an_invocation(self) -> None:
        """Verify invalid admission leaves no Session state or Runtime Events."""

        journal = InMemoryEventJournal()
        app = AutoAgentApp(runtime_journal=journal)
        workflow = Workflow("entry-admission", nodes=[Node("entry", identity)])
        try:
            with self.assertRaisesRegex(Exception, "Entry"):
                app.invoke(
                    workflow,
                    {"value": 1},
                    session_id="entry-session",
                    entry_node_id="missing",
                )

            self.assertIsNone(journal.state("entry-session").session)
            self.assertEqual(journal.events("entry-session"), ())
            result = app.invoke(
                workflow,
                {"value": 1},
                session_id="entry-session",
                entry_node_id="entry",
            )
            self.assertEqual(result.status, "completed")
        finally:
            app.close()

    def test_capability_resolver_cannot_inject_an_unregistered_operator(self) -> None:
        """Verify a resolver cannot substitute an equal unregistered Operator."""

        registered = Operator(identity, id="implementation", version="1")
        forged = Operator(forged_identity, id="implementation", version="1")
        capability = Capability("value-capability", registered.contract)
        app = AutoAgentApp(
            capability_resolver=lambda _capability, _value: forged,
        )
        try:
            app.register_operator(registered, capability_id=capability.id)
            result = app.invoke(
                Workflow(
                    "resolver-identity",
                    nodes=[Node("node", capability)],
                ),
                {"value": 1},
            )
            self.assertEqual(result.status, "failed")
            self.assertIsNotNone(result.error)
            self.assertIn("outside", result.error.message)
        finally:
            app.close()

    def test_loop_boundary_error_route_handles_failure(self) -> None:
        """Verify an Error Exit crossing a Loop boundary handles the failure."""

        app = AutoAgentApp()
        workflow = Workflow(
            "loop-error-route",
            nodes=[
                Node("start", identity),
                Node("header", identity),
                Node("body", fail),
                Node("handled", accept_error, input_mapping=error_mapping),
            ],
            edges=[
                Edge("start", "header"),
                Edge("header", "body"),
                Edge("body", "header", id="back"),
                Edge("body", "handled", on="error", id="error-exit"),
            ],
        )
        try:
            result = app.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"message": "failed"})
        finally:
            app.close()

    def test_parallel_condition_reads_context_committed_by_an_earlier_branch(self) -> None:
        """Verify a later parallel completion evaluates Condition on committed Context."""

        first_branch_committed = asyncio.Event()
        condition_saw_first = asyncio.Event()

        class CoordinatedJournal(InMemoryEventJournal):
            def append(self, event):  # type: ignore[no-untyped-def]
                state = super().append(event)
                if isinstance(event.payload, NodeOccurrenceCompleted):
                    invocation = state.invocation
                    assert invocation is not None
                    occurrence = invocation.scheduler.occurrences[
                        event.payload.occurrence_id
                    ]
                    if occurrence.node_id == "first":
                        first_branch_committed.set()
                return state

        async def first(value: Value) -> Value:
            return value

        async def second(value: Value) -> Value:
            await first_branch_committed.wait()
            return value

        def bind_first(_context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(
                invocation=(ContextOperation.set("first.done", True),)
            )

        async def sees_first(context: ConditionContext) -> bool:
            first_context = context.invocation_context.get("first", {})
            selected = bool(first_context.get("done"))  # type: ignore[union-attr]
            if selected:
                condition_saw_first.set()
            return selected

        journal = CoordinatedJournal()
        app = AutoAgentApp(runtime_journal=journal)
        workflow = Workflow(
            "parallel-condition-context",
            nodes=[
                Node("start", identity),
                Node("first", first, output_binding=bind_first),
                Node("second", second),
                Node("selected", identity),
            ],
            edges=[
                Edge("start", "first"),
                Edge("start", "second"),
                Edge("second", "selected", condition=sees_first),
            ],
        )
        try:
            result = app.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "completed")
            self.assertIn("selected", result.output)
            self.assertTrue(condition_saw_first.is_set())
        finally:
            app.close()


if __name__ == "__main__":
    unittest.main()
