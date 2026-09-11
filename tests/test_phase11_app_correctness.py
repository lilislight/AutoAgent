from __future__ import annotations

import asyncio
import threading
import time
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
    UserEventMapping,
    OutputBindingContext,
    Recovery,
    RuntimeTransitionError,
    Stream,
    StreamContext,
    Wait,
    Workflow,
)
from autoagent.core import (
    RuntimeRepository,
    NodeCompleted,
    SessionCheckpoint,
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


def map_value_event(context: OutputBindingContext) -> Value:
    return context.output  # type: ignore[return-value]


class AppCorrectnessTests(unittest.TestCase):
    def test_resident_invocations_and_terminal_session_unload_round_trip(self) -> None:
        """List exact resident refs and reload one atomically unloaded Session."""

        workflow = Workflow("unload-terminal", nodes=[Node("work", identity)])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {"value": 1}, session_id="unload-terminal")
            self.assertEqual(app.resident_invocations(), (result.ref,))
            checkpoint = app.unload_session(result.ref)
            self.assertEqual(checkpoint.session_id, result.session_id)
            self.assertEqual(app.resident_invocations(), ())
            with self.assertRaisesRegex(RuntimeTransitionError, "INVOCATION_REF_STALE"):
                app.status(result.ref)
            loaded = app.load_checkpoint(checkpoint)
            self.assertEqual(loaded.invocations, (result.ref,))
            self.assertEqual(app.status(result.ref).status, "completed")
        finally:
            app.close()

    def test_waiting_session_can_unload_then_reload_and_resume(self) -> None:
        """Resume a waiting Invocation after its Session leaves Core memory."""

        workflow = Workflow(
            "unload-waiting",
            nodes=[Node("wait", Wait(Value, Value)), Node("done", identity)],
            edges=[Edge("wait", "done")],
        )
        app = AutoAgentApp()
        try:
            waiting = app.invoke(workflow, {"value": 1}, session_id="unload-waiting")
            checkpoint = app.unload_session(waiting.ref)
            app.load_checkpoint(checkpoint)
            resumed = app.resume(
                waiting.ref,
                waiting.waits[0].id,
                {"value": 2},
            )
            self.assertEqual(resumed.status, "completed")
            self.assertEqual(resumed.output, {"value": 2})
        finally:
            app.close()

    def test_submitted_result_does_not_retain_updates_or_block_unload(self) -> None:
        """Allow explicit unload when a submitted result was never joined."""

        workflow = Workflow("unload-pending-result", nodes=[Node("work", identity)])
        app = AutoAgentApp()
        try:
            submitted = app.submit_invoke(
                workflow,
                {"value": 1},
                session_id="unload-pending-result",
            )
            for _ in range(100):
                invocation = app._repository.state(submitted.session_id).invocation
                if invocation is not None and invocation.terminal:
                    break
                time.sleep(0.001)
            self.assertFalse(hasattr(app, "_observations"))
            checkpoint = app.unload_session(submitted.ref)
            self.assertEqual(checkpoint.session_id, submitted.session_id)
            with self.assertRaisesRegex(RuntimeTransitionError, "INVOCATION_REF_STALE"):
                app.status(submitted.ref)
        finally:
            app.close()

    def test_child_unload_checks_graph_but_removes_only_requested_session(self) -> None:
        """Require a quiescent Child graph and unload only the selected Child."""

        child = Workflow("unload-child", nodes=[Node("work", identity)])
        parent = Workflow(
            "unload-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        app = AutoAgentApp()
        try:
            parent_result = app.invoke(parent, {"value": 1}, session_id="unload-parent")
            child_ref = parent_result.output
            self.assertIsInstance(child_ref, InvocationRef)
            app.join(child_ref, timeout=1)
            checkpoint = app.unload_session(child_ref)
            self.assertEqual(checkpoint.session_id, child_ref.session_id)
            self.assertEqual(app.resident_invocations(), (parent_result.ref,))
        finally:
            app.close()

    def test_parent_unload_rejects_a_running_child(self) -> None:
        """Keep a parent resident while any related Child is still running."""

        started = threading.Event()
        release = threading.Event()

        def block(value: Value) -> Value:
            started.set()
            release.wait(1)
            return value

        child = Workflow("unload-live-child", nodes=[Node("work", block)])
        parent = Workflow(
            "unload-live-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        app = AutoAgentApp()
        try:
            parent_result = app.invoke(
                parent, {"value": 1}, session_id="unload-live-parent"
            )
            self.assertTrue(started.wait(1))
            with self.assertRaisesRegex(
                RuntimeTransitionError, "RELATED_INVOCATION_NOT_UNLOADABLE"
            ):
                app.unload_session(parent_result.ref)
            release.set()
            app.join(parent_result.output, timeout=1)
        finally:
            release.set()
            app.close()

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
            state = app._repository.state(result.session_id)
            self.assertTrue(
                all(
                    occurrence.status != "running"
                    for occurrence in state.invocation.scheduler.occurrences.values()
                )
            )
            self.assertFalse(any(isinstance(item, InvocationUpdate) for item in items))
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
            state = app._repository.state(result.session_id)
            occurrence = next(iter(state.invocation.scheduler.occurrences.values()))
            self.assertEqual(occurrence.status, "failed")
        finally:
            app.close()

    def test_non_streaming_invoke_does_not_capture_a_checkpoint(self) -> None:
        """Build a checkpoint only for a lifecycle transfer operation."""

        class CountingJournal(RuntimeRepository):
            captures = 0

            def capture_checkpoint(self, root_session_id, *, captured_at_us=None):
                self.captures += 1
                return super().capture_checkpoint(
                    root_session_id, captured_at_us=captured_at_us
                )

        journal = CountingJournal()
        app = AutoAgentApp(runtime_repository=journal)
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
            self.assertEqual(journal.captures, 0)
            checkpoint = app.unload_session(result.ref)
            self.assertEqual(checkpoint.state.invocation.output, {"value": 1})
            self.assertEqual(journal.captures, 1)
        finally:
            app.close()






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
                app._repository.state(result.session_id),
            )
        finally:
            app.close()




    def test_parent_and_child_unload_checkpoints_are_independent(self) -> None:
        """Return an independent checkpoint for each unloaded Session."""

        child = Workflow("recover-child-phases", nodes=[Node("work", identity)])
        parent = Workflow("recover-parent-phases", nodes=[Node("child", child)])
        app = AutoAgentApp()
        try:
            items = list(app.stream(parent, {"value": 1}, session_id="recover-parent-session"))
            result = items[-1]
            self.assertIsInstance(result, InvocationResult)
            handles = app.child_invocations(result.ref)
            self.assertEqual(len(handles), 1)
            child_checkpoint = app.unload_session(handles[0])
            parent_checkpoint = app.unload_session(result.ref)
            self.assertEqual(child_checkpoint.session_id, handles[0].session_id)
            self.assertNotEqual(child_checkpoint.session_id, parent_checkpoint.session_id)
        finally:
            app.close()

    def test_spawn_child_unloads_through_its_ref(self) -> None:
        """Return a spawned Child checkpoint through its generic InvocationRef."""

        child = Workflow("recover-spawn-phase-child", nodes=[Node("work", identity)])
        parent = Workflow(
            "recover-spawn-phase-parent",
            nodes=[Node("child", child, execution_mode="spawn")],
        )
        app = AutoAgentApp()
        try:
            result = app.invoke(parent, {"value": 1}, session_id="recover-spawn-phase-root")
            child_result = app.join(result.output, timeout=1)
            child_checkpoint = app.unload_session(result.output)
            parent_checkpoint = app.unload_session(result.ref)
            self.assertEqual(parent_checkpoint.session_id, result.session_id)
            self.assertEqual(child_checkpoint.session_id, child_result.session_id)
            self.assertNotEqual(parent_checkpoint.session_id, child_checkpoint.session_id)
        finally:
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
                    RuntimeTransitionError, "SESSION_INVOCATION_ACTIVE"
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
                waiter = asyncio.create_task(app.ajoin(submission.ref))
                await asyncio.sleep(0)
                await app._await(app._submit(finish()))
                self.assertTrue(sink.completed.wait(1))
                with self.assertRaisesRegex(
                    RuntimeTransitionError, "SESSION_INVOCATION_ACTIVE"
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
                target.load_checkpoint(checkpoint).invocations,
                (),
            )
        finally:
            target.close()

    def test_checkpoint_load_defers_workflow_revision_check_until_recover(self) -> None:
        """Verify State loads without code while recovery requires its exact Revision."""

        workflow = Workflow(
            "checkpoint-exact-revision",
            nodes=[Node("source", identity)],
        )
        source = AutoAgentApp()
        try:
            result = source.invoke(workflow, {"value": 1})
            checkpoint = source.unload_session(result.ref)
        finally:
            source.close()

        mismatched = Workflow(
            "checkpoint-exact-revision",
            nodes=[Node("different", identity)],
        )
        for registered in (None, mismatched):
            target = AutoAgentApp()
            try:
                if registered is not None:
                    target.register_workflow(registered)
                loaded = target.load_checkpoint(checkpoint)
                self.assertEqual(len(loaded.invocations), 1)
                with self.assertRaisesRegex(
                    RuntimeTransitionError,
                    "WORKFLOW_NOT_REGISTERED",
                ):
                    target.recover(loaded.invocations[0])
                self.assertEqual(
                    tuple(target._repository.session_ids()),
                    (checkpoint.session_id,),
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
            source.join(result.output, timeout=1)
            child_checkpoint = source.unload_session(result.output)
            parent_checkpoint = source.unload_session(result.ref)
        finally:
            source.close()

        child_session_id = result.output.session_id
        root_one = parent_checkpoint.state
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
        second = SessionCheckpoint.from_state(root_two)
        target = AutoAgentApp()
        try:
            target.register_workflow(parent)
            target.load_checkpoint(AppCheckpoint((parent_checkpoint, child_checkpoint)))
            before = tuple(target._repository.session_ids())
            with self.assertRaisesRegex(
                RuntimeTransitionError, "CHECKPOINT_CHILD_OWNERSHIP_CONFLICT"
            ):
                target.load_checkpoint(second)
            self.assertEqual(tuple(target._repository.session_ids()), before)
            self.assertEqual(len(target.close().sessions), 2)
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
            app.join(handle, timeout=1)
            with self.assertRaisesRegex(
                RuntimeTransitionError, "SESSION_OWNED_BY_CHILD"
            ):
                app.invoke(
                    child,
                    {"value": 2},
                    session_id=handle.session_id,
                )
            checkpoint = app.close()
            self.assertEqual(len(checkpoint.sessions), 2)
            self.assertEqual(
                {item.session_id for item in checkpoint.sessions},
                {parent_result.session_id, handle.session_id},
            )
        finally:
            app.close()

    def test_attached_stream_reserves_session_before_invocation_open(self) -> None:
        """Verify another invoke fails promptly while a stream pauses at Session open."""

        async def run() -> None:
            app = AutoAgentApp()
            stream = app.astream(
                Workflow(
                    "reserved-stream",
                    nodes=[
                        Node(
                            "node",
                            identity,
                            user_events=(UserEventMapping("node.done", map_value_event),),
                        )
                    ],
                ),
                {"value": 1},
                session_id="reserved-stream-session",
            )
            try:
                first = await anext(stream)
                self.assertEqual(first.event.kind, "node.done")
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

        journal = RuntimeRepository()
        app = AutoAgentApp(runtime_repository=journal)
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

        class CoordinatedJournal(RuntimeRepository):
            async def commit(self, **kwargs):
                event = await super().commit(**kwargs)
                state = self.state(event.session_id)
                if isinstance(event.payload, NodeCompleted):
                    invocation = state.invocation
                    assert invocation is not None
                    occurrence = invocation.scheduler.occurrences[
                        event.payload.occurrence_id
                    ]
                    if occurrence.node_id == "first":
                        first_branch_committed.set()
                return event

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
        app = AutoAgentApp(runtime_repository=journal)
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
