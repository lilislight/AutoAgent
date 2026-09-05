from __future__ import annotations

import asyncio
import json
import threading
import unittest

from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp,
    Edge,
    InputMappingContext,
    InvocationRef,
    InvocationUpdate,
    Map,
    Node,
    Recovery,
    RuntimeInfrastructureError,
    RuntimeTransitionError,
    Workflow,
)
from autoagent.core import (
    InMemoryEventJournal,
    RuntimeCheckpointBundle,
    RuntimeEvent,
    SessionOpened,
    StateReducer,
)
from autoagent.core.runtime import StateTransition


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


def map_items(context: InputMappingContext) -> list[Value]:
    return context.invocation_input["items"]  # type: ignore[index,return-value]


def next_input(_context: InputMappingContext) -> Value:
    return {"value": 99}


class _UniqueSequenceSink:
    """Model a database primary key on (Session id, Event sequence)."""

    def __init__(self, events: tuple[RuntimeEvent, ...] = ()) -> None:
        self.events = list(events)
        self._by_sequence = {
            (event.session_id, event.sequence): event for event in events
        }

    async def append(self, event: RuntimeEvent) -> None:
        key = (event.session_id, event.sequence)
        existing = self._by_sequence.get(key)
        if existing is not None:
            if existing != event:
                raise RuntimeError("Runtime Event sequence fork")
            return
        self._by_sequence[key] = event
        self.events.append(event)


class _RejectChildCompletionSink(_UniqueSequenceSink):
    """Reject one Child terminal Event before it reaches durable storage."""

    def __init__(self, root_session_id: str) -> None:
        super().__init__()
        self.root_session_id = root_session_id
        self.reject_child_completion = True

    async def append(self, event: RuntimeEvent) -> None:
        if (
            self.reject_child_completion
            and event.session_id != self.root_session_id
            and any(log.event_name == "invocation.completed" for log in event.logs)
        ):
            raise RuntimeError("child completion was not committed")
        await super().append(event)


def _json_round_trip(events: tuple[RuntimeEvent, ...]) -> tuple[RuntimeEvent, ...]:
    return tuple(
        RuntimeEvent.from_record(json.loads(json.dumps(event.to_record())))
        for event in events
    )


def _checkpoint_from_events(
    root_session_id: str,
    events: tuple[RuntimeEvent, ...],
) -> RuntimeCheckpointBundle:
    states = {
        session_id: StateReducer().reduce(
            tuple(event for event in events if event.session_id == session_id)
        )
        for session_id in {event.session_id for event in events}
    }
    return RuntimeCheckpointBundle.from_states(root_session_id, states)


class ChildPersistenceIntegrityTests(unittest.TestCase):
    def test_cancelled_root_admission_cannot_publish_a_session_only_prefix(self) -> None:
        """Verify reopening one Session cannot fork its first Event sequence."""

        workflow = Workflow(
            "atomic-root-admission",
            nodes=[Node("work", identity)],
        )
        sink = _UniqueSequenceSink()
        app = AutoAgentApp(runtime_event_sink=sink)
        stream = app.stream(
            workflow,
            {"value": 1},
            session_id="atomic-root-session",
        )
        try:
            first = next(stream)
            self.assertIsInstance(first, InvocationUpdate)
            assert isinstance(first, InvocationUpdate)
            self.assertEqual(first.event.kind, "session.opened")
            self.assertEqual(sink.events, [])
            stream.close()

            result = app.invoke(
                workflow,
                {"value": 2},
                session_id="atomic-root-session",
            )
            self.assertEqual(result.status, "completed")
            root_events = tuple(
                event
                for event in sink.events
                if event.session_id == "atomic-root-session"
            )
            self.assertTrue(root_events)
            self.assertEqual(root_events[0].sequence, 1)
            self.assertEqual(
                tuple(log.event_name for log in root_events[0].logs),
                ("session.opened", "invocation.opened"),
            )
        finally:
            stream.close()
            app.close(timeout=1)

    def test_event_group_cannot_be_flushed_outside_its_commit_boundary(self) -> None:
        """Verify direct Journal flushing cannot expose a partial admission."""

        journal = InMemoryEventJournal()
        journal.begin_event_group("grouped-child")
        journal.apply_transition(
            StateTransition(
                session_id="grouped-child",
                invocation_id=None,
                payload=SessionOpened({}),
                occurred_at_ns=1,
            )
        )

        with self.assertRaisesRegex(RuntimeTransitionError, "EVENT_GROUP_ACTIVE"):
            journal.flush("grouped-child")
        self.assertEqual(journal.events("grouped-child"), ())
        self.assertTrue(journal.abort_event_group("grouped-child"))
        self.assertIsNone(journal.state("grouped-child").session)
        self.assertEqual(journal.pending_batches("grouped-child"), ())

    def test_partial_child_admission_never_publishes_session_only_event(self) -> None:
        """Verify a durable prefix cannot contain only Child SessionOpened."""

        child = Workflow(
            "atomic-admission-child",
            nodes=[Node("work", identity)],
        )
        parent = Workflow(
            "atomic-admission-parent",
            nodes=[Node("child", child)],
        )
        source_sink = _UniqueSequenceSink()
        source = AutoAgentApp(runtime_event_sink=source_sink)
        stream = source.stream(
            parent,
            {"value": 1},
            session_id="atomic-admission-root",
        )
        child_session_id: str | None = None
        try:
            for item in stream:
                if not isinstance(item, InvocationUpdate):
                    continue
                if (
                    item.event.session_id != "atomic-admission-root"
                    and item.event.kind == "session.opened"
                ):
                    child_session_id = item.event.session_id
                    break
            self.assertIsNotNone(child_session_id)
            assert child_session_id is not None
            self.assertFalse(
                any(
                    event.session_id == child_session_id
                    for event in source_sink.events
                )
            )
            durable_prefix = _json_round_trip(tuple(source_sink.events))
        finally:
            stream.close()
            source.close(timeout=1)

        checkpoint = _checkpoint_from_events(
            "atomic-admission-root", durable_prefix
        )
        self.assertNotIn(child_session_id, checkpoint.states)

        restored_sink = _UniqueSequenceSink(durable_prefix)
        restored = AutoAgentApp(runtime_event_sink=restored_sink)
        try:
            restored.register_workflow(parent)
            loaded = restored.load_checkpoint(checkpoint)
            result = restored.recover(loaded.roots[0])
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"value": 1})

            child_events = tuple(
                event
                for event in restored_sink.events
                if event.session_id == child_session_id
            )
            self.assertTrue(child_events)
            self.assertEqual(child_events[0].sequence, 1)
            self.assertEqual(
                tuple(log.event_name for log in child_events[0].logs),
                (
                    "session.opened",
                    "invocation.opened",
                    "invocation.started",
                    "scheduler.initialized",
                ),
            )
            _checkpoint_from_events(
                "atomic-admission-root", tuple(restored_sink.events)
            )
        finally:
            restored.close(timeout=1)

    def test_child_handle_is_hidden_until_its_checkpoint_is_self_contained(self) -> None:
        """Verify partial admission cannot expose an unrecoverable Child Result."""

        child = Workflow(
            "handle-admission-child",
            nodes=[Node("work", identity)],
        )
        parent = Workflow(
            "handle-admission-parent",
            nodes=[Node("child", child)],
        )
        app = AutoAgentApp()
        stream = app.stream(
            parent,
            {"value": 1},
            session_id="handle-admission-root",
        )
        parent_ref: InvocationRef | None = None
        child_session_id: str | None = None
        try:
            for item in stream:
                if not isinstance(item, InvocationUpdate):
                    continue
                if (
                    item.event.session_id == "handle-admission-root"
                    and item.event.kind == "invocation.opened"
                ):
                    assert item.event.invocation_id is not None
                    parent_ref = InvocationRef(
                        item.event.session_id,
                        item.event.invocation_id,
                    )
                    continue
                if item.event.session_id == "handle-admission-root":
                    continue
                if item.event.kind == "invocation.opened":
                    child_session_id = item.event.session_id
                    assert parent_ref is not None
                    self.assertEqual(app.child_handles(parent_ref), ())
                    with self.assertRaisesRegex(
                        RuntimeTransitionError,
                        "CHILD_ADMISSION_INCOMPLETE",
                    ):
                        app.child_status(  # type: ignore[arg-type]
                            {
                                "session_id": item.event.session_id,
                                "invocation_id": item.event.invocation_id,
                                "workflow_id": item.event.subject_ids["workflow_id"],
                                "workflow_revision_id": item.event.subject_ids[
                                    "workflow_revision_id"
                                ],
                            }
                        )
                    continue
                if item.event.kind == "scheduler.initialized":
                    assert parent_ref is not None
                    handles = app.child_handles(parent_ref)
                    self.assertEqual(len(handles), 1)
                    result = app.child_status(handles[0])
                    self.assertIn(handles[0]["session_id"], result.checkpoint.states)
                    self.assertEqual(handles[0]["session_id"], child_session_id)
                    break
        finally:
            stream.close()
            app.close(timeout=1)

    def test_batched_spawn_map_persists_children_before_parent_acceptance(self) -> None:
        """Verify batching cannot persist accepted parent units before Child admission."""

        child_started = threading.Event()
        parent_started = threading.Event()

        async def child_work(value: Value) -> Value:
            child_started.set()
            await asyncio.Event().wait()
            return value  # pragma: no cover

        async def parent_work(value: Value) -> Value:
            parent_started.set()
            await asyncio.Event().wait()
            return value  # pragma: no cover

        child = Workflow(
            "batched-admission-child",
            nodes=[Node("work", child_work)],
        )
        parent = Workflow(
            "batched-admission-parent",
            nodes=[
                Node(
                    "spawn",
                    child,
                    input_mapping=map_items,
                    map=Map(max_parallelism=1),
                    execution_mode="spawn",
                ),
                Node("next", parent_work, input_mapping=next_input),
            ],
            edges=[Edge("spawn", "next")],
        )
        journal = InMemoryEventJournal(
            max_batches_per_event=100,
            flush_event_names=frozenset(),
        )
        sink = _UniqueSequenceSink()
        app = AutoAgentApp(
            max_operator_concurrency=2,
            runtime_journal=journal,
            runtime_event_sink=sink,
        )
        try:
            app.submit_invoke(
                parent,
                {
                    "items": [
                        {"value": 1},
                        {"value": 2},
                        {"value": 3},
                    ]
                },
                session_id="batched-admission-root",
            )
            self.assertTrue(child_started.wait(1))
            self.assertTrue(parent_started.wait(1))
            durable_prefix = _json_round_trip(tuple(sink.events))

            root_events = tuple(
                event
                for event in durable_prefix
                if event.session_id == "batched-admission-root"
            )
            root_state = StateReducer().reduce(root_events)
            root_invocation = root_state.invocation
            self.assertIsNotNone(root_invocation)
            assert root_invocation is not None
            plan = next(iter(root_invocation.child_plans.values()))
            self.assertEqual(
                tuple(unit.phase for unit in plan.units),
                ("accepted", "accepted", "accepted"),
            )

            acceptance_index = next(
                index
                for index, event in enumerate(durable_prefix)
                if event.session_id == "batched-admission-root"
                and any(
                    log.event_name == "child_invocation.phase_changed"
                    and getattr(log.payload, "phase", None) == "accepted"
                    for log in event.logs
                )
            )
            for unit in plan.units:
                admission_index = next(
                    index
                    for index, event in enumerate(durable_prefix)
                    if event.session_id == unit.session_id
                )
                self.assertLess(admission_index, acceptance_index)
                admission = durable_prefix[admission_index]
                self.assertEqual(admission.sequence, 1)
                self.assertEqual(
                    tuple(log.event_name for log in admission.logs),
                    (
                        "session.opened",
                        "invocation.opened",
                        "invocation.started",
                        "scheduler.initialized",
                    ),
                )

            checkpoint = _checkpoint_from_events(
                "batched-admission-root", durable_prefix
            )
            self.assertEqual(
                set(checkpoint.states),
                {
                    "batched-admission-root",
                    *(unit.session_id for unit in plan.units),
                },
            )
        finally:
            app.close(timeout=1)

    def test_child_terminal_progress_is_durable_before_parent_terminal_phase(self) -> None:
        """Verify every persisted cross-Session prefix remains recoverable."""

        child = Workflow(
            "terminal-order-child",
            nodes=[
                Node(
                    "work",
                    identity,
                    recovery_mode=Recovery("replay_safe"),
                )
            ],
        )
        parent = Workflow(
            "terminal-order-parent",
            nodes=[Node("child", child)],
        )
        journal = InMemoryEventJournal(
            max_batches_per_event=100,
            flush_event_names=frozenset(),
        )
        sink = _RejectChildCompletionSink("terminal-order-root")
        source = AutoAgentApp(
            runtime_journal=journal,
            runtime_event_sink=sink,
        )
        try:
            with self.assertRaises(RuntimeInfrastructureError):
                source.invoke(
                    parent,
                    {"value": 1},
                    session_id="terminal-order-root",
                )
            durable_prefix = _json_round_trip(tuple(sink.events))
            checkpoint = _checkpoint_from_events(
                "terminal-order-root",
                durable_prefix,
            )
            root = checkpoint.state("terminal-order-root").invocation
            assert root is not None
            plan = next(iter(root.child_plans.values()))
            self.assertNotEqual(plan.units[0].phase, "terminal")
            self.assertNotEqual(root.status, "completed")
        finally:
            sink.reject_child_completion = False
            source.close(timeout=1)

        restored = AutoAgentApp()
        try:
            restored.register_workflow(parent)
            ref = restored.load_checkpoint(checkpoint).roots[0]
            result = restored.recover(ref)
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"value": 1})
        finally:
            restored.close(timeout=1)


if __name__ == "__main__":
    unittest.main()
