from __future__ import annotations

import json
import unittest
from dataclasses import FrozenInstanceError, replace

from autoagent.core import (
    InMemoryEventJournal,
    InvocationCancelled,
    InvocationCompleted,
    InvocationFailed,
    InvocationOpened,
    InvocationStarted,
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeState,
    RuntimeTransitionError,
    SessionOpened,
    StateOperation,
    StateOperationBatch,
    StateReducer,
)


def event(
    sequence: int,
    payload,
    *,
    session_id: str = "session-1",
    invocation_id: str | None = "invocation-1",
    event_id: str | None = None,
) -> RuntimeEvent:
    if isinstance(payload, SessionOpened):
        invocation_id = None
    return RuntimeEvent(
        session_id=session_id,
        invocation_id=invocation_id,
        sequence=sequence,
        payload=payload,
        id=event_id or f"{session_id}:event-{sequence}",
        occurred_at_ns=sequence * 10,
    )


def successful_events() -> tuple[RuntimeEvent, ...]:
    return (
        event(1, SessionOpened("workflow", {"history": []})),
        event(2, InvocationOpened("revision", "entry", {"value": 1})),
        event(3, InvocationStarted()),
        event(4, InvocationCompleted({"value": 2})),
    )


class RuntimeReducerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.reducer = StateReducer()

    def test_genesis_and_successful_lifecycle(self) -> None:
        """Verify genesis and successful lifecycle."""
        state = self.reducer.reduce(successful_events())
        self.assertEqual(state.sequence, 4)
        self.assertEqual(state.session.id, "session-1")
        self.assertEqual(state.session.latest_invocation_id, "invocation-1")
        self.assertEqual(state.invocation.status, "completed")
        self.assertEqual(state.to_record()["invocation"]["output"], {"value": 2})

    def test_every_event_prefix_reconstructs_the_committed_state(self) -> None:
        """Verify every event prefix reconstructs the committed state."""
        events = successful_events()
        state = RuntimeState()
        for index, item in enumerate(events, start=1):
            state = self.reducer.apply(state, item)
            replayed = self.reducer.reduce(events[:index])
            self.assertEqual(replayed, state)
            self.assertEqual(replayed.sequence, index)

    def test_failure_does_not_mutate_old_state(self) -> None:
        """Verify failure does not mutate old state."""
        opened = self.reducer.reduce(successful_events()[:2])
        before = opened.to_record()
        invalid = event(3, InvocationCompleted({"value": 2}))
        with self.assertRaisesRegex(RuntimeTransitionError, "INVOCATION_TRANSITION_INVALID"):
            self.reducer.apply(opened, invalid)
        self.assertEqual(opened.to_record(), before)
        self.assertEqual(opened.sequence, 2)

    def test_failure_and_cancel_are_explicit_terminal_transitions(self) -> None:
        """Verify failure and cancel are explicit terminal transitions."""
        base = successful_events()[:3]
        failed = self.reducer.reduce(
            (*base, event(4, InvocationFailed(RuntimeErrorInfo("ValueError", "bad"))))
        )
        self.assertEqual(failed.invocation.status, "failed")
        self.assertEqual(failed.invocation.error.type, "ValueError")

        cancelled = self.reducer.reduce(
            (*base, event(4, InvocationCancelled("user request")))
        )
        self.assertEqual(cancelled.invocation.status, "cancelled")
        self.assertEqual(cancelled.invocation.cancel_reason, "user request")
        self.assertIsNone(cancelled.invocation.output)

    def test_sequence_gap_conflict_schema_and_session_are_rejected(self) -> None:
        """Verify sequence gap conflict schema and session are rejected."""
        state = self.reducer.apply(RuntimeState(), successful_events()[0])
        cases = (
            (event(3, InvocationOpened("revision", "entry", {})), "EVENT_SEQUENCE_GAP"),
            (replace(successful_events()[0], id="different"), "EVENT_SEQUENCE_CONFLICT"),
            (replace(event(2, InvocationOpened("revision", "entry", {})), schema_version=99), "EVENT_SCHEMA_UNSUPPORTED"),
            (event(2, InvocationOpened("revision", "entry", {}), session_id="other"), "EVENT_SESSION_MISMATCH"),
        )
        for invalid, code in cases:
            with self.subTest(code=code):
                with self.assertRaisesRegex(RuntimeTransitionError, code):
                    self.reducer.apply(state, invalid)

    def test_latest_event_is_idempotent_but_changed_payload_is_not(self) -> None:
        """Verify latest event is idempotent but changed payload is not."""
        first = successful_events()[0]
        state = self.reducer.apply(RuntimeState(), first)
        self.assertIs(self.reducer.apply(state, first), state)
        changed_payload = SessionOpened("other", {})
        changed = replace(
            first,
            payload=changed_payload,
            logs=(replace(first.logs[0], payload=changed_payload),),
        )
        with self.assertRaisesRegex(RuntimeTransitionError, "EVENT_SEQUENCE_CONFLICT"):
            self.reducer.apply(state, changed)

    def test_new_invocation_replaces_only_a_terminal_latest_invocation(self) -> None:
        """Verify new invocation replaces only a terminal latest invocation."""
        state = self.reducer.reduce(successful_events())
        next_open = event(
            5,
            InvocationOpened("revision", "entry", {"value": 3}),
            invocation_id="invocation-2",
        )
        state = self.reducer.apply(state, next_open)
        self.assertEqual(state.invocation.id, "invocation-2")
        self.assertEqual(state.invocation.status, "created")

        with self.assertRaisesRegex(RuntimeTransitionError, "INVOCATION_ALREADY_ACTIVE"):
            self.reducer.apply(
                state,
                event(
                    6,
                    InvocationOpened("revision", "entry", {}),
                    invocation_id="invocation-3",
                ),
            )

    def test_state_and_nested_values_cannot_be_modified_outside_reducer(self) -> None:
        """Verify state and nested values cannot be modified outside reducer."""
        source = {"nested": {"items": [1]}}
        opened = event(1, SessionOpened("workflow", source))
        source["nested"]["items"].append(2)
        state = self.reducer.apply(RuntimeState(), opened)
        self.assertEqual(state.to_record()["session"]["context"], {"nested": {"items": [1]}})
        with self.assertRaises(FrozenInstanceError):
            state.sequence = 10  # type: ignore[misc]
        with self.assertRaises(TypeError):
            state.session.context["new"] = 1  # type: ignore[index]

    def test_event_record_round_trip_is_json_compatible(self) -> None:
        """Verify event record round trip is json compatible."""
        for item in (
            *successful_events(),
            event(4, InvocationFailed(RuntimeErrorInfo("Error", "message"))),
            event(4, InvocationCancelled(None)),
        ):
            record = json.loads(json.dumps(item.to_record()))
            self.assertEqual(RuntimeEvent.from_record(record), item)

    def test_event_codec_rejects_payload_type_coercion(self) -> None:
        """Verify Event codec rejects payload coercion and missing required values."""
        record = successful_events()[1].to_record()
        payload = record["payload"]
        assert isinstance(payload, dict)
        payload["entry_node_id"] = 7
        with self.assertRaisesRegex(TypeError, "entry_node_id must be a string"):
            RuntimeEvent.from_record(record)

        completed = successful_events()[-1].to_record()
        completed_payload = completed["payload"]
        assert isinstance(completed_payload, dict)
        del completed_payload["output"]
        with self.assertRaisesRegex(KeyError, "requires output"):
            RuntimeEvent.from_record(completed)

    def test_time_regression_and_non_durable_values_are_rejected(self) -> None:
        """Verify time regression and non durable values are rejected."""
        state = self.reducer.apply(RuntimeState(), successful_events()[0])
        backwards = replace(
            event(2, InvocationOpened("revision", "entry", {})),
            occurred_at_ns=5,
        )
        with self.assertRaisesRegex(RuntimeTransitionError, "EVENT_TIME_REGRESSION"):
            self.reducer.apply(state, backwards)

        cyclic: list[object] = []
        cyclic.append(cyclic)
        for value in ({1: "bad"}, {"bad": float("nan")}, {"bad": cyclic}):
            with self.subTest(value=value):
                with self.assertRaises(TypeError):
                    SessionOpened("workflow", value)  # type: ignore[arg-type]


class EventJournalTests(unittest.TestCase):
    def test_staged_changes_flush_into_one_event_with_reducible_state_versions(self) -> None:
        """Verify one Event retains every atomic State Operation Batch."""
        journal = InMemoryEventJournal()
        for index, item in enumerate(successful_events(), start=1):
            journal.stage(
                replace(item, sequence=1, id=f"pending-{index}", logs=())
            )
        self.assertEqual(journal.state("session-1").state_version, 4)
        self.assertEqual(journal.state("session-1").sequence, 0)
        self.assertEqual(journal.events("session-1"), ())
        self.assertEqual(len(journal.pending_batches("session-1")), 4)

        persisted = journal.flush("session-1")
        assert persisted is not None
        self.assertEqual(persisted.sequence, 1)
        self.assertEqual(
            (persisted.from_state_version, persisted.to_state_version),
            (0, 4),
        )
        self.assertEqual(len(persisted.operation_batches), 4)
        self.assertEqual(len(persisted.logs), 4)
        restored = RuntimeEvent.from_record(
            json.loads(json.dumps(persisted.to_record()))
        )
        self.assertEqual(
            StateReducer().apply(RuntimeState(), restored),
            journal.state("session-1"),
        )
        midway = RuntimeState()
        for batch in persisted.operation_batches[:2]:
            midway = StateReducer().apply_batch(midway, batch)
        self.assertEqual(midway.state_version, 2)
        self.assertEqual(midway.sequence, 0)
        self.assertEqual(midway.invocation.status, "created")

    def test_capture_interval_batches_until_a_mandatory_terminal_boundary(self) -> None:
        """Verify capture configuration changes Event count but not Runtime State."""
        journal = InMemoryEventJournal(max_batches_per_event=100)
        for index, item in enumerate(successful_events(), start=1):
            journal.append(
                replace(item, sequence=1, id=f"captured-{index}", logs=())
            )
        persisted = journal.events("session-1")
        self.assertEqual(len(persisted), 1)
        self.assertEqual(len(persisted[0].operation_batches), 4)
        self.assertEqual(len(persisted[0].logs), 4)
        self.assertEqual(journal.state("session-1").invocation.status, "completed")
        self.assertEqual(
            StateReducer().reduce(persisted), journal.state("session-1")
        )

    def test_persisted_replay_uses_operations_not_semantic_payload(self) -> None:
        """Verify policy and trace payload changes cannot alter historical State replay."""
        journal = InMemoryEventJournal()
        for item in successful_events():
            journal.append(item)
        persisted = journal.events("session-1")
        base = StateReducer().reduce(persisted[:3])
        final = persisted[3]
        trace_only = InvocationFailed(RuntimeErrorInfo("TraceOnly", "changed"))
        changed = replace(
            final,
            payload=trace_only,
            logs=(*final.logs[:-1], replace(final.logs[-1], payload=trace_only)),
        )
        replayed = StateReducer().apply(base, changed)
        self.assertEqual(replayed.invocation.status, "completed")
        self.assertEqual(replayed.invocation.output, final.payload.output)

    def test_invalid_operation_batch_is_atomic(self) -> None:
        """Verify a bad later operation leaves the original typed State untouched."""
        state = StateReducer().apply(RuntimeState(), successful_events()[0])
        before = state.to_record()
        batch = StateOperationBatch(
            from_state_version=state.state_version,
            to_state_version=state.state_version + 1,
            occurred_at_ns=20,
            operations=(
                StateOperation("replace", ("session", "workflow_id"), "changed"),
                StateOperation("replace", ("session", "missing"), "bad"),
            ),
        )
        with self.assertRaisesRegex(RuntimeTransitionError, "STATE_PATH_MISSING"):
            StateReducer().apply_batch(state, batch)
        self.assertEqual(state.to_record(), before)

        unknown = StateOperationBatch(
            from_state_version=state.state_version,
            to_state_version=state.state_version + 1,
            occurred_at_ns=20,
            operations=(StateOperation("add", ("unknown_root",), {}),),
        )
        with self.assertRaisesRegex(TypeError, "unknown"):
            StateReducer().apply_batch(state, unknown)
        self.assertEqual(state.to_record(), before)

    def test_append_many_rejects_late_conflict_without_partial_import(self) -> None:
        """Verify recovery prefix import is all-or-nothing on a late Event conflict."""
        source = InMemoryEventJournal()
        for item in successful_events():
            source.append(item)
        prefix = source.events("session-1")

        target = InMemoryEventJournal()
        target.append(
            event(
                1,
                SessionOpened("other-workflow", {}),
                session_id="other-session",
                event_id=prefix[-1].id,
            )
        )
        before = target.events("other-session")
        with self.assertRaisesRegex(RuntimeTransitionError, "EVENT_ID_CONFLICT"):
            target.append_many(prefix)
        self.assertIsNone(target.state("session-1").session)
        self.assertEqual(target.events("session-1"), ())
        self.assertEqual(target.events("other-session"), before)

    def test_sessions_have_independent_contiguous_event_streams(self) -> None:
        """Verify sessions have independent contiguous event streams."""
        journal = InMemoryEventJournal()
        first = event(1, SessionOpened("workflow", {}), session_id="one")
        second = event(1, SessionOpened("workflow", {}), session_id="two")
        journal.append(first)
        journal.append(second)
        self.assertEqual(journal.state("one").sequence, 1)
        self.assertEqual(journal.state("two").sequence, 1)
        persisted = journal.events("one")
        self.assertEqual(len(persisted), 1)
        self.assertEqual(persisted[0].id, first.id)
        self.assertEqual(persisted[0].from_state_version, 0)
        self.assertEqual(persisted[0].to_state_version, 1)
        self.assertEqual(len(persisted[0].operation_batches), 1)

    def test_append_is_atomic_and_duplicate_event_id_is_idempotent(self) -> None:
        """Verify append is atomic and duplicate event id is idempotent."""
        journal = InMemoryEventJournal()
        events = successful_events()
        for item in events:
            journal.append(item)
        current = journal.state("session-1")
        self.assertIs(journal.append(events[0]), current)
        self.assertEqual(len(journal.events("session-1")), 4)

        invalid = event(6, InvocationStarted(), event_id="new")
        with self.assertRaisesRegex(RuntimeTransitionError, "EVENT_SEQUENCE_GAP"):
            journal.append(invalid)
        self.assertIs(journal.state("session-1"), current)
        self.assertEqual(len(journal.events("session-1")), 4)

    def test_reducer_accepts_every_runtime_event_prefix(self) -> None:
        """Verify StateReducer reconstructs every Runtime Event prefix."""
        journal = InMemoryEventJournal()
        for item in successful_events():
            journal.append(item)
        events = journal.events("session-1")
        for sequence in range(len(events) + 1):
            state = StateReducer().reduce(events[:sequence])
            self.assertEqual(state.sequence, sequence)
        self.assertEqual(StateReducer().reduce(events), journal.state("session-1"))


if __name__ == "__main__":
    unittest.main()
