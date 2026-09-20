from __future__ import annotations

from tests.graph_fixtures import (
    root_snapshot,
)

import copy
import unittest
from dataclasses import replace
from types import MappingProxyType
from unittest.mock import patch

from autoagent.core.runtime import (
    Activation,
    ChildInvocationPhaseChanged,
    ChildInvocationPlan,
    ChildUnitState,
    EdgeResolution,
    InvocationState,
    LoopIteration,
    NodeCompleted,
    NodeOccurrenceState,
    OperatorCallState,
    SessionCheckpoint,
    RuntimeEvent,
    RuntimeState,
    SchedulerDelta,
    SchedulerState,
    SessionState,
    StateReducer,
    TransitionPlanner,
    WaitState,
    freeze,
)
from autoagent.core.runtime.scheduling import LoopBoundaryResolution


def _set_path(record: dict[str, object], path: tuple[object, ...], value: object) -> None:
    current: object = record
    for token in path[:-1]:
        current = current[token]  # type: ignore[index]
    current[path[-1]] = value  # type: ignore[index]


class RuntimeStateValidationTests(unittest.TestCase):
    def _valid_state(self) -> RuntimeState:
        source = NodeOccurrenceState(
            id="source@root",
            node_id="source",
            scope=(),
            status="completed",
            started_at_us=1,
            completed_at_us=2,
            started_sequence=1,
        )
        worker = NodeOccurrenceState(
            id="worker@root",
            node_id="worker",
            scope=(),
            status="running",
            started_at_us=3,
            started_sequence=2,
            recovery_attempts=1,
        )
        waiting = NodeOccurrenceState(
            id="waiting@root",
            node_id="waiting",
            scope=(),
            status="waiting",
            started_at_us=4,
            started_sequence=3,
        )
        activation = Activation("edge", "source@root", "ready")
        ready = NodeOccurrenceState(
            id="ready@root",
            node_id="ready",
            scope=(),
            status="ready",
            activations=(activation,),
        )
        resolution = EdgeResolution(
            "edge",
            "ready",
            (LoopIteration("loop", 0),),
            True,
            activation,
        )
        boundary = LoopBoundaryResolution(
            "loop",
            (LoopIteration("loop", 0),),
            "back",
            (LoopIteration("loop", 0),),
            "ready",
            True,
            Activation("back", "source@root", "ready"),
        )
        scheduler = SchedulerState(
            initialized=True,
            ready=(ready.id,),
            occurrences=MappingProxyType(
                {
                    source.id: source,
                    worker.id: worker,
                    waiting.id: waiting,
                    ready.id: ready,
                }
            ),
            resolutions=MappingProxyType({resolution.id: resolution}),
            boundary_resolutions=MappingProxyType({boundary.id: boundary}),
            operator_calls=MappingProxyType(
                {
                    "call": OperatorCallState(
                        id="call",
                        occurrence_id=worker.id,
                        operator_id="operator",
                        unit_index=0,
                        status="running",
                        input=freeze({"value": 1}),
                        started_at_us=4,
                    )
                }
            ),
            waits=MappingProxyType(
                {
                    "wait": WaitState(
                        id="wait",
                        occurrence_id=waiting.id,
                        status="waiting",
                        request=freeze({"question": "continue?"}),
                        created_at_us=5,
                    )
                }
            ),
        )
        return RuntimeState(
            session=SessionState(
                id="session",
                context=freeze({}),
                created_at_us=0,
                updated_at_us=5,
                latest_invocation_id="invocation",
                context_path_revisions=MappingProxyType({("shared",): 1}),
            ),
            invocation=InvocationState(
                id="invocation",
                workflow_id="workflow",
                workflow_revision_id="revision",
                entry_node_id="source",
                status="running",
                input=freeze({"value": 1}),
                context=freeze({}),
                created_at_us=0,
                started_at_us=1,
                scheduler=scheduler,
                child_plans=MappingProxyType(
                    {
                        "creation": ChildInvocationPlan(
                            creation_id="creation",
                            parent_occurrence_id=worker.id,
                            mode="spawn",
                            workflow_id="child-workflow",
                            workflow_revision_id="child-revision",
                            units=(
                                ChildUnitState(
                                    unit_index=0,
                                    session_id="child-session",
                                    invocation_id="child-invocation",
                                    input=freeze({"value": 1}),
                                ),
                            ),
                        )
                    }
                ),
                context_path_revisions=MappingProxyType({("local",): 2}),
            ),
            sequence=5,
            last_event_id="event-5",
        )

    def test_runtime_state_rejects_every_negative_runtime_number(self) -> None:
        """Verify versions, timestamps, attempts, indexes and iterations are non-negative."""

        paths = {
            "event sequence": ("sequence",),
            "session created": ("session", "created_at_us"),
            "session updated": ("session", "updated_at_us"),
            "session context revision": (
                "session",
                "context_path_revisions",
                "/shared",
            ),
            "invocation created": ("invocation", "created_at_us"),
            "invocation started": ("invocation", "started_at_us"),
            "invocation completed": ("invocation", "completed_at_us"),
            "invocation context revision": (
                "invocation",
                "context_path_revisions",
                "/local",
            ),
            "occurrence started": (
                "invocation",
                "scheduler",
                "occurrences",
                "worker@root",
                "started_at_us",
            ),
            "occurrence completed": (
                "invocation",
                "scheduler",
                "occurrences",
                "source@root",
                "completed_at_us",
            ),
            "occurrence state version": (
                "invocation",
                "scheduler",
                "occurrences",
                "worker@root",
                "started_sequence",
            ),
            "recovery attempts": (
                "invocation",
                "scheduler",
                "occurrences",
                "worker@root",
                "recovery_attempts",
            ),
            "operator unit index": (
                "invocation",
                "scheduler",
                "operator_calls",
                "call",
                "unit_index",
            ),
            "operator started": (
                "invocation",
                "scheduler",
                "operator_calls",
                "call",
                "started_at_us",
            ),
            "operator completed": (
                "invocation",
                "scheduler",
                "operator_calls",
                "call",
                "completed_at_us",
            ),
            "wait created": (
                "invocation",
                "scheduler",
                "waits",
                "wait",
                "created_at_us",
            ),
            "wait resumed": (
                "invocation",
                "scheduler",
                "waits",
                "wait",
                "resumed_at_us",
            ),
            "child unit index": (
                "invocation",
                "child_plans",
                "creation",
                "units",
                0,
                "unit_index",
            ),
            "resolution loop iteration": (
                "invocation",
                "scheduler",
                "resolutions",
                "edge@loop:0",
                "target_scope",
                0,
                "iteration",
            ),
            "boundary loop iteration": (
                "invocation",
                "scheduler",
                "boundary_resolutions",
                "boundary:loop:back@loop:0",
                "loop_scope",
                0,
                "iteration",
            ),
            "boundary source iteration": (
                "invocation",
                "scheduler",
                "boundary_resolutions",
                "boundary:loop:back@loop:0",
                "source_scope",
                0,
                "iteration",
            ),
        }
        original = self._valid_state().to_record()
        for label, path in paths.items():
            with self.subTest(field=label):
                record = copy.deepcopy(original)
                _set_path(record, path, -1)
                with self.assertRaises((TypeError, ValueError)):
                    RuntimeState.from_record(record)

    def test_runtime_state_rejects_broken_scheduler_references(self) -> None:
        """Verify Scheduler queues, identities and occurrence references are closed."""

        def rename_key(
            record: dict[str, object], collection: str, old: str, new: str
        ) -> None:
            values = record["invocation"]["scheduler"][collection]  # type: ignore[index]
            values[new] = values.pop(old)  # type: ignore[union-attr]

        mutations = {
            "ready unknown": lambda record: _set_path(
                record, ("invocation", "scheduler", "ready"), ["missing"]
            ),
            "ready duplicate": lambda record: _set_path(
                record,
                ("invocation", "scheduler", "ready"),
                ["ready@root", "ready@root"],
            ),
            "ready status mismatch": lambda record: _set_path(
                record, ("invocation", "scheduler", "ready"), ["worker@root"]
            ),
            "ready occurrence omitted": lambda record: _set_path(
                record, ("invocation", "scheduler", "ready"), []
            ),
            "occurrence key": lambda record: rename_key(
                record, "occurrences", "worker@root", "wrong"
            ),
            "resolution key": lambda record: rename_key(
                record, "resolutions", "edge@loop:0", "wrong"
            ),
            "boundary key": lambda record: rename_key(
                record,
                "boundary_resolutions",
                "boundary:loop:back@loop:0",
                "wrong",
            ),
            "operator key": lambda record: rename_key(
                record, "operator_calls", "call", "wrong"
            ),
            "wait key": lambda record: rename_key(
                record, "waits", "wait", "wrong"
            ),
            "child plan key": lambda record: (
                record["invocation"]["child_plans"].update(  # type: ignore[index,union-attr]
                    {
                        "wrong": record["invocation"]["child_plans"].pop(  # type: ignore[index,union-attr]
                            "creation"
                        )
                    }
                )
            ),
            "child parent": lambda record: _set_path(
                record,
                (
                    "invocation",
                    "child_plans",
                    "creation",
                    "parent_occurrence_id",
                ),
                "missing",
            ),
            "operator occurrence": lambda record: _set_path(
                record,
                (
                    "invocation",
                    "scheduler",
                    "operator_calls",
                    "call",
                    "occurrence_id",
                ),
                "missing",
            ),
            "wait occurrence": lambda record: _set_path(
                record,
                (
                    "invocation",
                    "scheduler",
                    "waits",
                    "wait",
                    "occurrence_id",
                ),
                "missing",
            ),
            "resolution activation source": lambda record: _set_path(
                record,
                (
                    "invocation",
                    "scheduler",
                    "resolutions",
                    "edge@loop:0",
                    "activation",
                    "source_occurrence_id",
                ),
                "missing",
            ),
            "boundary activation source": lambda record: _set_path(
                record,
                (
                    "invocation",
                    "scheduler",
                    "boundary_resolutions",
                    "boundary:loop:back@loop:0",
                    "activation",
                    "source_occurrence_id",
                ),
                "missing",
            ),
            "occurrence activation source": lambda record: _set_path(
                record,
                (
                    "invocation",
                    "scheduler",
                    "occurrences",
                    "ready@root",
                    "activations",
                    0,
                    "source_occurrence_id",
                ),
                "missing",
            ),
        }
        original = self._valid_state().to_record()
        for label, mutate in mutations.items():
            with self.subTest(reference=label):
                record = copy.deepcopy(original)
                mutate(record)
                with self.assertRaises((TypeError, ValueError)):
                    RuntimeState.from_record(record)

    def test_runtime_state_rejects_occurrence_identity_conflicts(self) -> None:
        """Occurrence keys must agree with their Node and Scope identities."""

        mutations = {
            "occurrence node identity": (
                (
                    (
                        "invocation",
                        "scheduler",
                        "occurrences",
                        "source@root",
                        "node_id",
                    ),
                    "another-node",
                ),
            ),
            "occurrence scope identity": (
                (
                    (
                        "invocation",
                        "scheduler",
                        "occurrences",
                        "source@root",
                        "scope",
                    ),
                    [{"loop_region_id": "loop", "iteration": 0}],
                ),
            ),
        }
        original = self._valid_state().to_record()
        for label, updates in mutations.items():
            with self.subTest(invariant=label):
                record = copy.deepcopy(original)
                for path, value in updates:
                    _set_path(record, path, value)
                with self.assertRaises((TypeError, ValueError)):
                    RuntimeState.from_record(record)

    def test_activations_require_a_terminal_source_occurrence(self) -> None:
        """Verify every selected Activation originates from completed or failed work."""

        record = self._valid_state().to_record()
        _set_path(
            record,
            (
                "invocation",
                "scheduler",
                "occurrences",
                "source@root",
                "status",
            ),
            "running",
        )
        _set_path(
            record,
            (
                "invocation",
                "scheduler",
                "occurrences",
                "source@root",
                "completed_at_us",
            ),
            None,
        )
        with self.assertRaisesRegex(ValueError, "Activation source"):
            RuntimeState.from_record(record)

    def test_operator_call_status_and_occurrence_matrix_is_enforced(self) -> None:
        """Verify Call completion fields and owning Occurrence status agree."""

        call_status_path = (
            "invocation",
            "scheduler",
            "operator_calls",
            "call",
            "status",
        )
        call_completed_path = (
            "invocation",
            "scheduler",
            "operator_calls",
            "call",
            "completed_at_us",
        )
        call_error_path = (
            "invocation",
            "scheduler",
            "operator_calls",
            "call",
            "error",
        )
        owner_status_path = (
            "invocation",
            "scheduler",
            "occurrences",
            "worker@root",
            "status",
        )
        owner_completed_path = (
            "invocation",
            "scheduler",
            "occurrences",
            "worker@root",
            "completed_at_us",
        )
        error = {"type": "Failure", "message": "bad"}
        mutations = {
            "running has completion": ((call_completed_path, 5),),
            "terminal lacks completion": ((call_status_path, "completed"),),
            "failed lacks error": (
                (call_status_path, "failed"),
                (call_completed_path, 5),
            ),
            "non-failed has error": ((call_error_path, error),),
            "running owner is terminal": (
                (owner_status_path, "completed"),
                (owner_completed_path, 5),
            ),
            "cancelled owner is running": (
                (call_status_path, "cancelled"),
                (call_completed_path, 5),
            ),
        }
        original = self._valid_state().to_record()
        for label, updates in mutations.items():
            with self.subTest(invariant=label):
                record = copy.deepcopy(original)
                for path, value in updates:
                    _set_path(record, path, value)
                with self.assertRaises((TypeError, ValueError)):
                    RuntimeState.from_record(record)

    def test_wait_status_and_occurrence_matrix_is_enforced(self) -> None:
        """Verify Wait resume fields, owner status and active uniqueness agree."""

        wait_status_path = (
            "invocation",
            "scheduler",
            "waits",
            "wait",
            "status",
        )
        resumed_path = (
            "invocation",
            "scheduler",
            "waits",
            "wait",
            "resumed_at_us",
        )
        owner_status_path = (
            "invocation",
            "scheduler",
            "occurrences",
            "waiting@root",
            "status",
        )
        owner_completed_path = (
            "invocation",
            "scheduler",
            "occurrences",
            "waiting@root",
            "completed_at_us",
        )
        mutations = {
            "waiting has resume time": ((resumed_path, 5),),
            "resumed lacks time": ((wait_status_path, "resumed"),),
            "cancelled has resume time": (
                (wait_status_path, "cancelled"),
                (resumed_path, 5),
            ),
            "waiting owner is running": ((owner_status_path, "running"),),
            "cancelled owner is waiting": ((wait_status_path, "cancelled"),),
            "resumed owner is skipped": (
                (wait_status_path, "resumed"),
                (resumed_path, 5),
                (owner_status_path, "skipped"),
                (owner_completed_path, 5),
            ),
        }
        original = self._valid_state().to_record()
        for label, updates in mutations.items():
            with self.subTest(invariant=label):
                record = copy.deepcopy(original)
                for path, value in updates:
                    _set_path(record, path, value)
                with self.assertRaises((TypeError, ValueError)):
                    RuntimeState.from_record(record)

        duplicate = copy.deepcopy(original)
        waits = duplicate["invocation"]["scheduler"]["waits"]  # type: ignore[index]
        waits["second"] = copy.deepcopy(waits["wait"])  # type: ignore[index]
        waits["second"]["id"] = "second"  # type: ignore[index]
        with self.assertRaisesRegex(ValueError, "multiple waiting Waits"):
            RuntimeState.from_record(duplicate)

    def test_invocation_status_and_timestamp_matrix_is_enforced(self) -> None:
        """Verify Invocation lifecycle fields agree without forbidding early failure."""

        status_path = ("invocation", "status")
        started_path = ("invocation", "started_at_us")
        completed_path = ("invocation", "completed_at_us")
        error_path = ("invocation", "error")
        mutations = {
            "created has start": ((status_path, "created"),),
            "running lacks start": ((started_path, None),),
            "running has completion": ((completed_path, 5),),
            "completed lacks completion": ((status_path, "completed"),),
            "completed lacks start": (
                (status_path, "completed"),
                (started_path, None),
                (completed_path, 5),
            ),
            "failed lacks error": (
                (status_path, "failed"),
                (completed_path, 5),
            ),
            "running has error": (
                (error_path, {"type": "Failure", "message": "bad"}),
            ),
        }
        original = self._valid_state().to_record()
        for label, updates in mutations.items():
            with self.subTest(invariant=label):
                record = copy.deepcopy(original)
                for path, value in updates:
                    _set_path(record, path, value)
                with self.assertRaises((TypeError, ValueError)):
                    RuntimeState.from_record(record)

    def test_every_transition_advances_the_session_time_boundary(self) -> None:
        """Wall time may regress while Event ordering remains contiguous."""
        state = self._valid_state()
        for index, timestamp in enumerate((6, 4), 1):
            payload = ChildInvocationPhaseChanged("creation", 0, "opened" if index == 1 else "accepted")
            delta = TransitionPlanner().plan(state, payload, session_id="session", invocation_id="invocation", occurred_at_us=timestamp)
            event = RuntimeEvent("session", state.sequence+1, payload, "invocation", delta=delta, occurred_at_us=timestamp)
            state = StateReducer().apply(state, event)
            self.assertEqual(state.session.updated_at_us, timestamp)

    def test_trusted_checkpoint_reuses_the_reducer_owned_state(self) -> None:
        """Capture an already-validated Journal State without copying or rescanning it."""

        state = self._valid_state()
        with patch.object(RuntimeState, "to_record", side_effect=AssertionError):
            checkpoint = SessionCheckpoint._from_runtime_state(
                "session", state, captured_at_us=1
            )
        self.assertIs(root_snapshot(checkpoint).state, state)

    def test_checkpoint_rejects_an_invalid_external_state(self) -> None:
        """External checkpoint validation rejects broken durable references."""
        state = self._valid_state()
        scheduler = state.invocation.scheduler
        state = replace(state, invocation=replace(state.invocation,
            scheduler=replace(scheduler, ready=("missing@root",))))
        with self.assertRaises(ValueError):
            SessionCheckpoint.from_state(state)


if __name__ == "__main__":
    unittest.main()
