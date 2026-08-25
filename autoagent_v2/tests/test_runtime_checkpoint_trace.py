from __future__ import annotations

import json
import unittest
from dataclasses import replace
from types import MappingProxyType
from unittest.mock import patch

from typing_extensions import TypedDict

from autoagent.core import InMemoryEventJournal, InvocationOpened, SessionOpened
from autoagent.core.app import AutoAgentApp
from autoagent.core.runtime import (
    ChildAwaitReady,
    ChildAwaitSuspended,
    ChildInvocationPhaseChanged,
    ChildInvocationPlanned,
    ChildUnitSpec,
    InvocationState,
    NodeOccurrenceState,
    OperatorCallStarted,
    RuntimeCheckpointBundle,
    RuntimeEvent,
    RuntimeState,
    SchedulerState,
    SessionState,
    StateReducer,
    StateTransition,
    freeze,
    project_trace_event,
    project_trace_events,
)
from autoagent.core.workflow import Node, Workflow


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


class RuntimeCheckpointTraceTests(unittest.TestCase):
    def _running_parent_state(self) -> RuntimeState:
        occurrence = NodeOccurrenceState(
            id="child-node@root",
            node_id="child-node",
            scope=(),
            status="running",
            started_at_ns=1,
            started_state_version=1,
        )
        return RuntimeState(
            session=SessionState(
                id="root",
                context=freeze({}),
                created_at_ns=0,
                updated_at_ns=1,
                latest_invocation_id="parent",
            ),
            invocation=InvocationState(
                id="parent",
                workflow_id="parent-workflow",
                workflow_revision_id="parent-revision",
                entry_node_id="child-node",
                status="running",
                input=freeze({}),
                context=freeze({}),
                started_at_ns=1,
                scheduler=SchedulerState(
                    initialized=True,
                    occurrences=MappingProxyType({occurrence.id: occurrence}),
                ),
            ),
            state_version=1,
        )

    @staticmethod
    def _apply(state: RuntimeState, payload) -> RuntimeState:
        return StateReducer().apply(
            state,
            RuntimeEvent(
                session_id="root",
                invocation_id="parent",
                sequence=state.sequence + 1,
                occurred_at_ns=state.sequence + 1,
                payload=payload,
            ),
        )

    def test_runtime_event_codec_requires_exact_schema_and_fields(self) -> None:
        """Verify Runtime Event decoding rejects unknown schema and fields."""

        event = RuntimeEvent(
            session_id="session",
            invocation_id=None,
            sequence=1,
            payload=SessionOpened({}),
        )
        record = event.to_record()
        unknown = dict(record)
        unknown["unknown"] = True
        with self.assertRaises((TypeError, ValueError)):
            RuntimeEvent.from_record(unknown)
        unsupported = dict(record)
        unsupported["schema_version"] = 999
        with self.assertRaises((TypeError, ValueError)):
            RuntimeEvent.from_record(unsupported)

    def test_runtime_event_codec_rejects_inconsistent_runtime_logs(self) -> None:
        """Verify persisted Logs cannot disagree with their sealed Event envelope."""

        journal = InMemoryEventJournal(max_batches_per_event=100)
        journal.append(
            RuntimeEvent(
                session_id="session",
                invocation_id=None,
                sequence=1,
                occurred_at_ns=1,
                payload=SessionOpened({}),
            )
        )
        journal.append(
            RuntimeEvent(
                session_id="session",
                invocation_id="invocation",
                sequence=1,
                occurred_at_ns=2,
                payload=InvocationOpened(
                    "workflow", "revision", "entry", {"value": 1}
                ),
            )
        )
        persisted = journal.flush("session")
        self.assertIsNotNone(persisted)
        assert persisted is not None
        record = persisted.to_record()
        self.assertEqual(RuntimeEvent.from_record(record), persisted)
        self.assertIsNone(record["logs"][0]["invocation_id"])  # type: ignore[index]

        invalid_records = []
        negative_time = json.loads(json.dumps(record))
        negative_time["logs"][0]["occurred_at_ns"] = -1
        invalid_records.append(negative_time)
        wrong_invocation = json.loads(json.dumps(record))
        wrong_invocation["logs"][-1]["invocation_id"] = "another-invocation"
        invalid_records.append(wrong_invocation)
        out_of_range_version = json.loads(json.dumps(record))
        out_of_range_version["logs"][-1]["state_version"] += 1
        invalid_records.append(out_of_range_version)

        for invalid in invalid_records:
            with self.subTest(record=invalid):
                with self.assertRaises((TypeError, ValueError)):
                    RuntimeEvent.from_record(invalid)

    def test_runtime_event_chain_is_sealed_and_validated(self) -> None:
        """Verify persisted Runtime Events carry and validate the previous chain."""

        journal = InMemoryEventJournal()
        journal.append(
            RuntimeEvent(
                session_id="session",
                invocation_id=None,
                sequence=1,
                payload=SessionOpened({}),
            )
        )
        first = journal.events("session")[0]
        journal.append(
            RuntimeEvent(
                session_id="session",
                invocation_id="invocation",
                sequence=2,
                payload=InvocationOpened("workflow", "revision", "entry", {"value": 1}),
            )
        )
        second = journal.events("session")[1]
        first_state = StateReducer().apply(RuntimeState(), first)
        self.assertEqual(second.previous_event_id, first.id)
        self.assertEqual(second.previous_event_digest, first_state.last_event_digest)
        with self.assertRaisesRegex(Exception, "CHAIN|chain|previous"):
            StateReducer().apply(
                first_state,
                replace(second, previous_event_id="another-event"),
            )

    def test_trace_projection_excludes_runtime_operations_and_user_values(self) -> None:
        """Verify public Trace Events expose allowlisted metadata without State patches."""

        runtime_event = RuntimeEvent(
            session_id="session",
            invocation_id="invocation",
            sequence=1,
            payload=OperatorCallStarted(
                "call",
                "node@root",
                "operator",
                2,
                {"secret": "must-not-leak"},
            ),
        )
        traces = project_trace_events(runtime_event, start_sequence=1)
        self.assertEqual(len(traces), 1)
        trace = traces[0]
        self.assertEqual(trace.subject_ids["call_id"], "call")
        self.assertEqual(trace.status, "running")
        self.assertEqual(trace.attributes["unit_index"], 2)
        record = trace.to_record()
        encoded = json.dumps(record, sort_keys=True)
        for forbidden in (
            "operation_batches",
            "operations",
            "delta",
            "patch",
            "must-not-leak",
        ):
            self.assertNotIn(forbidden, encoded)
        self.assertEqual(type(trace).from_record(record), trace)

    def test_trace_projects_an_immediate_transition_without_runtime_event_identity(self) -> None:
        """Verify SDK Trace projection does not require a sealed Runtime Event."""

        transition = StateTransition(
            session_id="session",
            invocation_id="invocation",
            payload=OperatorCallStarted(
                "call", "occurrence", "operator", 0, {"secret": True}
            ),
        )
        trace = project_trace_event("session", 7, transition)
        self.assertEqual(trace.trace_sequence, 7)
        self.assertNotIn("runtime_event", json.dumps(trace.to_record()))
        self.assertNotIn("secret", json.dumps(trace.to_record()))

    def test_child_plan_and_await_boundaries_are_durable_state_transitions(self) -> None:
        """Verify planned Child work can suspend and later ready the parent occurrence."""

        state = self._running_parent_state()
        plan = ChildInvocationPlanned(
            creation_id="creation",
            parent_occurrence_id="child-node@root",
            mode="await",
            workflow_id="child-workflow",
            workflow_revision_id="child-revision",
            units=(ChildUnitSpec(0, "child-session", "child-invocation", {"v": 1}),),
        )
        for payload in (
            plan,
            ChildInvocationPhaseChanged("creation", 0, "opened"),
            ChildInvocationPhaseChanged("creation", 0, "accepted"),
            ChildAwaitSuspended("creation", "child-node@root"),
        ):
            state = self._apply(state, payload)
        self.assertEqual(state.invocation.status, "waiting")
        self.assertEqual(
            state.invocation.scheduler.occurrences["child-node@root"].status,
            "waiting",
        )
        state = self._apply(
            state, ChildInvocationPhaseChanged("creation", 0, "terminal")
        )
        state = self._apply(state, ChildAwaitReady("creation", "child-node@root"))
        self.assertEqual(state.invocation.status, "running")
        self.assertEqual(
            state.invocation.scheduler.occurrences["child-node@root"].status,
            "ready",
        )

    def test_spawn_child_phase_can_advance_after_parent_is_terminal(self) -> None:
        """Verify a spawned Child may report terminal after its parent Invocation."""

        state = self._running_parent_state()
        state = self._apply(
            state,
            ChildInvocationPlanned(
                "creation",
                "child-node@root",
                "spawn",
                "child-workflow",
                "child-revision",
                (ChildUnitSpec(0, "child-session", "child-invocation", {}),),
            ),
        )
        state = self._apply(state, ChildInvocationPhaseChanged("creation", 0, "opened"))
        state = self._apply(state, ChildInvocationPhaseChanged("creation", 0, "accepted"))
        occurrence = state.invocation.scheduler.occurrences["child-node@root"]
        terminal_parent = replace(
            state,
            invocation=replace(
                state.invocation,
                status="completed",
                completed_at_ns=state.session.updated_at_ns,
                scheduler=replace(
                    state.invocation.scheduler,
                    occurrences=MappingProxyType(
                        {
                            "child-node@root": replace(
                                occurrence,
                                status="completed",
                                completed_at_ns=state.session.updated_at_ns,
                            )
                        }
                    ),
                ),
            ),
        )
        terminal_parent = self._apply(
            terminal_parent,
            ChildInvocationPhaseChanged("creation", 0, "terminal"),
        )
        self.assertEqual(
            terminal_parent.invocation.child_plans["creation"].units[0].phase,
            "terminal",
        )

    def test_checkpoint_capture_is_shallow_and_allows_unopened_planned_child(self) -> None:
        """Verify stream-boundary capture avoids State serialization and permits plan gaps."""

        state = self._apply(
            self._running_parent_state(),
            ChildInvocationPlanned(
                "creation",
                "child-node@root",
                "spawn",
                "child-workflow",
                "child-revision",
                (ChildUnitSpec(0, "child-session", "child-invocation", {}),),
            ),
        )
        with patch.object(RuntimeState, "to_record", side_effect=AssertionError):
            checkpoint = RuntimeCheckpointBundle._from_runtime_states(
                "root", {"root": state}, captured_at_ns=1
            )
        self.assertIs(checkpoint.states["root"], state)

    def test_checkpoint_bundle_round_trips_a_complete_child_graph(self) -> None:
        """Verify one Root checkpoint contains each linked Child Runtime State once."""

        journal = InMemoryEventJournal()
        app = AutoAgentApp(runtime_journal=journal)
        try:
            child = Workflow("checkpoint-child", nodes=[Node("work", identity)])
            parent = Workflow(
                "checkpoint-parent",
                nodes=[Node("spawn", child, execution_mode="spawn")],
            )
            result = app.invoke(parent, {"value": 1}, session_id="root-session")
            app.wait_child(result.output, timeout=1)
            checkpoint = journal.capture_checkpoint("root-session")
            self.assertEqual(checkpoint.root_session_id, "root-session")
            self.assertEqual(set(checkpoint.states), {"root-session", result.output["session_id"]})
            record = json.loads(json.dumps(checkpoint.to_record()))
            self.assertEqual(RuntimeCheckpointBundle.from_record(record), checkpoint)
        finally:
            app.close()

    def test_checkpoint_bundle_rejects_missing_and_orphan_child_states(self) -> None:
        """Verify checkpoint graph validation rejects missing and unreachable States."""

        journal = InMemoryEventJournal()
        app = AutoAgentApp(runtime_journal=journal)
        try:
            child = Workflow("graph-child", nodes=[Node("work", identity)])
            parent = Workflow(
                "graph-parent",
                nodes=[Node("spawn", child, execution_mode="spawn")],
            )
            result = app.invoke(parent, {"value": 1}, session_id="graph-root")
            app.wait_child(result.output, timeout=1)
            checkpoint = journal.capture_checkpoint("graph-root")
            with self.assertRaisesRegex(ValueError, "Child|child|missing"):
                RuntimeCheckpointBundle.from_states(
                    "graph-root",
                    {"graph-root": checkpoint.states["graph-root"]},
                )
            child_session_id = result.output["session_id"]
            with self.assertRaisesRegex(ValueError, "orphan|reachable|Root"):
                RuntimeCheckpointBundle.from_states(
                    child_session_id,
                    checkpoint.states,
                )
        finally:
            app.close()

    def test_journal_installs_checkpoint_states_atomically_and_idempotently(self) -> None:
        """Verify checkpoint State installation cannot leave a partial Runtime graph."""

        source = InMemoryEventJournal()
        source.append(
            RuntimeEvent(
                session_id="source",
                invocation_id=None,
                sequence=1,
                payload=SessionOpened({}),
            )
        )
        state = source.state("source")
        target = InMemoryEventJournal()
        target.install_states({"source": state})
        self.assertEqual(target.state("source"), state)
        target.install_states({"source": state})
        before = target.state("source")
        with self.assertRaises((TypeError, ValueError)):
            target.install_states({"wrong-key": state})
        self.assertIs(target.state("source"), before)
        self.assertIsNone(target.state("wrong-key").session)


if __name__ == "__main__":
    unittest.main()
