from __future__ import annotations

import json
import unittest
from dataclasses import replace
from types import MappingProxyType
from unittest.mock import patch

from typing_extensions import TypedDict

from autoagent.core import RuntimeRepository, SessionOpened
from autoagent.core.app import AutoAgentApp
from autoagent.core.runtime import (
    TransitionPlanner,
    ChildAwaitReady,
    ChildAwaitSuspended,
    ChildInvocationPhaseChanged,
    ChildInvocationPlanned,
    ChildUnitSpec,
    InvocationState,
    NodeOccurrenceState,
    OperatorCallStarted,
    SessionCheckpoint,
    RuntimeEvent,
    RuntimeState,
    SchedulerState,
    SessionState,
    StateReducer,
    freeze,
)
from autoagent.core.workflow import Node, Workflow


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


class CoreCheckpointTests(unittest.TestCase):
    def _running_parent_state(self) -> RuntimeState:
        occurrence = NodeOccurrenceState(
            id="child-node@root",
            node_id="child-node",
            scope=(),
            status="running",
            started_at_us=1,
            started_sequence=1,
        )
        return RuntimeState(
            session=SessionState(
                id="root",
                context=freeze({}),
                created_at_us=0,
                updated_at_us=1,
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
                started_at_us=1,
                scheduler=SchedulerState(
                    initialized=True,
                    occurrences=MappingProxyType({occurrence.id: occurrence}),
                ),
            ),
            sequence=1,
        )

    @staticmethod
    def _apply(state: RuntimeState, payload) -> RuntimeState:
        delta = TransitionPlanner().plan(state, payload, session_id="root", invocation_id="parent",
            occurred_at_us=state.sequence + 1)
        return StateReducer().apply(
            state,
            RuntimeEvent(delta=delta,
                session_id="root",
                invocation_id="parent",
                sequence=state.sequence + 1,
                occurred_at_us=state.sequence + 1,
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
            "running",
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
                completed_at_us=state.session.updated_at_us,
                scheduler=replace(
                    state.invocation.scheduler,
                    occurrences=MappingProxyType(
                        {
                            "child-node@root": replace(
                                occurrence,
                                status="completed",
                                completed_at_us=state.session.updated_at_us,
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
            checkpoint = SessionCheckpoint._from_runtime_state(
                "root", state, captured_at_us=1
            )
        self.assertIs(checkpoint.state, state)

    def test_session_checkpoints_round_trip_parent_and_child_independently(self) -> None:
        """Verify Parent and Child Runtime Sessions produce independent checkpoints."""

        journal = RuntimeRepository()
        app = AutoAgentApp(runtime_repository=journal)
        try:
            child = Workflow("checkpoint-child", nodes=[Node("work", identity)])
            parent = Workflow(
                "checkpoint-parent",
                nodes=[Node("spawn", child, execution_mode="spawn")],
            )
            result = app.invoke(parent, {"value": 1}, session_id="root-session")
            app.join(result.output, timeout=1)
            checkpoint = journal.capture_checkpoint("root-session")
            child_checkpoint = journal.capture_checkpoint(result.output.session_id)
            self.assertEqual(checkpoint.session_id, "root-session")
            self.assertEqual(child_checkpoint.session_id, result.output.session_id)
            record = json.loads(json.dumps(checkpoint.to_record()))
            self.assertEqual(SessionCheckpoint.from_record(record), checkpoint)
        finally:
            app.close()

    def test_parent_checkpoint_does_not_require_child_state(self) -> None:
        """Verify one Parent checkpoint remains valid without its Child State."""

        journal = RuntimeRepository()
        app = AutoAgentApp(runtime_repository=journal)
        try:
            child = Workflow("graph-child", nodes=[Node("work", identity)])
            parent = Workflow(
                "graph-parent",
                nodes=[Node("spawn", child, execution_mode="spawn")],
            )
            result = app.invoke(parent, {"value": 1}, session_id="graph-root")
            app.join(result.output, timeout=1)
            checkpoint = journal.capture_checkpoint("graph-root")
            rebuilt = SessionCheckpoint.from_state(checkpoint.state)
            self.assertEqual(rebuilt.session_id, "graph-root")
            self.assertNotEqual(rebuilt.id, checkpoint.id)
        finally:
            app.close()

    def test_repository_installs_checkpoint_states_atomically_and_idempotently(self) -> None:
        """Verify checkpoint State installation cannot leave a partial Runtime graph."""

        source = RuntimeRepository()
        import asyncio
        asyncio.run(source.commit(
                session_id="source",
                invocation_id=None,
                payload=SessionOpened({}),
            )
        )
        state = source.state("source")
        target = RuntimeRepository()
        target.install_states({"source": state})
        self.assertEqual(target.state("source"), state)
        self.assertIsNot(target.state("source"), state)
        target.install_states({"source": state})
        before = target.state("source")
        with self.assertRaises((TypeError, ValueError)):
            target.install_states({"source": state, "wrong-key": state})
        self.assertIs(target.state("source"), before)
        self.assertIsNone(target.state("wrong-key").session)


if __name__ == "__main__":
    unittest.main()
