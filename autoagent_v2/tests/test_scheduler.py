from __future__ import annotations

import unittest
from uuid import UUID, uuid4

from autoagent.core import AutoAgentApp, Edge, InvocationState, Node, Workflow, WorkflowCompiler
from autoagent.core.scheduler import Scheduler
from autoagent.core.runtime import RuntimeState
from tests.helpers import always_true, identity_int


identity = identity_int


def diamond() -> Workflow:
    return Workflow(
        "diamond",
        nodes=[
            Node("start", identity),
            Node("left", identity),
            Node("right", identity),
            Node("join", identity),
            Node("finish", identity),
        ],
        edges=[
            Edge("start", "left", id="start-left"),
            Edge("start", "right", id="start-right"),
            Edge("left", "join", id="left-join"),
            Edge("right", "join", id="right-join"),
            Edge("join", "finish", id="join-finish"),
        ],
    )


class SchedulerTests(unittest.TestCase):
    def scheduler(self, value: Workflow) -> Scheduler:
        workflow = WorkflowCompiler().compile(value)
        state = RuntimeState.create(
            workflow_id=workflow.workflow_id,
            workflow_revision_id=workflow.workflow_revision_id,
            session_id="scheduler-test",
            invocation_id=UUID("00000000-0000-0000-0000-000000000001"),
            event_mode="full",
            invocation_input=None,
            session_created_at_ms=1,
            invocation_created_at_ms=1,
        )
        scheduler = Scheduler(workflow, state)
        scheduler.initialize()
        return scheduler

    def test_initialize_enqueues_every_structural_entry_once(self) -> None:
        scheduler = self.scheduler(
            Workflow(
                "entries",
                nodes=[Node("left", identity), Node("right", identity)],
            )
        )
        self.assertEqual(
            [request.node_id for request in scheduler.drain_ready()],
            ["left", "right"],
        )
        scheduler.initialize()
        self.assertEqual(scheduler.drain_ready(), ())

    def test_failed_transition_discards_temporary_scheduler_state(self) -> None:
        scheduler = self.scheduler(diamond())
        start = scheduler.drain_ready()[0]
        before = scheduler._runtime_state.checkpoint_record()

        with self.assertRaisesRegex(ValueError, "every outgoing Edge"):
            scheduler.resolve_outgoing(start, uuid4(), {"start-left": True})

        self.assertIsNone(scheduler._working)
        self.assertEqual(scheduler._runtime_state.checkpoint_record(), before)
        self.assertEqual(scheduler.scheduled, {start.occurrence})

    def test_fan_out_enqueues_each_selected_target(self) -> None:
        scheduler = self.scheduler(diamond())
        start = scheduler.drain_ready()[0]
        scheduler.resolve_outgoing(
            start,
            uuid4(),
            {"start-left": True, "start-right": True},
        )
        ready = scheduler.drain_ready()
        self.assertEqual([request.node_id for request in ready], ["left", "right"])

    def test_complete_fan_in_waits_for_every_incoming_resolution(self) -> None:
        scheduler = self.scheduler(diamond())
        start = scheduler.drain_ready()[0]
        scheduler.resolve_outgoing(
            start,
            uuid4(),
            {"start-left": True, "start-right": True},
        )
        left, right = scheduler.drain_ready()
        left_execution = uuid4()
        scheduler.resolve_outgoing(left, left_execution, {"left-join": True})
        self.assertEqual(scheduler.drain_ready(), ())
        right_execution = uuid4()
        scheduler.resolve_outgoing(right, right_execution, {"right-join": True})
        join = scheduler.drain_ready()[0]
        self.assertEqual(join.node_id, "join")
        self.assertEqual(
            {activation.source_execution_id for activation in join.activations},
            {left_execution, right_execution},
        )

    def test_fan_in_runs_with_only_the_selected_incoming_activation(self) -> None:
        scheduler = self.scheduler(diamond())
        start = scheduler.drain_ready()[0]
        scheduler.resolve_outgoing(
            start,
            uuid4(),
            {"start-left": True, "start-right": True},
        )
        left, right = scheduler.drain_ready()
        scheduler.resolve_outgoing(left, uuid4(), {"left-join": False})
        selected_execution = uuid4()
        scheduler.resolve_outgoing(right, selected_execution, {"right-join": True})
        join = scheduler.drain_ready()[0]
        self.assertEqual(len(join.activations), 1)
        self.assertEqual(join.activations[0].source_execution_id, selected_execution)

    def test_all_unselected_incoming_edges_skip_target_and_descendants(self) -> None:
        scheduler = self.scheduler(diamond())
        start = scheduler.drain_ready()[0]
        scheduler.resolve_outgoing(
            start,
            uuid4(),
            {"start-left": True, "start-right": True},
        )
        left, right = scheduler.drain_ready()
        scheduler.resolve_outgoing(left, uuid4(), {"left-join": False})
        skipped = scheduler.resolve_outgoing(right, uuid4(), {"right-join": False})
        self.assertEqual({item.node_id for item in skipped}, {"join", "finish"})
        self.assertEqual(scheduler.drain_ready(), ())

    def test_scheduler_restore_preserves_cursor_without_history(self) -> None:
        original = self.scheduler(diamond())
        start = original.drain_ready()[0]
        original.resolve_outgoing(
            start,
            uuid4(),
            {"start-left": True, "start-right": True},
        )
        ready = original.drain_ready()

        restored = self.scheduler(diamond())
        restored.restore(
            ready=tuple(ready),
            resolutions=tuple(original.resolutions.items()),
            scheduled=tuple(original.scheduled),
            skipped=tuple(original.skipped),
        )
        self.assertEqual(restored.drain_ready(), tuple(ready))
        self.assertEqual(restored.resolutions, original.resolutions)
        self.assertEqual(restored.scheduled, original.scheduled)

    def test_loop_cannot_select_back_and_exit_edges_together(self) -> None:
        value = Workflow(
            "ambiguous-loop",
            nodes=[
                Node("start", identity),
                Node("header", identity),
                Node("body", identity),
                Node("finish", identity),
            ],
            edges=[
                Edge("start", "header"),
                Edge("header", "body"),
                Edge("body", "header", always_true),
                Edge("body", "finish", always_true),
            ],
        )
        app = AutoAgentApp()
        app.register_workflow(value)
        invocation = app.invoke(value, 1)
        self.assertEqual(invocation.state, InvocationState.FAILED)
        self.assertIn("LOOP_BACK_EXIT_CONFLICT", invocation.error.message)
        app.close()


if __name__ == "__main__":
    unittest.main()
