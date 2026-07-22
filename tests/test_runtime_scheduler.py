from __future__ import annotations

import unittest
from uuid import uuid4

from autoagent.core.runtime import EdgeActivation, LoopIteration, SchedulerContext


class SchedulerContextTests(unittest.TestCase):
    def test_ready_queue_drains_batch_and_respects_limit(self) -> None:
        scheduler = SchedulerContext()
        upstream = uuid4()
        activation = EdgeActivation(
            edge_id="edge_upstream_a",
            source_node_id="upstream",
            source_execution_id=upstream,
        )
        scheduler.enqueue_ready("a", activations=(activation,))
        scheduler.enqueue_ready("b")
        scheduler.enqueue_ready("c")

        first_batch = scheduler.drain_ready(limit=2)
        second_batch = scheduler.drain_ready()

        self.assertEqual([request.node_id for request in first_batch], ["a", "b"])
        self.assertEqual(first_batch[0].source_execution_ids, (upstream,))
        self.assertEqual([request.node_id for request in second_batch], ["c"])
        self.assertEqual(scheduler.drain_ready(), [])

    def test_waiting_execution_is_keyed_by_wait_key(self) -> None:
        scheduler = SchedulerContext()
        execution_id = uuid4()
        scheduler.add_waiting_execution(
            wait_key="approval:1",
            node_execution_id=execution_id,
            node_id="approve",
            wait_type="human",
            payload={"request_id": "1"},
        )

        self.assertIn("approval:1", scheduler.waiting_executions)
        waiting = scheduler.remove_waiting_execution("approval:1")

        self.assertEqual(waiting.node_execution_id, execution_id)
        self.assertEqual(waiting.node_id, "approve")
        self.assertEqual(waiting.wait_type, "human")
        self.assertEqual(waiting.payload, {"request_id": "1"})
        self.assertEqual(scheduler.waiting_executions, {})

    def test_active_wait_key_must_be_unique_within_invocation(self) -> None:
        scheduler = SchedulerContext()
        scheduler.add_waiting_execution(
            wait_key="approval:1",
            node_execution_id=uuid4(),
            node_id="first",
        )

        with self.assertRaisesRegex(ValueError, "Duplicate active wait key"):
            scheduler.add_waiting_execution(
                wait_key="approval:1",
                node_execution_id=uuid4(),
                node_id="second",
            )

        self.assertEqual(
            scheduler.waiting_executions["approval:1"].node_id,
            "first",
        )

    def test_transition_queue_drains_completed_states(self) -> None:
        scheduler = SchedulerContext()
        first = uuid4()
        second = uuid4()
        scheduler.enqueue_transition(
            node_execution_id=first,
            node_id="a",
            state="completed",
        )
        scheduler.enqueue_transition(
            node_execution_id=second,
            node_id="b",
            state="waiting",
        )

        transitions = scheduler.drain_transitions()

        self.assertEqual([transition.node_id for transition in transitions], ["a", "b"])
        self.assertEqual([transition.state for transition in transitions], ["completed", "waiting"])
        self.assertEqual(scheduler.drain_transitions(), [])

    def test_scheduler_context_round_trips_current_cursor(self) -> None:
        scheduler = SchedulerContext()
        execution_id = uuid4()
        activation = EdgeActivation(
            edge_id="edge_sleep_next",
            source_node_id="sleep",
            source_execution_id=execution_id,
        )
        scheduler.enqueue_ready("next", activations=(activation,))
        scheduler.resolve_edge(
            activation.edge_id,
            state="selected",
            activation=activation,
        )
        scheduler.scheduled_node_instances.add("next")
        scheduler.entered_loop_instances.add("loop_1")
        scheduler.add_waiting_execution(
            wait_key="timer:1",
            node_execution_id=execution_id,
            node_id="sleep",
            wait_type="timer",
        )
        scheduler.enqueue_transition(
            node_execution_id=execution_id,
            node_id="sleep",
            state="waiting",
        )

        loaded = SchedulerContext.from_record(scheduler.to_record())

        self.assertEqual([request.node_id for request in loaded.ready_queue], ["next"])
        self.assertEqual(loaded.ready_queue[0].activations, (activation,))
        self.assertEqual(
            loaded.edge_resolutions[activation.edge_id].activation,
            activation,
        )
        self.assertEqual(loaded.scheduled_node_instances, {"next"})
        self.assertEqual(loaded.entered_loop_instances, {"loop_1"})
        self.assertEqual(
            list(loaded.waiting_executions),
            ["timer:1"],
        )
        self.assertEqual([transition.state for transition in loaded.transition_queue], ["waiting"])

    def test_scheduler_context_round_trips_scoped_loop_occurrences(self) -> None:
        scheduler = SchedulerContext()
        execution_id = uuid4()
        scope = (
            LoopIteration("outer", 2),
            LoopIteration("inner", 1),
        )
        activation = EdgeActivation(
            edge_id="inner_back",
            source_node_id="collect",
            source_execution_id=execution_id,
        )
        scheduler.enqueue_ready(
            "inner",
            activations=(activation,),
            execution_scope=scope,
        )
        scheduler.resolve_edge(
            "inner_back",
            state="selected",
            activation=activation,
            scope=scope,
        )
        scheduler.resolve_loop_boundary(
            loop_region_id="inner",
            loop_scope=scope,
            edge_id="inner_exit",
            state="skipped",
        )

        loaded = SchedulerContext.from_record(scheduler.to_record())

        self.assertEqual(loaded.ready_queue[0].execution_scope, scope)
        self.assertEqual(
            next(iter(loaded.edge_resolutions.values())).scope,
            scope,
        )
        self.assertEqual(
            next(iter(loaded.loop_boundary_resolutions.values())).scope,
            scope,
        )


if __name__ == "__main__":
    unittest.main()
