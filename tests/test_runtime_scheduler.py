from __future__ import annotations

import unittest
from uuid import uuid4

from autoagent.runtime import SchedulerContext


class SchedulerContextTests(unittest.TestCase):
    def test_ready_queue_drains_batch_and_respects_limit(self) -> None:
        scheduler = SchedulerContext()
        upstream = uuid4()
        scheduler.enqueue_ready("a", source_execution_ids=(upstream,))
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
        scheduler.enqueue_ready("next", source_execution_ids=(execution_id,))
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
        self.assertEqual(
            list(loaded.waiting_executions),
            ["timer:1"],
        )
        self.assertEqual([transition.state for transition in loaded.transition_queue], ["waiting"])


if __name__ == "__main__":
    unittest.main()
