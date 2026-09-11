from __future__ import annotations

import json
from dataclasses import dataclass, replace
from types import SimpleNamespace
from autoagent.core.runtime import (RuntimeState, TransitionPlanner, InvocationState,
    NodeStarted, NodeCompleted, NodeFailed, OutputBound)
from autoagent.core.context import ContextPatch
import unittest
from typing_extensions import TypedDict

from autoagent.core import (
    ConditionContext,
    Edge,
    InputMappingContext,
    InvocationStarted,
    Node,
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeTransitionError,
    Scheduler,
    SessionOpened,
    StateReducer,
    Workflow,
    WorkflowCompiler,
)


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


def mapping(context: InputMappingContext) -> Value:
    return next(iter(context.incoming.values()))  # type: ignore[return-value]


def condition(context: ConditionContext) -> bool:
    return context.output is not None


@dataclass
class PlannedCompletion:
    occurrence_id: str
    output: object
    delta: object
    patch: ContextPatch = ContextPatch()


@dataclass
class PlannedFailure:
    occurrence_id: str
    error: object
    delta: object


class HarnessScheduler(Scheduler):
    """Attach test intent to pure Scheduler results without changing Core APIs."""
    def complete(self, workflow, state, occurrence_id, output, **kwargs):
        return PlannedCompletion(occurrence_id, output,
            super().complete(workflow, state, occurrence_id, output, **kwargs))

    def fail(self, workflow, state, occurrence_id, error, **kwargs):
        return PlannedFailure(occurrence_id, error,
            super().fail(workflow, state, occurrence_id, error, **kwargs))


class Harness:
    def __init__(self, workflow: Workflow, *, entry: str) -> None:
        self.workflow = WorkflowCompiler().compile_or_raise(workflow)
        self.scheduler = HarnessScheduler()
        self._state = RuntimeState()
        self._events = []
        self.planner = TransitionPlanner()
        self.reducer = StateReducer()
        self.journal = SimpleNamespace(state=lambda _: self._state,
            events=lambda _: tuple(self._events), reducer=self.reducer)
        self.emit(SessionOpened({}), invocation_id=None)
        payload = InvocationStarted(self.workflow.workflow_id, self.workflow.workflow_revision_id, entry, {"value":1})
        initial = InvocationState("invocation", payload.workflow_id, payload.workflow_revision_id, entry, "running", payload.input, {})
        graph = self.scheduler.initialize(self.workflow, replace(self._state, invocation=initial))
        self.emit(payload, scheduler_delta=graph)

    @property
    def state(self):
        return self._state

    def emit(self, payload, *, invocation_id="invocation", scheduler_delta=None):
        if isinstance(payload, SessionOpened):
            invocation_id = None
        if isinstance(payload, PlannedCompletion):
            if payload.patch != ContextPatch():
                self.emit(OutputBound(payload.occurrence_id, payload.patch, 0))
            scheduler_delta = payload.delta
            payload = NodeCompleted(payload.occurrence_id, payload.output)
        elif isinstance(payload, PlannedFailure):
            scheduler_delta = payload.delta
            payload = NodeFailed(payload.occurrence_id, payload.error)
        sequence = self._state.sequence+1
        delta = self.planner.plan(self._state, payload, session_id="session", invocation_id=invocation_id,
            occurred_at_us=sequence*10, scheduler_delta=scheduler_delta)
        event = RuntimeEvent("session", sequence, payload, invocation_id,
            delta=delta, id=f"event-{sequence}", occurred_at_us=sequence*10)
        self._state = self.reducer.apply(self._state, event)
        self._events.append(event)
        return event

    def start(self, occurrence_id):
        self.emit(NodeStarted(occurrence_id))

    def complete(self, occurrence_id, selected=None):
        self.emit(self.scheduler.complete(self.workflow, self.state, occurrence_id,
            {"value":1}, selected_edge_ids=selected or set()))


class DAGSchedulerTests(unittest.TestCase):
    def test_serial_nodes_advance_by_semantic_events(self) -> None:
        """Verify serial nodes advance by semantic events."""
        harness = Harness(
            Workflow(
                "serial",
                nodes=[Node("a", identity), Node("b", identity)],
                edges=[Edge("a", "b")],
            ),
            entry="a",
        )
        self.assertEqual(harness.state.invocation.scheduler.ready, ("a@root",))
        harness.start("a@root")
        harness.complete("a@root", {"a->b"})
        scheduler = harness.state.invocation.scheduler
        self.assertEqual(scheduler.occurrences["a@root"].status, "completed")
        self.assertEqual(scheduler.ready, ("b@root",))
        activation = scheduler.occurrences["b@root"].activations[0]
        self.assertEqual(activation.edge_id, "a->b")
        self.assertEqual(activation.source_occurrence_id, "a@root")
        activation_record = next(op.to_record()["value"]["activations"][0] for op in harness.journal.events("session")[-1].delta.operations if "occurrences" in op.path and op.op == "add")
        self.assertNotIn("output", activation_record)

    def test_fan_out_ready_order_is_compiled_edge_order(self) -> None:
        """Verify fan out ready order is compiled edge order."""
        harness = Harness(
            Workflow(
                "fanout",
                nodes=[Node(name, identity) for name in ("start", "left", "right")],
                edges=[Edge("start", "left"), Edge("start", "right")],
            ),
            entry="start",
        )
        harness.start("start@root")
        harness.complete("start@root", {"start->left", "start->right"})
        self.assertEqual(
            harness.state.invocation.scheduler.ready,
            ("left@root", "right@root"),
        )

    def test_complete_fan_in_waits_for_every_incoming_resolution(self) -> None:
        """Verify complete fan in waits for every incoming resolution."""
        harness = Harness(
            Workflow(
                "join",
                nodes=[
                    Node("start", identity),
                    Node("left", identity),
                    Node("right", identity),
                    Node("join", identity, input_mapping=mapping),
                ],
                edges=[
                    Edge("start", "left"),
                    Edge("start", "right"),
                    Edge("left", "join"),
                    Edge("right", "join"),
                ],
            ),
            entry="start",
        )
        harness.start("start@root")
        harness.complete("start@root", {"start->left", "start->right"})
        harness.start("left@root")
        harness.complete("left@root", {"left->join"})
        self.assertNotIn("join@root", harness.state.invocation.scheduler.occurrences)
        harness.start("right@root")
        harness.complete("right@root", {"right->join"})
        self.assertEqual(harness.state.invocation.scheduler.ready, ("join@root",))

    def test_unselected_branch_propagates_skip_and_join_uses_selected_path(self) -> None:
        """Verify unselected branch propagates skip and join uses selected path."""
        harness = Harness(
            Workflow(
                "conditional",
                nodes=[
                    Node("start", identity),
                    Node("left", identity),
                    Node("right", identity),
                    Node("join", identity, input_mapping=mapping),
                ],
                edges=[
                    Edge("start", "left", condition),
                    Edge("start", "right", condition),
                    Edge("left", "join"),
                    Edge("right", "join"),
                ],
            ),
            entry="start",
        )
        harness.start("start@root")
        harness.complete("start@root", {"start->left"})
        scheduler = harness.state.invocation.scheduler
        self.assertEqual(scheduler.occurrences["right@root"].status, "skipped")
        self.assertFalse(scheduler.resolutions["right->join@root"].selected)
        harness.start("left@root")
        harness.complete("left@root", {"left->join"})
        self.assertEqual(harness.state.invocation.scheduler.ready, ("join@root",))

    def test_all_unselected_paths_skip_downstream_graph(self) -> None:
        """Verify all unselected paths skip downstream graph."""
        harness = Harness(
            Workflow(
                "no-route",
                nodes=[Node(name, identity) for name in ("start", "middle", "finish")],
                edges=[Edge("start", "middle", condition), Edge("middle", "finish")],
            ),
            entry="start",
        )
        harness.start("start@root")
        harness.complete("start@root")
        scheduler = harness.state.invocation.scheduler
        self.assertEqual(scheduler.ready, ())
        self.assertEqual(scheduler.occurrences["middle@root"].status, "skipped")
        self.assertEqual(scheduler.occurrences["finish@root"].status, "skipped")

    def test_non_selected_entry_is_skipped_and_can_unlock_join(self) -> None:
        """Verify non selected entry is skipped and can unlock join."""
        harness = Harness(
            Workflow(
                "entries",
                nodes=[
                    Node("one", identity),
                    Node("two", identity),
                    Node("join", identity, input_mapping=mapping),
                ],
                edges=[Edge("one", "join"), Edge("two", "join")],
            ),
            entry="one",
        )
        self.assertEqual(
            harness.state.invocation.scheduler.occurrences["two@root"].status,
            "skipped",
        )
        harness.start("one@root")
        harness.complete("one@root", {"one->join"})
        self.assertEqual(harness.state.invocation.scheduler.ready, ("join@root",))

    def test_error_route_resolves_complete_edges_as_skipped(self) -> None:
        """Verify error route resolves complete edges as skipped."""
        workflow = Workflow(
            "error",
            nodes=[
                Node("start", identity),
                Node("success", identity),
                Node("failure", identity, input_mapping=mapping),
            ],
            edges=[
                Edge("start", "success"),
                Edge("start", "failure", on="error"),
            ],
        )
        harness = Harness(workflow, entry="start")
        harness.start("start@root")
        harness.emit(
            harness.scheduler.fail(
                harness.workflow,
                harness.state,
                "start@root",
                RuntimeErrorInfo("Error", "failed"),
                selected_edge_ids={"start->failure"},
            )
        )
        scheduler = harness.state.invocation.scheduler
        self.assertEqual(scheduler.occurrences["start@root"].status, "failed")
        self.assertEqual(scheduler.occurrences["success@root"].status, "skipped")
        self.assertEqual(scheduler.ready, ("failure@root",))

    def test_every_scheduler_event_prefix_replays_identically(self) -> None:
        """Verify every scheduler event prefix replays identically."""
        harness = Harness(
            Workflow(
                "replay",
                nodes=[Node("a", identity), Node("b", identity)],
                edges=[Edge("a", "b")],
            ),
            entry="a",
        )
        harness.start("a@root")
        harness.complete("a@root", {"a->b"})
        events = harness.journal.events("session")
        for sequence in range(len(events) + 1):
            self.assertEqual(
                StateReducer().reduce(events[:sequence]).sequence,
                sequence,
            )
        restored = RuntimeEvent.from_record(
            json.loads(json.dumps(events[-1].to_record()))
        )
        self.assertEqual(restored, events[-1])

    def test_invalid_route_decisions_are_rejected_without_state_change(self) -> None:
        """Verify invalid route decisions are rejected without state change."""
        harness = Harness(
            Workflow(
                "invalid-route",
                nodes=[Node("a", identity), Node("b", identity)],
                edges=[Edge("a", "b")],
            ),
            entry="a",
        )
        harness.start("a@root")
        before = harness.state
        with self.assertRaisesRegex(RuntimeTransitionError, "EDGE_UNCONDITIONAL_NOT_SELECTED"):
            harness.scheduler.complete(
                harness.workflow,
                harness.state,
                "a@root",
                {"value": 1},
            )
        self.assertIs(harness.state, before)


if __name__ == "__main__":
    unittest.main()
