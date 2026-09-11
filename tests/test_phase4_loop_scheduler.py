from __future__ import annotations

import json
import unittest
from typing_extensions import TypedDict

from autoagent.core import (
    ConditionContext,
    Edge,
    InputMappingContext,
    LoopControlError,
    Node,
    RuntimeEvent,
    StateReducer,
    Workflow,
)
from autoagent.core.runtime import LoopIteration, occurrence_key

from tests.test_phase3_scheduler import Harness


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


def route(_context: ConditionContext) -> bool:
    return True


def first_input(context: InputMappingContext) -> Value:
    return next(iter(context.incoming.values()))  # type: ignore[return-value]


def simple_loop() -> Workflow:
    return Workflow(
        "simple-loop",
        nodes=[Node(name, identity) for name in ("start", "header", "body", "finish")],
        edges=[
            Edge("start", "header", id="enter"),
            Edge("header", "body", id="body"),
            Edge("body", "header", route, id="back"),
            Edge("body", "finish", route, id="exit"),
        ],
    )


class LoopSchedulerTests(unittest.TestCase):
    def test_self_loop_uses_one_occurrence_per_iteration(self) -> None:
        """Verify self loop uses one occurrence per iteration."""
        harness = Harness(
            Workflow(
                "self-loop",
                nodes=[Node(name, identity) for name in ("start", "header", "finish")],
                edges=[
                    Edge("start", "header", id="enter"),
                    Edge("header", "header", route, id="back"),
                    Edge("header", "finish", route, id="exit"),
                ],
            ),
            entry="start",
        )
        region = harness.workflow.loop_regions[0]
        harness.start("start@root")
        harness.complete("start@root", {"enter"})
        first = occurrence_key("header", (LoopIteration(region.id, 1),))
        harness.start(first)
        harness.complete(first, {"back"})
        second = occurrence_key("header", (LoopIteration(region.id, 2),))
        self.assertEqual(harness.state.invocation.scheduler.ready, (second,))

    def test_nested_loops_keep_independent_iteration_frames(self) -> None:
        """Verify nested loops keep independent iteration frames."""
        harness = Harness(
            Workflow(
                "nested-loop",
                nodes=[
                    Node(name, identity)
                    for name in ("start", "outer", "inner", "body", "latch", "finish")
                ],
                edges=[
                    Edge("start", "outer", id="enter-outer"),
                    Edge("outer", "inner", id="enter-inner"),
                    Edge("inner", "body", id="inner-body"),
                    Edge("body", "inner", route, id="inner-back"),
                    Edge("body", "latch", route, id="inner-exit"),
                    Edge("latch", "outer", route, id="outer-back"),
                    Edge("latch", "finish", route, id="outer-exit"),
                ],
            ),
            entry="start",
        )
        outer = next(item for item in harness.workflow.loop_regions if item.header_node_id == "outer")
        inner = next(item for item in harness.workflow.loop_regions if item.header_node_id == "inner")
        outer1 = (LoopIteration(outer.id, 1),)
        inner1 = (*outer1, LoopIteration(inner.id, 1))

        harness.start("start@root")
        harness.complete("start@root", {"enter-outer"})
        outer_occurrence = occurrence_key("outer", outer1)
        harness.start(outer_occurrence)
        harness.complete(outer_occurrence, {"enter-inner"})
        inner_occurrence = occurrence_key("inner", inner1)
        harness.start(inner_occurrence)
        harness.complete(inner_occurrence, {"inner-body"})
        body = occurrence_key("body", inner1)
        harness.start(body)
        harness.complete(body, {"inner-exit"})
        latch = occurrence_key("latch", outer1)
        self.assertEqual(harness.state.invocation.scheduler.ready, (latch,))
        harness.start(latch)
        harness.complete(latch, {"outer-back"})
        outer2 = (LoopIteration(outer.id, 2),)
        self.assertEqual(
            harness.state.invocation.scheduler.ready,
            (occurrence_key("outer", outer2),),
        )

    def test_shared_header_can_switch_sibling_loop_scopes_then_exit(self) -> None:
        """Verify shared header can switch sibling loop scopes then exit."""
        harness = Harness(
            Workflow(
                "sibling-loops",
                nodes=[
                    Node(name, identity)
                    for name in ("start", "header", "a", "b", "finish")
                ],
                edges=[
                    Edge("start", "header", id="enter"),
                    Edge("header", "a", route, id="to-a"),
                    Edge("a", "header", id="back-a"),
                    Edge("header", "b", route, id="to-b"),
                    Edge("b", "header", id="back-b"),
                    Edge("header", "finish", route, id="exit"),
                ],
            ),
            entry="start",
        )
        a_region = next(item for item in harness.workflow.loop_regions if "a" in item.node_ids)
        b_region = next(item for item in harness.workflow.loop_regions if "b" in item.node_ids)

        harness.start("start@root")
        harness.complete("start@root", {"enter"})
        harness.start("header@root")
        harness.complete("header@root", {"to-a"})
        a1 = (LoopIteration(a_region.id, 1),)
        a = occurrence_key("a", a1)
        harness.start(a)
        harness.complete(a, {"back-a"})
        header_a2 = occurrence_key("header", (LoopIteration(a_region.id, 2),))
        harness.start(header_a2)
        harness.complete(header_a2, {"to-b"})

        b1 = (LoopIteration(b_region.id, 1),)
        b = occurrence_key("b", b1)
        self.assertEqual(harness.state.invocation.scheduler.ready, (b,))
        harness.start(b)
        harness.complete(b, {"back-b"})
        header_b2 = occurrence_key("header", (LoopIteration(b_region.id, 2),))
        harness.start(header_b2)
        harness.complete(header_b2, {"exit"})
        self.assertEqual(harness.state.invocation.scheduler.ready, ("finish@root",))

    def test_back_creates_new_scoped_occurrence_and_exit_returns_to_root(self) -> None:
        """Verify back creates new scoped occurrence and exit returns to root."""
        harness = Harness(simple_loop(), entry="start")
        region = harness.workflow.loop_regions[0]
        first = (LoopIteration(region.id, 1),)
        second = (LoopIteration(region.id, 2),)

        harness.start("start@root")
        harness.complete("start@root", {"enter"})
        header1 = occurrence_key("header", first)
        self.assertEqual(harness.state.invocation.scheduler.ready, (header1,))

        harness.start(header1)
        harness.complete(header1, {"body"})
        body1 = occurrence_key("body", first)
        harness.start(body1)
        harness.complete(body1, {"back"})

        header2 = occurrence_key("header", second)
        scheduler = harness.state.invocation.scheduler
        self.assertEqual(scheduler.ready, (header2,))
        self.assertEqual(scheduler.boundary_resolutions, {})
        self.assertEqual(
            scheduler.occurrences[header2].activations[0].edge_id,
            "back",
        )

        harness.start(header2)
        harness.complete(header2, {"body"})
        body2 = occurrence_key("body", second)
        harness.start(body2)
        harness.complete(body2, {"exit"})
        self.assertEqual(harness.state.invocation.scheduler.ready, ("finish@root",))

    def test_parallel_loop_waits_for_every_active_branch_before_back(self) -> None:
        """Verify parallel loop waits for every active branch before back."""
        value = Workflow(
            "parallel-loop",
            nodes=[
                Node(name, identity)
                for name in ("start", "header", "left", "right", "finish")
            ],
            edges=[
                Edge("start", "header", id="enter"),
                Edge("header", "left", id="to-left"),
                Edge("header", "right", id="to-right"),
                Edge("left", "join", id="left-join"),
                Edge("right", "join", id="right-join"),
                Edge("join", "header", route, id="back"),
                Edge("join", "finish", route, id="exit"),
            ],
        )
        value.nodes.insert(4, Node("join", identity, input_mapping=first_input))
        harness = Harness(value, entry="start")
        region = harness.workflow.loop_regions[0]
        first = (LoopIteration(region.id, 1),)
        harness.start("start@root")
        harness.complete("start@root", {"enter"})
        header = occurrence_key("header", first)
        harness.start(header)
        harness.complete(header, {"to-left", "to-right"})
        left = occurrence_key("left", first)
        right = occurrence_key("right", first)
        harness.start(left)
        harness.start(right)
        harness.complete(left, {"left-join"})
        self.assertNotIn(occurrence_key("join", first), harness.state.invocation.scheduler.ready)
        harness.complete(right, {"right-join"})
        join = occurrence_key("join", first)
        harness.start(join)
        harness.complete(join, {"back"})
        self.assertEqual(
            harness.state.invocation.scheduler.ready,
            (occurrence_key("header", (LoopIteration(region.id, 2),)),),
        )

    def test_selecting_back_and_exit_is_rejected_without_state_change(self) -> None:
        """Verify selecting back and exit is rejected without state change."""
        harness = Harness(simple_loop(), entry="start")
        region = harness.workflow.loop_regions[0]
        scope = (LoopIteration(region.id, 1),)
        harness.start("start@root")
        harness.complete("start@root", {"enter"})
        header = occurrence_key("header", scope)
        harness.start(header)
        harness.complete(header, {"body"})
        body = occurrence_key("body", scope)
        harness.start(body)
        before = harness.state.to_record()
        with self.assertRaisesRegex(LoopControlError, "LOOP_BACK_EXIT_CONFLICT"):
            harness.scheduler.complete(
                harness.workflow,
                harness.state,
                body,
                {"value": 1},
                selected_edge_ids={"back", "exit"},
            )
        self.assertEqual(harness.state.to_record(), before)

    def test_every_loop_event_prefix_replays_identically(self) -> None:
        """Verify every Loop Event prefix and serialized boundary replays identically."""
        harness = Harness(simple_loop(), entry="start")
        region = harness.workflow.loop_regions[0]
        first = (LoopIteration(region.id, 1),)
        harness.start("start@root")
        harness.complete("start@root", {"enter"})
        header = occurrence_key("header", first)
        harness.start(header)
        harness.complete(header, {"body"})
        body = occurrence_key("body", first)
        harness.start(body)
        harness.complete(body, {"exit"})
        events = harness.journal.events("session")
        for event in events:
            record = json.loads(json.dumps(event.to_record()))
            self.assertEqual(RuntimeEvent.from_record(record), event)
        for length in range(1, len(events) + 1):
            replayed = harness.journal.reducer.reduce(events[:length])
            self.assertEqual(
                replayed.to_record(),
                StateReducer().reduce(events[:length]).to_record(),
            )


if __name__ == "__main__":
    unittest.main()
