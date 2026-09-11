from __future__ import annotations

import unittest
from typing_extensions import TypedDict

from autoagent.core import (
    ConditionContext,
    ContextOperation,
    ContextPatch,
    Edge,
    InputMappingContext,
    Node,
    NodeExecutor,
    OutputBindingContext,
    RuntimeEvent,
    StateReducer,
    RuntimeTransitionError,
    Workflow,
)

from tests.test_phase3_scheduler import Harness


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


def map_join(context: InputMappingContext) -> Value:
    return {"value": sum(item["value"] for item in context.incoming.values())}  # type: ignore[index]


def bind_output(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(
        invocation=(ContextOperation.set("result.value", context.output["value"]),),  # type: ignore[index]
    )


def positive(context: ConditionContext) -> bool:
    return context.output is not None and context.output["value"] > 0  # type: ignore[index]


class ContextTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.executor = NodeExecutor()

    async def asyncTearDown(self) -> None:
        self.executor.close()

    async def test_input_mapping_binding_and_condition_receive_read_only_views(self) -> None:
        """Verify input mapping binding and condition receive read only views."""
        workflow = Workflow(
            "hooks",
            nodes=[
                Node("left", identity),
                Node("right", identity),
                Node("join", identity, input_mapping=map_join, output_binding=bind_output),
                Node("finish", identity),
            ],
            edges=[
                Edge("left", "join"),
                Edge("right", "join"),
                Edge("join", "finish", positive),
            ],
        )
        harness = Harness(workflow, entry="left")
        node = harness.workflow.node("join")
        mapped = await self.executor.map_input(
            node,
            invocation_input={"value": 0},
            incoming={"left": {"value": 2}, "right": {"value": 3}},
            invocation_context={"x": 1},
            session_context={"y": 2},
        )
        self.assertEqual(mapped, {"value": 5})
        patch = await self.executor.bind_output(
            node,
            mapped,
            invocation_context={},
            session_context={},
        )
        self.assertEqual(patch.invocation[0].path, ("result", "value"))
        selected = await self.executor.select_edges(
            harness.workflow.outgoing("join"),
            source_status="complete",
            source_node_id="join",
            output=mapped,
            error=None,
            invocation_context={},
            session_context={},
        )
        self.assertEqual(selected, {"join->finish"})

    async def test_node_completion_atomically_commits_patch_output_and_successors(self) -> None:
        """Verify node completion atomically commits patch output and successors."""
        harness = Harness(
            Workflow(
                "atomic",
                nodes=[Node("start", identity), Node("finish", identity)],
                edges=[Edge("start", "finish")],
            ),
            entry="start",
        )
        harness.start("start@root")
        payload = harness.scheduler.complete(
            harness.workflow,
            harness.state,
            "start@root",
            {"value": 7},
            selected_edge_ids={"start->finish"},
        )
        payload = type(payload)(
            payload.occurrence_id,
            payload.output,
            payload.delta,
            ContextPatch(
                invocation=(
                    ContextOperation.set("result", {"value": 7}),
                    ContextOperation.set(
                        ("literal.dot", "slash/key", "tilde~key"),
                        9,
                    ),
                ),
                session=(ContextOperation.set("last", 7),),
            ),
        )
        harness.emit(payload)
        state = harness.state
        self.assertEqual(state.invocation.context["result"], {"value": 7})
        self.assertEqual(
            state.invocation.context["literal.dot"]["slash/key"]["tilde~key"],
            9,
        )
        self.assertEqual(state.session.context["last"], 7)
        self.assertEqual(state.invocation.scheduler.ready, ("finish@root",))
        events = harness.journal.events("session")
        restored = RuntimeEvent.from_record(events[-1].to_record())
        self.assertEqual(restored, events[-1])
        self.assertEqual(StateReducer().reduce((*events[:-1], restored)), state)

    async def test_parallel_overlapping_patch_conflicts_but_disjoint_patch_succeeds(self) -> None:
        """Verify parallel overlapping patch conflicts but disjoint patch succeeds."""
        harness = Harness(
            Workflow(
                "parallel-patch",
                nodes=[Node(name, identity) for name in ("start", "left", "right", "disjoint")],
                edges=[Edge("start", "left"), Edge("start", "right"), Edge("start", "disjoint")],
            ),
            entry="start",
        )
        harness.start("start@root")
        harness.complete("start@root", {"start->left", "start->right", "start->disjoint"})
        harness.start("left@root")
        harness.start("right@root")
        harness.start("disjoint@root")

        left = harness.scheduler.complete(
            harness.workflow, harness.state, "left@root", {"value": 1}
        )
        harness.emit(
            type(left)(
                left.occurrence_id,
                left.output,
                left.delta,
                ContextPatch(invocation=(ContextOperation.set("shared", 1),)),
            )
        )
        right = harness.scheduler.complete(
            harness.workflow, harness.state, "right@root", {"value": 2}
        )
        conflict = type(right)(
            right.occurrence_id,
            right.output,
            right.delta,
            ContextPatch(invocation=(ContextOperation.set("shared.child", 2),)),
        )
        before = harness.state.to_record()
        with self.assertRaisesRegex(RuntimeTransitionError, "CONTEXT_WRITE_CONFLICT"):
            harness.emit(conflict)
        self.assertEqual(harness.state.session.context, before["session"]["context"])
        self.assertEqual(harness.state.invocation.context, before["invocation"]["context"])
        self.assertEqual(harness.state.invocation.scheduler.occurrences["right@root"].status, "running")

        planned = harness.scheduler.complete(harness.workflow, harness.state, "disjoint@root", {"value": 2})
        disjoint = type(planned)(
            planned.occurrence_id,
            planned.output,
            planned.delta,
            ContextPatch(invocation=(ContextOperation.set("other", 2),)),
        )
        harness.emit(disjoint)
        self.assertEqual(harness.state.invocation.context["other"], 2)


if __name__ == "__main__":
    unittest.main()
