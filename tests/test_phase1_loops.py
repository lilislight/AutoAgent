from __future__ import annotations

import unittest
from typing_extensions import TypedDict

from autoagent.core import (
    ConditionContext,
    Edge,
    InputMappingContext,
    Node,
    Workflow,
    WorkflowCompileError,
    WorkflowCompiler,
)


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


def yes(context: ConditionContext) -> bool:
    return True


def merge(context: InputMappingContext) -> Value:
    return next(iter(context.incoming.values()))  # type: ignore[return-value]


class LoopCompilerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.compiler = WorkflowCompiler()

    def test_natural_loop_compiles_one_region(self) -> None:
        """Verify natural loop compiles one region."""
        workflow = Workflow(
            "loop",
            nodes=[Node(name, identity) for name in ("start", "header", "body", "finish")],
            edges=[
                Edge("start", "header"),
                Edge("header", "body"),
                Edge("body", "header", yes, id="back"),
                Edge("body", "finish", yes, id="exit"),
            ],
        )
        ir = self.compiler.compile_or_raise(workflow)
        self.assertEqual(len(ir.loop_regions), 1)
        self.assertEqual(ir.loop_regions[0].header_node_id, "header")
        self.assertEqual(ir.loop_regions[0].back_edge_id, "back")

    def test_nested_loops_compile_with_parent_relationship(self) -> None:
        """Verify nested loops compile with parent relationship."""
        workflow = Workflow(
            "nested",
            nodes=[
                Node(name, identity)
                for name in ("start", "outer", "inner", "body", "latch", "finish")
            ],
            edges=[
                Edge("start", "outer"),
                Edge("outer", "inner"),
                Edge("inner", "body"),
                Edge("body", "inner", yes, id="inner-back"),
                Edge("body", "latch", yes, id="inner-exit"),
                Edge("latch", "outer", yes, id="outer-back"),
                Edge("latch", "finish", yes, id="outer-exit"),
            ],
        )
        ir = self.compiler.compile_or_raise(workflow)
        self.assertEqual(len(ir.loop_regions), 2)
        inner = next(loop for loop in ir.loop_regions if loop.header_node_id == "inner")
        outer = next(loop for loop in ir.loop_regions if loop.header_node_id == "outer")
        self.assertEqual(inner.parent_loop_region_id, outer.id)

    def test_self_loop_compiles_one_node_region(self) -> None:
        """Verify self loop compiles one node region."""
        ir = self.compiler.compile_or_raise(
            Workflow(
                "self",
                nodes=[Node(name, identity) for name in ("start", "header", "finish")],
                edges=[
                    Edge("start", "header"),
                    Edge("header", "header", yes, id="back"),
                    Edge("header", "finish", yes, id="exit"),
                ],
            )
        )
        self.assertEqual(ir.loop_regions[0].node_ids, ("header",))

    def test_shared_header_sibling_loops_are_supported(self) -> None:
        """Verify shared header sibling loops are supported."""
        ir = self.compiler.compile_or_raise(
            Workflow(
                "siblings",
                nodes=[Node(name, identity) for name in ("start", "header", "a", "b", "finish")],
                edges=[
                    Edge("start", "header"),
                    Edge("header", "a", yes),
                    Edge("a", "header", id="back-a"),
                    Edge("header", "b", yes),
                    Edge("b", "header", id="back-b"),
                    Edge("header", "finish", yes),
                ],
            )
        )
        self.assertEqual(len(ir.loop_regions), 2)
        self.assertEqual(
            {frozenset(loop.node_ids) for loop in ir.loop_regions},
            {frozenset(("header", "a")), frozenset(("header", "b"))},
        )

    def test_same_header_strictly_nested_loops_are_supported(self) -> None:
        """Verify same header strictly nested loops are supported."""
        ir = self.compiler.compile_or_raise(
            Workflow(
                "same-header-nested",
                nodes=[Node(name, identity) for name in ("start", "header", "inner", "outer", "finish")],
                edges=[
                    Edge("start", "header"),
                    Edge("header", "inner"),
                    Edge("inner", "header", yes, id="inner-back"),
                    Edge("inner", "outer", yes, id="inner-exit"),
                    Edge("outer", "header", yes, id="outer-back"),
                    Edge("outer", "finish", yes, id="outer-exit"),
                ],
            )
        )
        regions = sorted(ir.loop_regions, key=lambda item: len(item.node_ids))
        self.assertEqual(len(regions), 2)
        self.assertEqual(regions[0].parent_loop_region_id, regions[1].id)

    def test_two_back_edges_for_the_same_region_are_rejected(self) -> None:
        """Verify two back edges for the same region are rejected."""
        with self.assertRaisesRegex(WorkflowCompileError, "LOOP_MULTIPLE_BACK_EDGES"):
            self.compiler.compile_or_raise(
                Workflow(
                    "two-backs",
                    nodes=[Node(name, identity) for name in ("start", "header", "left", "right", "finish")],
                    edges=[
                        Edge("start", "header"),
                        Edge("header", "left"),
                        Edge("left", "right"),
                        Edge("right", "left", yes),
                        Edge("left", "header", yes, id="left-back"),
                        Edge("right", "header", yes, id="right-back"),
                        Edge("right", "finish", yes),
                    ],
                )
            )

    def test_parallel_loop_requires_explicit_join_but_has_one_back_edge(self) -> None:
        """Verify parallel loop requires explicit join but has one back edge."""
        ir = self.compiler.compile_or_raise(
            Workflow(
                "parallel",
                nodes=[
                    Node("start", identity),
                    Node("header", identity),
                    Node("left", identity),
                    Node("right", identity),
                    Node("join", identity, input_mapping=merge),
                    Node("finish", identity),
                ],
                edges=[
                    Edge("start", "header"),
                    Edge("header", "left"),
                    Edge("header", "right"),
                    Edge("left", "join"),
                    Edge("right", "join"),
                    Edge("join", "header", yes, id="back"),
                    Edge("join", "finish", yes, id="exit"),
                ],
            )
        )
        self.assertEqual(len(ir.loop_regions), 1)
        self.assertEqual(set(ir.loop_regions[0].node_ids), {"header", "left", "right", "join"})

    def test_loop_without_exit_is_rejected(self) -> None:
        """Verify loop without exit is rejected."""
        with self.assertRaisesRegex(WorkflowCompileError, "LOOP_WITHOUT_EXIT|WORKFLOW_WITHOUT_EXIT"):
            self.compiler.compile_or_raise(
                Workflow(
                    "no-exit",
                    nodes=[Node("start", identity), Node("header", identity)],
                    edges=[Edge("start", "header"), Edge("header", "header", yes)],
                )
            )

    def test_non_header_entry_is_rejected(self) -> None:
        """Verify non header entry is rejected."""
        with self.assertRaisesRegex(WorkflowCompileError, "LOOP_NON_HEADER_ENTRY"):
            self.compiler.compile_or_raise(
                Workflow(
                    "bad-entry",
                    nodes=[Node(name, identity) for name in ("start", "other", "header", "body", "finish")],
                    edges=[
                        Edge("start", "header"),
                        Edge("other", "body"),
                        Edge("header", "body"),
                        Edge("body", "header", yes, id="back"),
                        Edge("body", "finish", yes, id="exit"),
                    ],
                )
            )

    def test_irreducible_cycle_is_rejected(self) -> None:
        """Verify irreducible cycle is rejected."""
        with self.assertRaisesRegex(
            WorkflowCompileError,
            "LOOP_IRREDUCIBLE|LOOP_REGION_OVERLAP|LOOP_NON_HEADER_ENTRY",
        ):
            self.compiler.compile_or_raise(
                Workflow(
                    "irreducible",
                    nodes=[Node(name, identity) for name in ("ea", "eb", "a", "b", "c", "finish")],
                    edges=[
                        Edge("ea", "a"),
                        Edge("eb", "b"),
                        Edge("a", "c"),
                        Edge("b", "c"),
                        Edge("c", "a", yes),
                        Edge("c", "b", yes),
                        Edge("c", "finish", yes),
                    ],
                )
            )

    def test_unconditional_continue_and_exit_are_rejected(self) -> None:
        """Verify unconditional continue and exit are rejected."""
        with self.assertRaisesRegex(WorkflowCompileError, "LOOP_STATIC_CONTROL_CONFLICT"):
            self.compiler.compile_or_raise(
                Workflow(
                    "conflict",
                    nodes=[Node(name, identity) for name in ("start", "header", "body", "finish")],
                    edges=[
                        Edge("start", "header"),
                        Edge("header", "body"),
                        Edge("body", "header", id="back"),
                        Edge("body", "finish", id="exit"),
                    ],
                )
            )

    def test_complete_continue_and_error_exit_are_status_exclusive(self) -> None:
        """Verify complete and error routes do not statically conflict in a Loop."""

        ir = self.compiler.compile_or_raise(
            Workflow(
                "status-exclusive-loop",
                nodes=[
                    Node("start", identity),
                    Node("header", identity),
                    Node("body", identity),
                    Node("handled", identity, input_mapping=merge),
                ],
                edges=[
                    Edge("start", "header"),
                    Edge("header", "body"),
                    Edge("body", "header", id="back"),
                    Edge("body", "handled", on="error", id="error-exit"),
                ],
            )
        )
        self.assertEqual(ir.loop_regions[0].back_edge_id, "back")
        self.assertEqual(ir.loop_regions[0].exit_edge_ids, ("error-exit",))

    def test_long_acyclic_graph_does_not_use_recursive_dfs(self) -> None:
        """Verify long acyclic graph does not use recursive dfs."""
        size = 1500
        workflow = Workflow(
            "long",
            nodes=[Node(f"n{index}", identity) for index in range(size)],
            edges=[Edge(f"n{index}", f"n{index + 1}") for index in range(size - 1)],
        )
        ir = self.compiler.compile_or_raise(workflow)
        self.assertEqual(len(ir.nodes), size)
        self.assertEqual(ir.loop_regions, ())


if __name__ == "__main__":
    unittest.main()
