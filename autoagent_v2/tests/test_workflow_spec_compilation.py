"""Compiler conformance tests derived only from ``workflow.md``."""

from __future__ import annotations

import unittest

from workflow_spec_support import (
    Edge,
    Node,
    always,
    assert_compile_error,
    compile_workflow,
    conditional_false,
    conditional_true,
    node,
    passthrough,
    workflow,
)


class WorkflowStructureCompilationTests(unittest.TestCase):
    def test_long_acyclic_workflow_compiles_without_recursion_failure(self) -> None:
        node_ids = [f"node_{index}" for index in range(1_500)]
        definition = workflow(
            "long_dag",
            node_ids,
            [
                Edge(node_ids[index], node_ids[index + 1], id=f"edge_{index}")
                for index in range(len(node_ids) - 1)
            ],
        )
        ir = compile_workflow(definition)
        self.assertEqual(len(ir.nodes), len(node_ids))
        self.assertEqual(ir.loop_regions, ())

    def test_empty_workflow_has_no_entry(self) -> None:
        assert_compile_error(
            self,
            workflow("empty", [], []),
            "WORKFLOW_NO_ENTRY",
        )

    def test_cycle_with_an_exit_but_no_structural_entry_is_rejected(self) -> None:
        definition = workflow(
            "no_entry",
            ["a", "b", "outside"],
            [
                Edge("a", "b", id="a_b"),
                Edge("b", "a", condition=conditional_true, id="b_a"),
                Edge("b", "outside", condition=conditional_false, id="b_out"),
            ],
        )
        assert_compile_error(self, definition, "WORKFLOW_NO_ENTRY")

    def test_reachable_cycle_without_any_structural_workflow_exit_is_rejected(self) -> None:
        definition = workflow(
            "no_exit",
            ["entry", "a", "b"],
            [
                Edge("entry", "a", id="entry_a"),
                Edge("a", "b", id="a_b"),
                Edge("b", "a", id="b_a"),
            ],
        )
        assert_compile_error(self, definition, "WORKFLOW_NO_EXIT")

    def test_explicit_entry_is_only_an_assertion_and_must_have_no_incoming_edge(self) -> None:
        definition = workflow(
            "invalid_explicit_entry",
            [],
            [Edge("a", "b", id="a_b")],
            nodes=[node("a"), node("b", entry=True)],
        )
        assert_compile_error(self, definition)

    def test_all_structural_entries_remain_entries_when_one_is_explicit(self) -> None:
        definition = workflow(
            "multiple_entries",
            [],
            [],
            nodes=[node("a", entry=True), node("b")],
        )
        ir = compile_workflow(definition)
        self.assertEqual(set(ir.entry_node_ids), {"a", "b"})
        self.assertEqual(set(ir.exit_node_ids), {"a", "b"})

    def test_disconnected_cycle_is_reported_as_unreachable(self) -> None:
        definition = workflow(
            "unreachable_cycle",
            ["entry", "outside", "a", "b", "cycle_exit"],
            [
                Edge("entry", "outside", id="entry_out"),
                Edge("a", "b", id="a_b"),
                Edge("b", "a", condition=conditional_true, id="b_a"),
                Edge("b", "cycle_exit", condition=conditional_false, id="b_exit"),
            ],
        )
        assert_compile_error(self, definition, "WORKFLOW_UNREACHABLE_NODE")

    def test_duplicate_node_id_is_rejected(self) -> None:
        definition = workflow(
            "duplicate_node",
            [],
            [],
            nodes=[node("same"), node("same")],
        )
        assert_compile_error(self, definition)

    def test_empty_node_id_is_rejected(self) -> None:
        assert_compile_error(self, workflow("empty_node", [""], []))

    def test_edge_with_unknown_target_is_rejected(self) -> None:
        definition = workflow(
            "unknown_target",
            ["entry"],
            [Edge("entry", "missing", id="bad")],
        )
        assert_compile_error(self, definition)


class LoopRegionCompilationTests(unittest.TestCase):
    def test_simple_reducible_loop_is_classified(self) -> None:
        definition = workflow(
            "simple_loop",
            ["entry", "header", "body", "outside"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "body", id="header_body"),
                Edge("body", "header", condition=conditional_true, id="back"),
                Edge("body", "outside", condition=conditional_false, id="exit"),
            ],
        )
        ir = compile_workflow(definition)
        self.assertEqual(len(ir.loop_regions), 1)
        region = ir.loop_regions[0]
        self.assertEqual(region.header_node_id, "header")
        self.assertEqual(set(region.node_ids), {"header", "body"})
        self.assertEqual(region.back_edge_ids, ("back",))
        self.assertEqual(region.exit_edge_ids, ("exit",))

    def test_single_node_self_loop_is_classified(self) -> None:
        definition = workflow(
            "self_loop",
            ["entry", "header", "outside"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "header", condition=conditional_true, id="self_back"),
                Edge("header", "outside", condition=conditional_false, id="exit"),
            ],
        )
        ir = compile_workflow(definition)
        self.assertEqual(len(ir.loop_regions), 1)
        region = ir.loop_regions[0]
        self.assertEqual(region.node_ids, ("header",))
        self.assertEqual(region.back_edge_ids, ("self_back",))

    def test_external_entry_into_loop_body_is_rejected(self) -> None:
        definition = workflow(
            "non_header_entry",
            ["entry", "other_entry", "header", "body", "outside"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("other_entry", "body", id="other_body"),
                Edge("header", "body", id="header_body"),
                Edge("body", "header", condition=conditional_true, id="back"),
                Edge("body", "outside", condition=conditional_false, id="exit"),
            ],
        )
        assert_compile_error(self, definition, "LOOP_NON_HEADER_ENTRY")

    def test_irreducible_multi_entry_cycle_is_rejected(self) -> None:
        definition = workflow(
            "irreducible",
            ["entry_a", "entry_b", "a", "b", "c", "outside"],
            [
                Edge("entry_a", "a", id="ea_a"),
                Edge("entry_b", "b", id="eb_b"),
                Edge("a", "c", id="a_c"),
                Edge("b", "c", id="b_c"),
                Edge("c", "a", condition=conditional_true, id="c_a"),
                Edge("c", "b", condition=conditional_false, id="c_b"),
                Edge("c", "outside", condition=conditional_false, id="c_out"),
            ],
        )
        assert_compile_error(
            self,
            definition,
            {"LOOP_IRREDUCIBLE", "LOOP_REGION_OVERLAP", "LOOP_NON_HEADER_ENTRY"},
        )

    def test_distinct_header_nested_loops_form_parent_child_regions(self) -> None:
        definition = workflow(
            "nested_distinct_headers",
            ["entry", "outer_header", "inner_header", "inner_body", "outer_latch", "outside"],
            [
                Edge("entry", "outer_header", id="entry_outer"),
                Edge("outer_header", "inner_header", id="outer_inner"),
                Edge("inner_header", "inner_body", id="inner_body"),
                Edge("inner_body", "inner_header", condition=conditional_true, id="inner_back"),
                Edge("inner_body", "outer_latch", condition=conditional_false, id="inner_exit"),
                Edge("outer_latch", "outer_header", condition=conditional_true, id="outer_back"),
                Edge("outer_latch", "outside", condition=conditional_false, id="outer_exit"),
            ],
        )
        ir = compile_workflow(definition)
        self.assertEqual(len(ir.loop_regions), 2)
        inner = next(r for r in ir.loop_regions if set(r.node_ids) == {"inner_header", "inner_body"})
        outer = next(
            r
            for r in ir.loop_regions
            if set(r.node_ids)
            == {"outer_header", "inner_header", "inner_body", "outer_latch"}
        )
        self.assertEqual(inner.parent_loop_region_id, outer.id)
        self.assertIsNone(outer.parent_loop_region_id)

    def test_same_header_strictly_nested_loops_are_distinct(self) -> None:
        definition = workflow(
            "nested_shared_header",
            ["entry", "header", "inner_latch", "outer_latch", "outside"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "inner_latch", id="header_inner"),
                Edge("inner_latch", "header", condition=conditional_true, id="inner_back"),
                Edge("inner_latch", "outer_latch", condition=conditional_false, id="inner_exit"),
                Edge("outer_latch", "header", condition=conditional_true, id="outer_back"),
                Edge("outer_latch", "outside", condition=conditional_false, id="outer_exit"),
            ],
        )
        ir = compile_workflow(definition)
        self.assertEqual(len(ir.loop_regions), 2)
        regions = sorted(ir.loop_regions, key=lambda region: len(region.node_ids))
        self.assertEqual(set(regions[0].node_ids), {"header", "inner_latch"})
        self.assertEqual(
            set(regions[1].node_ids),
            {"header", "inner_latch", "outer_latch"},
        )
        self.assertEqual(regions[0].parent_loop_region_id, regions[1].id)

    def test_shared_header_independent_back_edges_are_sibling_loops(self) -> None:
        definition = workflow(
            "sibling_loops",
            ["entry", "header", "a", "b", "outside"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "a", condition=conditional_true, id="enter_a"),
                Edge("a", "header", id="back_a"),
                Edge("header", "b", condition=conditional_false, id="enter_b"),
                Edge("b", "header", id="back_b"),
                Edge("header", "outside", condition=conditional_false, id="exit"),
            ],
        )
        ir = compile_workflow(definition)
        self.assertEqual(len(ir.loop_regions), 2)
        self.assertEqual(
            {frozenset(region.node_ids) for region in ir.loop_regions},
            {frozenset({"header", "a"}), frozenset({"header", "b"})},
        )
        self.assertEqual(
            {region.back_edge_ids for region in ir.loop_regions},
            {("back_a",), ("back_b",)},
        )

    def test_sibling_loop_body_cross_edge_is_rejected(self) -> None:
        definition = workflow(
            "sibling_cross_edge",
            ["entry", "header", "a", "b", "outside"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "a", condition=conditional_true, id="enter_a"),
                Edge("a", "header", condition=conditional_true, id="back_a"),
                Edge("header", "b", condition=conditional_false, id="enter_b"),
                Edge("b", "header", condition=conditional_true, id="back_b"),
                Edge("a", "b", condition=conditional_false, id="a_b"),
                Edge("header", "outside", condition=conditional_false, id="exit"),
            ],
        )
        assert_compile_error(
            self,
            definition,
            {"LOOP_IRREDUCIBLE", "LOOP_REGION_OVERLAP", "LOOP_NON_HEADER_ENTRY"},
        )

    def test_explicit_join_and_one_back_edge_define_one_parallel_loop(self) -> None:
        definition = workflow(
            "parallel_loop",
            ["entry", "header", "a", "b", "join", "outside"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "a", id="header_a"),
                Edge("header", "b", id="header_b"),
                Edge("a", "join", id="a_join"),
                Edge("b", "join", id="b_join"),
                Edge("join", "header", condition=conditional_true, id="back"),
                Edge("join", "outside", condition=conditional_false, id="exit"),
            ],
        )
        ir = compile_workflow(definition)
        self.assertEqual(len(ir.loop_regions), 1)
        region = ir.loop_regions[0]
        self.assertEqual(
            set(region.node_ids),
            {"header", "a", "b", "join"},
        )
        self.assertEqual(region.back_edge_ids, ("back",))

    def test_loop_without_its_own_structural_exit_is_rejected(self) -> None:
        definition = workflow(
            "loop_without_exit",
            ["entry", "header", "body", "independent_exit"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "body", id="header_body"),
                Edge("body", "header", id="back"),
            ],
            nodes=[node("entry"), node("header"), node("body"), node("independent_exit")],
        )
        assert_compile_error(self, definition, "LOOP_WITHOUT_EXIT")

    def test_sibling_loops_with_no_edge_leaving_the_scc_are_rejected(self) -> None:
        definition = workflow(
            "cyclic_region_without_exit",
            ["entry", "header", "a", "b", "independent_exit"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "a", condition=conditional_true, id="enter_a"),
                Edge("a", "header", id="back_a"),
                Edge("header", "b", condition=conditional_false, id="enter_b"),
                Edge("b", "header", id="back_b"),
            ],
        )
        assert_compile_error(self, definition, "CYCLIC_REGION_WITHOUT_EXIT")


class StaticLoopControlCompilationTests(unittest.TestCase):
    def test_unconditional_back_and_exit_conflict_is_rejected(self) -> None:
        definition = workflow(
            "static_back_exit_conflict",
            ["entry", "header", "body", "outside"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "body", id="header_body"),
                Edge("body", "header", id="back"),
                Edge("body", "outside", id="exit"),
            ],
        )
        assert_compile_error(self, definition, "LOOP_STATIC_CONTROL_CONFLICT")

    def test_unconditional_entries_to_sibling_bodies_are_rejected(self) -> None:
        definition = workflow(
            "static_sibling_conflict",
            ["entry", "header", "a", "b", "outside"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "a", id="enter_a"),
                Edge("a", "header", id="back_a"),
                Edge("header", "b", id="enter_b"),
                Edge("b", "header", id="back_b"),
                Edge("header", "outside", condition=conditional_false, id="exit"),
            ],
        )
        assert_compile_error(self, definition, "LOOP_STATIC_CONTROL_CONFLICT")

    def test_conditional_potential_back_exit_conflict_compiles(self) -> None:
        definition = workflow(
            "runtime_back_exit_conflict",
            ["entry", "header", "body", "outside"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "body", id="header_body"),
                Edge("body", "header", condition=conditional_true, id="back"),
                Edge("body", "outside", condition=conditional_true, id="exit"),
            ],
        )
        ir = compile_workflow(definition)
        self.assertEqual(len(ir.loop_regions), 1)

    def test_conditional_multiple_exits_with_same_scope_depth_compile(self) -> None:
        definition = workflow(
            "compatible_exits",
            ["entry", "header", "body", "outside_a", "outside_b"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "body", id="header_body"),
                Edge("body", "header", condition=conditional_false, id="back"),
                Edge("body", "outside_a", condition=conditional_true, id="exit_a"),
                Edge("body", "outside_b", condition=conditional_true, id="exit_b"),
            ],
        )
        ir = compile_workflow(definition)
        self.assertEqual(set(ir.loop_regions[0].exit_edge_ids), {"exit_a", "exit_b"})

    def test_unconditional_nested_exits_with_different_depths_are_rejected(self) -> None:
        definition = workflow(
            "static_incompatible_exit_depth",
            ["entry", "header", "inner", "outer_latch", "outside"],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "inner", id="header_inner"),
                Edge("inner", "header", condition=conditional_false, id="inner_back"),
                Edge("inner", "outer_latch", id="inner_only_exit"),
                Edge("inner", "outside", id="all_scopes_exit"),
                Edge("outer_latch", "header", condition=conditional_false, id="outer_back"),
                Edge("outer_latch", "outside", condition=conditional_true, id="outer_exit"),
            ],
        )
        assert_compile_error(self, definition, "LOOP_STATIC_CONTROL_CONFLICT")


if __name__ == "__main__":
    unittest.main()
