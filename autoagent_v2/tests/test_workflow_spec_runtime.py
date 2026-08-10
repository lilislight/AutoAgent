"""Runtime graph and Loop conformance tests derived from ``workflow.md``."""

from __future__ import annotations

import threading
import unittest

from workflow_spec_support import (
    AutoAgentApp,
    ContextPatch,
    Edge,
    InputMappingContext,
    EdgeConditionContext,
    OutputBindingContext,
    InvocationState,
    Payload,
    always,
    assert_completed,
    assert_runtime_error,
    conditional_false,
    conditional_true,
    never,
    node,
    run_workflow,
    traced_operator,
    workflow,
)


def increment_count(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(
        invocation={"count": int(context.invocation_context.get("count", 0)) + 1}
    )


def count_less_than_three(context: EdgeConditionContext) -> bool:
    return int(context.invocation_context.get("count", 0)) < 3


def count_at_least_three(context: EdgeConditionContext) -> bool:
    return int(context.invocation_context.get("count", 0)) >= 3


def increment_header_count(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(
        invocation={
            "header_count": int(context.invocation_context.get("header_count", 0)) + 1
        }
    )


def header_count_is_one(context: EdgeConditionContext) -> bool:
    return int(context.invocation_context.get("header_count", 0)) == 1


def header_count_is_two(context: EdgeConditionContext) -> bool:
    return int(context.invocation_context.get("header_count", 0)) == 2


def header_count_is_three(context: EdgeConditionContext) -> bool:
    return int(context.invocation_context.get("header_count", 0)) >= 3


def increment_inner_count(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(
        invocation={
            "inner_count": int(context.invocation_context.get("inner_count", 0)) + 1
        }
    )


def inner_count_less_than_two(context: EdgeConditionContext) -> bool:
    return int(context.invocation_context.get("inner_count", 0)) < 2


def inner_count_at_least_two(context: EdgeConditionContext) -> bool:
    return int(context.invocation_context.get("inner_count", 0)) >= 2


def advance_outer_and_reset_inner(
    context: OutputBindingContext,
) -> ContextPatch:
    return ContextPatch(
        invocation={
            "outer_count": int(context.invocation_context.get("outer_count", 0)) + 1,
            "inner_count": 0,
        }
    )


def outer_count_less_than_two(context: EdgeConditionContext) -> bool:
    return int(context.invocation_context.get("outer_count", 0)) < 2


def outer_count_at_least_two(context: EdgeConditionContext) -> bool:
    return int(context.invocation_context.get("outer_count", 0)) >= 2


async def async_selected(context: EdgeConditionContext) -> bool:
    return bool(context.invocation_input.get("route", 1))


class OrdinaryGraphRuntimeTests(unittest.TestCase):
    def test_multiple_structural_entries_start_independently(self) -> None:
        trace: list[str] = []
        a_started = threading.Event()
        b_started = threading.Event()
        release = threading.Event()
        definition = workflow(
            "parallel_entries_runtime",
            [],
            [],
            nodes=[
                node("a", traced_operator("a", trace, started=a_started, release=release)),
                node("b", traced_operator("b", trace, started=b_started, release=release)),
            ],
        )
        app = AutoAgentApp(max_executor_concurrency=4)
        try:
            app.register_workflow(definition)
            invocation = app.submit_invoke(definition.id, {"route": 1})
            self.assertTrue(a_started.wait(1.5), "first Entry did not start")
            self.assertTrue(b_started.wait(1.5), "second Entry was serialized")
            release.set()
            invocation.wait(timeout=3.0)
            assert_completed(self, invocation.snapshot())
        finally:
            release.set()
            app.close()

    def test_all_matching_dag_edges_fan_out(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "dag_fanout",
            [],
            [
                Edge("root", "a", id="root_a"),
                Edge("root", "b", id="root_b"),
            ],
            nodes=[
                node("root", traced_operator("root", trace)),
                node("a", traced_operator("a", trace)),
                node("b", traced_operator("b", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_completed(self, snapshot)
        self.assertEqual(trace.count("root"), 1)
        self.assertEqual(trace.count("a"), 1)
        self.assertEqual(trace.count("b"), 1)

    def test_awaitable_edge_condition_selects_normally(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "async_condition",
            [],
            [Edge("root", "outside", condition=async_selected, id="selected")],
            nodes=[
                node("root", traced_operator("root", trace)),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_completed(self, snapshot)
        self.assertEqual(trace, ["root", "outside"])

    def test_complete_fan_in_runs_once_for_selected_and_skipped_inputs(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "mixed_fan_in",
            [],
            [
                Edge("root", "a", condition=always, id="root_a"),
                Edge("root", "b", condition=never, id="root_b"),
                Edge("a", "join", id="a_join"),
                Edge("b", "join", id="b_join"),
            ],
            nodes=[
                node("root", traced_operator("root", trace)),
                node("a", traced_operator("a", trace)),
                node("b", traced_operator("b", trace)),
                node("join", traced_operator("join", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_completed(self, snapshot)
        self.assertEqual(trace.count("a"), 1)
        self.assertEqual(trace.count("b"), 0)
        self.assertEqual(trace.count("join"), 1)

    def test_all_skipped_fan_in_skips_target_and_propagates_skip(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "all_skipped_fan_in",
            [],
            [
                Edge("root", "a", condition=never, id="root_a"),
                Edge("root", "b", condition=never, id="root_b"),
                Edge("a", "join", id="a_join"),
                Edge("b", "join", id="b_join"),
                Edge("join", "outside", id="join_outside"),
            ],
            nodes=[
                node("root", traced_operator("root", trace)),
                node("a", traced_operator("a", trace)),
                node("b", traced_operator("b", trace)),
                node("join", traced_operator("join", trace)),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_completed(self, snapshot)
        self.assertEqual(trace, ["root"])

    def test_join_is_scheduled_once_for_two_selected_inputs(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "join_once",
            [],
            [
                Edge("root", "a", id="root_a"),
                Edge("root", "b", id="root_b"),
                Edge("a", "join", id="a_join"),
                Edge("b", "join", id="b_join"),
            ],
            nodes=[
                node("root", traced_operator("root", trace)),
                node("a", traced_operator("a", trace)),
                node("b", traced_operator("b", trace)),
                node("join", traced_operator("join", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_completed(self, snapshot)
        self.assertEqual(trace.count("join"), 1)


class LoopDecisionRuntimeTests(unittest.TestCase):
    def test_back_then_exit_runs_three_scoped_iterations(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "three_iterations",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "header", condition=count_less_than_three, id="back"),
                Edge("header", "outside", condition=count_at_least_three, id="exit"),
            ],
            nodes=[
                node(
                    "entry",
                    traced_operator("entry", trace),
                ),
                node(
                    "header",
                    traced_operator("header", trace),
                    output_binding=increment_count,
                ),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_completed(self, snapshot)
        self.assertEqual(trace.count("header"), 3)
        self.assertEqual(trace.count("outside"), 1)

    def test_selected_back_and_exit_fail_without_scheduling_exit_target(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "runtime_back_exit_conflict",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "body", id="header_body"),
                Edge("body", "header", condition=conditional_true, id="back"),
                Edge("body", "outside", condition=conditional_true, id="exit"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node("header", traced_operator("header", trace)),
                node("body", traced_operator("body", trace)),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_runtime_error(self, snapshot, "LOOP_BACK_EXIT_CONFLICT")
        self.assertNotIn("outside", trace)
        self.assertEqual(trace.count("header"), 1)

    def test_stable_boundary_with_no_back_or_exit_fails_no_route(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "runtime_no_route",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "body", id="header_body"),
                Edge("body", "header", condition=conditional_false, id="back"),
                Edge("body", "outside", condition=conditional_false, id="exit"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node("header", traced_operator("header", trace)),
                node("body", traced_operator("body", trace)),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_runtime_error(self, snapshot, "LOOP_NO_ROUTE")
        self.assertNotIn("outside", trace)

    def test_multiple_compatible_exits_fan_out(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "compatible_runtime_exits",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "body", id="header_body"),
                Edge("body", "header", condition=conditional_false, id="back"),
                Edge("body", "outside_a", condition=conditional_true, id="exit_a"),
                Edge("body", "outside_b", condition=conditional_true, id="exit_b"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node("header", traced_operator("header", trace)),
                node("body", traced_operator("body", trace)),
                node("outside_a", traced_operator("outside_a", trace)),
                node("outside_b", traced_operator("outside_b", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_completed(self, snapshot)
        self.assertEqual(trace.count("outside_a"), 1)
        self.assertEqual(trace.count("outside_b"), 1)

    def test_incompatible_nested_exit_depths_fail_atomically(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "incompatible_exit_depth",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "inner", id="header_inner"),
                Edge("inner", "header", condition=conditional_false, id="inner_back"),
                Edge("inner", "outer_latch", condition=conditional_true, id="inner_only_exit"),
                Edge("inner", "outside", condition=conditional_true, id="all_scopes_exit"),
                Edge("outer_latch", "header", condition=conditional_false, id="outer_back"),
                Edge("outer_latch", "outside", condition=conditional_true, id="outer_exit"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node("header", traced_operator("header", trace)),
                node("inner", traced_operator("inner", trace)),
                node("outer_latch", traced_operator("outer_latch", trace)),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_runtime_error(self, snapshot, "LOOP_CONTROL_CONFLICT")
        self.assertNotIn("outer_latch", trace)
        self.assertNotIn("outside", trace)

    def test_one_edge_can_exit_multiple_nested_scopes_atomically(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "cross_level_exit",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "inner", id="header_inner"),
                Edge("inner", "header", condition=conditional_false, id="inner_back"),
                Edge("inner", "outer_latch", condition=conditional_false, id="inner_exit"),
                Edge("inner", "outside", condition=conditional_true, id="cross_level_exit"),
                Edge("outer_latch", "header", condition=conditional_false, id="outer_back"),
                Edge("outer_latch", "outside", condition=conditional_true, id="outer_exit"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node("header", traced_operator("header", trace)),
                node("inner", traced_operator("inner", trace)),
                node("outer_latch", traced_operator("outer_latch", trace)),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_completed(self, snapshot)
        self.assertEqual(trace.count("outside"), 1)
        self.assertNotIn("outer_latch", trace)

    def test_simultaneous_sibling_entries_fail_before_either_body_runs(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "sibling_runtime_conflict",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "a", condition=conditional_true, id="enter_a"),
                Edge("a", "header", id="back_a"),
                Edge("header", "b", condition=conditional_true, id="enter_b"),
                Edge("b", "header", id="back_b"),
                Edge("header", "outside", condition=conditional_false, id="exit"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node("header", traced_operator("header", trace)),
                node("a", traced_operator("a", trace)),
                node("b", traced_operator("b", trace)),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_runtime_error(self, snapshot, "LOOP_CONTROL_CONFLICT")
        self.assertNotIn("a", trace)
        self.assertNotIn("b", trace)

    def test_shared_header_can_switch_between_sibling_loops_then_exit(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "sibling_switch",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "a", condition=header_count_is_one, id="enter_a"),
                Edge("a", "header", id="back_a"),
                Edge("header", "b", condition=header_count_is_two, id="enter_b"),
                Edge("b", "header", id="back_b"),
                Edge("header", "outside", condition=header_count_is_three, id="exit"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node(
                    "header",
                    traced_operator("header", trace),
                    output_binding=increment_header_count,
                ),
                node("a", traced_operator("a", trace)),
                node("b", traced_operator("b", trace)),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_completed(self, snapshot)
        self.assertEqual(trace.count("header"), 3)
        self.assertEqual(trace.count("a"), 1)
        self.assertEqual(trace.count("b"), 1)
        self.assertEqual(trace.count("outside"), 1)

    def test_outer_back_resets_inner_iteration_and_shared_header_runs_once(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "nested_runtime_scopes",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "inner_latch", id="header_inner"),
                Edge("inner_latch", "header", condition=inner_count_less_than_two, id="inner_back"),
                Edge("inner_latch", "outer_latch", condition=inner_count_at_least_two, id="inner_exit"),
                Edge("outer_latch", "header", condition=outer_count_less_than_two, id="outer_back"),
                Edge("outer_latch", "outside", condition=outer_count_at_least_two, id="outer_exit"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node("header", traced_operator("header", trace)),
                node(
                    "inner_latch",
                    traced_operator("inner_latch", trace),
                    output_binding=increment_inner_count,
                ),
                node(
                    "outer_latch",
                    traced_operator("outer_latch", trace),
                    output_binding=advance_outer_and_reset_inner,
                ),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_completed(self, snapshot)
        self.assertEqual(trace.count("header"), 4)
        self.assertEqual(trace.count("inner_latch"), 4)
        self.assertEqual(trace.count("outer_latch"), 2)
        self.assertEqual(trace.count("outside"), 1)

    def test_old_loop_occurrences_do_not_unlock_later_join_iterations(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "scoped_join_iterations",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "a", id="header_a"),
                Edge("header", "b", id="header_b"),
                Edge("a", "join", id="a_join"),
                Edge("b", "join", id="b_join"),
                Edge("join", "header", condition=count_less_than_three, id="back"),
                Edge("join", "outside", condition=count_at_least_three, id="exit"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node("header", traced_operator("header", trace)),
                node("a", traced_operator("a", trace)),
                node("b", traced_operator("b", trace)),
                node(
                    "join",
                    traced_operator("join", trace),
                    output_binding=increment_count,
                ),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_completed(self, snapshot)
        self.assertEqual(trace.count("a"), 3)
        self.assertEqual(trace.count("b"), 3)
        self.assertEqual(trace.count("join"), 3)

    def test_multiple_external_header_entries_use_complete_fan_in(self) -> None:
        trace: list[str] = []
        slow_started = threading.Event()
        release_slow = threading.Event()
        header_started = threading.Event()
        definition = workflow(
            "external_header_fan_in",
            [],
            [
                Edge("entry_fast", "header", id="fast_header"),
                Edge("entry_slow", "header", id="slow_header"),
                Edge("header", "header", condition=conditional_false, id="back"),
                Edge("header", "outside", condition=conditional_true, id="exit"),
            ],
            nodes=[
                node("entry_fast", traced_operator("entry_fast", trace)),
                node(
                    "entry_slow",
                    traced_operator(
                        "entry_slow",
                        trace,
                        started=slow_started,
                        release=release_slow,
                    ),
                ),
                node(
                    "header",
                    traced_operator(
                        "header",
                        trace,
                        started=header_started,
                    ),
                ),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        app = AutoAgentApp(max_executor_concurrency=4)
        try:
            app.register_workflow(definition)
            invocation = app.submit_invoke(definition.id, {"route": 1})
            self.assertTrue(slow_started.wait(1.5))
            self.assertFalse(
                header_started.wait(0.15),
                "Header ignored an unresolved external Entry occurrence",
            )
            release_slow.set()
            self.assertTrue(header_started.wait(1.5))
            invocation.wait(timeout=3.0)
            assert_completed(self, invocation.snapshot())
            self.assertEqual(trace.count("header"), 1)
        finally:
            release_slow.set()
            app.close()


if __name__ == "__main__":
    unittest.main()
