"""Node execution safety tests derived from ``workflow.md``."""

from __future__ import annotations

import unittest

from workflow_spec_support import (
    ContextPatch,
    Edge,
    InputMappingContext,
    EdgeConditionContext,
    AggregationContext,
    ItemSelectorContext,
    OutputBindingContext,
    Payload,
    assert_completed,
    assert_runtime_error,
    conditional_false,
    conditional_true,
    node,
    run_workflow,
    traced_operator,
    workflow,
)
from autoagent import MapPolicy, NodePolicy, ResourcePolicy


def select_five(
    context: ItemSelectorContext,
) -> list[Payload]:
    return [{"unit": unit} for unit in range(5)]


def aggregate_units(
    context: AggregationContext,
) -> Payload:
    return {"units": len(context.operator_outputs)}


class NodeExecutionLimitTests(unittest.TestCase):
    def test_explicit_limit_stops_repeated_back_edge(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "explicit_node_limit",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "header", condition=conditional_true, id="back"),
                Edge("header", "outside", condition=conditional_false, id="exit"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node(
                    "header",
                    traced_operator("header", trace),
                    policy=NodePolicy(
                        resource=ResourcePolicy(
                            max_node_executions_per_invocation=3
                        )
                    ),
                ),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_runtime_error(self, snapshot, "NODE_EXECUTION_LIMIT_EXCEEDED")
        self.assertEqual(trace.count("header"), 3)
        evidence = snapshot.error.message.lower()
        self.assertIn("header", evidence)
        self.assertIn("3", evidence, "allowed execution count is missing")
        self.assertIn("4", evidence, "attempted execution count is missing")
        self.assertIn("scope", evidence)
        self.assertIn("iteration", evidence)

    def test_unconfigured_node_still_inherits_a_finite_implicit_limit(self) -> None:
        trace: list[str] = []
        definition = workflow(
            "implicit_node_limit",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "header", condition=conditional_true, id="back"),
                Edge("header", "outside", condition=conditional_false, id="exit"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node("header", traced_operator("header", trace)),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition, timeout=10.0)
        assert_runtime_error(self, snapshot, "NODE_EXECUTION_LIMIT_EXCEEDED")
        self.assertGreater(trace.count("header"), 0)
        self.assertNotIn("outside", trace)

    def test_map_units_do_not_count_as_separate_node_executions(self) -> None:
        calls: list[str] = []
        definition = workflow(
            "map_units_are_not_nodes",
            [],
            [],
            nodes=[
                node(
                    "mapped",
                    traced_operator("unit", calls),
                    policy=NodePolicy(
                        map=MapPolicy(
                            item_selector=select_five,
                            output_aggregator=aggregate_units,
                            max_parallelism=2,
                        ),
                        resource=ResourcePolicy(
                            max_node_executions_per_invocation=1,
                            max_operator_attempts_per_invocation=5,
                        ),
                    ),
                )
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_completed(self, snapshot)
        self.assertEqual(calls.count("unit"), 5)

    def test_shared_header_nested_scopes_use_one_invocation_wide_counter(self) -> None:
        trace: list[str] = []

        def increment_inner(
            context: OutputBindingContext,
        ) -> ContextPatch:
            return ContextPatch(
                invocation={
                    "inner": int(context.invocation_context.get("inner", 0)) + 1
                }
            )

        def inner_back(context: EdgeConditionContext) -> bool:
            return int(context.invocation_context.get("inner", 0)) < 2

        def inner_exit(context: EdgeConditionContext) -> bool:
            return int(context.invocation_context.get("inner", 0)) >= 2

        def reset_inner(
            context: OutputBindingContext,
        ) -> ContextPatch:
            return ContextPatch(
                invocation={
                    "inner": 0,
                    "outer": int(context.invocation_context.get("outer", 0)) + 1,
                }
            )

        def outer_back(context: EdgeConditionContext) -> bool:
            return int(context.invocation_context.get("outer", 0)) < 2

        def outer_exit(context: EdgeConditionContext) -> bool:
            return int(context.invocation_context.get("outer", 0)) >= 2

        definition = workflow(
            "nested_counter_scope",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "inner_latch", id="header_inner"),
                Edge("inner_latch", "header", condition=inner_back, id="inner_back"),
                Edge("inner_latch", "outer_latch", condition=inner_exit, id="inner_exit"),
                Edge("outer_latch", "header", condition=outer_back, id="outer_back"),
                Edge("outer_latch", "outside", condition=outer_exit, id="outer_exit"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node(
                    "header",
                    traced_operator("header", trace),
                    policy=NodePolicy(
                        resource=ResourcePolicy(
                            max_node_executions_per_invocation=3
                        )
                    ),
                ),
                node(
                    "inner_latch",
                    traced_operator("inner_latch", trace),
                    output_binding=increment_inner,
                ),
                node(
                    "outer_latch",
                    traced_operator("outer_latch", trace),
                    output_binding=reset_inner,
                ),
                node("outside", traced_operator("outside", trace)),
            ],
        )
        _, snapshot = run_workflow(definition)
        assert_runtime_error(self, snapshot, "NODE_EXECUTION_LIMIT_EXCEEDED")
        self.assertEqual(trace.count("header"), 3)
        self.assertNotIn("outside", trace)


if __name__ == "__main__":
    unittest.main()
