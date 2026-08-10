"""Focused conformance tests for logical Node input and output contracts."""

from __future__ import annotations

import unittest

from autoagent.core import (
    AggregationContext,
    AutoAgentApp,
    ContextPatch,
    Edge,
    EdgeConditionContext,
    InputMappingContext,
    InvocationState,
    ItemSelectorContext,
    MapPolicy,
    Node,
    NodePolicy,
    OutputBindingContext,
    Workflow,
    WorkflowCompileError,
    WorkflowCompiler,
)


def identity_int(value: int) -> int:
    return value


def identity_str(value: str) -> str:
    return value


def identity_int_list(value: list[int]) -> list[int]:
    return value


def double(value: int) -> int:
    return value * 2


def select_input_items(context: ItemSelectorContext) -> list[int]:
    return list(context.input)


def aggregate_sum(context: AggregationContext) -> int:
    return sum(context.operator_outputs)


class CompilerDataContractTests(unittest.TestCase):
    def test_rejects_statically_incompatible_single_activation(self) -> None:
        definition = Workflow(
            "incompatible-edge-contract",
            nodes=[Node("source", identity_int), Node("target", identity_str)],
            edges=[Edge("source", "target", id="source_target")],
        )

        with self.assertRaises(WorkflowCompileError):
            WorkflowCompiler().compile(definition)

    def test_rejects_implicit_merge_of_multiple_selected_activations(self) -> None:
        definition = Workflow(
            "ambiguous-multiple-activations",
            nodes=[
                Node("left", identity_int),
                Node("right", identity_int),
                Node("join", identity_int),
            ],
            edges=[
                Edge("left", "join", id="left_join"),
                Edge("right", "join", id="right_join"),
            ],
        )

        with self.assertRaises(WorkflowCompileError):
            WorkflowCompiler().compile(definition)

    def test_logical_output_contract_follows_parallel_aggregation(self) -> None:
        mapped = Workflow(
            "mapped-list-output",
            nodes=[
                Node(
                    "mapped",
                    double,
                    policy=NodePolicy(
                        map=MapPolicy(item_selector=select_input_items)
                    ),
                )
            ],
        )
        aggregated = Workflow(
            "mapped-aggregate-output",
            nodes=[
                Node(
                    "mapped",
                    double,
                    policy=NodePolicy(
                        map=MapPolicy(
                            item_selector=select_input_items,
                            output_aggregator=aggregate_sum,
                        )
                    ),
                )
            ],
        )

        mapped_ir = WorkflowCompiler().compile(mapped)
        aggregated_ir = WorkflowCompiler().compile(aggregated)

        self.assertEqual(mapped_ir.node("mapped").output_contract.annotation, list[int])
        self.assertIs(
            aggregated_ir.node("mapped").output_contract.annotation,
            int,
        )


class RuntimeDataContractTests(unittest.TestCase):
    def test_explicit_input_mapping_constructs_one_logical_join_input(self) -> None:
        seen: list[tuple[str, int]] = []

        def merge(context: InputMappingContext) -> list[int]:
            seen.extend(
                (activation.edge_id, activation.value)
                for activation in context.incoming
            )
            return [activation.value for activation in context.incoming]

        definition = Workflow(
            "explicit-activation-merge",
            nodes=[
                Node("left", identity_int),
                Node("right", identity_int),
                Node("join", identity_int_list, input_mapping=merge),
            ],
            edges=[
                Edge("left", "join", id="left_join"),
                Edge("right", "join", id="right_join"),
            ],
        )
        app = AutoAgentApp()
        try:
            app.register_workflow(definition)
            invocation = app.invoke(definition, 3)

            self.assertEqual(invocation.result(), {"join": [3, 3]})
            self.assertEqual(seen, [("left_join", 3), ("right_join", 3)])
        finally:
            app.close()

    def test_duplicate_source_activations_remain_distinct_by_edge_id(self) -> None:
        seen_edge_ids: list[str] = []

        def merge(context: InputMappingContext) -> list[int]:
            seen_edge_ids.extend(item.edge_id for item in context.incoming)
            return [item.value for item in context.incoming]

        definition = Workflow(
            "duplicate-source-activations",
            nodes=[
                Node("source", identity_int),
                Node("join", identity_int_list, input_mapping=merge),
            ],
            edges=[
                Edge("source", "join", id="first"),
                Edge("source", "join", id="second"),
            ],
        )
        app = AutoAgentApp()
        try:
            app.register_workflow(definition)
            invocation = app.invoke(definition, 5)

            self.assertEqual(invocation.result(), {"join": [5, 5]})
            self.assertEqual(seen_edge_ids, ["first", "second"])
        finally:
            app.close()

    def test_terminal_map_unit_failure_skips_aggregation_and_binding(self) -> None:
        calls: list[int] = []
        aggregated = False
        bound = False

        def select(_context: ItemSelectorContext) -> list[int]:
            return [0, 1, 2]

        def fail_first(value: int) -> int:
            calls.append(value)
            if value == 0:
                raise ValueError("unit failed")
            return value

        def aggregate(context: AggregationContext) -> int:
            nonlocal aggregated
            aggregated = True
            return sum(context.operator_outputs)

        def bind(_context: OutputBindingContext) -> ContextPatch:
            nonlocal bound
            bound = True
            return ContextPatch(invocation={"bound": True})

        definition = Workflow(
            "atomic-map-unit-failure",
            nodes=[
                Node(
                    "mapped",
                    fail_first,
                    output_binding=bind,
                    policy=NodePolicy(
                        map=MapPolicy(
                            item_selector=select,
                            output_aggregator=aggregate,
                            max_parallelism=1,
                        )
                    ),
                )
            ],
        )
        app = AutoAgentApp()
        try:
            app.register_workflow(definition)
            invocation = app.invoke(definition, 0)

            self.assertEqual(invocation.state, InvocationState.FAILED)
            self.assertEqual(calls, [0])
            self.assertFalse(aggregated)
            self.assertFalse(bound)
        finally:
            app.close()

    def test_condition_failure_keeps_committed_output_binding(self) -> None:
        target_calls = 0

        def bind(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(session={"committed": context.output})

        def fail_condition(_context: EdgeConditionContext) -> bool:
            raise LookupError("condition failed")

        def target(value: int) -> int:
            nonlocal target_calls
            target_calls += 1
            return value

        definition = Workflow(
            "condition-failure-after-commit",
            nodes=[
                Node("source", identity_int, output_binding=bind),
                Node("target", target),
            ],
            edges=[
                Edge(
                    "source",
                    "target",
                    condition=fail_condition,
                    id="source_target",
                )
            ],
        )
        app = AutoAgentApp()
        try:
            app.register_workflow(definition)
            invocation = app.invoke(definition, 7, session_id="session")

            self.assertEqual(invocation.state, InvocationState.FAILED)
            self.assertEqual(app._sessions["session"].context, {"committed": 7})
            self.assertEqual(target_calls, 0)
        finally:
            app.close()


if __name__ == "__main__":
    unittest.main()
