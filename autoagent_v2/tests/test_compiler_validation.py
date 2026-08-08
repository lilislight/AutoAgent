from __future__ import annotations

import unittest

from autoagent.core import (
    Edge,
    MapPolicy,
    Node,
    NodePolicy,
    Operator,
    UserEventMapping,
    Workflow,
    WorkflowCompileError,
    WorkflowCompiler,
)


def identity(value: int) -> int:
    return value


class CompilerValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.compiler = WorkflowCompiler()

    def assert_compile_error(self, workflow: Workflow, text: str) -> None:
        with self.assertRaisesRegex(WorkflowCompileError, text):
            self.compiler.compile(workflow)

    def test_rejects_empty_workflow_id(self) -> None:
        self.assert_compile_error(Workflow(" ", nodes=[Node("node", identity)]), "id")

    def test_rejects_workflow_without_nodes(self) -> None:
        self.assert_compile_error(Workflow("empty"), "at least one")

    def test_rejects_empty_and_path_like_node_ids(self) -> None:
        self.assert_compile_error(
            Workflow("empty-node", nodes=[Node("", identity)]), "Node ids"
        )
        self.assert_compile_error(
            Workflow("path-node", nodes=[Node("parent/child", identity)]), "Node ids"
        )

    def test_rejects_duplicate_node_ids(self) -> None:
        self.assert_compile_error(
            Workflow("duplicate", nodes=[Node("same", identity), Node("same", identity)]),
            "Duplicate Node",
        )

    def test_rejects_unknown_edge_endpoint(self) -> None:
        self.assert_compile_error(
            Workflow(
                "unknown-edge",
                nodes=[Node("known", identity)],
                edges=[Edge("known", "missing")],
            ),
            "unknown Node",
        )

    def test_rejects_duplicate_edge_ids(self) -> None:
        self.assert_compile_error(
            Workflow(
                "duplicate-edge",
                nodes=[Node("start", identity), Node("left", identity), Node("right", identity)],
                edges=[
                    Edge("start", "left", id="edge"),
                    Edge("start", "right", id="edge"),
                ],
            ),
            "Duplicate Edge",
        )

    def test_rejects_non_callable_operator_and_condition(self) -> None:
        self.assert_compile_error(
            Workflow("bad-operator", nodes=[Node("node", 42)]),  # type: ignore[arg-type]
            "Operator is not callable",
        )
        self.assert_compile_error(
            Workflow(
                "bad-condition",
                nodes=[Node("start", identity), Node("finish", identity)],
                edges=[Edge("start", "finish", condition=True)],  # type: ignore[arg-type]
            ),
            "condition is not callable",
        )

    def test_rejects_invalid_hook_arities(self) -> None:
        self.assert_compile_error(
            Workflow(
                "bad-input",
                nodes=[Node("node", identity, input_mapping=lambda: 1)],  # type: ignore[arg-type]
            ),
            "Input Mapping must declare exactly 1",
        )
        self.assert_compile_error(
            Workflow(
                "bad-output",
                nodes=[Node("node", identity, output_binding=lambda value: None)],  # type: ignore[arg-type]
            ),
            "Output Binding must declare exactly 2",
        )
        self.assert_compile_error(
            Workflow(
                "bad-edge-hook",
                nodes=[Node("start", identity), Node("finish", identity)],
                edges=[Edge("start", "finish", condition=lambda: True)],  # type: ignore[arg-type]
            ),
            "condition must declare exactly 1",
        )

    def test_rejects_invalid_map_and_user_event_hooks(self) -> None:
        self.assert_compile_error(
            Workflow(
                "bad-map",
                nodes=[
                    Node(
                        "node",
                        identity,
                        policy=NodePolicy(map=MapPolicy(item_selector=lambda: [])),  # type: ignore[arg-type]
                    )
                ],
            ),
            "item selector must declare exactly 2",
        )
        self.assert_compile_error(
            Workflow(
                "bad-event-name",
                nodes=[
                    Node(
                        "node",
                        identity,
                        user_event_mappings=(UserEventMapping("Bad.Name", identity),),
                    )
                ],
            ),
            "lowercase snake_case",
        )
        self.assert_compile_error(
            Workflow(
                "bad-event-hook",
                nodes=[
                    Node(
                        "node",
                        identity,
                        user_event_mappings=(UserEventMapping("output", 1),),  # type: ignore[arg-type]
                    )
                ],
            ),
            "transform is not callable",
        )

    def test_rejects_fallback_contract_mismatch(self) -> None:
        def fallback(value: str) -> str:
            return value

        self.assert_compile_error(
            Workflow(
                "fallback-contract",
                nodes=[Node("node", identity, fallback_operators=(fallback,))],
            ),
            "does not match",
        )

    def test_rejects_recursive_child_workflow(self) -> None:
        recursive = Workflow("recursive")
        recursive.nodes.append(Node("self", recursive))
        self.assert_compile_error(recursive, "cannot contain itself")

    def test_rejects_child_placeholder_behavior(self) -> None:
        child = Workflow("child", nodes=[Node("run", identity)])
        parent = Workflow(
            "parent",
            nodes=[Node("child", child, input_mapping=lambda context: context.invocation_input)],
        )
        self.assert_compile_error(parent, "cannot define Node behavior")

    def test_requires_child_boundary_selection_when_ambiguous(self) -> None:
        child = Workflow(
            "child",
            nodes=[Node("left", identity), Node("right", identity)],
        )
        self.assert_compile_error(
            Workflow("parent", nodes=[Node("child", child)]), "has 2 entry Nodes"
        )

    def test_child_boundary_selectors_choose_one_entry_and_exit(self) -> None:
        child = Workflow(
            "child",
            nodes=[
                Node("left", identity),
                Node("right", identity),
                Node("left_exit", identity),
                Node("right_exit", identity),
            ],
            edges=[Edge("left", "left_exit"), Edge("right", "right_exit")],
        )
        parent = Workflow(
            "parent",
            nodes=[
                Node("before", identity),
                Node(
                    "child",
                    child,
                    child_entry_node_id="left",
                    child_exit_node_id="left_exit",
                ),
                Node("after", identity),
            ],
            edges=[Edge("before", "child"), Edge("child", "after")],
        )
        ir = self.compiler.compile(parent)
        connections = {(edge.source, edge.target) for edge in ir.edges}
        self.assertIn(("before", "child/left"), connections)
        self.assertIn(("child/left_exit", "after"), connections)

    def test_rejects_explicit_entry_with_incoming_edge(self) -> None:
        self.assert_compile_error(
            Workflow(
                "incoming-entry",
                nodes=[Node("start", identity), Node("entry", identity, entry=True)],
                edges=[Edge("start", "entry")],
            ),
            "cannot have incoming",
        )

    def test_operator_id_changes_revision(self) -> None:
        first = Workflow(
            "operator-id",
            nodes=[Node("node", Operator(identity, id="first-id"))],
        )
        second = Workflow(
            "operator-id",
            nodes=[Node("node", Operator(identity, id="second-id"))],
        )
        self.assertNotEqual(
            self.compiler.compile(first).workflow_revision_id,
            self.compiler.compile(second).workflow_revision_id,
        )

    def test_operator_version_and_schema_change_revision(self) -> None:
        first = Workflow(
            "operator-version",
            nodes=[Node("node", Operator(identity, id="operator", version=1))],
        )
        second = Workflow(
            "operator-version",
            nodes=[Node("node", Operator(identity, id="operator", version=2))],
        )

        def string_identity(value: str) -> str:
            return value

        third = Workflow(
            "operator-version",
            nodes=[Node("node", Operator(string_identity, id="operator", version=1))],
        )
        revisions = {
            self.compiler.compile(item).workflow_revision_id
            for item in (first, second, third)
        }
        self.assertEqual(len(revisions), 3)


if __name__ == "__main__":
    unittest.main()
