from __future__ import annotations

import unittest

from autoagent.core import (
    Edge,
    MapPolicy,
    Node,
    NodePolicy,
    Workflow,
    WorkflowCompileError,
    WorkflowCompiler,
)
from tests.helpers import always_false, always_true, identity_int


class WorkflowCompilerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.compiler = WorkflowCompiler()

    def test_expands_child_workflow_and_preserves_workflow_path(self) -> None:
        child = Workflow(
            id="child",
            nodes=[Node("first", identity_int), Node("last", identity_int)],
            edges=[Edge("first", "last")],
        )
        parent = Workflow(
            id="parent",
            nodes=[
                Node("start", identity_int),
                Node("child_step", child),
                Node("finish", identity_int),
            ],
            edges=[Edge("start", "child_step"), Edge("child_step", "finish")],
        )

        compiled = self.compiler.compile(parent)

        self.assertEqual(
            tuple(node.id for node in compiled.nodes),
            ("start", "child_step/first", "child_step/last", "finish"),
        )
        self.assertEqual(compiled.node("child_step/first").workflow_path, ("child_step",))
        self.assertEqual(compiled.subworkflows[0].workflow_id, "child")
        self.assertEqual(
            [(edge.source, edge.target) for edge in compiled.edges],
            [
                ("child_step/first", "child_step/last"),
                ("start", "child_step/first"),
                ("child_step/last", "finish"),
            ],
        )

        child.version = 2
        changed = self.compiler.compile(parent)
        self.assertNotEqual(compiled.workflow_revision_id, changed.workflow_revision_id)

    def test_detects_natural_loop_and_classifies_its_edges(self) -> None:
        workflow = Workflow(
            id="loop",
            nodes=[
                Node("start", identity_int),
                Node("header", identity_int),
                Node("body", identity_int),
                Node("finish", identity_int),
            ],
            edges=[
                Edge("start", "header", id="enter"),
                Edge("header", "body", id="body"),
                Edge("body", "header", condition=always_true, id="back"),
                Edge("body", "finish", condition=always_false, id="exit"),
            ],
        )

        region = self.compiler.compile(workflow).loop_regions[0]

        self.assertEqual(region.header_node_id, "header")
        self.assertEqual(region.node_ids, ("header", "body"))
        self.assertEqual(region.entry_edge_ids, ("enter",))
        self.assertEqual(region.back_edge_ids, ("back",))
        self.assertEqual(region.exit_edge_ids, ("exit",))

    def test_rejects_external_edge_into_non_header_loop_node(self) -> None:
        workflow = Workflow(
            id="irreducible",
            nodes=[
                Node("start", identity_int),
                Node("header", identity_int),
                Node("body", identity_int),
                Node("finish", identity_int),
            ],
            edges=[
                Edge("start", "header"),
                Edge("start", "body"),
                Edge("header", "body"),
                Edge("body", "header"),
                Edge("body", "finish"),
            ],
        )

        with self.assertRaisesRegex(WorkflowCompileError, "non-header"):
            self.compiler.compile(workflow)

    def test_all_zero_incoming_nodes_are_entries_and_requires_structural_exit(self) -> None:
        multiple_entries = Workflow(
            id="multiple-entries",
            nodes=[Node("start", identity_int, entry=True), Node("orphan", identity_int)],
        )
        self.assertEqual(
            self.compiler.compile(multiple_entries).entry_node_ids,
            ("start", "orphan"),
        )

        no_exit = Workflow(
            id="no-exit",
            nodes=[Node("a", identity_int), Node("b", identity_int)],
            edges=[Edge("a", "b"), Edge("b", "a")],
        )
        with self.assertRaises(WorkflowCompileError):
            self.compiler.compile(no_exit)

    def test_policy_and_hook_versions_are_revision_semantics(self) -> None:
        def operator(value: int) -> int:
            return value

        first = Workflow("identity", nodes=[Node("node", operator, hook_version=1)])
        second = Workflow("identity", nodes=[Node("node", operator, hook_version=2)])
        third = Workflow(
            "identity",
            nodes=[Node("node", operator, hook_version=1, policy=NodePolicy(map=MapPolicy()))],
        )

        first_id = self.compiler.compile(first).workflow_revision_id
        self.assertNotEqual(first_id, self.compiler.compile(second).workflow_revision_id)
        self.assertNotEqual(first_id, self.compiler.compile(third).workflow_revision_id)


if __name__ == "__main__":
    unittest.main()
