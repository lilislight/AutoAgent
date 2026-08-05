from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from autoagent import (
    CapabilityRef,
    Edge,
    MapPolicy,
    Node,
    NodePolicy,
    Workflow,
)
from tests.helpers import isolated_app


def source(value: str) -> str:
    return value


def target(value: str) -> str:
    return value.upper()


class WorkflowPreviewTests(unittest.TestCase):
    def test_child_workflow_preview_uses_expanded_execution_graph(self) -> None:
        child = Workflow(
            id="child",
            nodes=[Node(id="work", capability=source)],
        )
        parent = Workflow(id="parent")
        parent.add_node(child, node_id="child")

        diagram = parent.diagram()

        self.assertTrue(diagram.compiled)
        self.assertEqual(len(diagram.analysis.nodes), 1)
        node = diagram.analysis.nodes[0]
        self.assertEqual("child/work", node.id)
        self.assertEqual("work", node.local_id)
        self.assertEqual(("child",), node.workflow_path)
        self.assertTrue(node.entry)
        self.assertTrue(node.exit)

    def test_valid_workflow_generates_directed_mermaid(self) -> None:
        workflow = Workflow(
            id="preview",
            name="Preview Workflow",
            nodes=[
                Node(id="source", capability=source),
                Node(id="target", capability=target),
            ],
            edges=[Edge(from_node="source", to_node="target")],
        )

        diagram = workflow.diagram()
        mermaid = diagram.to_mermaid()
        self.assertTrue(diagram.compiled)
        self.assertEqual(diagram.error_count, 0)
        self.assertEqual(
            diagram.edge_status(diagram.analysis.edges[0]),
            "normal",
        )
        self.assertIn("flowchart LR", mermaid)
        self.assertIn("n0 -->|edge_source_target| n1", mermaid)

    def test_preview_writes_mermaid_and_returns_absolute_path(self) -> None:
        workflow = Workflow(
            id="saved_preview",
            nodes=[Node(id="source", capability=source)],
        )

        with tempfile.TemporaryDirectory() as directory:
            target_path = Path(directory) / "nested" / "workflow.mmd"
            result = workflow.preview(target_path)

            self.assertTrue(result.is_absolute())
            self.assertEqual(result, target_path.resolve())
            self.assertIn("flowchart LR", result.read_text(encoding="utf-8"))

    def test_unknown_target_marks_auto_id_edge_red_and_adds_missing_node(self) -> None:
        workflow = Workflow(
            id="invalid_endpoint",
            nodes=[Node(id="source", capability=source)],
            edges=[Edge(from_node="source", to_node="missing")],
        )

        diagram = workflow.diagram()
        edge = diagram.analysis.edges[0]

        self.assertFalse(diagram.compiled)
        self.assertEqual(edge.id, "edge_source_missing")
        self.assertEqual(diagram.edge_status(edge), "error")
        diagnostic = next(
            item for item in diagram.diagnostics if item.code == "EDGE_UNKNOWN_NODE"
        )
        self.assertEqual(0, diagnostic.source_index)
        self.assertIsNone(diagram.analysis.entry_node_ids)
        self.assertIn("Missing: missing", diagram.to_mermaid())
        self.assertIn("linkStyle 0 stroke:#dc2626", diagram.to_mermaid())

    def test_terminal_includes_edge_with_unknown_source(self) -> None:
        workflow = Workflow(
            id="invalid_source",
            nodes=[Node(id="target", capability=target)],
            edges=[Edge(id="broken", from_node="missing", to_node="target")],
        )

        terminal = workflow.diagram().to_terminal()

        self.assertIn("missing [MISSING SOURCE]", terminal)
        self.assertIn("broken [error] → target", terminal)

    def test_auto_id_edge_with_string_condition_is_located_and_marked_red(self) -> None:
        workflow = Workflow(
            id="invalid_condition",
            nodes=[
                Node(id="source", capability=source),
                Node(id="target", capability=target),
            ],
            edges=[
                Edge(
                    from_node="source",
                    to_node="target",
                    condition="output.accepted == true",
                )
            ],
        )

        diagram = workflow.diagram()
        edge = diagram.analysis.edges[0]

        self.assertEqual(edge.id, "edge_source_target")
        self.assertEqual(diagram.edge_status(edge), "error")
        diagnostic = next(
            item
            for item in diagram.diagnostics
            if item.code == "STRING_CONDITION_UNSUPPORTED"
        )
        self.assertEqual("edge_source_target", diagnostic.object_id)

    def test_invalid_aggregator_contract_marks_map_node_red(self) -> None:
        def item_source(value: str) -> list[dict[str, str]]:
            return [{"value": value}]

        workflow = Workflow(
            id="warning_edge",
            nodes=[
                Node(id="source", capability=item_source),
                Node(
                    id="target",
                    capability=target,
                    policy=NodePolicy(
                        map=MapPolicy(
                            output_aggregator=lambda ctx: ctx.item_outputs
                        )
                    ),
                ),
            ],
            edges=[
                Edge(
                    id="mapped",
                    from_node="source",
                    to_node="target",
                )
            ],
        )

        diagram = workflow.diagram()

        self.assertFalse(diagram.compiled)
        self.assertEqual(diagram.error_count, 1)
        node = diagram.analysis.nodes[1]
        self.assertIsNotNone(node.map_policy)
        assert node.map_policy is not None
        self.assertFalse(node.map_policy.has_item_selector)
        self.assertTrue(node.map_policy.has_output_aggregator)
        self.assertEqual(diagram.node_status(node), "error")
        self.assertEqual(
            diagram.diagnostics[0].code,
            "POLICY_AGGREGATOR_CONTRACT_INVALID",
        )
        self.assertIn("class n1 error", diagram.to_mermaid())

    def test_forbidden_second_loop_entry_edge_is_marked_red(self) -> None:
        def condition(_ctx) -> bool:
            return True

        workflow = Workflow(
            id="invalid_loop_entry",
            nodes=[
                Node(id="start", capability=source),
                Node(id="review", capability=target),
                Node(id="refine", capability=target),
            ],
            edges=[
                Edge(id="normal_entry", from_node="start", to_node="review"),
                Edge(id="review_refine", from_node="review", to_node="refine", condition=condition),
                Edge(id="refine_review", from_node="refine", to_node="review"),
                Edge(id="forbidden_entry", from_node="start", to_node="refine"),
            ],
        )

        diagram = workflow.diagram()
        forbidden = next(
            edge
            for edge in diagram.analysis.edges
            if edge.id == "forbidden_entry"
        )

        self.assertEqual(diagram.edge_status(forbidden), "error")
        diagnostic = next(
            item for item in diagram.diagnostics if item.code == "LOOP_ENTRY_INVALID"
        )
        self.assertEqual(
            diagnostic.metadata["expected_entry_node_id"],
            "review",
        )

    def test_app_preview_uses_registered_capability_contracts(self) -> None:
        app = isolated_app()

        @app.capability("uppercase")
        def uppercase(value: str) -> str:
            return value.upper()

        workflow = Workflow(
            id="capability_preview",
            nodes=[Node(id="uppercase", capability=CapabilityRef(id="uppercase"))],
        )

        with tempfile.TemporaryDirectory() as directory:
            path = app.preview(workflow, Path(directory) / "preview.mmd")
            mermaid = path.read_text(encoding="utf-8")

        self.assertIn("flowchart LR", mermaid)
        self.assertIn("Capability: uppercase", mermaid)

    def test_complex_example_marks_auto_id_and_schema_errors(self) -> None:
        from tests.fixtures.workflow_validation_preview import (
            build_app,
            build_workflow,
            demonstrate_schema_registration_error,
        )

        app = build_app()
        diagram = build_workflow().diagram(compiler=app.compiler)
        edge_errors = {
            edge.id: tuple(
                item.code
                for item in diagram.diagnostics
                if item.object_type == "edge"
                and (
                    item.object_id == edge.id
                    or item.source_index == edge.source_index
                )
            )
            for edge in diagram.analysis.edges
            if diagram.edge_status(edge) == "error"
        }

        self.assertIn(
            "LOOP_ENTRY_INVALID",
            edge_errors["edge_classify_incident_refine_remediation"],
        )
        node_errors = {
            node.id: tuple(
                item.code
                for item in diagram.diagnostics
                if item.object_type == "node" and item.object_id == node.id
            )
            for node in diagram.analysis.nodes
            if diagram.node_status(node) == "error"
        }
        self.assertIn(
            "POLICY_MAP_SELECTOR_REQUIRED",
            node_errors["assess_blast_radius"],
        )
        schema_error = demonstrate_schema_registration_error(app)
        self.assertIn("does not match Capability incident_log_analysis", schema_error)
        self.assertIn("cannot accept Capability parameter 'incident_id'", schema_error)

    def test_terminal_and_json_renderers_share_compiler_analysis(self) -> None:
        workflow = Workflow(
            id="all_formats",
            nodes=[
                Node(id="source", capability=source),
                Node(id="target", capability=target),
            ],
            edges=[Edge(from_node="source", to_node="target")],
        )

        preview = workflow.diagram()
        terminal = preview.to_terminal()
        json_text = preview.to_json()

        self.assertIn("STATUS valid", terminal)
        self.assertIn("edge_source_target", terminal)
        self.assertIn('"workflow_id": "all_formats"', json_text)
        self.assertIn('"from_node_id": "source"', json_text)


if __name__ == "__main__":
    unittest.main()
