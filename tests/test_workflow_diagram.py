from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from autoagent import (
    AutoAgentApp,
    CapabilityRef,
    Edge,
    EdgePolicy,
    MapPolicy,
    Node,
    Workflow,
)


def source(value: str) -> str:
    return value


def target(value: str) -> str:
    return value.upper()


class WorkflowDiagramTests(unittest.TestCase):
    def test_child_workflow_placeholder_keeps_source_boundary_markers(self) -> None:
        child = Workflow(
            id="child",
            nodes=[Node(id="work", capability=source)],
        )
        parent = Workflow(id="parent")
        parent.add_node(child, node_id="child")

        diagram = parent.diagram()

        self.assertTrue(diagram.compiled)
        self.assertEqual(len(diagram.nodes), 1)
        self.assertTrue(diagram.nodes[0].entry)
        self.assertTrue(diagram.nodes[0].exit)
        self.assertEqual(diagram.nodes[0].capability, "Workflow: child")

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
        self.assertEqual(diagram.edges[0].status, "normal")
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
        edge = diagram.edges[0]

        self.assertFalse(diagram.compiled)
        self.assertEqual(edge.id, "edge_source_missing")
        self.assertEqual(edge.status, "error")
        self.assertEqual(edge.diagnostics[0].code, "EDGE_UNKNOWN_NODE")
        self.assertEqual(edge.diagnostics[0].metadata["source_index"], 0)
        self.assertTrue(any(node.missing for node in diagram.nodes))
        self.assertIn("linkStyle 0 stroke:#dc2626", diagram.to_mermaid())

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
        edge = diagram.edges[0]

        self.assertEqual(edge.id, "edge_source_target")
        self.assertEqual(edge.status, "error")
        self.assertEqual(edge.diagnostics[0].subject, "edge_source_target")
        self.assertEqual(edge.diagnostics[0].code, "STRING_CONDITION_UNSUPPORTED")

    def test_compiler_warning_marks_map_edge_amber(self) -> None:
        def item_source(value: str) -> list[dict[str, str]]:
            return [{"value": value}]

        workflow = Workflow(
            id="warning_edge",
            nodes=[
                Node(id="source", capability=item_source),
                Node(id="target", capability=target),
            ],
            edges=[
                Edge(
                    id="mapped",
                    from_node="source",
                    to_node="target",
                    policy=EdgePolicy(
                        map=MapPolicy(
                            output_aggregator=lambda ctx: ctx.item_outputs
                        )
                    ),
                )
            ],
        )

        diagram = workflow.diagram()

        self.assertTrue(diagram.compiled)
        self.assertEqual(diagram.warning_count, 1)
        self.assertEqual(diagram.edges[0].status, "warning")
        self.assertEqual(
            diagram.edges[0].diagnostics[0].code,
            "POLICY_AGGREGATOR_OUTPUT_UNVERIFIED",
        )
        self.assertIn("linkStyle 0 stroke:#b45309", diagram.to_mermaid())

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
        forbidden = next(edge for edge in diagram.edges if edge.id == "forbidden_entry")

        self.assertEqual(forbidden.status, "error")
        self.assertEqual(forbidden.diagnostics[0].code, "LOOP_ENTRY_INVALID")
        self.assertEqual(
            forbidden.diagnostics[0].metadata["expected_entry_node_id"],
            "review",
        )

    def test_app_preview_uses_registered_capability_contracts(self) -> None:
        app = AutoAgentApp()

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
        from examples.workflow_validation_preview import (
            build_app,
            build_workflow,
            demonstrate_schema_registration_error,
        )

        app = build_app()
        diagram = build_workflow().diagram(compiler=app.compiler)
        errors = {
            edge.id: tuple(item.code for item in edge.diagnostics)
            for edge in diagram.edges
            if edge.status == "error"
        }

        self.assertIn(
            "POLICY_MAP_FAN_IN_UNSUPPORTED",
            errors["edge_security_review_assess_blast_radius"],
        )
        self.assertIn(
            "LOOP_ENTRY_INVALID",
            errors["edge_classify_incident_refine_remediation"],
        )
        schema_error = demonstrate_schema_registration_error(app)
        self.assertIn("does not match Capability incident_log_analysis", schema_error)
        self.assertIn("cannot accept Capability parameter 'incident_id'", schema_error)


if __name__ == "__main__":
    unittest.main()
