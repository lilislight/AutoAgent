from __future__ import annotations

import unittest
from unittest.mock import patch

from real_workflow import (
    EscalationPacket,
    PublishedResolution,
    _new_incident_sample,
    _resume_incident_sample,
    build_incident_response_app,
)


class RealWorkflowTests(unittest.TestCase):
    def test_compiler_expands_selected_child_path_without_diagnostics(self) -> None:
        app, workflow = build_incident_response_app()

        result = app.compiler.compile(workflow)

        self.assertTrue(result.ok)
        self.assertEqual(result.diagnostics, [])
        self.assertEqual(
            result.workflow_ir.entry_node_ids,
            ("new_incident", "resume_incident"),
        )
        self.assertEqual(
            result.workflow_ir.exit_node_ids,
            ("publish_resolution", "escalate_incident"),
        )
        self.assertIn("investigation/plan_investigation", result.workflow_ir.nodes)
        self.assertIn("investigation/finalize_report", result.workflow_ir.nodes)
        self.assertNotIn("investigation/audit_entry", result.workflow_ir.nodes)
        self.assertNotIn("investigation/audit_exit", result.workflow_ir.nodes)

    def test_new_incident_executes_map_loop_parallel_routes_and_publish_exit(
        self,
    ) -> None:
        app, workflow = build_incident_response_app()

        with patch("real_workflow.random.random", return_value=0.99):
            invocation = app.invoke(
                workflow,
                input={"request": _new_incident_sample()},
                entry_node_id="new_incident",
                session_id="real-workflow-new-test",
            )

        self.assertEqual(invocation.state, "completed")
        self.assertIsInstance(invocation.result["output"], PublishedResolution)
        self.assertEqual(invocation.result["output"].status, "resolved")

        node_ids = [execution.node_id for execution in invocation.node_executions]
        self.assertEqual(node_ids.count("investigation/synthesize_findings"), 2)
        self.assertEqual(node_ids.count("investigation/quality_gate"), 2)
        self.assertIn("security_review", node_ids)
        self.assertIn("reliability_review", node_ids)
        self.assertNotIn("fast_track_review", node_ids)
        self.assertIn("publish_resolution", node_ids)
        self.assertNotIn("escalate_incident", node_ids)

        map_execution = invocation.latest_node_execution(
            "investigation/gather_evidence"
        )
        self.assertEqual(len(map_execution.operator_calls), 3)
        self.assertEqual(
            [call.kind for call in map_execution.operator_calls],
            ["map_item", "map_item", "map_item"],
        )

        replicated = invocation.latest_node_execution("final_decision")
        self.assertEqual(len(replicated.operator_calls), 3)
        self.assertEqual(
            [call.kind for call in replicated.operator_calls],
            ["replica", "replica", "replica"],
        )

        fallback_execution = invocation.latest_node_execution("reliability_review")
        self.assertEqual(
            [call.operator_id for call in fallback_execution.operator_calls],
            ["slow_reliability_review", "reliability_review_fallback"],
        )
        self.assertEqual(
            [call.state for call in fallback_execution.operator_calls],
            ["failed", "completed"],
        )

    def test_resume_entry_reaches_escalation_exit(self) -> None:
        app, workflow = build_incident_response_app()

        with patch("real_workflow.random.random", return_value=0.99):
            invocation = app.invoke(
                workflow,
                input={"checkpoint": _resume_incident_sample()},
                entry_node_id="resume_incident",
                session_id="real-workflow-resume-test",
            )

        self.assertEqual(invocation.state, "completed")
        self.assertIsInstance(invocation.result["output"], EscalationPacket)
        self.assertEqual(invocation.result["output"].status, "escalated")
        node_ids = [execution.node_id for execution in invocation.node_executions]
        self.assertEqual(node_ids[0], "resume_incident")
        self.assertIn("escalate_incident", node_ids)
        self.assertNotIn("publish_resolution", node_ids)

    def test_random_security_failure_exhausts_retry_and_fails_invocation(self) -> None:
        app, workflow = build_incident_response_app()

        with patch("real_workflow.random.random", side_effect=[0.0, 0.0]):
            invocation = app.invoke(
                workflow,
                input={"request": _new_incident_sample()},
                entry_node_id="new_incident",
                session_id="real-workflow-random-failure-test",
            )

        self.assertEqual(invocation.state, "failed")
        failed = invocation.latest_node_execution("security_review")
        self.assertEqual(failed.state, "failed")
        self.assertEqual(
            [call.kind for call in failed.operator_calls],
            ["normal", "retry"],
        )
        self.assertTrue(
            all(call.error is not None for call in failed.operator_calls)
        )


if __name__ == "__main__":
    unittest.main()
