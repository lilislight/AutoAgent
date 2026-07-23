from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from autoagent.core.runtime import DatabaseBackend, RuntimeStore
from real_workflow import (
    EscalationPacket,
    PublishedResolution,
    _new_incident_sample,
    _resume_incident_sample,
    build_incident_response_app,
)


def build_test_app():
    """Keep unit tests isolated from the durable demonstration database."""

    return build_incident_response_app(runtime_store=RuntimeStore())


class RealWorkflowTests(unittest.TestCase):
    def test_demo_defaults_to_a_sqlite_runtime_store(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "demo.sqlite3"
            app, _ = build_incident_response_app(database_path=database_path)
            self.assertIsInstance(app.runtime_store, RuntimeStore)
            self.assertIsInstance(app.runtime_store.backend, DatabaseBackend)
            app.close()

    def test_compiler_expands_selected_child_path_without_diagnostics(self) -> None:
        app, workflow = build_test_app()

        result = app.compiler.compile(workflow)

        self.assertTrue(result.ok)
        self.assertEqual(result.diagnostics, [])
        self.assertEqual(
            result.workflow_ir.entry_node_ids,
            ("new_incident", "resume_incident", "manual_approval_wait"),
        )
        self.assertEqual(
            result.workflow_ir.exit_node_ids,
            (
                "manual_approval_result",
                "publish_resolution",
                "escalate_incident",
            ),
        )
        self.assertIn("investigation/plan_investigation", result.workflow_ir.nodes)
        self.assertIn("investigation/finalize_report", result.workflow_ir.nodes)
        self.assertNotIn("investigation/audit_entry", result.workflow_ir.nodes)
        self.assertNotIn("investigation/audit_exit", result.workflow_ir.nodes)

    def test_new_incident_executes_map_loop_parallel_routes_and_publish_exit(
        self,
    ) -> None:
        app, workflow = build_test_app()

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

        self.assertEqual(
            "INC-2048",
            invocation.context.data["normalized_incident"]["incident_id"],
        )
        session = app.runtime_store.find_session(
            namespace=app.namespace,
            workflow_id=workflow.id,
            session_key="real-workflow-new-test",
        )
        assert session is not None
        self.assertIn("latest_incident_decision", session.context.data)
        events = asyncio.run(
            app.runtime_store.alist_runtime_events(
                invocation_id=invocation.id,
                limit=10_000,
            )
        )
        self.assertEqual(
            list(range(1, len(events) + 1)),
            [event.sequence for event in events],
        )

        map_execution = invocation.latest_node_execution(
            "investigation/gather_evidence"
        )
        self.assertEqual(len(map_execution.operator_executions), 1)
        self.assertEqual("map", map_execution.operator_executions[0].kind)
        self.assertEqual(3, map_execution.operator_executions[0].summary.call_count)

        replicated = invocation.latest_node_execution("final_decision")
        self.assertEqual(len(replicated.operator_executions), 1)
        self.assertEqual("replication", replicated.operator_executions[0].kind)
        self.assertEqual(3, replicated.operator_executions[0].summary.call_count)

        fallback_execution = invocation.latest_node_execution("reliability_review")
        self.assertEqual(
            [call.operator_id for call in fallback_execution.operator_executions],
            ["slow_reliability_review", "reliability_review_fallback"],
        )
        self.assertEqual(
            [call.state for call in fallback_execution.operator_executions],
            ["failed", "completed"],
        )

    def test_resume_entry_reaches_escalation_exit(self) -> None:
        app, workflow = build_test_app()

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
        app, workflow = build_test_app()

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
            [call.reason for call in failed.operator_executions],
            ["normal", "retry"],
        )
        self.assertTrue(
            all(call.error is not None for call in failed.operator_executions)
        )

    def test_manual_approval_wait_branch_resumes_without_touching_normal_paths(
        self,
    ) -> None:
        app, workflow = build_test_app()

        waiting = app.invoke(
            workflow,
            input={
                "wait_key": "approval:real-workflow-test",
                "wait_type": "human",
                "payload": {"ticket": "INC-9000"},
            },
            entry_node_id="manual_approval_wait",
            session_id="real-workflow-approval-test",
        )

        self.assertEqual(waiting.state, "waiting")
        self.assertEqual(
            [execution.node_id for execution in waiting.node_executions],
            ["manual_approval_wait"],
        )

        resumed = app.resume(
            workflow,
            session_id="real-workflow-approval-test",
            wait_key="approval:real-workflow-test",
            output={
                "approved": True,
                "reviewer": "sre-lead",
                "note": "Manual release approval granted.",
            },
        )

        self.assertEqual(resumed.state, "completed")
        self.assertEqual(
            resumed.result["output"],
            {
                "approved": True,
                "reviewer": "sre-lead",
                "note": "Manual release approval granted.",
            },
        )
        node_ids = [execution.node_id for execution in resumed.node_executions]
        self.assertEqual(
            node_ids,
            ["manual_approval_wait", "manual_approval_result"],
        )
        self.assertNotIn("new_incident", node_ids)
        self.assertNotIn("resume_incident", node_ids)


if __name__ == "__main__":
    unittest.main()
