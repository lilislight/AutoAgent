from __future__ import annotations

from pathlib import Path
import sqlite3
import unittest
from unittest.mock import MagicMock, patch

from autoagent import AutoAgentSettings, Workflow
from autoagent.project import (
    LoadedWorkflow,
    ProjectDefinition,
    ProjectHost,
    ProjectMetadata,
    WorkflowLocator,
)


def process_connection(connection: sqlite3.Connection) -> str:
    return str(connection)


def valid_task() -> str:
    return "ok"


def project_for(workflow: Workflow) -> ProjectDefinition:
    return ProjectDefinition(
        manifest_path=None,
        root=Path.cwd(),
        metadata=ProjectMetadata(name="host-test", version="1"),
        workflows=(
            LoadedWorkflow(
                locator=WorkflowLocator(entrypoint="tests.fixture:workflow"),
                workflow=workflow,
            ),
        ),
    )


class ProjectHostTests(unittest.TestCase):
    def test_host_rejects_invalid_callable_schema_during_registration(self) -> None:
        workflow = Workflow(id="invalid_host_contract")
        workflow.add_node(process_connection, node_id="connection")

        with self.assertRaisesRegex(
            ValueError,
            "OPERATOR_CONTRACT_INVALID.*non-serializable Workflow type",
        ):
            ProjectHost(
                project_for(workflow),
                app_settings=AutoAgentSettings(),
                environment={},
            )

    def test_host_closes_app_when_workflow_registration_fails(self) -> None:
        workflow = Workflow(id="registration_failure")
        workflow.add_node(valid_task, node_id="task")
        app = MagicMock()
        app.register_workflow.side_effect = ValueError("invalid contract")

        with (
            patch("autoagent.project.host.AutoAgentApp", return_value=app),
            self.assertRaisesRegex(ValueError, "invalid contract"),
        ):
            ProjectHost(
                project_for(workflow),
                app_settings=AutoAgentSettings(),
                environment={},
            )

        app.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
