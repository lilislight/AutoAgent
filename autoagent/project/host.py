from __future__ import annotations

from collections.abc import Mapping

from autoagent.ai import (
    LLM_CALL_CAPABILITY_ID,
    OpenAICompatibleConfig,
    register_openai_compatible_operator,
)
from autoagent.core.app import AutoAgentApp, AutoAgentSettings
from autoagent.core.workflow import CapabilityRef, Workflow
from autoagent.project.loader import ProjectDefinition


class ProjectHost:
    """Own one configured App and every Workflow declared by a project."""

    def __init__(
        self,
        project: ProjectDefinition,
        *,
        app_settings: AutoAgentSettings,
        environment: Mapping[str, str],
    ) -> None:
        self.project = project
        self.environment = dict(environment)
        self._started = False
        self._closed = False

        required_capabilities = _project_capability_ids(project)
        llm_config = None
        if LLM_CALL_CAPABILITY_ID in required_capabilities:
            llm_config = OpenAICompatibleConfig.from_env(
                env_file=None,
                environ=self.environment,
            )

        self.app = AutoAgentApp(settings=app_settings)
        try:
            if llm_config is not None:
                register_openai_compatible_operator(self.app, llm_config)
            for loaded in project.workflows:
                self.app.register_workflow(loaded.workflow)
        except BaseException:
            self.app.close()
            self._closed = True
            raise

    def workflow(self, workflow_id: str) -> Workflow:
        entry = self.app.workflow_registry.get(workflow_id)
        if entry is None:
            raise KeyError(f"Unknown Workflow: {workflow_id}")
        return entry.workflow

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("ProjectHost is closed.")
        if not self._started:
            await self.app.astart()
            self._started = True

    async def close(self) -> None:
        if not self._closed:
            await self.app.aclose()
            self._closed = True

    async def __aenter__(self) -> ProjectHost:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


def _project_capability_ids(project: ProjectDefinition) -> set[str]:
    capability_ids: set[str] = set()
    visited: set[int] = set()

    def visit(workflow: Workflow) -> None:
        if id(workflow) in visited:
            return
        visited.add(id(workflow))
        for node in workflow.nodes:
            capability = node.capability
            if isinstance(capability, CapabilityRef):
                capability_ids.add(capability.id)
            elif isinstance(capability, str):
                capability_ids.add(capability)
            elif isinstance(capability, Workflow):
                visit(capability)

    for loaded in project.workflows:
        visit(loaded.workflow)
    return capability_ids
