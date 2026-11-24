"""The product's resource view, consumed through the ordinary SDK registry API."""

from contextlib import asynccontextmanager
from autoagent.agent.runtime.resources import AgentResources


class ApplicationResources(AgentResources):
    def __init__(self, service, **configuration):
        self.service = service
        from autoagent.agent.plan import PlanTools
        from autoagent.agent.plan import is_plan_tool
        tools = tuple(configuration.get("tools", ()))
        configuration["tools"] = tools if any(is_plan_tool(item) for item in tools) else (*tools, *PlanTools().tools())
        super().__init__(**configuration)

    def snapshot(self, context=None):
        if context is None:
            return self.catalog()
        service = self.service
        options = service.workflow_options(context)
        workflow_id = options.workflow_id
        from .workflow import BUILTIN_AGENT_WORKFLOW_ID
        if workflow_id != BUILTIN_AGENT_WORKFLOW_ID:
            raise ValueError("Application resources are restricted to the Agent application.")
        with self._lock:
            tools, skills = self.catalog()
            workspace_tools, workspace_skills = service._workspaces.catalog(context, workflow_id)
            tools, skills = (*tools, *workspace_tools), (*skills, *workspace_skills)
            managed_tools, managed_skills = self.managed_catalog(options, tools, skills)
            return ((*tools, *managed_tools), service.shared_resources.filter((*skills, *managed_skills)))

    def managed_catalog(self, options, tools, skills):
        names = {"ask_user"} if options.include_ask_user else set()
        if options.subagents.enabled:
            from autoagent.agent.workflow.builtins import _resources
            names.update(_resources(options.subagents.model_dump_json()))
        return self.service.shared_resources.catalog((tools, skills), tool_names=names)

    def filter_tools(self, tools, *, context=None):
        return self.service.shared_resources.filter(tools)

    @asynccontextmanager
    async def borrow(self, resource_id):
        async with super().borrow(resource_id), self.service.shared_resources.borrow(resource_id):
            yield
