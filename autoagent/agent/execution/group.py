"""Ordinary command Tools; a working directory does not require an App Workspace."""

from contextlib import asynccontextmanager
from pathlib import Path
from ..tools.group import ToolGroup
from .service import LocalExecutionService
from .tools import execution_tools


class CommandTools(ToolGroup):
    group_id = 'commands'

    def __init__(self, cwd=None, *, service=None, config=None, manager=None, **adapters):
        if service is not None and cwd is not None:
            raise ValueError('Pass cwd or an ExecutionService, not both.')
        self.service = service if service is not None else LocalExecutionService(
            cwd if cwd is not None else Path.cwd(), config=config, manager=manager, **adapters)
        self._owns_manager = service is None and manager is None
        self._tools = execution_tools(self.service)
        self._bind_group()

    async def aclose(self):
        if self._owns_manager:
            await self.service.manager.aclose()

    @asynccontextmanager
    async def lifespan(self):
        try:
            yield self
        finally:
            await self.aclose()
