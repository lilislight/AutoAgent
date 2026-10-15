"""Model-facing Skill reads and optional runner calls on the existing Tool boundary."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from ..models import LLMToolDefinition, ToolOutput
from ..tools import Tool, ToolBusinessError
from .errors import SkillError, SkillScriptError
from .files import resource_path
from .models import SkillScriptResult
from .rendering import document_message

if TYPE_CHECKING:
    from .skillset import SkillSet
    from .runner import SkillScriptRunner


class _SkillTool(Tool):
    def __init__(self, handler, *, name: str, skills: SkillSet):
        super().__init__(handler, name=name)
        self._names = tuple(value.name for value in skills.model_metadata)

    @property
    def definition(self) -> LLMToolDefinition:
        value = super().definition
        value.parameters["properties"]["name"]["enum"] = list(self._names)
        return value


def skill_tools(skills: SkillSet, *, prefix: str, runner: SkillScriptRunner | None = None) -> tuple[Tool, ...]:
    if not skills.model_metadata:
        return ()

    def load(name: str) -> ToolOutput:
        """Load Skill instructions before using them; list supporting files separately."""
        try:
            skill = skills.require_model_access(name)
            document = skill.load()
            return ToolOutput(content=document_message(document).content, tool_metadata={"skill": {
                "name": name, "revision": document.revision, "operation": "load", "snapshot_revision": document.snapshot_revision,
            }})
        except SkillError as error:
            raise ToolBusinessError(str(error), code=type(error).__name__) from error

    def list_resources(name: str, prefix: str | None = None, cursor: str | None = None, limit: int | None = None) -> ToolOutput:
        """List a page of Skill resources; next_cursor continues an unchanged listing."""
        try:
            skill = skills.require_model_access(name)
            page = skill.list_resources(prefix=prefix, cursor=cursor, limit=limit)
            return ToolOutput(structured_content={"skill_name": name, **page.model_dump(mode="json")})
        except SkillError as error:
            raise ToolBusinessError(str(error), code=type(error).__name__) from error

    def read_resource(name: str, path: str) -> ToolOutput:
        """Read one bounded resource; script source is returned without execution."""
        try:
            skill = skills.require_model_access(name)
            value = skill.read_resource(path)
            return ToolOutput(content=value.content, tool_metadata={"skill": {
                "name": name, "path": value.resource.path,
                "revision": value.revision, "mime_type": value.resource.mime_type,
                "snapshot_revision": value.snapshot_revision,
                "operation": "read_resource",
            }})
        except SkillError as error:
            raise ToolBusinessError(str(error), code=type(error).__name__) from error

    def read_text(name: str, path: str, start_line: int = 1, max_lines: int | None = None, max_bytes: int | None = None) -> ToolOutput:
        """Read complete UTF-8 lines; next_line continues with bounded scan/output sizes."""
        try:
            skill = skills.require_model_access(name)
            value = skill.read_text(path, start_line=start_line, max_lines=max_lines, max_bytes=max_bytes)
            return ToolOutput(structured_content=value.model_dump(mode="json"))
        except SkillError as error:
            raise ToolBusinessError(str(error), code=type(error).__name__) from error

    handlers = [("load", load), ("list_resources", list_resources),
                ("read_resource", read_resource), ("read_text", read_text)]
    if runner is not None:
        from .runner import SkillScriptRunner
        if not isinstance(runner, SkillScriptRunner):
            raise TypeError("Skill runner must implement SkillScriptRunner.")

        async def run_script(name: str, path: str, arguments: tuple[str, ...] = (), stdin: str | None = None,
                             timeout_seconds: float | None = None) -> ToolOutput:
            """Execute an application-bound script with bounded arguments, stdin and timeout."""
            try:
                resource_path(path)
                if not path.startswith("scripts/"):
                    raise SkillScriptError("Skill execution paths must be under scripts/.")
                skill = await asyncio.to_thread(skills.require_model_access, name)
                result = await runner.arun(skill, path, arguments=arguments, stdin=stdin, timeout_seconds=timeout_seconds)
                result = SkillScriptResult.model_validate(result)
                if result.skill_name != name or result.path != path:
                    raise ValueError("Runner result does not match its script request.")
                return ToolOutput(structured_content=result.model_dump(mode="json"), is_error=result.returncode != 0,
                                  tool_metadata={"skill": {"name": name, "path": path, "operation": "run_script"}})
            except SkillError as error:
                raise ToolBusinessError(str(error), code=type(error).__name__) from error

        handlers.append(("run_script", run_script))
    return tuple(_SkillTool(handler, name=prefix + suffix, skills=skills) for suffix, handler in handlers)
