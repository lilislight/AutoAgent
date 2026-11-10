"""Model-facing Skill reads and optional runner calls on the existing Tool boundary."""

from __future__ import annotations

import asyncio
from typing import Annotated, TYPE_CHECKING

from pydantic import Field

from ..models import LLMToolDefinition, TextContent, ToolOutput
from ..tools import Tool, ToolBusinessError
from .errors import SkillError, SkillResourceError, SkillScriptError
from .files import resource_path
from .sources import is_text
from .models import SkillScriptResult
from .rendering import document_message

if TYPE_CHECKING:
    from .skillset import SkillSet
    from .runner import SkillScriptRunner


class _SkillTool(Tool):
    def __init__(self, handler, *, name: str, skills: SkillSet):
        super().__init__(handler, name=name)
        self._names = tuple(value.name for value in skills.model_metadata)
        # Schema construction uses the catalog's configured bounds, without
        # reading mutable resources. Handlers revalidate access inside their
        # SkillError boundary so changes become model-visible Tool results.
        self._limits = tuple(skills[name].limits for name in self._names)

    @property
    def definition(self) -> LLMToolDefinition:
        value = super().definition
        props = value.parameters["properties"]
        props["name"]["enum"] = list(self._names)
        for parameter, limit in (("limit", "max_resources"), ("max_lines", "max_text_lines"), ("max_bytes", "max_text_bytes")):
            if parameter in props:
                props[parameter]["anyOf"][0]["maximum"] = max(getattr(item, limit) for item in self._limits)
        details = (" Page sizes are upper bounds and are clamped to the selected Skill's configured limits; omit them to use its defaults."
                   if any(name in props for name in ("limit", "max_lines", "max_bytes")) else "")
        if "cursor" in props:
            details += (f" Resource scans are bounded by the selected Skill (up to {max(item.max_scan_entries for item in self._limits)} entries,"
                        f" depth {max(item.max_resource_depth for item in self._limits)}); narrow prefix if a scan limit is reached.")
        return value.model_copy(update={"description": value.description + details})


def skill_tools(skills: SkillSet, *, prefix: str, runner: SkillScriptRunner | None = None) -> tuple[Tool, ...]:
    if not skills.model_metadata:
        return ()

    def load(name: str) -> ToolOutput:
        """Load Skill instructions before using them; list supporting files separately."""
        try:
            skill = skills.require_model_access(name)
            document = skill.load()
            return ToolOutput(content=document_message(document, include_revisions=False).content, tool_metadata={"skill": {
                "name": name, "revision": document.revision, "operation": "load", "snapshot_revision": document.snapshot_revision,
            }})
        except SkillError as error:
            raise ToolBusinessError(str(error), code=type(error).__name__) from error

    def list_resources(name: str, prefix: str | None = None, cursor: str | None = None,
                       limit: Annotated[int | None, Field(gt=0)] = None) -> ToolOutput:
        """List a page of Skill resources; next_cursor continues an unchanged listing."""
        try:
            skill = skills.require_model_access(name)
            page = skill.list_resources(prefix=prefix, cursor=cursor,
                limit=min(limit, skill.limits.max_resources) if limit is not None else None)
            return ToolOutput(structured_content={"skill_name": name,
                "resources": [resource.model_dump(mode="json", exclude_none=True) for resource in page.resources],
                **({"next_cursor": page.next_cursor} if page.next_cursor is not None else {})},
                tool_metadata={"skill": {"name": name, "revision": page.revision,
                    "snapshot_revision": page.snapshot_revision, "operation": "list_resources"}})
        except SkillError as error:
            raise ToolBusinessError(str(error), code=type(error).__name__) from error

    def read_resource(name: str, path: str, start_line: Annotated[int, Field(gt=0)] = 1,
                      max_lines: Annotated[int | None, Field(gt=0)] = None,
                      max_bytes: Annotated[int | None, Field(gt=0)] = None) -> ToolOutput:
        """Read a Skill resource. Text is paged by lines with next_line for continuation; other resources retain their media type. Script source is never executed."""
        try:
            skill = skills.require_model_access(name)
            info = skill.source.resource_info(resource_path(path))
            if is_text(info.mime_type):
                value = skill.read_text(path, start_line=start_line,
                    max_lines=min(max_lines, skill.limits.max_text_lines) if max_lines is not None else None,
                    max_bytes=min(max_bytes, skill.limits.max_text_bytes) if max_bytes is not None else None)
                return ToolOutput(content=(TextContent(text=value.text),), structured_content={"skill_name": name,
                    "path": path, "start_line": value.start_line, "end_line": value.end_line, "next_line": value.next_line},
                    tool_metadata={"skill": {"name": name, "path": path, "revision": value.revision, "mime_type": value.resource.mime_type,
                        "snapshot_revision": value.snapshot_revision, "operation": "read_resource"}})
            if start_line != 1 or max_lines is not None or max_bytes is not None:
                raise SkillResourceError("Line and text byte limits apply only to text resources.")
            value = skill.read_resource(path)
            return ToolOutput(content=value.content, tool_metadata={"skill": {
                "name": name, "path": value.resource.path,
                "revision": value.revision, "mime_type": value.resource.mime_type,
                "snapshot_revision": value.snapshot_revision,
                "operation": "read_resource",
            }})
        except SkillError as error:
            raise ToolBusinessError(str(error), code=type(error).__name__) from error

    handlers = [("load", load), ("list_resources", list_resources),
                ("read_resource", read_resource)]
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
