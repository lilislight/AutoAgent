"""Typed application-directed Skill operations usable directly as Core Nodes."""

from __future__ import annotations

from typing import Literal

from autoagent.core import Operator, run_sync

from ..models._base import Model, NonEmpty
from .models import SkillDocument, SkillResourceContent, SkillResourcePage, SkillTextContent, SkillScriptResult
from .skillset import SkillSet
from .runner import SkillScriptRunner
from .files import resource_path
from .errors import SkillScriptError


class SkillLoadRequest(Model):
    name: NonEmpty


class SkillListResourcesRequest(SkillLoadRequest):
    prefix: str | None = None
    cursor: str | None = None
    limit: int | None = None


class SkillReadResourceRequest(SkillLoadRequest):
    path: NonEmpty


class SkillReadTextRequest(SkillReadResourceRequest):
    start_line: int = 1
    max_lines: int | None = None
    max_bytes: int | None = None


class SkillRunScriptRequest(SkillReadResourceRequest):
    arguments: tuple[str, ...] = ()
    stdin: str | None = None
    timeout_seconds: float | None = None


class SkillOperations:
    """Borrow Skills/runner; access is application-directed, including disabled catalog entries.

    No model activation, prompt injection or permissions are changed. Existing
    Skill read limits and script bindings remain enforced by those resources.
    """

    def __init__(self, skills: SkillSet, *, runner: SkillScriptRunner | None = None):
        if not isinstance(skills, SkillSet):
            raise TypeError("SkillOperations requires a SkillSet.")
        if runner is not None and not isinstance(runner, SkillScriptRunner):
            raise TypeError("Runner must implement SkillScriptRunner.")
        self.skills = skills
        self.runner = runner

    async def aload(self, request: SkillLoadRequest) -> SkillDocument:
        request = SkillLoadRequest.model_validate(request)
        return await run_sync(self.skills.load, request.name)

    async def alist_resources(self, request: SkillListResourcesRequest) -> SkillResourcePage:
        request = SkillListResourcesRequest.model_validate(request)
        return await run_sync(self.skills.list_resources, **request.model_dump(mode="python"))

    async def aread_resource(self, request: SkillReadResourceRequest) -> SkillResourceContent:
        request = SkillReadResourceRequest.model_validate(request)
        return await run_sync(self.skills.read_resource, request.name, request.path)

    async def aread_text(self, request: SkillReadTextRequest) -> SkillTextContent:
        request = SkillReadTextRequest.model_validate(request)
        return await run_sync(self.skills.read_text, **request.model_dump(mode="python"))

    async def arun_script(self, request: SkillRunScriptRequest) -> SkillScriptResult:
        request = SkillRunScriptRequest.model_validate(request)
        resource_path(request.path)
        if not request.path.startswith("scripts/"):
            raise SkillScriptError("Skill execution paths must be under scripts/.")
        if self.runner is None:
            raise RuntimeError("No Skill script runner is configured.")
        result = await self.runner.arun(self.skills[request.name], request.path, arguments=request.arguments,
                                       stdin=request.stdin, timeout_seconds=request.timeout_seconds)
        result = SkillScriptResult.model_validate(result)
        if result.skill_name != request.name or result.path != request.path:
            raise ValueError("Runner result does not match its script request.")
        return result

    def as_operator(self, operation: Literal["load", "list_resources", "read_resource", "read_text", "run_script"], *, id: str | None = None) -> Operator:
        if operation not in {"load", "list_resources", "read_resource", "read_text", "run_script"}:
            raise ValueError("Unknown Skill operation.")
        if operation == "run_script" and self.runner is None:
            raise ValueError("Configure a runner before adapting run_script.")
        return Operator(getattr(self, "a" + operation), id=id or "skill." + operation)


__all__ = ["SkillLoadRequest", "SkillListResourcesRequest", "SkillReadResourceRequest", "SkillReadTextRequest",
           "SkillRunScriptRequest", "SkillOperations"]
