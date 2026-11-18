"""Stable Workspace identities and Session bindings, independent of Core."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from pydantic import field_validator

from ..models._base import Model, NonEmpty

if TYPE_CHECKING:
    from ..skills import Skill
    from ..skills.models import SkillDiagnostic


class WorkspaceError(ValueError):
    """A Workspace is unknown, unavailable, or outside its directory boundary."""


class WorkspaceBinding(Model):
    workspace_id: NonEmpty
    working_directory: str = "."

    @field_validator("working_directory")
    @classmethod
    def _relative_directory(cls, value):
        path = PurePosixPath(value)
        if not value or "\\" in value or "\x00" in value or ":" in value or path.is_absolute() or ".." in path.parts:
            raise ValueError("Working directory must be relative to its Workspace.")
        return path.as_posix()


class WorkspaceInfo(Model):
    id: NonEmpty
    name: NonEmpty
    backend: NonEmpty = "local"


class WorkspaceInstruction(Model):
    path: NonEmpty
    text: str


@dataclass(frozen=True)
class WorkspaceDiscovery:
    """Borrowed resources; serialize instructions and catalogs, never Skill instances."""

    workspace: WorkspaceInfo
    working_directory: str
    instructions: tuple[WorkspaceInstruction, ...] = ()
    skills: tuple[Skill, ...] = ()
    skill_diagnostics: tuple[SkillDiagnostic, ...] = ()


@runtime_checkable
class Workspace(Protocol):
    """Directory-backed resources whose implementation may live outside Host."""

    @property
    def info(self) -> WorkspaceInfo: ...
    async def avalidate(self, working_directory: str = ".") -> None: ...
    async def adiscover(self, working_directory: str = ".") -> WorkspaceDiscovery: ...


@runtime_checkable
class WorkspaceResolver(Protocol):
    """Resolve an authorized stable identity without exposing caller-chosen OS paths."""

    async def aresolve(self, workspace_id: str) -> Workspace: ...
