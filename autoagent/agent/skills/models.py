"""Portable Skill descriptions and loaded content, separate from file handles."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, JsonValue, field_validator, model_validator

from ..models import LLMContent, LLMMessage
from ..models._base import Model, NonEmpty


SkillName = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[a-z0-9]+(-[a-z0-9]+)*$")]
Digest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


class SkillMetadata(Model):
    name: SkillName
    description: Annotated[str, Field(min_length=1, max_length=1024, pattern=r"\S")]
    license: NonEmpty | None = None
    compatibility: Annotated[str, Field(min_length=1, max_length=500, pattern=r"\S")] | None = None
    metadata: dict[str, str] = Field(default_factory=dict)
    allowed_tools: NonEmpty | None = None
    extensions: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _model_invocation_flag(self):
        if "disable-model-invocation" in self.extensions and not isinstance(self.extensions["disable-model-invocation"], bool):
            raise ValueError("disable-model-invocation must be a boolean.")
        return self

    @property
    def model_invocation_enabled(self) -> bool:
        return not self.extensions.get("disable-model-invocation", False)


class SkillLimits(Model):
    max_document_bytes: int = Field(default=256 * 1024, gt=0)
    max_resource_bytes: int = Field(default=8 * 1024 * 1024, gt=0)
    max_resources: int = Field(default=256, gt=0)
    max_scan_entries: int = Field(default=2000, gt=0)
    max_resource_depth: int = Field(default=8, gt=0)
    max_text_bytes: int = Field(default=64 * 1024, gt=0)
    max_text_lines: int = Field(default=200, gt=0)
    max_snapshot_bytes: int = Field(default=32 * 1024 * 1024, gt=0)


class SkillResource(Model):
    path: NonEmpty
    size_bytes: int | None = Field(default=None, ge=0)
    mime_type: NonEmpty

    @field_validator("path")
    @classmethod
    def _relative_path(cls, value: str) -> str:
        from .files import resource_path

        return resource_path(value)


class SkillDocument(Model):
    metadata: SkillMetadata
    instructions: str
    revision: Digest
    snapshot_revision: Digest | None = None

    def to_message(self, *, role: Literal["user", "developer"] = "user") -> LLMMessage:
        from .rendering import document_message

        return document_message(self, role=role)

    def restrict_tools(self, context, *, bindings=None):
        """Narrow a runtime ToolContext using this document's allowed-tools.

        Plain entries are advertised Tool names. Applications must explicitly
        map foreign names or command patterns to executable names; metadata
        cannot grant OS permissions or expand an existing Tool restriction.
        """
        import re
        from ..tools.context import ToolContext

        if not isinstance(context, ToolContext):
            raise TypeError("Skill Tool restrictions require a ToolContext.")
        if self.metadata.allowed_tools is None:
            return context
        names = set()
        for entry in re.split(r"[\s,]+", self.metadata.allowed_tools.strip()):
            if bindings is not None and entry in bindings:
                values = bindings[entry]
                if isinstance(values, str):
                    raise TypeError("Skill Tool bindings require collections of names.")
                names.update(values)
            elif re.fullmatch(r"[A-Za-z0-9_-]{1,64}", entry):
                names.add(entry)
            else:
                raise ValueError(f"Skill Tool entry requires an explicit application binding: {entry!r}.")
        return context.restrict(names)


class SkillResourceContent(Model):
    skill_name: SkillName
    resource: SkillResource
    revision: Digest
    snapshot_revision: Digest | None = None
    content: tuple[LLMContent, ...] = Field(min_length=1)


class SkillResourcePage(Model):
    resources: tuple[SkillResource, ...] = ()
    revision: Digest
    snapshot_revision: Digest | None = None
    next_cursor: NonEmpty | None = None


class SkillTextContent(Model):
    skill_name: SkillName
    resource: SkillResource
    start_line: int = Field(ge=1)
    end_line: int = Field(ge=0)
    next_line: int | None = Field(default=None, ge=1)
    text: str
    revision: Digest
    snapshot_revision: Digest | None = None

    @model_validator(mode="after")
    def _range(self):
        if self.end_line < self.start_line - 1 or (self.next_line is not None and self.next_line != self.end_line + 1):
            raise ValueError("Invalid text line range.")
        return self


class SkillDiagnostic(Model):
    location: NonEmpty
    code: NonEmpty
    message: NonEmpty
    field: str | None = None
    line: int | None = Field(default=None, ge=1)


class SkillScriptSpec(Model):
    skill_name: SkillName
    path: NonEmpty
    command: tuple[NonEmpty, ...] = Field(min_length=1)
    resources: tuple[str, ...] = ()

    @field_validator("command")
    @classmethod
    def _command(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any("\x00" in part for part in value):
            raise ValueError("Script commands cannot contain NUL characters.")
        return value

    @field_validator("path")
    @classmethod
    def _script_path(cls, value: str) -> str:
        from .files import resource_path

        resource_path(value)
        if not value.startswith("scripts/"):
            raise ValueError("Script bindings must select a file under scripts/.")
        return value

    @field_validator("resources")
    @classmethod
    def _resource_paths(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        from .files import resource_path

        for path in value:
            resource_path(path)
        return value


class SkillScriptResult(Model):
    skill_name: SkillName
    path: NonEmpty
    revision: Digest
    snapshot_revision: Digest | None = None
    returncode: int
    stdout: str
    stderr: str


__all__ = [
    "SkillMetadata", "SkillLimits", "SkillResource", "SkillDocument", "SkillResourceContent",
    "SkillResourcePage", "SkillTextContent", "SkillDiagnostic", "SkillScriptSpec", "SkillScriptResult",
]
