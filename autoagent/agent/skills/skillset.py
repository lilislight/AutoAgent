"""Explicit Skill discovery, diagnostics and model-access filtering."""

from __future__ import annotations

from collections.abc import Collection, Iterable
import os
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal

from ..models import LLMMessage
from .errors import SkillAccessError, SkillError, SkillLimitError, SkillNotFoundError, SkillResourceError
from .models import (
    SkillDiagnostic, SkillDocument, SkillLimits, SkillMetadata,
    SkillResourceContent, SkillResourcePage, SkillTextContent,
)
from .skill import Skill
from .sources import SkillSource

if TYPE_CHECKING:
    from ..tools import Tool
    from .runner import SkillScriptRunner


def _diagnostic(location: str, error: SkillError) -> SkillDiagnostic:
    return SkillDiagnostic(location=location, code=type(error).__name__, message=str(error),
                           field=getattr(error, "field", None), line=getattr(error, "line", None))


class SkillSet:
    """Fixed catalog; manual APIs and model-facing capabilities have separate access."""

    def __init__(self, skills: Iterable[Skill] = (), *, diagnostics: Iterable[SkillDiagnostic] = ()):
        registered = {}
        for skill in skills:
            if not isinstance(skill, Skill):
                raise TypeError("SkillSet accepts Skill objects.")
            name = skill.metadata.name
            if name in registered:
                raise SkillError(f"Duplicate Skill name: {name!r}.")
            registered[name] = skill
        self._skills = MappingProxyType(registered)
        self.diagnostics = tuple(SkillDiagnostic.model_validate(value) for value in diagnostics)

    @classmethod
    def from_sources(cls, sources: Iterable[SkillSource], *, limits: SkillLimits | None = None,
                     errors: Literal["raise", "skip"] = "raise") -> SkillSet:
        if errors not in {"raise", "skip"}:
            raise ValueError("Skill discovery errors must be 'raise' or 'skip'.")
        found, diagnostics, names = [], [], set()
        for source in sources:
            try:
                skill = Skill.from_source(source, limits=limits)
                if skill.metadata.name in names:
                    raise SkillError(f"Duplicate Skill name: {skill.metadata.name!r}.")
                names.add(skill.metadata.name)
                found.append(skill)
            except SkillError as error:
                if errors == "raise":
                    raise
                diagnostics.append(_diagnostic(source.location, error))
        return cls(found, diagnostics=diagnostics)

    @classmethod
    def discover(cls, roots: Iterable[str | Path], *, limits: SkillLimits | None = None,
                 errors: Literal["raise", "skip"] = "raise") -> SkillSet:
        """Scan direct children of roots; skipped failures retain their locations.

        Strict mode fails immediately. Skip mode keeps the first valid duplicate
        and reports subsequent collisions. Limits stop scanning rather than being
        bypassed by skip mode. Import/dependency failures always propagate.
        """
        if isinstance(roots, (str, Path)):
            raise TypeError("Skill discovery takes a collection of roots.")
        if errors not in {"raise", "skip"}:
            raise ValueError("Skill discovery errors must be 'raise' or 'skip'.")
        limits = SkillLimits.model_validate(limits or SkillLimits())
        found, diagnostics, names, seen_roots = [], [], set(), set()
        count = 0
        for configured in roots:
            try:
                root = Path(configured).resolve(strict=True)
                if root in seen_roots:
                    continue
                seen_roots.add(root)
                entries = []
                with os.scandir(root) as scan:
                    for entry in scan:
                        count += 1
                        if count > limits.max_scan_entries:
                            raise SkillLimitError("Skill discovery exceeds its entry limit.")
                        entries.append(entry)
            except (OSError, RuntimeError, SkillError) as error:
                failure = error if isinstance(error, SkillError) else SkillResourceError("Configured Skill root is inaccessible.")
                if errors == "raise":
                    raise failure from error if failure is not error else None
                diagnostics.append(_diagnostic(str(configured), failure))
                if isinstance(failure, SkillLimitError):
                    break
                continue
            for entry in sorted(entries, key=lambda value: value.name):
                marker = Path(entry.path) / "SKILL.md"
                try:
                    if not entry.is_dir(follow_symlinks=False) or not (marker.exists() or marker.is_symlink()):
                        continue
                    skill = Skill.from_directory(entry.path, limits=limits)
                    if skill.metadata.name in names:
                        raise SkillError(f"Duplicate Skill name: {skill.metadata.name!r}.")
                    found.append(skill)
                    names.add(skill.metadata.name)
                except (OSError, SkillError) as error:
                    failure = error if isinstance(error, SkillError) else SkillResourceError("Skill directory is inaccessible.")
                    if errors == "raise":
                        raise failure from error if failure is not error else None
                    diagnostics.append(_diagnostic(str(marker), failure))
        return cls(found, diagnostics=diagnostics)

    def __len__(self) -> int:
        return len(self._skills)

    def __getitem__(self, name: str) -> Skill:
        try:
            return self._skills[name]
        except KeyError as error:
            raise SkillNotFoundError(f"Unknown Skill: {name!r}.") from error

    @property
    def metadata(self) -> tuple[SkillMetadata, ...]:
        return tuple(skill.metadata for skill in self._skills.values())

    @property
    def model_metadata(self) -> tuple[SkillMetadata, ...]:
        return tuple(value for value in self.metadata if value.model_invocation_enabled)

    def select(self, *, include: Collection[str] | None = None, exclude: Collection[str] = ()) -> SkillSet:
        allowed = None if include is None else frozenset(include)
        denied = frozenset(exclude)
        if allowed is not None and (missing := allowed - self._skills.keys()):
            raise SkillNotFoundError(f"Unknown included Skills: {sorted(missing)!r}.")
        return SkillSet((value for name, value in self._skills.items()
                         if (allowed is None or name in allowed) and name not in denied), diagnostics=self.diagnostics)

    def for_model(self) -> SkillSet:
        return self.select(include=tuple(value.name for value in self.model_metadata))

    def freeze(self) -> SkillSet:
        """Freeze each Skill version before exposing model-facing tools."""
        return SkillSet((skill.freeze() for skill in self._skills.values()), diagnostics=self.diagnostics)

    def require_model_access(self, name: str) -> Skill:
        skill = self[name]
        # Metadata edits cannot turn a stale catalog into new permissions.
        document = skill.load()
        if not document.metadata.model_invocation_enabled:
            raise SkillAccessError("Skill is available only for application-directed use.")
        return skill

    def catalog_message(self) -> LLMMessage | None:
        from .rendering import catalog_message

        return catalog_message(self.model_metadata)

    def load(self, name: str) -> SkillDocument:
        return self[name].load()

    def list_resources(self, name: str, *, prefix: str | None = None, cursor: str | None = None,
                       limit: int | None = None) -> SkillResourcePage:
        return self[name].list_resources(prefix=prefix, cursor=cursor, limit=limit)

    def read_resource(self, name: str, path: str) -> SkillResourceContent:
        return self[name].read_resource(path)

    def read_text(self, name: str, path: str, *, start_line: int = 1,
                  max_lines: int | None = None, max_bytes: int | None = None) -> SkillTextContent:
        return self[name].read_text(path, start_line=start_line, max_lines=max_lines, max_bytes=max_bytes)

    def as_tools(self, *, prefix: str = "skill_", runner: SkillScriptRunner | None = None) -> tuple[Tool, ...]:
        from .tools import skill_tools

        return skill_tools(self, prefix=prefix, runner=runner)


__all__ = ["SkillSet"]
