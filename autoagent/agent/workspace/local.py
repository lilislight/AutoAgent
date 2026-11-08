"""Local directories and bounded discovery of project instructions and Skills."""

import asyncio
from dataclasses import dataclass
from pathlib import Path

from ..skills import SkillLimits, SkillSet
from ..skills.files import identity, read_bytes
from ..skills.errors import SkillError
from .models import WorkspaceBinding, WorkspaceDiscovery, WorkspaceError, WorkspaceInfo, WorkspaceInstruction


@dataclass(frozen=True)
class LocalWorkspace:
    id: str
    root: Path | str
    name: str | None = None

    def __post_init__(self):
        WorkspaceInfo(id=self.id, name=self.name or self.id)
        # Do not resolve symlinks here: later reads must recheck their boundary.
        object.__setattr__(self, "root", Path(self.root).expanduser().absolute())

    @property
    def info(self):
        return WorkspaceInfo(id=self.id, name=self.name or self.id)

    def directory(self, working_directory="."):
        binding = WorkspaceBinding(workspace_id=self.id, working_directory=working_directory)
        try:
            root = self.root.resolve(strict=True)
            directory = (root / binding.working_directory).resolve(strict=True)
            if not root.is_dir() or not directory.is_dir() or not directory.is_relative_to(root):
                raise WorkspaceError("Working directory is outside its Workspace or is not a directory.")
            return root, directory
        except (OSError, RuntimeError) as error:
            raise WorkspaceError(f"Workspace {self.id!r} is unavailable.") from error

    async def avalidate(self, working_directory="."):
        await asyncio.to_thread(self.directory, working_directory)

    def discover(self, working_directory=".") -> WorkspaceDiscovery:
        root, directory = self.directory(working_directory)
        instructions, skills = [], []
        limits = SkillLimits()
        ancestors = [root]
        ancestors.extend(root.joinpath(*directory.relative_to(root).parts[:index])
                         for index in range(1, len(directory.relative_to(root).parts) + 1))
        total = 0
        for current in ancestors:
            instruction = current / "AGENTS.md"
            if instruction.exists() or instruction.is_symlink():
                selected = self._contained(instruction, root)
                try:
                    content = read_bytes(root, selected.relative_to(root).as_posix(), identity(root), limits.max_document_bytes)
                    total += len(content)
                    if len(content) > limits.max_document_bytes or total > 1024 * 1024:
                        raise WorkspaceError("Workspace instructions exceed their size limit.")
                    instructions.append(WorkspaceInstruction(path=instruction.relative_to(root).as_posix(),
                        text=content.decode("utf-8-sig")))
                except (OSError, UnicodeError, SkillError) as error:
                    raise WorkspaceError("Workspace instructions are unreadable.") from error
            skill_root = current / ".agents" / "skills"
            if skill_root.exists() or skill_root.is_symlink():
                selected = self._contained(skill_root, root)
                found = SkillSet.discover([selected])
                if self._contained(skill_root, root) != selected:
                    raise WorkspaceError("Workspace Skill directory changed during discovery.")
                for item in found.metadata:
                    skill = found[item.name]
                    if not skill.source.directory.is_relative_to(root):
                        raise WorkspaceError("Discovered Skill is outside its Workspace.")
                    skills.append(skill.freeze())
        # Keep the same explicit collision rule as Build and Host resources.
        SkillSet(skills)
        return WorkspaceDiscovery(self.info, WorkspaceBinding(workspace_id=self.id,
            working_directory=working_directory).working_directory, tuple(instructions), tuple(skills))

    @staticmethod
    def _contained(path, root):
        try:
            selected = path.resolve(strict=True)
            if not selected.is_relative_to(root):
                raise WorkspaceError("Workspace discovery cannot follow paths outside its root.")
            return selected
        except (OSError, RuntimeError) as error:
            raise WorkspaceError("Workspace discovery path is unavailable.") from error

    async def adiscover(self, working_directory=".") -> WorkspaceDiscovery:
        return await asyncio.to_thread(self.discover, working_directory)
