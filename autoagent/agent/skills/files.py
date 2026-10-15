"""Constrained local resource paths and bounded regular-file reads."""

from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path, PurePosixPath
import stat

from .errors import SkillChangedError, SkillLimitError, SkillResourceError


def resource_path(value: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\\" in value or "\x00" in value:
        raise SkillResourceError("Resource path must be a nonempty relative POSIX path.")
    try:
        value.encode("utf-8")
    except UnicodeError as error:
        raise SkillResourceError("Resource paths must be valid UTF-8.") from error
    parts = value.split("/")
    if PurePosixPath(value).is_absolute() or any(not part.strip() or part in {".", ".."} for part in parts) or ":" in parts[0]:
        raise SkillResourceError("Resource paths cannot be absolute or traverse directories.")
    return value


def identity(path: Path) -> tuple[int, int]:
    value = path.stat()
    if not stat.S_ISDIR(value.st_mode):
        raise SkillResourceError("Skill root must be a directory.")
    return value.st_dev, value.st_ino


def resolve_resource(root: Path, relative: str) -> Path:
    resource_path(relative)
    try:
        path = (root / relative).resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise SkillResourceError("Skill resource is missing or inaccessible.") from error
    if not path.is_relative_to(root):
        raise SkillResourceError("Skill resource resolves outside its directory.")
    return path


@contextmanager
def open_resource(root: Path, relative: str, root_identity: tuple[int, int]):
    path = resolve_resource(root, relative)
    descriptors = []
    try:
        if os.open in os.supports_dir_fd and hasattr(os, "O_NOFOLLOW"):
            directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            current = os.open(root, directory_flags)
            descriptors.append(current)
            status = os.fstat(current)
            if (status.st_dev, status.st_ino) != root_identity:
                raise SkillChangedError("Skill root changed; rediscover the Skill.")
            parts = path.relative_to(root).parts
            if not parts:
                raise SkillResourceError("Skill resource must be a regular file.")
            for part in parts[:-1]:
                current = os.open(part, directory_flags, dir_fd=current)
                descriptors.append(current)
            descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=current)
            descriptors.append(descriptor)
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise SkillResourceError("Skill resource must be a regular file.")
            # fdopen owns its duplicate; all traversal descriptors close below.
            with os.fdopen(os.dup(descriptor), "rb") as stream:
                yield stream
        else:
            # Portable fallback for application-owned directories. Descriptor
            # traversal above additionally protects POSIX readers from link swaps.
            if identity(root) != root_identity:
                raise SkillChangedError("Skill root changed; rediscover the Skill.")
            if not path.is_file():
                raise SkillResourceError("Skill resource must be a regular file.")
            with path.open("rb") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise SkillResourceError("Skill resource must be a regular file.")
                yield stream
    except OSError as error:
        raise SkillResourceError("Skill resource is missing or inaccessible.") from error
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def read_bytes(root: Path, relative: str, root_identity: tuple[int, int], maximum: int) -> bytes:
    with open_resource(root, relative, root_identity) as stream:
        if os.fstat(stream.fileno()).st_size > maximum:
            raise SkillLimitError("Skill file exceeds its configured byte limit.")
        value = stream.read(maximum + 1)
    if len(value) > maximum:
        raise SkillLimitError("Skill file exceeds its configured byte limit.")
    return value
