"""Portable file configuration and model-facing mutation arguments."""

from typing import Annotated, Literal
from pydantic import Field
from ..models._base import Model, NonEmpty
from ..tools.tool_policy import ToolPolicy


class FileToolsConfig(Model):
    policy: ToolPolicy = Field(default_factory=ToolPolicy)
    max_file_bytes: int = Field(default=8 * 1024 * 1024, gt=0)
    max_output_bytes: int = Field(default=64 * 1024, gt=0)
    max_entries: int = Field(default=10000, gt=0)
    max_scan_bytes: int = Field(default=64 * 1024 * 1024, gt=0)
    max_depth: int = Field(default=32, ge=0, le=128)
    generated_directories: tuple[str, ...] = (".venv", "venv", "node_modules", "__pycache__")
    regex_timeout_seconds: float = Field(default=0.1, gt=0, le=10)
    search_timeout_seconds: float = Field(default=5, gt=0, le=60)


class FileEdit(Model):
    old_text: Annotated[str, Field(min_length=1, description="Exact original text, including whitespace, without read_file line labels. Include surrounding lines if it occurs more than once.")]
    new_text: str = Field(description="Replacement text, or an empty string to remove the match.")
    replace_all: bool = Field(default=False, description="Explicitly replace every non-overlapping occurrence of old_text; otherwise it must match exactly once.")


class FileStat(Model):
    operation: Literal["stat"]
    path: NonEmpty


class FileMkdir(Model):
    operation: Literal["mkdir"]
    path: NonEmpty
    parents: bool = False


class FileTransfer(Model):
    operation: Literal["move", "copy"]
    path: NonEmpty
    destination: NonEmpty
    expected_version: str | None = Field(default=None, description="Optional source version for move. When supplied it must match. Destination is never overwritten.")


class FileDelete(Model):
    operation: Literal["delete"]
    path: NonEmpty
    expected_version: str | None = Field(default=None, description="Optional file version; when supplied it must match. Only empty directories can be deleted; this operation is never recursive.")


FileManagement = Annotated[FileStat | FileMkdir | FileTransfer | FileDelete, Field(discriminator="operation")]

__all__ = ["FileToolsConfig", "FileEdit", "FileManagement"]
