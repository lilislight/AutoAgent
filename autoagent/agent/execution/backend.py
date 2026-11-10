"""Backend seam for pipes now and optional terminal/remote adapters later."""

from dataclasses import dataclass
from typing import Mapping, Protocol


@dataclass(frozen=True)
class ExecutionCapabilities:
    stdio_modes: tuple[str, ...] = ("pipe",)
    persistent_shell: bool = False
    process_tree_cleanup: str = "best_effort"
    output_logs: bool = False


class ExecutionOutputLog(Protocol):
    """Runtime-owned bounded capture; adapters must close it on exit or failed start."""

    def write(self, data: bytes) -> None: ...
    def close(self) -> None: ...
    def summary(self) -> dict: ...


@dataclass(frozen=True)
class ResolvedExecution:
    argv: tuple[str, ...]
    cwd: str
    environment: Mapping[str, str]
    timeout_seconds: float
    max_retained_bytes: int
    keep_stdin_open: bool = False
    stdin: bytes | None = None
    output_log: ExecutionOutputLog | None = None


class ProcessHandle(Protocol):
    state: str
    async def read(self, cursor: int | None, *, max_bytes: int, wait_seconds: float) -> dict: ...
    async def write(self, data: bytes, *, close_stdin: bool = False) -> None: ...
    async def wait(self) -> None: ...
    async def terminate(self) -> None: ...


class ExecutionBackend(Protocol):
    capabilities: ExecutionCapabilities
    async def spawn(self, spec: ResolvedExecution) -> ProcessHandle: ...


__all__ = ["ExecutionOutputLog", "ExecutionCapabilities", "ResolvedExecution", "ProcessHandle", "ExecutionBackend"]
