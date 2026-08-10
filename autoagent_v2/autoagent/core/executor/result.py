"""Detached results returned from Node workers to the Invocation coordinator."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from ..workflow import ContextPatch


@dataclass(frozen=True, slots=True)
class NodePhaseResult:
    name: str
    status: Literal["completed", "failed"]
    started_at_ms: int
    completed_at_ms: int
    duration_ns: int
    executor_wait_ns: int = 0
    thread_pool_wait_ns: int = 0
    handler_ns: int = 0
    payload: Any = None


@dataclass(slots=True)
class NodeExecutionResult:
    mapped_input: Any = None
    output: Any = None
    patch: ContextPatch = field(default_factory=ContextPatch)
    waiting: bool = False
    wait_payload: Any = None
    error: BaseException | None = None
    cancelled: bool = False
    deferred_phases: list[NodePhaseResult] = field(default_factory=list)
