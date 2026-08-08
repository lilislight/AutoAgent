"""Detached results returned from Node workers to the Invocation coordinator."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from ..runtime import OperatorCallRecord
from ..workflow import ContextPatch


@dataclass(frozen=True, slots=True)
class NodePhaseResult:
    name: str
    status: Literal["completed", "failed"]
    duration_ns: int
    payload: Any = None


@dataclass(slots=True)
class NodeExecutionResult:
    mapped_input: Any = None
    output: Any = None
    patch: ContextPatch = field(default_factory=ContextPatch)
    waiting: bool = False
    wait_payload: Any = None
    phases: list[NodePhaseResult] = field(default_factory=list)
    operator_calls: list[OperatorCallRecord] = field(default_factory=list)
    error: BaseException | None = None
    cancelled: bool = False
