"""Detached values returned by the transient execution layer."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExecutionMetrics:
    duration_ns: int
    call_count: int
    peak_parallelism: int


@dataclass(frozen=True, slots=True)
class NodeExecutionResult:
    output: object
    metrics: ExecutionMetrics
