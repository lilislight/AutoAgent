"""Durable scheduling identities stored inside Runtime State."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, order=True, slots=True)
class LoopIteration:
    loop_region_id: str
    iteration: int


ExecutionScope = tuple[LoopIteration, ...]


def scope_key(scope: ExecutionScope) -> str:
    if not scope:
        return "root"
    return "/".join(f"{item.loop_region_id}:{item.iteration}" for item in scope)


def occurrence_key(node_id: str, scope: ExecutionScope = ()) -> str:
    return f"{node_id}@{scope_key(scope)}"


def current_occurrence_key(scheduler, node_id: str, scope: ExecutionScope = ()) -> str:
    key = occurrence_key(node_id, scope)
    return scheduler.occurrence_ids.get(key, key)


def resolution_key(edge_id: str, target_scope: ExecutionScope = ()) -> str:
    return f"{edge_id}@{scope_key(target_scope)}"


def boundary_resolution_key(
    loop_region_id: str, edge_id: str, loop_scope: ExecutionScope
) -> str:
    return f"boundary:{loop_region_id}:{edge_id}@{scope_key(loop_scope)}"


def boundary_key(loop_region_id: str, loop_scope: ExecutionScope) -> str:
    return f"boundary:{loop_region_id}@{scope_key(loop_scope)}"


@dataclass(frozen=True, slots=True)
class Activation:
    edge_id: str
    source_occurrence_id: str
    target_node_id: str


@dataclass(frozen=True, slots=True)
class EdgeResolution:
    edge_id: str
    target_node_id: str
    target_scope: ExecutionScope
    selected: bool
    activation: Activation | None = None

    def __post_init__(self) -> None:
        if self.selected != (self.activation is not None):
            raise ValueError("Selected Edge Resolution requires exactly one Activation.")

    @property
    def id(self) -> str:
        return resolution_key(self.edge_id, self.target_scope)


@dataclass(frozen=True, slots=True)
class LoopBoundaryResolution:
    loop_region_id: str
    loop_scope: ExecutionScope
    edge_id: str
    source_scope: ExecutionScope
    target_node_id: str
    selected: bool
    activation: Activation | None = None

    def __post_init__(self) -> None:
        if self.selected != (self.activation is not None):
            raise ValueError("Selected Loop boundary requires exactly one Activation.")

    @property
    def id(self) -> str:
        return boundary_resolution_key(
            self.loop_region_id, self.edge_id, self.loop_scope
        )


@dataclass(frozen=True, slots=True)
class OccurrencePlan:
    id: str
    node_id: str
    scope: ExecutionScope
    resolution_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SchedulerDelta:
    resolutions: tuple[EdgeResolution, ...] = ()
    boundary_resolutions: tuple[LoopBoundaryResolution, ...] = ()
    closed_boundaries: tuple[str, ...] = ()
    consumed_resolution_ids: tuple[str, ...] = ()
    ready: tuple[OccurrencePlan, ...] = ()
    skipped: tuple[OccurrencePlan, ...] = ()
    revived: tuple[OccurrencePlan, ...] = ()
