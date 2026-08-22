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


def delta_to_record(value: SchedulerDelta) -> dict[str, object]:
    return {
        "resolutions": [_resolution_to_record(item) for item in value.resolutions],
        "boundary_resolutions": [
            _boundary_to_record(item) for item in value.boundary_resolutions
        ],
        "closed_boundaries": list(value.closed_boundaries),
        "consumed_resolution_ids": list(value.consumed_resolution_ids),
        "ready": [_plan_to_record(item) for item in value.ready],
        "skipped": [_plan_to_record(item) for item in value.skipped],
        "revived": [_plan_to_record(item) for item in value.revived],
    }


def delta_from_record(value: dict[str, object]) -> SchedulerDelta:
    record = _record(value, "Scheduler Delta")
    return SchedulerDelta(
        resolutions=tuple(
            _resolution_from_record(item)
            for item in _list(record, "resolutions")
        ),
        boundary_resolutions=tuple(
            _boundary_from_record(item)
            for item in _list(record, "boundary_resolutions")
        ),
        closed_boundaries=tuple(
            _string_value(item, "closed boundary")
            for item in _list(record, "closed_boundaries")
        ),
        consumed_resolution_ids=tuple(
            _string_value(item, "consumed Resolution id")
            for item in _list(record, "consumed_resolution_ids")
        ),
        ready=tuple(
            _plan_from_record(item)
            for item in _list(record, "ready")
        ),
        skipped=tuple(
            _plan_from_record(item)
            for item in _list(record, "skipped")
        ),
        revived=tuple(
            _plan_from_record(item)
            for item in _list(record, "revived")
        ),
    )


def _scope_to_record(scope: ExecutionScope) -> list[dict[str, object]]:
    return [
        {"loop_region_id": item.loop_region_id, "iteration": item.iteration}
        for item in scope
    ]


def _scope_from_record(values: object) -> ExecutionScope:
    if not isinstance(values, list):
        raise TypeError("Execution Scope must be a list.")
    return tuple(
        LoopIteration(
            _string(_record(item, "Loop Iteration"), "loop_region_id"),
            _integer(_record(item, "Loop Iteration"), "iteration"),
        )
        for item in values
    )


def _plan_to_record(value: OccurrencePlan) -> dict[str, object]:
    return {
        "id": value.id,
        "node_id": value.node_id,
        "scope": _scope_to_record(value.scope),
        "resolution_ids": list(value.resolution_ids),
    }


def _plan_from_record(value: object) -> OccurrencePlan:
    record = _record(value, "Occurrence Plan")
    return OccurrencePlan(
        _string(record, "id"),
        _string(record, "node_id"),
        _scope_from_record(record.get("scope")),
        tuple(
            _string_value(item, "Occurrence Resolution id")
            for item in _list(record, "resolution_ids")
        ),
    )


def _resolution_to_record(value: EdgeResolution) -> dict[str, object]:
    return {
        "edge_id": value.edge_id,
        "target_node_id": value.target_node_id,
        "target_scope": _scope_to_record(value.target_scope),
        "selected": value.selected,
        "activation": (
            {
                "edge_id": value.activation.edge_id,
                "source_occurrence_id": value.activation.source_occurrence_id,
                "target_node_id": value.activation.target_node_id,
            }
            if value.activation is not None
            else None
        ),
    }


def _resolution_from_record(value: object) -> EdgeResolution:
    record = _record(value, "Edge Resolution")
    activation = _activation_from_record(record.get("activation"))
    return EdgeResolution(
        _string(record, "edge_id"),
        _string(record, "target_node_id"),
        _scope_from_record(record.get("target_scope")),
        _boolean(record, "selected"),
        activation,
    )


def _boundary_to_record(value: LoopBoundaryResolution) -> dict[str, object]:
    return {
        "loop_region_id": value.loop_region_id,
        "loop_scope": _scope_to_record(value.loop_scope),
        "edge_id": value.edge_id,
        "source_scope": _scope_to_record(value.source_scope),
        "target_node_id": value.target_node_id,
        "selected": value.selected,
        "activation": (
            {
                "edge_id": value.activation.edge_id,
                "source_occurrence_id": value.activation.source_occurrence_id,
                "target_node_id": value.activation.target_node_id,
            }
            if value.activation is not None
            else None
        ),
    }


def _boundary_from_record(value: object) -> LoopBoundaryResolution:
    record = _record(value, "Loop Boundary Resolution")
    activation = _activation_from_record(record.get("activation"))
    return LoopBoundaryResolution(
        _string(record, "loop_region_id"),
        _scope_from_record(record.get("loop_scope")),
        _string(record, "edge_id"),
        _scope_from_record(record.get("source_scope")),
        _string(record, "target_node_id"),
        _boolean(record, "selected"),
        activation,
    )


def _activation_from_record(value: object) -> Activation | None:
    if value is None:
        return None
    record = _record(value, "Activation")
    return Activation(
        _string(record, "edge_id"),
        _string(record, "source_occurrence_id"),
        _string(record, "target_node_id"),
    )


def _record(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be a mapping.")
    return value


def _list(record: dict[str, object], key: str) -> list[object]:
    value = record.get(key, [])
    if not isinstance(value, list):
        raise TypeError(f"{key} must be a list.")
    return value


def _string(record: dict[str, object], key: str) -> str:
    if key not in record:
        raise KeyError(key)
    return _string_value(record[key], key)


def _string_value(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string.")
    return value


def _integer(record: dict[str, object], key: str) -> int:
    value = record.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{key} must be an integer.")
    return value


def _boolean(record: dict[str, object], key: str) -> bool:
    value = record.get(key)
    if type(value) is not bool:
        raise TypeError(f"{key} must be bool.")
    return value
