"""Shared validation for one Node terminal route decision."""

from __future__ import annotations

from collections.abc import Set

from ..errors import RuntimeTransitionError
from ..workflow import EdgeIR


def validate_edge_selection(
    outgoing: tuple[EdgeIR, ...],
    source_status: str,
    selected_edge_ids: Set[str],
) -> None:
    """Validate selected ids against one source Node terminal status."""

    known = {edge.id for edge in outgoing}
    unknown = selected_edge_ids - known
    if unknown:
        raise RuntimeTransitionError(
            "EDGE_SELECTION_UNKNOWN",
            "Selected Edge is not outgoing from the Node: "
            + ", ".join(sorted(unknown)),
        )

    matching = {edge.id for edge in outgoing if edge.on == source_status}
    invalid = selected_edge_ids - matching
    if invalid:
        raise RuntimeTransitionError(
            "EDGE_SELECTION_INVALID",
            "Selected Edges do not match the source terminal state: "
            + ", ".join(sorted(invalid)),
        )

    required = {
        edge.id
        for edge in outgoing
        if edge.on == source_status and edge.condition is None
    }
    missing = required - selected_edge_ids
    if missing:
        raise RuntimeTransitionError(
            "EDGE_UNCONDITIONAL_NOT_SELECTED",
            "Unconditional matching Edges must be selected: "
            + ", ".join(sorted(missing)),
        )


__all__ = ["validate_edge_selection"]
