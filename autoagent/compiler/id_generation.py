from __future__ import annotations

from uuid import uuid4


def generate_workflow_id() -> str:
    return f"workflow_{uuid4().hex[:8]}"


def generate_node_id(used_ids: set[str], start_index: int) -> tuple[str, int]:
    index = start_index
    while True:
        node_id = f"node_{index}"
        index += 1
        if node_id not in used_ids:
            return node_id, index


def generate_edge_id(base: str, used_ids: set[str]) -> str:
    if base not in used_ids:
        return base

    index = 2
    while True:
        edge_id = f"{base}_{index}"
        if edge_id not in used_ids:
            return edge_id
        index += 1
