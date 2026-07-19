from __future__ import annotations


def generate_edge_id(base: str, used_ids: set[str]) -> str:
    if base not in used_ids:
        return base

    index = 2
    while True:
        edge_id = f"{base}_{index}"
        if edge_id not in used_ids:
            return edge_id
        index += 1
