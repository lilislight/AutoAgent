"""Bounded Writer-only metadata for immutable Workflow revisions."""

from collections import OrderedDict
from dataclasses import dataclass

from .codec import retained_objects


@dataclass(frozen=True, slots=True)
class OutgoingEdge:
    id: str
    target: str
    on: str
    conditional: bool


def outgoing_edges(snapshot):
    """Detach routing metadata from schemas, code and other definition bodies."""
    graph = {}
    for edge in snapshot["definition"]["edges"]:
        graph.setdefault(edge["source"], []).append(
            OutgoingEdge(edge["id"], edge["target"], edge["on"], edge["condition"] is not None)
        )
    return {node: tuple(edges) for node, edges in graph.items()}


class WorkflowGraphCache:
    """LRU charged for retained objects plus conservative per-entry overhead."""

    def __init__(self, budget):
        self.budget = budget
        self.entries = OrderedDict()
        self.bytes = 0
        self.hits = self.misses = self.evictions = 0

    def get(self, revision):
        entry = self.entries.get(revision)
        if entry is None:
            self.misses += 1
            return None
        self.entries.move_to_end(revision)
        self.hits += 1
        return entry[0]

    def put(self, revision, graph):
        size = sum(size for _, size in retained_objects((revision, graph)).values()) + 256
        if size > self.budget:
            return
        self.discard(revision)
        while self.entries and self.bytes + size > self.budget:
            _, (_, removed) = self.entries.popitem(last=False)
            self.bytes -= removed
            self.evictions += 1
        self.entries[revision] = graph, size
        self.bytes += size

    def discard(self, revision):
        entry = self.entries.pop(revision, None)
        if entry is not None:
            self.bytes -= entry[1]

    def clear(self):
        self.entries.clear()
        self.bytes = 0
