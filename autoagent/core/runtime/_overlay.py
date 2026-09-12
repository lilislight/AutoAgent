"""Transient planning map: share the base and store only local edits."""
from collections.abc import MutableMapping


class PlanningOverlay(MutableMapping):
    """Never publish this mutable workspace as committed Runtime State."""

    def __init__(self, base):
        self.base = base
        self.edits = {}
        self.removed = set()

    def __getitem__(self, key):
        if key in self.removed:
            raise KeyError(key)
        if key in self.edits:
            return self.edits[key]
        return self.base[key]

    def __setitem__(self, key, value):
        self.removed.discard(key)
        self.edits[key] = value

    def __delitem__(self, key):
        self[key]
        self.edits.pop(key, None)
        self.removed.add(key)

    def __iter__(self):
        yield from (key for key in self.base if key not in self.removed)
        yield from (key for key in self.edits if key not in self.base)

    def __len__(self):
        return sum(1 for _ in self)
