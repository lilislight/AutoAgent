"""Bounded committed metadata shared with local Event admission."""

from collections import OrderedDict
from copy import deepcopy
from threading import RLock

from .codec import retained_objects


class MetadataCache:
    """Cache positive rows only; transaction overlays are never published early."""

    def __init__(self, budget):
        self.lock = RLock()
        self.budget = budget
        self.entries = OrderedDict()
        self.bytes = 0
        self.hits = self.misses = self.evictions = 0

    def get(self, key):
        with self.lock:
            item = self.entries.get(key)
            if item is None:
                self.misses += 1
                return None
            self.entries.move_to_end(key)
            self.hits += 1
            return deepcopy(item[0])

    def put(self, key, value):
        with self.lock:
            # Negative results must not mask rows created by later transactions.
            if value is None:
                return
            self.discard(key)
            size = sum(size for _, size in retained_objects((key, value)).values()) + 256
            if size > self.budget:
                return
            while self.entries and self.bytes + size > self.budget:
                _, (_, removed) = self.entries.popitem(last=False)
                self.bytes -= removed
                self.evictions += 1
            self.entries[key] = deepcopy(value), size
            self.bytes += size

    def discard(self, key):
        with self.lock:
            old = self.entries.pop(key, None)
            if old is not None:
                self.bytes -= old[1]

    def clear(self):
        with self.lock:
            self.entries.clear()
            self.bytes = 0
