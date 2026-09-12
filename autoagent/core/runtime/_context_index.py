"""Derived Context conflict queries and task-local, short-lived previews."""
from contextlib import contextmanager
from contextvars import ContextVar


class ContextRevisionIndex:
    """Exact ancestors use State; only strict ancestors need subtree maxima."""

    def __init__(self, revisions):
        self.revisions = revisions
        self.descendants = {}
        for path, revision in revisions.items():
            self._add(path, revision)

    def _add(self, path, revision):
        for size in range(1, len(path)):
            prefix = path[:size]
            self.descendants[prefix] = max(self.descendants.get(prefix, -1), revision)

    def conflicts(self, path, started):
        return (self.descendants.get(path, -1) > started or
                any(self.revisions.get(path[:size], -1) > started
                    for size in range(1, len(path) + 1)))

    def advance(self, revisions, operations):
        # Normal commits only increase revisions. Other replacements rebuild.
        if any(revisions[op.path] < self.revisions.get(op.path, -1) for op in operations):
            return ContextRevisionIndex(revisions)
        for operation in operations:
            self._add(operation.path, revisions[operation.path])
        self.revisions = revisions
        return self


class _PreviewCache(list):
    closed = False


_indexes = ContextVar('context_revision_indexes', default=())
_previews = ContextVar('context_previews', default=None)


@contextmanager
def planning_indexes(indexes):
    token = _indexes.set(indexes)
    try:
        yield
    finally:
        _indexes.reset(token)


@contextmanager
def context_previews():
    """Release preview data on success, failure and cancellation alike."""
    cache = _PreviewCache()
    token = _previews.set(cache)
    try:
        yield
    finally:
        cache.closed = True
        cache.clear()
        _previews.reset(token)


def revision_index(revisions):
    return next((index for index in _indexes.get() if index.revisions is revisions), None)
