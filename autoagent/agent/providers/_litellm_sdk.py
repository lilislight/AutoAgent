"""Serialize optional SDK initialization, including its global logging callbacks."""

import importlib
import logging
from threading import RLock


_import_lock = RLock()


class _ImportFilter:
    def __init__(self, delegate, attempt):
        self.delegate, self.attempt = delegate, attempt

    def filter(self, record):
        # LiteLLM installs these filters before its import is complete. Another
        # thread must not enter their lazy imports while the importer owns the
        # parent module lock. The importing thread can still log reentrantly.
        with _import_lock:
            return False if self.attempt["failed"] else self.delegate.filter(record)


def load_litellm_sdk():
    """Load once through Python's module cache; create no loop-bound clients.

    During cold import, newly installed LiteLLM filters share the import gate.
    Restore the logging API and original filters afterwards, so normal logging
    keeps its redaction behavior and has no additional locking overhead.
    """
    with _import_lock:
        original_add = logging.Filterer.addFilter
        installed = []
        attempt = {"failed": False}

        def add_filter(owner, value):
            if type(value).__module__.startswith("litellm.") and value not in owner.filters:
                guarded = _ImportFilter(value, attempt)
                installed.append((owner, guarded, value))
                return original_add(owner, guarded)
            return original_add(owner, value)

        logging.Filterer.addFilter = add_filter
        try:
            sdk = importlib.import_module("litellm")
            for operation in ("acompletion", "aresponses", "aembedding"):
                getattr(sdk, operation)
            return sdk
        except BaseException:
            attempt["failed"] = True
            raise
        finally:
            if logging.Filterer.addFilter is add_filter:
                logging.Filterer.addFilter = original_add
            for owner, guarded, value in installed:
                # Preserve filters installed or removed concurrently by others.
                owner.filters[:] = [
                    value if item is guarded else item
                    for item in owner.filters
                    if item is not guarded or not attempt["failed"]
                ]


__all__ = ["load_litellm_sdk"]
