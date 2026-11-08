"""Bounded reuse of static SDK schemas, without retaining project types or values."""

from collections import OrderedDict
import json
import threading

from pydantic import TypeAdapter


_MAX_ENTRIES = 128
_MAX_BYTES = 2 * 1024 * 1024
_MAX_ENTRY_BYTES = 256 * 1024
_cache = OrderedDict()
_lock = threading.RLock()
_cached_bytes = 0


def _generate(annotation):
    return json.dumps(TypeAdapter(annotation).json_schema(), sort_keys=True, separators=(',', ':'))


def schema_json(annotation):
    """Return immutable JSON; callers decoding it receive independent dictionaries.

    Project types are regenerated so dynamic configuration and custom schema
    hooks keep their semantics. SDK model rebuilds invalidate their entry.
    """
    global _cached_bytes
    config = getattr(annotation, 'model_config', {})
    if (not getattr(annotation, '__module__', '').startswith('autoagent.')
            or callable(config.get('json_schema_extra'))):
        return _generate(annotation)
    with _lock:
        generation = getattr(annotation, '__pydantic_core_schema__', None)
        configuration = repr(config)
        entry = _cache.get(annotation)
        if entry is not None and entry[0] is generation and entry[1] == configuration:
            _cache.move_to_end(annotation)
            return entry[2]
        if entry is not None:
            _cached_bytes -= _cache.pop(annotation)[3]
        value = _generate(annotation)
        size = len(value.encode('utf-8'))
        if size <= _MAX_ENTRY_BYTES:
            while _cache and (len(_cache) >= _MAX_ENTRIES or _cached_bytes + size > _MAX_BYTES):
                _cached_bytes -= _cache.popitem(last=False)[1][3]
            _cache[annotation] = (getattr(annotation, '__pydantic_core_schema__', None), configuration, value, size)
            _cached_bytes += size
        return value
