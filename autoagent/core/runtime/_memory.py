"""Walk retained objects once, including privately owned container storage."""

import gc
import sys
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from functools import lru_cache
from types import MappingProxyType

from .values import _FrozenMap


@lru_cache(maxsize=128)
def _field_names(kind):
    return tuple(field.name for field in fields(kind))


def retained_entries(values):
    """Yield unique object identities, references and shallow byte charges."""
    pending, seen = list(values), set()
    while pending:
        item = pending.pop()
        identity = id(item)
        if identity in seen:
            continue
        seen.add(identity)
        yield identity, item, sys.getsizeof(item)
        kind = type(item)
        if kind in (str, bytes, int, float, bool, type(None)):
            continue
        if is_dataclass(item) and not isinstance(item, type):
            pending.extend(getattr(item, name) for name in _field_names(kind))
            if hasattr(item, '__dict__'):
                pending.append(vars(item))
        elif kind is _FrozenMap:
            pending.append(item._data)
        elif isinstance(item, MappingProxyType):
            pending.extend(gc.get_referents(item))
        elif isinstance(item, Mapping):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, (tuple, list, set, frozenset)):
            pending.extend(item)
        elif hasattr(item, '__dict__'):
            pending.append(vars(item))
