"""Detach external values once and share Core-owned immutable values internally."""

from __future__ import annotations

from collections.abc import Mapping
import math
from types import MappingProxyType


DurableValue = (
    None | bool | int | float | str
    | tuple['DurableValue', ...] | Mapping[str, 'DurableValue']
)


class _FrozenMap(Mapping):
    """Owned immutable mapping; unlike arbitrary mappingproxy its backing is private."""

    __slots__ = ('_data',)

    def __init__(self, value: Mapping) -> None:
        if hasattr(self, '_data'):
            raise AttributeError('Core values are immutable.')
        owned = freeze(dict(value))
        object.__setattr__(self, '_data', owned._data)

    def __setattr__(self, name, value):
        raise AttributeError('Core values are immutable.')

    def __delattr__(self, name):
        raise AttributeError('Core values are immutable.')

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def items(self):
        return self._data.items()

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def get(self, key, default=None):
        return self._data.get(key, default)

    def copy(self):
        return dict(self._data)

    def __repr__(self):
        return repr(self._data)

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


class _FrozenTuple(tuple):
    __slots__ = ()

    def __new__(cls, values=()):
        return tuple.__new__(cls, (freeze(item) for item in values))

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


def freeze(value: object) -> DurableValue:
    """Isolate external containers; already-owned values are passed by reference.

    Only our private owned types bypass traversal. External mappingproxy/tuple
    values still need validation and detachment, including nested containers.
    """
    if type(value) in (_FrozenMap, _FrozenTuple):
        return value
    return _freeze(value, set(), {})


def _freeze(value: object, active: set[int], memo: dict[int, object]) -> DurableValue:
    if value is None or isinstance(value, (bool, int, float, str)):
        if isinstance(value, float) and not math.isfinite(value):
            raise TypeError('Durable floats must be finite.')
        return value
    if type(value) in (_FrozenMap, _FrozenTuple):
        return value
    if isinstance(value, (Mapping, list, tuple)):
        identity = id(value)
        if identity in active:
            raise TypeError('Durable runtime values cannot contain cycles.')
        if identity in memo:
            return memo[identity]
        active.add(identity)
        if isinstance(value, Mapping):
            data = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError('Durable mappings require string keys.')
                data[key] = _freeze(item, active, memo)
            frozen = object.__new__(_FrozenMap)
            object.__setattr__(frozen, '_data', MappingProxyType(data))
        else:
            frozen = tuple.__new__(_FrozenTuple, (_freeze(item, active, memo) for item in value))
        active.remove(identity)
        memo[identity] = frozen
        return frozen
    raise TypeError(f'Unsupported durable runtime value: {type(value).__name__}.')


def thaw(value: DurableValue) -> object:
    """Return an isolated mutable/JSON view at an external boundary."""
    if isinstance(value, Mapping):
        return {key: thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw(item) for item in value]
    return value
