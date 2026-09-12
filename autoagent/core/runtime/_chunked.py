"""Ordered structural sharing for large Runtime mappings, without history chains."""
from collections.abc import Mapping, MutableMapping, Sequence
from types import MappingProxyType

_BLOCK = 128
_BUCKETS = 256
_THRESHOLD = 512


class ChunkedMap(Mapping):
    """Immutable insertion-ordered blocks plus a partitioned lookup directory.

    A value update copies its block and the block directory, O(128 + N/128).
    Adding/removing a key also copies one hash directory bucket, O(N/256).
    This deliberately does not claim constant-time persistent updates.
    """
    __slots__ = ('_blocks', '_index', '_size')

    def __init__(self, source):
        if hasattr(self, '_blocks'):
            raise AttributeError('Runtime mappings are immutable.')
        blocks = []
        index = [{} for _ in range(_BUCKETS)]
        block = {}
        for key, value in source.items():
            if len(block) == _BLOCK:
                blocks.append(MappingProxyType(block))
                block = {}
            index[hash(key) % _BUCKETS][key] = len(blocks)
            block[key] = value
        if block:
            blocks.append(MappingProxyType(block))
        object.__setattr__(self, '_blocks', tuple(blocks))
        object.__setattr__(self, '_index', tuple(MappingProxyType(item) for item in index))
        object.__setattr__(self, '_size', len(source))

    def __setattr__(self, name, value):
        raise AttributeError('Runtime mappings are immutable.')

    def __delattr__(self, name):
        raise AttributeError('Runtime mappings are immutable.')

    def __len__(self):
        return self._size

    def __iter__(self):
        for block in self._blocks:
            yield from block

    def __getitem__(self, key):
        position = self._index[hash(key) % _BUCKETS][key]
        return self._blocks[position][key]

    def items(self):
        for block in self._blocks:
            yield from block.items()

    def values(self):
        for block in self._blocks:
            yield from block.values()

    def _updated(self, updates, deleted):
        blocks = list(self._blocks)
        index = list(self._index)
        changed_blocks = set()
        changed_buckets = set()
        size = self._size

        def writable_block(position):
            if position not in changed_blocks:
                blocks[position] = dict(blocks[position])
                changed_blocks.add(position)
            return blocks[position]

        def writable_bucket(key):
            bucket = hash(key) % _BUCKETS
            if bucket not in changed_buckets:
                index[bucket] = dict(index[bucket])
                changed_buckets.add(bucket)
            return index[bucket]

        for key in deleted:
            bucket = index[hash(key) % _BUCKETS]
            if key in bucket:
                del writable_block(bucket[key])[key]
                del writable_bucket(key)[key]
                size -= 1
        for key, value in updates.items():
            bucket = index[hash(key) % _BUCKETS]
            if key in bucket:
                writable_block(bucket[key])[key] = value
            else:
                if not blocks or len(blocks[-1]) == _BLOCK:
                    blocks.append({})
                    changed_blocks.add(len(blocks) - 1)
                position = len(blocks) - 1
                writable_block(position)[key] = value
                writable_bucket(key)[key] = position
                size += 1
        for position in changed_blocks:
            blocks[position] = MappingProxyType(blocks[position])
        for bucket in changed_buckets:
            index[bucket] = MappingProxyType(index[bucket])
        result = object.__new__(ChunkedMap)
        object.__setattr__(result, '_blocks', tuple(blocks))
        object.__setattr__(result, '_index', tuple(index))
        object.__setattr__(result, '_size', size)
        # Bound empty blocks after sustained delete/append churn.
        if deleted and size < len(blocks) * _BLOCK // 2:
            return ChunkedMap(result) if size >= _THRESHOLD else MappingProxyType(dict(result.items()))
        return result


class MapEdit(MutableMapping):
    """Unpublished transaction edits, including delete/reinsert order."""
    def __init__(self, base):
        self.base = base
        self.updates = {}
        self.deleted = set()

    def __getitem__(self, key):
        if key in self.updates:
            return self.updates[key]
        if key in self.deleted:
            raise KeyError(key)
        return self.base[key]

    def __setitem__(self, key, value):
        self.updates[key] = value

    def __delitem__(self, key):
        self[key]
        self.updates.pop(key, None)
        self.deleted.add(key)

    def __iter__(self):
        yield from (key for key in self.base if key not in self.deleted)
        yield from (key for key in self.updates if key not in self.base or key in self.deleted)

    def __len__(self):
        return len(self.base) - sum(key in self.base for key in self.deleted) + sum(
            key not in self.base or key in self.deleted for key in self.updates)

    def finish(self):
        if not self.updates and not self.deleted:
            return self.base
        if isinstance(self.base, ChunkedMap):
            return self.base._updated(self.updates, self.deleted)
        if len(self.base) >= _THRESHOLD:
            return ChunkedMap(self.base)._updated(self.updates, self.deleted)
        result = dict(self.base)
        for key in self.deleted:
            result.pop(key, None)
        result.update(self.updates)
        return ChunkedMap(result) if len(result) >= _THRESHOLD else MappingProxyType(result)


class ChunkedUnits(Sequence):
    """Immutable Child unit sequence with shared fixed-size blocks."""
    __slots__ = ('_blocks', '_size')
    __hash__ = None

    def __init__(self, values):
        if hasattr(self, '_blocks'):
            raise AttributeError('Child units are immutable.')
        values = tuple(values)
        object.__setattr__(self, '_blocks', tuple(values[i:i + _BLOCK] for i in range(0, len(values), _BLOCK)))
        object.__setattr__(self, '_size', len(values))

    def __setattr__(self, name, value):
        raise AttributeError('Child units are immutable.')

    def __delattr__(self, name):
        raise AttributeError('Child units are immutable.')

    def __len__(self):
        return self._size

    def __iter__(self):
        for block in self._blocks:
            yield from block

    def __getitem__(self, index):
        if isinstance(index, slice):
            return tuple(self)[index]
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError('Child unit index out of range')
        return self._blocks[index // _BLOCK][index % _BLOCK]

    def __eq__(self, other):
        if not isinstance(other, (tuple, ChunkedUnits)):
            return NotImplemented
        return len(self) == len(other) and all(left == right for left, right in zip(self, other))

    def replace_at(self, index, value):
        self[index]
        if index < 0:
            index += len(self)
        blocks = list(self._blocks)
        block = list(blocks[index // _BLOCK])
        block[index % _BLOCK] = value
        blocks[index // _BLOCK] = tuple(block)
        result = object.__new__(ChunkedUnits)
        object.__setattr__(result, '_blocks', tuple(blocks))
        object.__setattr__(result, '_size', len(self))
        return result


def runtime_mapping(values):
    """Choose a compact immutable representation when constructing owned State."""
    return ChunkedMap(values) if len(values) >= _THRESHOLD else MappingProxyType(values)


def child_units(values):
    return ChunkedUnits(values) if len(values) >= _THRESHOLD else values
