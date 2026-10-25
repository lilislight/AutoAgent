"""Bounded conservative membership for immutable source identities.

A negative answer proves absence only after the initial database scan. Positive
answers always fall back to SQL. Bits are added before inserts and never removed,
including on rollback or deletion, so failures can only cause extra queries.
"""
from hashlib import blake2b


class SourceIdentities:
    def __init__(self, budget):
        # Power-of-two bit count makes double-hash probing inexpensive.
        size = 1 << (budget.bit_length() - 1) if budget else 0
        self.bits = bytearray(size)
        self.mask = size * 8 - 1
        self.ready = False

    def _positions(self, kind, identity):
        digest = blake2b(kind.encode() + b'\0' + identity.encode(), digest_size=16).digest()
        first = int.from_bytes(digest[:8], 'little')
        step = int.from_bytes(digest[8:], 'little') | 1
        return ((first + index * step) & self.mask for index in range(4))

    def add(self, kind, identity):
        if self.bits:
            for position in self._positions(kind, identity):
                self.bits[position >> 3] |= 1 << (position & 7)

    def may_contain(self, kind, identity):
        if not self.ready or not self.bits:
            return True
        return all(self.bits[position >> 3] & (1 << (position & 7))
                   for position in self._positions(kind, identity))

    def clear(self):
        self.ready = False
        self.bits = bytearray(len(self.bits))
