"""Lossless JSON storage encoding; references live outside user JSON."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from collections import OrderedDict
from dataclasses import dataclass

from autoagent.core.runtime._memory import retained_entries

DOMAIN = b"autoagent-artifact-json-v1\0"
BUSINESS_FIELDS = frozenset(
    {
        "input",
        "output",
        "context",
        "mapped_input",
        "aggregate_output",
        "request",
        "response",
        "metrics",
        "origin",
        "patch",
        "pending_context_patch",
        "error",
        "fault",
        "definition",
        "payload",
        "reason",
        "cancel_reason",
    }
)
SURROGATES = re.compile(r"[\ud800-\udfff]")


def _json_text(value):
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_text(value) -> str:
    """Match canonical UTF-8 JSON, including escaped unpaired surrogates."""
    result = _json_text(value)
    return result.encode("utf-8", errors="backslashreplace").decode() if SURROGATES.search(result) else result


def canonical(value) -> bytes:
    return _json_text(value).encode("utf-8", errors="backslashreplace")


def pointer(path) -> str:
    return (
        ""
        if not path
        else "/" + "/".join(str(x).replace("~", "~0").replace("/", "~1") for x in path)
    )


def tokens(path: str, root) -> tuple:
    result = []
    item = root
    for part in path[1:].split("/") if path else ():
        token = part.replace("~1", "/").replace("~0", "~")
        if isinstance(item, list):
            token = int(token)
        result.append(token)
        item = item[token]
    return tuple(result)


def assign(root, path: str, value):
    if not path:
        return value
    parts = tokens(path, root)
    target = root
    for part in parts[:-1]:
        target = target[part]
    target[parts[-1]] = value
    return root


@dataclass(slots=True)
class Encoded:
    record: dict
    refs: list[tuple[str, str]]
    artifacts: dict[str, dict]
    encoded_bytes: int


class EncodingMemo:
    """Bounded batch-local reuse of owned conversions, packed roots and strings."""

    def __init__(self, budget: int):
        self.budget, self.used = budget, 0
        self.items = OrderedDict()
        self.keys = OrderedDict()
        self.key_bytes = 0
        self.key_budget = min(budget // 16, 64 * 1024)
        self.converted, self.packed = OrderedDict(), OrderedDict()
        self.owned = {}
        self.converted_bytes = self.packed_bytes = 0
        self.converting = False
        self.conversion_charge = 0

    @staticmethod
    def _record_charge(value, record):
        # Charge newly built containers and conservatively include their owned
        # backing storage. Nested containers are charged as they are converted.
        charge = 2 * sys.getsizeof(record) + sys.getsizeof(value) + 64
        if isinstance(record, dict):
            charge += sum(sys.getsizeof(key) for key in record)
            children = record.values()
        else:
            children = record
        return charge + sum(sys.getsizeof(child) for child in children
                            if not isinstance(child, (dict, list)))

    def record_value(self, value, convert):
        """Convert only Core-owned immutable roots; never cache external JSON."""
        entry = self.converted.get(id(value))
        if entry is not None:
            self.converted.move_to_end(id(value))
            if self.converting:
                self.conversion_charge += entry[2]
            return entry[1]
        if self.converting:
            record = convert(value)
            self.conversion_charge += self._record_charge(value, record)
            return record
        self.converting = True
        self.conversion_charge = 0
        try:
            record = convert(value)
        finally:
            self.converting = False
        charge = self.conversion_charge + self._record_charge(value, record) + 256
        limit = min(self.budget // 3, self.budget - self.key_budget - self.used - self.packed_bytes)
        if charge <= limit:
            while self.converted and self.converted_bytes + charge > limit:
                _, (original, previous, removed) = self.converted.popitem(last=False)
                self.owned.pop(id(previous), None)
                self.converted_bytes -= removed
            self.converted[id(value)] = (value, record, charge)
            self.owned[id(record)] = charge
            self.converted_bytes += charge
        return record

    def packed_value(self, item, threshold):
        key = (id(item), threshold)
        entry = self.packed.get(key)
        if entry is not None:
            self.packed.move_to_end(key)
            return entry[1]
        return None

    def remember_packed(self, item, threshold, result, charge):
        if id(item) not in self.owned:
            return
        charge += self.owned[id(item)] + 256
        limit = min(self.budget // 3, self.budget - self.key_budget - self.used - self.converted_bytes)
        if charge <= limit:
            while self.packed and self.packed_bytes + charge > limit:
                _, (_, _, removed) = self.packed.popitem(last=False)
                self.packed_bytes -= removed
            self.packed[(id(item), threshold)] = (item, result, charge)
            self.packed_bytes += charge

    def short_string(self, key):
        cached = self.keys.get(key)
        if cached is not None:
            self.keys.move_to_end(key)
            return cached[0]
        data = canonical(key)
        # Only short metadata keys; bound both count and retained key length.
        charge = sys.getsizeof(key) + sys.getsizeof(data) + 128
        if len(key) <= 128 and charge <= self.key_budget:
            while self.keys and (len(self.keys) >= 256 or self.key_bytes + charge > self.key_budget):
                _, (_, old) = self.keys.popitem(last=False)
                self.key_bytes -= old
            self.keys[key] = data, charge
            self.key_bytes += charge
        return data

    def key_size(self, key):
        return len(self.short_string(key))

    def string(self, value):
        if len(value) <= 128:
            return self.short_string(value)
        entry = self.items.get(id(value))
        if entry is not None and entry[0] is value:
            self.items.move_to_end(id(value))
            return entry[1]
        data = canonical(value)
        string_budget = self.budget - self.key_budget
        if len(value) >= 1024 and len(data) <= string_budget:
            while self.used + self.converted_bytes + self.packed_bytes + len(data) > string_budget:
                if self.items:
                    _, old = self.items.popitem(last=False)
                    self.used -= len(old[1])
                elif self.packed:
                    _, (_, _, removed) = self.packed.popitem(last=False)
                    self.packed_bytes -= removed
                else:
                    _, (_, previous, removed) = self.converted.popitem(last=False)
                    self.owned.pop(id(previous), None)
                    self.converted_bytes -= removed
            self.items[id(value)] = (value, data, None)
            self.used += len(data)
        return data

    def digest(self, value, data):
        entry = self.items.get(id(value)) if isinstance(value, str) else None
        if entry is not None and entry[0] is value and entry[2] is not None:
            return entry[2]
        hasher = hashlib.sha256(DOMAIN)
        hasher.update(data)
        digest = hasher.hexdigest()
        if entry is not None and entry[0] is value:
            self.items[id(value)] = (value, entry[1], digest)
        return digest


def encode(record: dict, kind: str, threshold: int, memo: EncodingMemo) -> Encoded:
    artifacts = {}

    def visit(item, path=(), business=False, measure=False):
        if business and isinstance(item, (dict, list)):
            cached = memo.packed_value(item, threshold)
            if cached is not None:
                body, refs, size, shared_artifacts, retained = cached
                artifacts.update(shared_artifacts)
                return body, refs, size, retained
        cacheable = business and id(item) in memo.owned
        measure = measure or cacheable
        retained = 0
        if isinstance(item, dict):
            body, refs, size = {}, [], 2 + max(0, len(item) - 1)
            for key, child in item.items():
                eligible = business or key in BUSINESS_FIELDS
                if path == () and key == "payload":
                    eligible = kind == "user_event"
                # Only actual State Operation wrappers receive this special case.
                if key == "value" and "operations" in path and not business:
                    op_path = item.get("path", [])
                    eligible = bool(op_path and op_path[-1] in BUSINESS_FIELDS)
                value, child_refs, child_size, child_retained = visit(child, (*path, key), eligible, measure)
                body[key] = value
                refs.extend(((key, *p), h) for p, h in child_refs)
                size += memo.key_size(key) + 1 + child_size
                if measure:
                    retained += sys.getsizeof(key) + child_retained
        elif isinstance(item, list):
            body, refs, size = [], [], 2 + max(0, len(item) - 1)
            for index, child in enumerate(item):
                value, child_refs, child_size, child_retained = visit(child, (*path, index), business, measure)
                body.append(value)
                refs.extend(((index, *p), h) for p, h in child_refs)
                size += child_size
                if measure:
                    retained += child_retained
        else:
            body, refs = item, []
            size = (4 if item is None else 4 if item is True else 5 if item is False
                    else len(str(item)) if type(item) is int
                    else len(memo.string(item) if isinstance(item, str) else canonical(item)))
        if measure:
            retained += sys.getsizeof(body)
        if business and size >= threshold:
            data = memo.string(item) if isinstance(item, str) else canonical(body)
            logical = canonical(item) if refs else data
            digest = memo.digest(item, logical)
            artifacts.setdefault(digest, {
                "hash": digest,
                "body": data,
                "logical_bytes": len(logical),
                "value_type": "string"
                if isinstance(item, str)
                else "array"
                if isinstance(item, list)
                else "object",
                "deps": [(pointer(p), h) for p, h in refs],
            })
            body, refs, size = None, [((), digest)], 4
            retained = sys.getsizeof(None)
        if cacheable:
            # Include already-seen dependencies too: reused roots in the next
            # Event must carry every Artifact required by their references.
            required = {}
            def include(digest):
                if digest not in required:
                    artifact = required[digest] = artifacts[digest]
                    for _, child in artifact['deps']:
                        include(child)
            for _, digest in refs:
                include(digest)
            charge = retained + sys.getsizeof(refs) + sum(256 + 48 * len(path) for path, _ in refs)
            charge += sys.getsizeof(required) + sum(
                1024 + sys.getsizeof(artifact['body']) + sys.getsizeof(artifact['deps'])
                + sum(256 + sys.getsizeof(path) for path, _ in artifact['deps'])
                for artifact in required.values())
            memo.remember_packed(item, threshold, (body, refs, size, required, retained), charge)
        return body, refs, size, retained

    body, refs, size, _ = visit(record)
    return Encoded(
        body,
        [(pointer(p), h) for p, h in refs],
        artifacts,
        size + sum(len(a["body"]) for a in artifacts.values()),
    )


def retained_objects(value) -> dict[int, tuple[object, int]]:
    """Estimate shared retained Python objects; accounting itself has overhead."""
    return {key: (item, size) for key, item, size in retained_entries((value,))}
