"""Lossless JSON storage encoding; references live outside user JSON."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass

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
    """Bounded batch-local cache of large shared strings, never a global cache."""

    def __init__(self, budget: int):
        self.budget, self.used = budget, 0
        self.items = OrderedDict()
        self.keys = OrderedDict()
        self.key_bytes = 0
        self.key_budget = min(budget // 16, 64 * 1024)

    def key_size(self, key):
        cached = self.keys.get(key)
        if cached is not None:
            self.keys.move_to_end(key)
            return cached[0]
        size = len(canonical(key))
        # Only short metadata keys; bound both count and retained key length.
        charge = sys.getsizeof(key) + sys.getsizeof(size) + 128
        if len(key) <= 128 and charge <= self.key_budget:
            while self.keys and (len(self.keys) >= 256 or self.key_bytes + charge > self.key_budget):
                _, (_, old) = self.keys.popitem(last=False)
                self.key_bytes -= old
            self.keys[key] = size, charge
            self.key_bytes += charge
        return size

    def string(self, value):
        entry = self.items.get(id(value))
        if entry is not None and entry[0] is value:
            self.items.move_to_end(id(value))
            return entry[1]
        data = canonical(value)
        string_budget = self.budget - self.key_budget
        if len(value) >= 1024 and len(data) <= string_budget:
            while self.items and self.used + len(data) > string_budget:
                _, old = self.items.popitem(last=False)
                self.used -= len(old[1])
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

    def visit(item, path=(), business=False):
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
                value, child_refs, child_size = visit(child, (*path, key), eligible)
                body[key] = value
                refs.extend(((key, *p), h) for p, h in child_refs)
                size += memo.key_size(key) + 1 + child_size
        elif isinstance(item, list):
            body, refs, size = [], [], 2 + max(0, len(item) - 1)
            for index, child in enumerate(item):
                value, child_refs, child_size = visit(child, (*path, index), business)
                body.append(value)
                refs.extend(((index, *p), h) for p, h in child_refs)
                size += child_size
        else:
            body, refs = item, []
            size = (4 if item is None else 4 if item is True else 5 if item is False
                    else len(memo.string(item) if isinstance(item, str) else canonical(item)))
        if not business or size < threshold:
            return body, refs, size
        data = memo.string(item) if isinstance(item, str) else canonical(body)
        logical = canonical(item) if refs else data
        digest = memo.digest(item, logical)
        artifacts.setdefault(
            digest,
            {
                "hash": digest,
                "body": data,
                "logical_bytes": len(logical),
                "value_type": "string"
                if isinstance(item, str)
                else "array"
                if isinstance(item, list)
                else "object",
                "deps": [(pointer(p), h) for p, h in refs],
            },
        )
        return None, [((), digest)], 4

    body, refs, size = visit(record)
    return Encoded(
        body,
        [(pointer(p), h) for p, h in refs],
        artifacts,
        size + sum(len(a["body"]) for a in artifacts.values()),
    )


def retained_objects(value) -> dict[int, tuple[object, int]]:
    """Estimate shared retained Python objects; accounting itself has overhead."""
    pending, seen = [value], {}
    while pending:
        item = pending.pop()
        key = id(item)
        if key in seen:
            continue
        seen[key] = (item, sys.getsizeof(item))
        if is_dataclass(item) and not isinstance(item, type):
            pending.extend(getattr(item, f.name) for f in fields(item))
        elif isinstance(item, Mapping):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, (tuple, list)):
            pending.extend(item)
    return seen
