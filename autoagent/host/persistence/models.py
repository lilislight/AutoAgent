"""Public configuration, query cursors and immutable persistence descriptors."""

from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import dataclass, field
from typing import Literal

from autoagent.core.operators import Operator

MIB = 1024 * 1024
WriteMode = Literal["durable", "buffered"]
ReadView = Literal["durable", "accepted"]


class PersistenceError(RuntimeError):
    """A persistence boundary failed without advancing its durable frontier."""


class ConflictError(PersistenceError):
    """An immutable identity or ordered stream conflicts with stored facts."""


class CapacityError(PersistenceError):
    """One record exceeds the configured retained-data budget."""


class ClosedError(PersistenceError):
    """Persistence is closing or closed."""


class FormatError(PersistenceError):
    """Stored format or projection version does not match this implementation."""


@dataclass(frozen=True, slots=True)
class PersistenceConfig:
    mode: WriteMode = "durable"
    artifact_threshold_bytes: int = MIB
    batch_count: int = 256
    batch_bytes: int = 4 * MIB
    batch_delay_ms: float = 100
    durable_batch_delay_ms: float = 0
    batch_queue_ratio: float = 0.8
    queue_count: int = 10_000
    queue_bytes: int = 128 * MIB
    encoding_cache_bytes: int = 16 * MIB
    retry_attempts: int = 3
    retry_delay_ms: float = 50

    def __post_init__(self):
        if self.mode not in {"durable", "buffered"}:
            raise ValueError("mode must be durable or buffered.")
        for name in (
            "artifact_threshold_bytes",
            "batch_count",
            "batch_bytes",
            "queue_count",
            "queue_bytes",
            "encoding_cache_bytes",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer.")
        for name in ("batch_delay_ms", "durable_batch_delay_ms", "retry_delay_ms"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not 0 <= value < float("inf")
            ):
                raise ValueError(f"{name} must be finite and nonnegative.")
        if isinstance(self.batch_queue_ratio, bool) or not isinstance(self.batch_queue_ratio, (int, float)) or not 0 < self.batch_queue_ratio <= 1:
            raise ValueError("batch_queue_ratio must be in (0, 1].")
        if type(self.retry_attempts) is not int or self.retry_attempts < 0:
            raise ValueError("retry_attempts must be nonnegative.")


@dataclass(frozen=True, slots=True)
class OperatorDefinition:
    operator_id: str
    version: str
    description_text: str
    definition_id: str = field(init=False)

    def __post_init__(self):
        if not self.operator_id.strip() or not self.version.strip():
            raise ValueError("Operator identity and version cannot be empty.")
        description = json.loads(self.description_text)
        if not isinstance(description, dict):
            raise TypeError("Operator description must be an object.")
        text = json.dumps(
            description,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        object.__setattr__(self, "description_text", text)
        object.__setattr__(
            self,
            "definition_id",
            hashlib.sha256(
                json.dumps(
                    [self.operator_id, self.version, description],
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode()
            ).hexdigest(),
        )

    @classmethod
    def from_operator(cls, operator: Operator) -> OperatorDefinition:
        handler = operator.handler
        try:
            source_file = inspect.getsourcefile(handler)
            source_line = inspect.getsourcelines(handler)[1]
        except (OSError, TypeError):
            source_file, source_line = None, None

        def contract(value):
            return json.loads(value.schema) if value is not None else None

        return cls(
            operator.id,
            operator.version,
            json.dumps(
                {
                    "handler": {
                        "module": getattr(handler, "__module__", None),
                        "qualname": getattr(
                            handler, "__qualname__", type(handler).__qualname__
                        ),
                        "source_file": source_file,
                        "source_line": source_line,
                    },
                    "input_contract": contract(operator.contract.input),
                    "output_contract": contract(operator.contract.output),
                    "stream_chunk_contract": contract(operator.contract.stream_chunk),
                },
                allow_nan=False,
            ),
        )

    def to_record(self):
        return {
            "definition_id": self.definition_id,
            "operator_id": self.operator_id,
            "version": self.version,
            "definition": json.loads(self.description_text),
        }


@dataclass(frozen=True, slots=True)
class Page:
    """A stable page; frontier is the view's Runtime Event store_order."""

    items: tuple[dict, ...]
    next_cursor: tuple | None = None
    frontier: int = 0


@dataclass(frozen=True, slots=True)
class PersistenceHealth:
    """Frontiers are local operation tickets, including definitions and User Events."""

    state: Literal["open", "closing", "closed", "faulted"]
    queued_count: int
    retained_bytes: int
    accepted_frontier: int
    durable_frontier: int
    fault: str | None = None
