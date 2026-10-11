"""Project configuration and values used by CLI and Server adapters."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
import math

from sqlalchemy.engine import make_url

from .persistence import PersistenceConfig


class HostError(RuntimeError):
    """A high-level operation cannot be completed by this Host."""


class HostCapacityError(HostError):
    """Configured admission or memory capacity is exhausted."""


class ProjectError(HostError):
    """Project discovery, imports or configuration are invalid."""


@dataclass(frozen=True, slots=True)
class HostSettings:
    database_url: str = "sqlite+aiosqlite:///.autoagent/runtime.db"
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
    max_operator_concurrency: int = 32
    max_node_executions_per_invocation: int = 1000

    memory_budget_bytes: int = 0
    memory_target_ratio: float = 0.65
    memory_high_ratio: float = 0.80
    memory_critical_ratio: float = 0.90
    cold_after_seconds: float = 300
    max_idle_seconds: float = 1800
    min_residency_seconds: float = 5
    short_timer_seconds: float = 60
    residency_interval_seconds: float = 1
    checkpoint_tail_events: int = 2048
    checkpoint_tail_bytes: int = 16777216
    checkpoint_interval_seconds: float = 300
    max_resident_graphs: int = 128
    max_requests: int = 128
    control_request_reserve: int = 8
    max_request_bytes: int = 16777216
    unload_concurrency: int = 2
    background_batch_size: int = 32
    background_retry_seconds: float = 5

    def __post_init__(self):
        url = make_url(self.database_url)
        if url.drivername not in ("sqlite+aiosqlite", "postgresql+asyncpg"):
            raise ValueError("Host requires async SQLite or PostgreSQL.")
        for name in (
            "max_operator_concurrency", "max_node_executions_per_invocation",
            "max_resident_graphs", "max_requests", "control_request_reserve", "max_request_bytes",
            "unload_concurrency", "background_batch_size", "checkpoint_tail_events",
            "checkpoint_tail_bytes",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError(f"{name} must be a positive integer.")

        if type(self.memory_budget_bytes) is not int or self.memory_budget_bytes < 0:
            raise ValueError("memory_budget_bytes must be nonnegative (0 means automatic).")
        for name in (
            "cold_after_seconds", "max_idle_seconds", "min_residency_seconds",
            "short_timer_seconds", "residency_interval_seconds",
            "checkpoint_interval_seconds", "background_retry_seconds",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and nonnegative.")
        if self.residency_interval_seconds == 0 or self.background_retry_seconds == 0:
            raise ValueError("Background intervals must be positive.")
        ratios = (self.memory_target_ratio, self.memory_high_ratio, self.memory_critical_ratio)
        if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) for x in ratios) or not 0 < ratios[0] < ratios[1] < ratios[2] < 1:
            raise ValueError("Memory ratios must satisfy 0 < target < high < critical < 1.")
        if self.background_batch_size > 1000:
            raise ValueError("background_batch_size cannot exceed 1000.")
        if self.max_idle_seconds < self.cold_after_seconds:
            raise ValueError("max_idle_seconds cannot be shorter than cold_after_seconds.")

    @classmethod
    def from_environment(cls, env: Mapping[str, str]) -> HostSettings:
        def integer(key, default):
            return int(env.get(key, str(default)))

        def number(key, default):
            return float(env.get(key, str(default)))

        return cls(
            memory_budget_bytes=integer("AUTOAGENT_HOST_MEMORY_BUDGET_BYTES", 0),
            memory_target_ratio=number("AUTOAGENT_HOST_MEMORY_TARGET_RATIO", 0.65),
            memory_high_ratio=number("AUTOAGENT_HOST_MEMORY_HIGH_RATIO", 0.8),
            memory_critical_ratio=number("AUTOAGENT_HOST_MEMORY_CRITICAL_RATIO", 0.9),
            cold_after_seconds=number("AUTOAGENT_HOST_COLD_AFTER_SECONDS", 300),
            max_idle_seconds=number("AUTOAGENT_HOST_MAX_IDLE_SECONDS", 1800),
            min_residency_seconds=number("AUTOAGENT_HOST_MIN_RESIDENCY_SECONDS", 5),
            short_timer_seconds=number("AUTOAGENT_HOST_SHORT_TIMER_SECONDS", 60),
            residency_interval_seconds=number("AUTOAGENT_HOST_RESIDENCY_INTERVAL_SECONDS", 1),
            checkpoint_tail_events=integer("AUTOAGENT_HOST_CHECKPOINT_TAIL_EVENTS", 2048),
            checkpoint_tail_bytes=integer("AUTOAGENT_HOST_CHECKPOINT_TAIL_BYTES", 16777216),
            checkpoint_interval_seconds=number("AUTOAGENT_HOST_CHECKPOINT_INTERVAL_SECONDS", 300),
            max_resident_graphs=integer("AUTOAGENT_HOST_MAX_RESIDENT_GRAPHS", 128),
            max_requests=integer("AUTOAGENT_HOST_MAX_REQUESTS", 128),
            control_request_reserve=integer("AUTOAGENT_HOST_CONTROL_REQUEST_RESERVE", 8),
            max_request_bytes=integer("AUTOAGENT_HOST_MAX_REQUEST_BYTES", 16777216),
            unload_concurrency=integer("AUTOAGENT_HOST_UNLOAD_CONCURRENCY", 2),
            background_batch_size=integer("AUTOAGENT_HOST_BACKGROUND_BATCH_SIZE", 32),
            background_retry_seconds=number("AUTOAGENT_HOST_BACKGROUND_RETRY_SECONDS", 5),
            database_url=env.get("AUTOAGENT_DATABASE_URL", cls().database_url),
            max_operator_concurrency=integer("AUTOAGENT_MAX_OPERATOR_CONCURRENCY", 32),
            max_node_executions_per_invocation=integer(
                "AUTOAGENT_MAX_NODE_EXECUTIONS_PER_INVOCATION", 1000
            ),
            persistence=PersistenceConfig(
                mode=env.get("AUTOAGENT_PERSISTENCE_MODE", "durable"),
                artifact_threshold_bytes=integer(
                    "AUTOAGENT_ARTIFACT_THRESHOLD_BYTES", 1048576
                ),
                batch_count=integer("AUTOAGENT_BATCH_COUNT", 256),
                batch_bytes=integer("AUTOAGENT_BATCH_BYTES", 4194304),
                batch_delay_ms=number("AUTOAGENT_BATCH_DELAY_MS", 100),
                durable_batch_delay_ms=number("AUTOAGENT_DURABLE_BATCH_DELAY_MS", 0),
                batch_queue_ratio=number("AUTOAGENT_BATCH_QUEUE_RATIO", 0.8),
                queue_count=integer("AUTOAGENT_QUEUE_COUNT", 10000),
                queue_bytes=integer("AUTOAGENT_QUEUE_BYTES", 134217728),
                encoding_cache_bytes=integer(
                    "AUTOAGENT_ENCODING_CACHE_BYTES", 16777216
                ),
                retry_attempts=integer("AUTOAGENT_RETRY_ATTEMPTS", 3),
                retry_delay_ms=float(env.get("AUTOAGENT_RETRY_DELAY_MS", "50")),
            ),
        )

    def resolved_database_url(self, root: Path) -> str:
        url = make_url(self.database_url)
        if url.drivername == "sqlite+aiosqlite" and url.database not in (
            None,
            "",
            ":memory:",
        ):
            path = Path(url.database).expanduser()
            if not path.is_absolute():
                path = root / path
            url = url.set(database=str(path.resolve()))
        return url.render_as_string(hide_password=False)


@dataclass(frozen=True, slots=True)
class ProjectCheck:
    """Compile results without opening persistence or runtime resources."""

    name: str
    workflows: tuple

    @property
    def ok(self) -> bool:
        return bool(self.workflows) and all(result.ok for result in self.workflows)

    def to_record(self):
        return {
            "name": self.name,
            "ok": self.ok,
            "workflows": [result.to_diagnostic_document() for result in self.workflows],
        }
