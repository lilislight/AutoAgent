from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Mapping, cast

from dotenv import dotenv_values

from autoagent.core.runtime import (
    ArtifactPolicy,
    DatabaseBackend,
    JsonRuntimeSerializer,
    PersistencePolicy,
    RuntimeRetentionPolicy,
    RuntimeStore,
)
from autoagent.core.runtime.retention import RuntimeRetentionMode


_PREFIX = "AUTOAGENT_"
AUTOAGENT_ENV_KEYS = frozenset(
    {
        "AUTOAGENT_DATABASE_URL",
        "AUTOAGENT_DATABASE_ECHO",
        "AUTOAGENT_SERIALIZER_MAX_INLINE_BYTES",
        "AUTOAGENT_PERSISTENCE_QUEUE_LOW_WATERMARK_BYTES",
        "AUTOAGENT_PERSISTENCE_QUEUE_HIGH_WATERMARK_BYTES",
        "AUTOAGENT_PERSISTENCE_QUEUE_HARD_WATERMARK_BYTES",
        "AUTOAGENT_PERSISTENCE_ADMISSION_TIMEOUT_MS",
        "AUTOAGENT_DATABASE_BATCH_MAX_ITEMS",
        "AUTOAGENT_DATABASE_BATCH_MAX_BYTES",
        "AUTOAGENT_DATABASE_BATCH_MAX_DELAY_MS",
        "AUTOAGENT_DATABASE_RECOVERY_EVENT_INTERVAL",
        "AUTOAGENT_SQLITE_SYNCHRONOUS",
        "AUTOAGENT_EXECUTOR_MAX_THREAD_WORKERS",
        "AUTOAGENT_EXECUTOR_MAX_PARALLEL_UNITS",
        "AUTOAGENT_SHUTDOWN_GRACE_TIMEOUT_MS",
        "AUTOAGENT_ARTIFACT_ENABLED",
        "AUTOAGENT_ARTIFACT_INLINE_MAX_BYTES",
        "AUTOAGENT_RETENTION_MODE",
        "AUTOAGENT_RETENTION_MAX_TERMINAL_INVOCATIONS",
        "AUTOAGENT_RETENTION_MAX_REPLAY_CHECKPOINTS_PER_INVOCATION",
    }
)


@dataclass(frozen=True, slots=True)
class AutoAgentSettings:
    """Deployment-level App settings loaded from ``.env`` and the environment.

    Explicit process environment variables override values read from ``.env``.
    Workflow, Node, retry, and routing policies deliberately do not belong
    here: those values are part of a Workflow definition rather than an App
    deployment.
    """

    database_url: str | None = None
    database_echo: bool = False
    serializer_max_inline_bytes: int | None = None

    persistence_queue_low_watermark_bytes: int = 128 * 1024 * 1024
    persistence_queue_high_watermark_bytes: int = 256 * 1024 * 1024
    persistence_queue_hard_watermark_bytes: int = 512 * 1024 * 1024
    persistence_admission_timeout_ms: float = 5_000

    database_batch_max_items: int = 256
    database_batch_max_bytes: int = 4 * 1024 * 1024
    database_batch_max_delay_ms: int = 5
    database_recovery_event_interval: int = 200
    sqlite_synchronous: str = "FULL"

    executor_max_thread_workers: int = 8
    executor_max_parallel_units: int = 8
    shutdown_grace_timeout_ms: int = 5_000

    artifact_enabled: bool = True
    artifact_inline_max_bytes: int = 8 * 1024

    retention_mode: RuntimeRetentionMode = "retain_all"
    retention_max_terminal_invocations: int = 128
    retention_max_replay_checkpoints_per_invocation: int = 8

    def __post_init__(self) -> None:
        database_url = (
            None
            if self.database_url is None or not self.database_url.strip()
            else self.database_url.strip()
        )
        object.__setattr__(self, "database_url", database_url)

        # Reuse the runtime policy validators so environment and code-based
        # configuration have exactly the same constraints.
        self.persistence_policy()
        self.retention_policy()
        self.artifact_policy()
        if (
            self.serializer_max_inline_bytes is not None
            and self.serializer_max_inline_bytes < 1
        ):
            raise ValueError(
                "AUTOAGENT_SERIALIZER_MAX_INLINE_BYTES must be positive or empty."
            )
        if (
            self.database_batch_max_items < 1
            or self.database_batch_max_bytes < 1
            or self.database_batch_max_delay_ms < 0
        ):
            raise ValueError("Invalid AUTOAGENT_DATABASE_BATCH_* settings.")
        if self.database_recovery_event_interval < 1:
            raise ValueError(
                "AUTOAGENT_DATABASE_RECOVERY_EVENT_INTERVAL must be positive."
            )
        if (
            self.executor_max_thread_workers < 1
            or self.executor_max_parallel_units < 1
        ):
            raise ValueError("AUTOAGENT_EXECUTOR_MAX_* values must be positive.")
        if self.shutdown_grace_timeout_ms < 0:
            raise ValueError(
                "AUTOAGENT_SHUTDOWN_GRACE_TIMEOUT_MS cannot be negative."
            )
        synchronous = self.sqlite_synchronous.upper()
        if synchronous not in {"FULL", "NORMAL"}:
            raise ValueError(
                "AUTOAGENT_SQLITE_SYNCHRONOUS must be FULL or NORMAL."
            )
        object.__setattr__(self, "sqlite_synchronous", synchronous)

    @classmethod
    def from_env(
        cls,
        *,
        env_file: str | Path | None = ".env",
        environ: Mapping[str, str] | None = None,
    ) -> AutoAgentSettings:
        """Load one immutable settings snapshot.

        ``env_file=None`` is useful for hosts that inject environment variables
        directly and for deterministic tests.
        """

        values: dict[str, str] = {}
        if env_file is not None:
            values.update(
                {
                    key: value
                    for key, value in dotenv_values(env_file).items()
                    if value is not None
                }
            )
        values.update(os.environ if environ is None else environ)
        return cls(
            database_url=_optional_text(values, "DATABASE_URL"),
            database_echo=_bool(values, "DATABASE_ECHO", False),
            serializer_max_inline_bytes=_optional_int(
                values,
                "SERIALIZER_MAX_INLINE_BYTES",
            ),
            persistence_queue_low_watermark_bytes=_int(
                values,
                "PERSISTENCE_QUEUE_LOW_WATERMARK_BYTES",
                128 * 1024 * 1024,
            ),
            persistence_queue_high_watermark_bytes=_int(
                values,
                "PERSISTENCE_QUEUE_HIGH_WATERMARK_BYTES",
                256 * 1024 * 1024,
            ),
            persistence_queue_hard_watermark_bytes=_int(
                values,
                "PERSISTENCE_QUEUE_HARD_WATERMARK_BYTES",
                512 * 1024 * 1024,
            ),
            persistence_admission_timeout_ms=_float(
                values,
                "PERSISTENCE_ADMISSION_TIMEOUT_MS",
                5_000,
            ),
            database_batch_max_items=_int(
                values,
                "DATABASE_BATCH_MAX_ITEMS",
                256,
            ),
            database_batch_max_bytes=_int(
                values,
                "DATABASE_BATCH_MAX_BYTES",
                4 * 1024 * 1024,
            ),
            database_batch_max_delay_ms=_int(
                values,
                "DATABASE_BATCH_MAX_DELAY_MS",
                5,
            ),
            database_recovery_event_interval=_int(
                values,
                "DATABASE_RECOVERY_EVENT_INTERVAL",
                200,
            ),
            sqlite_synchronous=_text(values, "SQLITE_SYNCHRONOUS", "FULL"),
            executor_max_thread_workers=_int(
                values,
                "EXECUTOR_MAX_THREAD_WORKERS",
                8,
            ),
            executor_max_parallel_units=_int(
                values,
                "EXECUTOR_MAX_PARALLEL_UNITS",
                8,
            ),
            shutdown_grace_timeout_ms=_int(
                values,
                "SHUTDOWN_GRACE_TIMEOUT_MS",
                5_000,
            ),
            artifact_enabled=_bool(values, "ARTIFACT_ENABLED", True),
            artifact_inline_max_bytes=_int(
                values,
                "ARTIFACT_INLINE_MAX_BYTES",
                8 * 1024,
            ),
            retention_mode=cast(
                RuntimeRetentionMode,
                _text(values, "RETENTION_MODE", "retain_all"),
            ),
            retention_max_terminal_invocations=_int(
                values,
                "RETENTION_MAX_TERMINAL_INVOCATIONS",
                128,
            ),
            retention_max_replay_checkpoints_per_invocation=_int(
                values,
                "RETENTION_MAX_REPLAY_CHECKPOINTS_PER_INVOCATION",
                8,
            ),
        )

    def serializer(self) -> JsonRuntimeSerializer:
        return JsonRuntimeSerializer(
            max_inline_bytes=self.serializer_max_inline_bytes,
        )

    def persistence_policy(self) -> PersistencePolicy:
        return PersistencePolicy(
            queue_low_watermark_bytes=(
                self.persistence_queue_low_watermark_bytes
            ),
            queue_high_watermark_bytes=(
                self.persistence_queue_high_watermark_bytes
            ),
            queue_hard_watermark_bytes=(
                self.persistence_queue_hard_watermark_bytes
            ),
            admission_timeout_ms=self.persistence_admission_timeout_ms,
        )

    def retention_policy(self) -> RuntimeRetentionPolicy:
        return RuntimeRetentionPolicy(
            mode=self.retention_mode,
            max_terminal_invocations=(
                self.retention_max_terminal_invocations
            ),
            max_replay_checkpoints_per_invocation=(
                self.retention_max_replay_checkpoints_per_invocation
            ),
        )

    def artifact_policy(self) -> ArtifactPolicy:
        return ArtifactPolicy(
            enabled=self.artifact_enabled,
            inline_max_bytes=self.artifact_inline_max_bytes,
        )

    def runtime_store(
        self,
        *,
        serializer: JsonRuntimeSerializer | None = None,
        database_read_only: bool = False,
    ) -> RuntimeStore:
        resolved_serializer = serializer or self.serializer()
        backend = None
        if self.database_url is not None:
            backend = DatabaseBackend(
                self.database_url,
                echo=self.database_echo,
                batch_max_items=self.database_batch_max_items,
                batch_max_bytes=self.database_batch_max_bytes,
                batch_max_delay_ms=self.database_batch_max_delay_ms,
                recovery_event_interval=(
                    self.database_recovery_event_interval
                ),
                artifact_policy=self.artifact_policy(),
                sqlite_synchronous=self.sqlite_synchronous,
                shutdown_timeout_ms=self.shutdown_grace_timeout_ms,
                read_only=database_read_only,
            )
        return RuntimeStore(
            backend=backend,
            serializer=resolved_serializer,
            retention_policy=self.retention_policy(),
            persistence_policy=(
                self.persistence_policy()
                if backend is not None
                else None
            ),
        )


def _environment_key(name: str) -> str:
    return f"{_PREFIX}{name}"


def _text(
    values: Mapping[str, str],
    name: str,
    default: str,
) -> str:
    value = values.get(_environment_key(name))
    return default if value is None else value


def _optional_text(
    values: Mapping[str, str],
    name: str,
) -> str | None:
    value = values.get(_environment_key(name))
    if value is None or not value.strip():
        return None
    return value


def _int(
    values: Mapping[str, str],
    name: str,
    default: int,
) -> int:
    value = values.get(_environment_key(name))
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(
            f"{_environment_key(name)} must be an integer."
        ) from exc


def _optional_int(
    values: Mapping[str, str],
    name: str,
) -> int | None:
    value = values.get(_environment_key(name))
    if value is None or not value.strip():
        return None
    return _int(values, name, 0)


def _float(
    values: Mapping[str, str],
    name: str,
    default: float,
) -> float:
    value = values.get(_environment_key(name))
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(
            f"{_environment_key(name)} must be a number."
        ) from exc


def _bool(
    values: Mapping[str, str],
    name: str,
    default: bool,
) -> bool:
    value = values.get(_environment_key(name))
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        f"{_environment_key(name)} must be true or false."
    )
