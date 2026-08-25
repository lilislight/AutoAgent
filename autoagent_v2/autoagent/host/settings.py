"""Immutable Host settings parsed from a project environment snapshot."""

from __future__ import annotations

import math
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

from .environment import load_project_environment
from .errors import HostDiagnostic, HostSettingsError


AUTOAGENT_ENV_KEYS = frozenset(
    {
        "AUTOAGENT_MAX_OPERATOR_CONCURRENCY",
        "AUTOAGENT_MAX_NODE_EXECUTIONS_PER_INVOCATION",
        "AUTOAGENT_RUNTIME_EVENT_SINK",
        "AUTOAGENT_SQLITE_PATH",
        "AUTOAGENT_HTTP_SINK_URL",
        "AUTOAGENT_HTTP_SINK_TOKEN",
        "AUTOAGENT_HTTP_SINK_TIMEOUT_SECONDS",
        "AUTOAGENT_TRACE_HOST",
        "AUTOAGENT_TRACE_PORT",
        "AUTOAGENT_TRACE_UI_DIRECTORY",
        "AUTOAGENT_TRACE_REFRESH_SECONDS",
    }
)
_HTTP_URL = TypeAdapter(AnyHttpUrl)


class HostSettings(BaseModel):
    """Validated deployment values used when a future Host assembles Core."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    max_operator_concurrency: int = 32
    max_node_executions_per_invocation: int = 1_000
    runtime_event_sink: Literal["sqlite", "http", "none"] = "sqlite"
    sqlite_path: Path = Path(".autoagent/runtime.db")
    http_sink_url: str | None = None
    http_sink_token: str | None = Field(default=None, repr=False)
    http_sink_timeout_seconds: float = 10.0
    trace_host: str = "127.0.0.1"
    trace_port: int = 8765
    trace_ui_directory: Path | None = None
    trace_refresh_seconds: float = 0.5

    @field_validator("http_sink_url")
    @classmethod
    def validate_http_sink_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        resolved = value.strip()
        if not resolved:
            return None
        if not _valid_http_url(resolved):
            raise ValueError("http_sink_url must be an absolute HTTP(S) URL")
        return resolved

    @field_validator("http_sink_token")
    @classmethod
    def normalize_http_sink_token(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value.strip() or None

    @field_validator("trace_host")
    @classmethod
    def normalize_trace_host(cls, value: str) -> str:
        return value.strip()

    @model_validator(mode="after")
    def validate_invariants(self) -> HostSettings:
        if self.max_operator_concurrency < 1:
            raise ValueError("max_operator_concurrency must be positive")
        if self.max_node_executions_per_invocation < 1:
            raise ValueError("max_node_executions_per_invocation must be positive")
        if not self.trace_host.strip():
            raise ValueError("trace_host cannot be empty")
        if not 1 <= self.trace_port <= 65_535:
            raise ValueError("trace_port must be between 1 and 65535")
        if not _positive_finite(self.http_sink_timeout_seconds):
            raise ValueError("http_sink_timeout_seconds must be positive and finite")
        if not _positive_finite(self.trace_refresh_seconds):
            raise ValueError("trace_refresh_seconds must be positive and finite")
        if self.runtime_event_sink == "http" and self.http_sink_url is None:
            raise ValueError(
                "http_sink_url is required when runtime_event_sink is 'http'"
            )
        return self


def load_host_settings(
    project_root: str | Path,
    *,
    env_file: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> HostSettings:
    """Resolve project environment values into one strict settings object."""

    root = Path(project_root).expanduser().resolve()
    environment = load_project_environment(
        root,
        env_file=env_file,
        environ=environ,
    )
    diagnostics: list[HostDiagnostic] = []

    def integer(key: str, default: int) -> int:
        raw = environment.get(key)
        if raw is None:
            return default
        try:
            value = int(raw.strip())
        except ValueError:
            diagnostics.append(_invalid_environment(key, "must be an integer"))
            return default
        if value < 1:
            diagnostics.append(_invalid_environment(key, "must be positive"))
            return default
        return value

    def number(key: str, default: float) -> float:
        raw = environment.get(key)
        if raw is None:
            return default
        try:
            value = float(raw.strip())
        except ValueError:
            diagnostics.append(_invalid_environment(key, "must be a number"))
            return default
        if not math.isfinite(value) or value <= 0:
            diagnostics.append(
                _invalid_environment(key, "must be positive and finite")
            )
            return default
        return value

    sink = environment.get("AUTOAGENT_RUNTIME_EVENT_SINK", "sqlite").strip()
    if sink not in {"sqlite", "http", "none"}:
        diagnostics.append(
            _invalid_environment(
                "AUTOAGENT_RUNTIME_EVENT_SINK",
                "must be 'sqlite', 'http', or 'none'",
            )
        )
        sink = "sqlite"

    sqlite_path = _project_path(
        root,
        environment.get("AUTOAGENT_SQLITE_PATH", ".autoagent/runtime.db"),
        "AUTOAGENT_SQLITE_PATH",
        diagnostics,
    )
    ui_raw = environment.get("AUTOAGENT_TRACE_UI_DIRECTORY")
    trace_ui_directory = (
        None
        if ui_raw is None
        else _project_path(
            root,
            ui_raw,
            "AUTOAGENT_TRACE_UI_DIRECTORY",
            diagnostics,
        )
    )
    trace_host = environment.get("AUTOAGENT_TRACE_HOST", "127.0.0.1").strip()
    if not trace_host:
        diagnostics.append(
            _invalid_environment("AUTOAGENT_TRACE_HOST", "cannot be empty")
        )
        trace_host = "127.0.0.1"

    trace_port = integer("AUTOAGENT_TRACE_PORT", 8765)
    if trace_port > 65_535:
        diagnostics.append(
            _invalid_environment(
                "AUTOAGENT_TRACE_PORT", "must be between 1 and 65535"
            )
        )
        trace_port = 8765

    max_operator_concurrency = integer(
        "AUTOAGENT_MAX_OPERATOR_CONCURRENCY", 32
    )
    max_node_executions_per_invocation = integer(
        "AUTOAGENT_MAX_NODE_EXECUTIONS_PER_INVOCATION", 1_000
    )
    http_sink_timeout_seconds = number(
        "AUTOAGENT_HTTP_SINK_TIMEOUT_SECONDS", 10.0
    )
    trace_refresh_seconds = number(
        "AUTOAGENT_TRACE_REFRESH_SECONDS", 0.5
    )

    url = _optional(environment.get("AUTOAGENT_HTTP_SINK_URL"))
    if url is not None and not _valid_http_url(url):
        diagnostics.append(
            _invalid_environment(
                "AUTOAGENT_HTTP_SINK_URL", "must be an absolute HTTP(S) URL"
            )
        )
        url = None
    if sink == "http" and url is None:
        diagnostics.append(
            _invalid_environment(
                "AUTOAGENT_HTTP_SINK_URL",
                "is required when AUTOAGENT_RUNTIME_EVENT_SINK=http",
            )
        )

    if diagnostics:
        raise HostSettingsError(diagnostics)

    data = {
        "max_operator_concurrency": max_operator_concurrency,
        "max_node_executions_per_invocation": (
            max_node_executions_per_invocation
        ),
        "runtime_event_sink": sink,
        "sqlite_path": sqlite_path,
        "http_sink_url": url,
        "http_sink_token": _optional(
            environment.get("AUTOAGENT_HTTP_SINK_TOKEN")
        ),
        "http_sink_timeout_seconds": http_sink_timeout_seconds,
        "trace_host": trace_host,
        "trace_port": trace_port,
        "trace_ui_directory": trace_ui_directory,
        "trace_refresh_seconds": trace_refresh_seconds,
    }
    try:
        return HostSettings.model_validate(data)
    except ValidationError as error:  # Defensive boundary for future fields.
        raise HostSettingsError(
            [
                HostDiagnostic(
                    code="HOST_SETTINGS_INVALID",
                    message=item["msg"],
                    field=".".join(str(part) for part in item["loc"]),
                    metadata={"type": item["type"]},
                )
                for item in error.errors(
                    include_url=False,
                    include_context=False,
                )
            ]
        ) from error


def _project_path(
    root: Path,
    raw: str,
    key: str,
    diagnostics: list[HostDiagnostic],
) -> Path:
    value = raw.strip()
    if not value:
        diagnostics.append(_invalid_environment(key, "cannot be empty"))
        return root
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _optional(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _valid_http_url(value: str) -> bool:
    try:
        _HTTP_URL.validate_python(value, strict=True)
    except ValidationError:
        return False
    return True


def _positive_finite(value: object) -> bool:
    """Return whether a strict numeric value is positive and finite."""

    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(value) and value > 0
    except OverflowError:
        return False


def _invalid_environment(key: str, reason: str) -> HostDiagnostic:
    return HostDiagnostic(
        code="HOST_ENVIRONMENT_INVALID",
        message=f"{key} {reason}.",
        field=key,
    )
