from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Mapping

from dotenv import dotenv_values


SERVER_ENV_KEYS = frozenset(
    {
        "AUTOAGENT_SERVER_HOST",
        "AUTOAGENT_SERVER_PORT",
        "AUTOAGENT_SERVER_URL",
        "AUTOAGENT_SERVER_ACCESS_TOKEN",
        "AUTOAGENT_SERVER_SECURE_COOKIES",
        "AUTOAGENT_SERVER_EXECUTION_ENABLED",
        "AUTOAGENT_SERVER_UI_DIRECTORY",
        "AUTOAGENT_SERVER_TRACE_CACHE_SIZE",
    }
)


@dataclass(frozen=True, slots=True)
class ServerSettings:
    """Deployment configuration owned by AutoAgentServer, not AutoAgentApp."""

    host: str = "0.0.0.0"
    port: int = 8765
    access_token: str | None = None
    secure_cookies: bool = False
    execution_enabled: bool = True
    ui_directory: Path | None = None
    trace_cache_size: int = 128

    def __post_init__(self) -> None:
        if not self.host.strip():
            raise ValueError("AUTOAGENT_SERVER_HOST cannot be empty.")
        if not 1 <= self.port <= 65_535:
            raise ValueError("AUTOAGENT_SERVER_PORT must be between 1 and 65535.")
        if self.access_token is not None and not self.access_token:
            raise ValueError("AUTOAGENT_SERVER_ACCESS_TOKEN cannot be empty.")
        if self.trace_cache_size < 1:
            raise ValueError("AUTOAGENT_SERVER_TRACE_CACHE_SIZE must be positive.")

    @classmethod
    def from_env(
        cls,
        *,
        env_file: str | Path | None = ".env",
        environ: Mapping[str, str] | None = None,
    ) -> ServerSettings:
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
        access_token = values.get("AUTOAGENT_SERVER_ACCESS_TOKEN", "").strip()
        ui_directory = values.get("AUTOAGENT_SERVER_UI_DIRECTORY", "").strip()
        try:
            port = int(values.get("AUTOAGENT_SERVER_PORT", "8765"))
        except ValueError as exc:
            raise ValueError("AUTOAGENT_SERVER_PORT must be an integer.") from exc
        try:
            trace_cache_size = int(
                values.get("AUTOAGENT_SERVER_TRACE_CACHE_SIZE", "128")
            )
        except ValueError as exc:
            raise ValueError(
                "AUTOAGENT_SERVER_TRACE_CACHE_SIZE must be an integer."
            ) from exc
        return cls(
            host=values.get("AUTOAGENT_SERVER_HOST", "0.0.0.0").strip(),
            port=port,
            access_token=access_token or None,
            secure_cookies=_server_bool(
                values,
                "AUTOAGENT_SERVER_SECURE_COOKIES",
                False,
            ),
            execution_enabled=_server_bool(
                values,
                "AUTOAGENT_SERVER_EXECUTION_ENABLED",
                True,
            ),
            ui_directory=Path(ui_directory) if ui_directory else None,
            trace_cache_size=trace_cache_size,
        )


def _server_bool(
    values: Mapping[str, str],
    key: str,
    default: bool,
) -> bool:
    value = values.get(key)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{key} must be a boolean.")
