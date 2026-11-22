"""Deployment settings for the standalone Server and its embedded UI."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Mapping
import math
import re


def route_prefix(value: str, *, root: bool = False) -> str:
    if root and value == "/":
        return value
    if not isinstance(value, str) or not re.fullmatch(r"(?:/[A-Za-z0-9_-]+)+", value):
        raise ValueError("Route prefixes must contain slash-separated letters, digits, underscores or hyphens.")
    return value


@dataclass(frozen=True, slots=True)
class ServerSettings:
    host: str = "127.0.0.1"
    port: int = 8765
    api_prefix: str = "/api/v2"
    ui_path: str = "/"
    ui_directory: Path | None = None
    page_size: int = 50
    max_page_size: int = 200
    query_timeout_seconds: float = 15
    shutdown_timeout_seconds: float = 15
    trace_max_subscribers: int = 128
    trace_coalesce_ms: float = 100
    trace_heartbeat_seconds: float = 10
    user_event_max_subscribers: int = 128
    user_event_buffer: int = 256
    user_event_heartbeat_seconds: float = 10
    access_log: bool = True
    workflow_ui_show_agent_app: bool = False

    def __post_init__(self):
        if type(self.access_log) is not bool:
            raise ValueError("Server access_log must be bool.")
        if type(self.workflow_ui_show_agent_app) is not bool:
            raise ValueError("workflow_ui_show_agent_app must be bool.")
        if not isinstance(self.host, str) or not self.host.strip():
            raise ValueError("Server host cannot be empty.")
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("Server port must be between 1 and 65535.")
        route_prefix(self.api_prefix)
        route_prefix(self.ui_path, root=True)
        if self.ui_path == self.api_prefix or self.ui_path.startswith(self.api_prefix + "/"):
            raise ValueError("The UI cannot be mounted inside the API prefix.")
        if any(type(v) is not int or not 1 <= v <= 1000 for v in (self.page_size, self.max_page_size)):
            raise ValueError("Page sizes must be between 1 and 1000.")
        if self.page_size > self.max_page_size:
            raise ValueError("Default page size cannot exceed the maximum.")
        if type(self.trace_max_subscribers) is not int or self.trace_max_subscribers < 1:
            raise ValueError('trace_max_subscribers must be a positive integer.')
        if type(self.user_event_max_subscribers) is not int or self.user_event_max_subscribers < 1:
            raise ValueError('user_event_max_subscribers must be positive.')
        if type(self.user_event_buffer) is not int or not 1 <= self.user_event_buffer <= 10000:
            raise ValueError('user_event_buffer must be between 1 and 10000.')
        if isinstance(self.user_event_heartbeat_seconds, bool) or not isinstance(self.user_event_heartbeat_seconds, (int, float)) or not math.isfinite(self.user_event_heartbeat_seconds) or not 0 < self.user_event_heartbeat_seconds <= 30:
            raise ValueError('user_event_heartbeat_seconds must be between 0 and 30.')
        for name in ('trace_coalesce_ms', 'trace_heartbeat_seconds'):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0 or name == 'trace_heartbeat_seconds' and not 0 < value <= 30:
                raise ValueError('Invalid trace notification timing.')
        for name in ('query_timeout_seconds', 'shutdown_timeout_seconds'):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f'{name} must be finite and positive.')

    @classmethod
    def from_environment(cls, env: Mapping[str, str], *, root: Path | None = None):
        access = env.get("AUTOAGENT_SERVER_ACCESS_LOG", "true").strip().lower()
        if access not in {"true", "false", "1", "0"}:
            raise ValueError("Server access log environment value must be true, false, 1 or 0.")
        show_agent = env.get("AUTOAGENT_SERVER_WORKFLOW_UI_SHOW_AGENT_APP", "false").strip().lower()
        if show_agent not in {"true", "false", "1", "0"}:
            raise ValueError("Invalid AUTOAGENT_SERVER_WORKFLOW_UI_SHOW_AGENT_APP; use true, false, 1 or 0.")
        directory = env.get("AUTOAGENT_SERVER_UI_DIRECTORY", "").strip()
        path = Path(directory).expanduser() if directory else None
        if path is not None and not path.is_absolute():
            path = (root or Path.cwd()) / path
        return cls(
            host=env.get("AUTOAGENT_SERVER_HOST", "127.0.0.1"),
            port=int(env.get("AUTOAGENT_SERVER_PORT", "8765")),
            api_prefix=env.get("AUTOAGENT_SERVER_API_PREFIX", "/api/v2"),
            ui_path=env.get("AUTOAGENT_SERVER_UI_PATH", "/"),
            ui_directory=path,
            page_size=int(env.get("AUTOAGENT_SERVER_PAGE_SIZE", "50")),
            max_page_size=int(env.get("AUTOAGENT_SERVER_MAX_PAGE_SIZE", "200")),
            query_timeout_seconds=float(env.get("AUTOAGENT_SERVER_QUERY_TIMEOUT_SECONDS", "15")),
            shutdown_timeout_seconds=float(env.get("AUTOAGENT_SERVER_SHUTDOWN_TIMEOUT_SECONDS", "15")),
            trace_max_subscribers=int(env.get('AUTOAGENT_SERVER_TRACE_MAX_SUBSCRIBERS', '128')),
            trace_coalesce_ms=float(env.get('AUTOAGENT_SERVER_TRACE_COALESCE_MS', '100')),
            trace_heartbeat_seconds=float(env.get('AUTOAGENT_SERVER_TRACE_HEARTBEAT_SECONDS', '10')),
            user_event_max_subscribers=int(env.get('AUTOAGENT_SERVER_USER_EVENT_MAX_SUBSCRIBERS', '128')),
            user_event_buffer=int(env.get('AUTOAGENT_SERVER_USER_EVENT_BUFFER', '256')),
            user_event_heartbeat_seconds=float(env.get('AUTOAGENT_SERVER_USER_EVENT_HEARTBEAT_SECONDS', '10')),
            access_log=access in {"true", "1"},
            workflow_ui_show_agent_app=show_agent in {"true", "1"},
        )
