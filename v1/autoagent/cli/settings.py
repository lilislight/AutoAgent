from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path
from typing import Mapping

from autoagent.core.app import AutoAgentSettings
from autoagent.core.server import ServerSettings


def add_runtime_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--store",
        choices=("auto", "memory", "database"),
        default="auto",
        help=(
            "Select memory, require AUTOAGENT_DATABASE_URL, or automatically "
            "use the configured database."
        ),
    )
    parser.add_argument(
        "--max-thread-workers",
        type=int,
        help="Override the shared synchronous Operator worker count.",
    )
    parser.add_argument(
        "--max-parallel-units",
        type=int,
        help="Override the App-wide Map/Replication unit limit.",
    )
    parser.add_argument(
        "--shutdown-timeout-ms",
        type=int,
        help="Override the graceful shutdown deadline.",
    )


def app_settings_from_arguments(
    arguments: argparse.Namespace,
    environment: Mapping[str, str],
) -> AutoAgentSettings:
    settings = AutoAgentSettings.from_env(
        env_file=None,
        environ=environment,
    )
    updates: dict[str, object] = {}
    if arguments.max_thread_workers is not None:
        updates["executor_max_thread_workers"] = arguments.max_thread_workers
    if arguments.max_parallel_units is not None:
        updates["executor_max_parallel_units"] = arguments.max_parallel_units
    if arguments.shutdown_timeout_ms is not None:
        updates["shutdown_grace_timeout_ms"] = arguments.shutdown_timeout_ms

    store = arguments.store
    if store == "memory":
        updates["database_url"] = None
    elif store == "database" and settings.database_url is None:
        raise ValueError(
            "--store database requires AUTOAGENT_DATABASE_URL in the environment."
        )
    return replace(settings, **updates)


def server_settings_from_arguments(
    arguments: argparse.Namespace,
    environment: Mapping[str, str],
) -> ServerSettings:
    settings = ServerSettings.from_env(env_file=None, environ=environment)
    updates: dict[str, object] = {}
    if arguments.host is not None:
        updates["host"] = arguments.host
    if arguments.port is not None:
        updates["port"] = arguments.port
    if arguments.read_only:
        updates["execution_enabled"] = False
    if arguments.secure_cookies:
        updates["secure_cookies"] = True
    if arguments.ui_directory is not None:
        updates["ui_directory"] = Path(arguments.ui_directory)
    if arguments.trace_cache_size is not None:
        updates["trace_cache_size"] = arguments.trace_cache_size
    return replace(settings, **updates)
