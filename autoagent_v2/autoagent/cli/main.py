"""Small, script-friendly commands for V2 projects and local tracing."""

from __future__ import annotations

import argparse
import ctypes
import io
import ipaddress
import json
import os
import sys
import tempfile
from collections.abc import Mapping, Sequence
from contextlib import contextmanager, redirect_stdout
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Iterator, TextIO

from pydantic import BaseModel

from autoagent.core.errors import AutoAgentError
from autoagent.core.compiler import WorkflowCompiler
from autoagent.host import (
    AutoAgentHost,
    HostConfigurationError,
    HostOperationError,
    ProjectLoader,
    load_host_settings,
)
from autoagent.hosting import RuntimeEventStoreError, SQLiteRuntimeStore


class _CliError(RuntimeError):
    """An expected command environment failure."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and return a conventional process exit code."""

    parser = _parser()
    arguments = parser.parse_args(argv)
    try:
        return int(arguments.handler(arguments))
    except HostConfigurationError as error:
        _write_error(
            "HOST_CONFIGURATION_INVALID",
            str(error),
            diagnostics=[item.model_dump(mode="json") for item in error.diagnostics],
        )
    except (AutoAgentError, HostOperationError, RuntimeEventStoreError, _CliError) as error:
        _write_error(_error_code(error), str(error))
    except (ValueError, TypeError, OSError) as error:
        _write_error(type(error).__name__.upper(), str(error))
    except KeyboardInterrupt:
        _write_error("INTERRUPTED", "Command interrupted.")
        return 130
    except SystemExit as error:
        _write_error(
            "USER_CODE_EXITED",
            f"User code terminated the command with SystemExit({error.code!r}).",
        )
    except Exception:
        _write_error(
            "INTERNAL_ERROR",
            "The command failed with an unexpected internal error.",
        )
    return 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="autoagent",
        description="Compile and run an AutoAgent V2 project.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    compile_command = commands.add_parser(
        "compile", help="Load and validate every configured Workflow."
    )
    compile_command.add_argument(
        "--project", default=".", help="Project directory or autoagent.toml."
    )
    compile_command.set_defaults(handler=_compile)

    invoke_command = commands.add_parser(
        "invoke", help="Invoke one configured Workflow to a stable boundary."
    )
    invoke_command.add_argument("workflow_id")
    invoke_command.add_argument(
        "--project", default=".", help="Project directory or autoagent.toml."
    )
    invoke_command.add_argument(
        "--input", required=True, help="A JSON value or @path to a JSON file."
    )
    invoke_command.add_argument("--session-id")
    invoke_command.add_argument("--entry", dest="entry_node_id")
    invoke_command.set_defaults(handler=_invoke)

    trace_command = commands.add_parser(
        "trace", help="Serve a read-only local tracing database and UI."
    )
    trace_command.add_argument(
        "--project", default=".", help="Project directory used for environment settings."
    )
    trace_command.add_argument("--database", help="Override the SQLite database path.")
    trace_command.add_argument("--host", help="Override the listen host.")
    trace_command.add_argument("--port", type=int, help="Override the listen port.")
    trace_command.add_argument("--ui-directory", help="Override static UI files.")
    trace_command.add_argument(
        "--allow-remote-without-auth",
        action="store_true",
        help="Explicitly expose full local traces on a non-loopback address.",
    )
    trace_command.set_defaults(handler=_trace)
    return parser


def _compile(arguments: argparse.Namespace) -> int:
    with _redirect_user_stdout():
        project = ProjectLoader().load(arguments.project)
        compiler = WorkflowCompiler()
        workflows: list[dict[str, object]] = []
        ok = True
        for loaded in project.workflows:
            result = compiler.compile(loaded.workflow)
            item = result.to_diagnostic_document()
            if result.workflow_definition_snapshot is not None:
                snapshot = result.workflow_definition_snapshot
                item.update(
                    {
                        "workflow_version": snapshot.workflow_version,
                        "workflow_revision_id": snapshot.workflow_revision_id,
                        "definition_hash": snapshot.definition_hash,
                    }
                )
            workflows.append(item)
            ok = ok and result.ok
    _write_json({"ok": ok, "project": project.manifest.project.name, "workflows": workflows})
    return 0 if ok else 1


def _invoke(arguments: argparse.Namespace) -> int:
    value = _read_json_argument(arguments.input)
    with _redirect_user_stdout():
        host = AutoAgentHost.from_project(arguments.project)
        try:
            result = host.invoke(
                arguments.workflow_id,
                value,
                session_id=arguments.session_id,
                entry_node_id=arguments.entry_node_id,
            )
        except BaseException as error:
            _close_after_failure(host, error)
            raise
        else:
            host.close()
    _write_json(_invocation_result_record(result))
    return 0 if result.status in {"completed", "waiting"} else 1


def _trace(arguments: argparse.Namespace) -> int:
    settings = load_host_settings(_project_root(arguments.project))
    database = (
        Path(arguments.database).expanduser().resolve()
        if arguments.database is not None
        else settings.sqlite_path
    )
    host = (
        settings.trace_host
        if arguments.host is None
        else arguments.host.strip()
    )
    if not host:
        raise ValueError("host cannot be empty")
    port = settings.trace_port if arguments.port is None else arguments.port
    if not 1 <= port <= 65_535:
        raise ValueError("port must be between 1 and 65535")
    if not arguments.allow_remote_without_auth and not _is_loopback_host(host):
        raise _CliError(
            "TRACING_REMOTE_UNAUTHENTICATED",
            "Tracing exposes Runtime context without authentication; use a "
            "loopback host or pass --allow-remote-without-auth explicitly.",
        )
    if arguments.ui_directory is not None and not arguments.ui_directory.strip():
        raise ValueError("ui-directory cannot be empty")
    ui_directory = (
        Path(arguments.ui_directory).expanduser().resolve()
        if arguments.ui_directory is not None
        else settings.trace_ui_directory
    )
    try:
        import uvicorn
        from autoagent.tracing import TracingDependencyError, create_tracing_app
    except ImportError as error:
        raise _CliError(
            "CLI_DEPENDENCY_MISSING",
            "The trace command requires the 'server' optional dependency."
        ) from error

    store = SQLiteRuntimeStore.open_read_only(
        database,
        refresh_seconds=settings.trace_refresh_seconds,
    )
    try:
        store.start()
        try:
            application = create_tracing_app(
                store,
                ui_directory=ui_directory,
                allowed_hosts=(
                    _local_tracing_hosts(host)
                    if _is_loopback_host(host)
                    else None
                ),
            )
        except TracingDependencyError as error:
            raise _CliError("CLI_DEPENDENCY_MISSING", str(error)) from error
        try:
            uvicorn.run(application, host=host, port=port)
        except SystemExit as error:
            raise _CliError(
                "TRACING_SERVER_FAILED",
                f"Tracing server exited with status {error.code!r}.",
            ) from error
    except BaseException as error:
        _close_after_failure(store, error)
        raise
    else:
        store.close()
    return 0


def _project_root(project: str | Path) -> Path:
    path = Path(project).expanduser().resolve()
    if path.is_dir():
        return path
    if path.is_file():
        return path.parent
    raise ValueError(f"project path does not exist: {path}")


def _close_after_failure(resource: object, primary: BaseException) -> None:
    try:
        resource.close()
    except BaseException as cleanup:
        primary.add_note(
            f"Cleanup also failed with {type(cleanup).__name__}: {cleanup}"
        )


@contextmanager
def _redirect_user_stdout() -> Iterator[None]:
    """Keep Python, native, and subprocess output off the JSON stdout channel."""

    destination = sys.stderr
    _flush_text_stream(sys.stdout)
    saved_stdout = os.dup(1)
    capture_reader: int | None = None
    capture_writer: int | None = None
    try:
        try:
            target = destination.fileno()
        except (AttributeError, io.UnsupportedOperation):
            target, path = tempfile.mkstemp(prefix="autoagent-cli-stdout-")
            capture_writer = target
            capture_reader = os.open(path, os.O_RDONLY)
            os.unlink(path)
        os.dup2(target, 1)
        if capture_writer is not None:
            os.close(capture_writer)
            capture_writer = None
        with redirect_stdout(destination):
            yield
    finally:
        _flush_text_stream(sys.stdout)
        _flush_native_streams()
        os.dup2(saved_stdout, 1)
        os.close(saved_stdout)
        if capture_writer is not None:
            os.close(capture_writer)
        if capture_reader is not None:
            chunks: list[bytes] = []
            while chunk := os.read(capture_reader, 8192):
                chunks.append(chunk)
            os.close(capture_reader)
            destination.write(b"".join(chunks).decode("utf-8", errors="replace"))
            destination.flush()


def _flush_text_stream(stream: TextIO) -> None:
    try:
        stream.flush()
    except (AttributeError, OSError, ValueError):
        pass


def _flush_native_streams() -> None:
    """Flush C stdio while fd 1 still points away from machine stdout."""

    try:
        ctypes.CDLL(None).fflush(None)
    except (AttributeError, OSError):
        pass


def _is_loopback_host(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _local_tracing_hosts(value: str) -> tuple[str, ...]:
    normalized = value.strip().lower()
    if normalized == "localhost":
        return ("localhost", "127.0.0.1", "::1")
    address = ipaddress.ip_address(normalized)
    if address in {
        ipaddress.ip_address("127.0.0.1"),
        ipaddress.ip_address("::1"),
    }:
        return ("localhost", "127.0.0.1", "::1")
    return (address.compressed,)


def _read_json_argument(source: str) -> object:
    if source.startswith("@"):
        path = Path(source[1:]).expanduser()
        if not source[1:]:
            raise ValueError("JSON file path cannot be empty")
        text = path.read_text(encoding="utf-8")
    else:
        text = source
    try:
        return json.loads(text, parse_constant=_reject_json_constant)
    except json.JSONDecodeError as error:
        raise ValueError(f"input is not valid JSON: {error.msg}") from error


def _invocation_result_record(result: object) -> dict[str, object]:
    return {
        "session_id": result.session_id,
        "invocation_id": result.invocation_id,
        "status": result.status,
        "output": _json_value(result.output),
        "error": _json_value(result.error),
        "waits": [
            {"id": wait.id, "request": _json_value(wait.request)}
            for wait in result.waits
        ],
        "checkpoint": {
            "id": result.checkpoint.id,
            "root_session_id": result.checkpoint.root_session_id,
            "captured_at_ns": str(result.checkpoint.captured_at_ns),
            "digest": result.checkpoint.digest,
        },
    }


def _json_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Enum):
        return _json_value(value.value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return {item.name: _json_value(getattr(value, item.name)) for item in fields(value)}
    raise TypeError(f"Cannot encode {type(value).__name__} as command JSON")


def _write_json(value: object, *, stream: object | None = None) -> None:
    destination = sys.stdout if stream is None else stream
    print(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ),
        file=destination,
    )


def _write_error(code: str, message: str, **details: object) -> None:
    _write_json(
        {"ok": False, "error": {"code": code, "message": message, **details}},
        stream=sys.stderr,
    )


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"input is not valid JSON: {value} is not permitted")


def _error_code(error: BaseException) -> str:
    code = getattr(error, "code", None)
    return code if isinstance(code, str) and code.strip() else type(error).__name__.upper()


__all__ = ["main"]
