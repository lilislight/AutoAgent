"""Workspace execution and process observations."""

import asyncio
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import time
from typing import Protocol, runtime_checkable

from ..models import ToolOutput
from ..tools import ToolBusinessError
from .backend import ResolvedExecution
from .local import LocalExecutionBackend, settle
from .manager import ProcessManager
from .models import ExecutionToolsConfig


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _is_windows():
    return os.name == "nt"


@runtime_checkable
class ExecutionService(Protocol):
    """Execution backend for already authorized Tool calls."""

    config: ExecutionToolsConfig
    version: str
    async def execute(self, name, arguments, context) -> ToolOutput: ...
    async def aclose(self): ...


class LocalExecutionService:
    """Local execution with application-owned adapters and runtime environment."""

    def __init__(self, root, *, working_directory=".", config=None, backend=None,
                 manager=None, environment=None, adapter_version="1"):
        self._windows = _is_windows()
        self.root = Path(root).resolve(strict=True)
        self.cwd = (self.root / working_directory).resolve(strict=True)
        if not self.root.is_dir() or not self.cwd.is_dir() or not self.cwd.is_relative_to(self.root):
            raise ValueError("Execution working directory must be inside its Workspace.")
        self.config = ExecutionToolsConfig.model_validate(config or ExecutionToolsConfig())
        self.backend = backend or LocalExecutionBackend()
        self.manager = manager or ProcessManager()
        from ..files import LocalFileService
        self.files = LocalFileService(self.root, working_directory=working_directory)
        root_stat = self.root.stat()
        self._root_identity = f"{root_stat.st_dev}:{root_stat.st_ino}"
        # Runtime-only environment. Do not inherit arbitrary application tokens,
        # PYTHONPATH, shell startup hooks or loader-injection variables.
        allowed = {"PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP", "TMPDIR",
            "HOME", "USERPROFILE", "LANG", "LC_ALL", "LC_CTYPE", "VIRTUAL_ENV"}
        self.environment = {key: value for key, value in os.environ.items() if key.upper() in allowed}
        if environment is not None:
            self.environment.update(environment)
        if any(not isinstance(k, str) or not isinstance(v, str) or "\x00" in k + v or "=" in k
               for k, v in self.environment.items()):
            raise ValueError("Execution environment requires valid string keys and values.")
        if self._windows:
            self.environment = {key.upper(): value for key, value in self.environment.items()}
        self.environment["PATH"] = os.pathsep.join(str((self.cwd / part).resolve())
            if not os.path.isabs(part) else part for part in self.environment.get("PATH", os.defpath).split(os.pathsep))
        self.version = digest(dict(config=self.config.model_dump(mode="json"), root=str(self.root),
            cwd=str(self.cwd), root_identity=self._root_identity, environment=digest(self.environment), adapter=adapter_version))

    def _directory(self, requested):
        # Directory mtime changes during ordinary commands; identity is inode,
        # not the content-sensitive executable fingerprint used below.
        try:
            root_stat = self.root.stat()
            if self._root_identity != f"{root_stat.st_dev}:{root_stat.st_ino}":
                raise ValueError("Workspace root changed.")
            raw = Path(requested).expanduser()
            path = (raw if raw.is_absolute() else self.cwd / raw).resolve(strict=True)
            if not path.is_dir():
                raise ValueError("Working directory is not a directory.")
            return path
        except (OSError, ValueError, RuntimeError) as error:
            raise ToolBusinessError(str(error), code="invalid_working_directory") from error

    def _binary(self, name, cwd):
        if not name or "\x00" in name:
            raise ToolBusinessError("Program must be a nonempty valid path/name.", code="invalid_command")
        if Path(name).is_absolute() or "/" in name or "\\" in name:
            selected = Path(name).expanduser()
            selected = selected if selected.is_absolute() else cwd / selected
        else:
            found = shutil.which(name, path=self.environment["PATH"])
            if found is None:
                raise ToolBusinessError("Executable is not available: " + name, code="program_unavailable")
            selected = Path(found)
        try:
            selected = selected.resolve(strict=True)
            if not selected.is_file() or not os.access(selected, os.X_OK):
                raise OSError("Program is not executable.")
            if self._windows and selected.suffix.lower() in {".bat", ".cmd"}:
                raise OSError("Batch files require an explicit supported Shell adapter.")
            return selected
        except OSError as error:
            raise ToolBusinessError(str(error), code="program_unavailable") from error

    def _shell_binary(self, shell, cwd):
        program = shell + ".exe" if self._windows and shell in {"pwsh", "powershell"} else shell
        try:
            return self._binary(program, cwd)
        except ToolBusinessError as error:
            if error.code != "program_unavailable" or not self._windows or shell != "powershell":
                raise
        # Windows PowerShell may exist even when its directory is absent from PATH.
        for name in ("SYSTEMROOT", "WINDIR"):
            if root := self.environment.get(name):
                candidate = Path(root) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
                if candidate.is_absolute():
                    try:
                        return self._binary(str(candidate), cwd)
                    except ToolBusinessError as error:
                        if error.code != "program_unavailable":
                            raise
        raise ToolBusinessError("Windows PowerShell (powershell.exe) was not found in PATH or the Windows system directory.",
            code="program_unavailable")

    @property
    def default_shell(self):
        if not self._windows:
            return "bash"
        for shell in ("pwsh", "powershell"):
            try:
                self._shell_binary(shell, self.cwd)
                return shell
            except ToolBusinessError as error:
                if error.code != "program_unavailable":
                    raise
        return "pwsh"

    @property
    def shell_description(self):
        shell = self.default_shell
        try:
            executable = self._shell_binary(shell, self.cwd)
        except ToolBusinessError:
            return ("No default shell is available. Install PowerShell 7 (pwsh.exe) or enable Windows PowerShell "
                "(powershell.exe); exec mode can still run installed programs.") if self._windows else (
                "Default shell bash is unavailable; install bash or select an installed shell explicitly.")
        label = {"bash": "Bash", "pwsh": "PowerShell 7+", "powershell": "Windows PowerShell 5.1"}[shell]
        return (f"Omit to use {label} ({shell}) at {executable}. "
            "Explicit shell selections require that interpreter and never switch to another. "
            "Shell scripts run without startup profiles." + (
                " Use Windows PowerShell 5.1 syntax; && and || are unavailable." if shell == "powershell" else ""))

    def _managed_path(self, requested):
        path = self.files._resolve(requested)
        if not path.is_relative_to(self.root):
            raise ToolBusinessError("Logs, caches and temporary directories must be inside the Workspace.", code="invalid_output_path")
        return path

    def _wait_limit(self, value):
        if not 0 <= value <= self.config.max_wait_seconds:
            raise ToolBusinessError(f"Wait must be between 0 and {self.config.max_wait_seconds:g} seconds.", code="invalid_wait")
        return value

    def _output_limit(self, arguments, *, logged=False):
        maximum = arguments.get("max_output_bytes")
        maximum = min(self.config.max_output_bytes, self.config.log_preview_bytes) if maximum is None and logged else (
            self.config.max_output_bytes if maximum is None else maximum)
        if not 4 <= maximum <= self.config.max_output_bytes:
            raise ToolBusinessError(f"Output limit must be between 4 and {self.config.max_output_bytes} bytes.", code="invalid_output_limit")
        return maximum

    def _plan(self, arguments):
        command = arguments
        cwd = self._directory(arguments.get("cwd", "."))
        timeout = arguments.get("timeout_seconds")
        timeout = self.config.default_timeout_seconds if timeout is None else timeout
        if not 0 < timeout <= self.config.max_timeout_seconds:
            raise ToolBusinessError(f"Execution timeout must be greater than 0 and at most {self.config.max_timeout_seconds:g} seconds.", code="invalid_timeout")
        self._wait_limit(self.config.default_yield_seconds if arguments.get("yield_seconds") is None else arguments["yield_seconds"])
        self._output_limit(arguments, logged=bool(arguments.get("log_path")))
        environment = dict(self.environment)
        managed = []
        for name in ("cache_directory", "temp_directory"):
            requested = arguments.get(name) if arguments.get(name) is not None else getattr(self.config, name)
            if requested is None:
                continue
            path = self._managed_path(requested)
            managed.append(path)
            directory = str(path)
            if name == "cache_directory":
                environment.update(XDG_CACHE_HOME=directory, PIP_CACHE_DIR=str(Path(directory) / "pip"),
                    HF_HOME=str(Path(directory) / "huggingface"), HF_HUB_CACHE=str(Path(directory) / "huggingface" / "hub"),
                    UV_CACHE_DIR=str(Path(directory) / "uv"))
            else:
                environment.update(TMPDIR=directory, TMP=directory, TEMP=directory)
        log = self._managed_path(arguments["log_path"]) if arguments.get("log_path") is not None else None
        if log is not None and not self.backend.capabilities.output_logs:
            raise ToolBusinessError("This execution backend does not support output logs.", code="logging_unavailable")
        if log is not None and any(value.is_relative_to(log) for value in managed):
            raise ToolBusinessError("The log file cannot be a cache/temp directory or its ancestor.", code="invalid_output_path")
        if command["mode"] == "exec":
            program, args = command["program"], tuple(command.get("args", ()))
        else:
            shell = command.get("shell") or self.default_shell
            program = str(self._shell_binary(shell, cwd))
            if shell == "bash":
                args = ("--noprofile", "--norc", "-c", command["script"])
            else:
                script = command["script"]
                if self._windows:
                    # Keep UTF-8 pipes consistent with the backend, including legacy PowerShell.
                    # A script block keeps a leading param() valid after initialization.
                    script = ("$OutputEncoding = [System.Text.UTF8Encoding]::new($false);\n"
                        "[Console]::InputEncoding = $OutputEncoding;\n"
                        "[Console]::OutputEncoding = $OutputEncoding;\n& {\n" + script + "\n}")
                args = ("-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script)
        try:
            command_bytes = sum(len(v.encode("utf-8")) for v in (program, *args))
        except UnicodeError as error:
            raise ToolBusinessError("Command must contain valid UTF-8 text.", code="invalid_command") from error
        if any("\x00" in arg for arg in args) or command_bytes > self.config.max_command_bytes:
            raise ToolBusinessError("Command is invalid or exceeds its size limit.", code="invalid_command")
        executable = self._binary(program, cwd)
        stdin = arguments.get("stdin")
        try:
            data = None if stdin is None else stdin.encode("utf-8")
        except UnicodeError as error:
            raise ToolBusinessError("Input must contain valid UTF-8 text.", code="invalid_input") from error
        if data is not None and len(data) > self.config.max_input_bytes:
            raise ToolBusinessError("Initial stdin exceeds its size limit.", code="input_limit_exceeded")
        spec = ResolvedExecution((str(executable), *args), str(cwd), environment, timeout,
            self.config.max_retained_bytes, arguments.get("keep_stdin_open", False), data)
        return spec, managed, log

    def _session(self, context):
        if not context.session_id:
            raise ToolBusinessError("Execution requires a Session identity in ToolContext.", code="session_required")
        return context.session_id

    async def execute(self, name, arguments, context):
        session_id = self._session(context)
        if name != "execute_command":
            return await self._manage(arguments["request"], context)
        spec, directories, log = self._plan(arguments)
        from autoagent.core import OperatorCallContext
        current = OperatorCallContext.current()
        invocation_id = current.invocation_id if current else (context.approval_scope or {}).get("invocation_id")
        sink = None
        def prepare(spec):
            nonlocal sink
            for directory in directories:
                self.files.create_directory(directory)
            if log is not None:
                from .logs import OutputLog
                sink = OutputLog(str(log), self.files.open_output(log), self.config.max_log_bytes)
                spec = replace(spec, output_log=sink)
            return spec
        try:
            record = await self.manager.start(self.backend, spec, session_id=session_id, invocation_id=invocation_id,
                scope=str(self.root), session_limit=self.config.max_running_processes, prepare=prepare)
        except FileExistsError as error:
            if sink is not None:
                sink.close()
            raise ToolBusinessError("The log path already exists; choose a new file.", code="log_exists") from error
        except OSError as error:
            if sink is not None:
                sink.close()
            raise ToolBusinessError(str(error), code="process_start_failed") from error
        except BaseException:
            if sink is not None:
                sink.close()
            raise
        try:
            seconds = arguments.get("yield_seconds")
            seconds = self.config.default_yield_seconds if seconds is None else seconds
            if seconds:
                try:
                    await asyncio.wait_for(record.handle.wait(), seconds)
                except TimeoutError:
                    pass
            result = await record.handle.read(None, max_bytes=self._output_limit(arguments, logged=sink is not None), wait_seconds=0,
                max_line_bytes=self.config.max_line_bytes)
            return self._output(record, result)
        except asyncio.CancelledError:
            await settle(asyncio.create_task(record.stop()))
            raise

    def _output(self, record, result):
        visible = {key: value for key, value in result.items()
            if key not in {"wall_time_seconds", "total_output_bytes", "retained_start"}
            and value is not None and value != "" and not (key == "truncated" and value is False)}
        if result["truncated"]:
            visible["retained_start"] = result["retained_start"]
            hints = []
            if result.get("preview_omitted_bytes"):
                hints.append(f"Earlier retained output is available with manage_process read cursor={result['retained_start']}.")
            if result.get("line_truncated"):
                hints.append("Use manage_process read with max_line_bytes=0 for raw lines.")
            if result.get("dropped_prefix_bytes"):
                hints.append("Older output left the memory buffer; consult log_path if configured.")
            if hints:
                visible["output_hint"] = " ".join(hints)
        return ToolOutput(structured_content={"process_id": record.id, "cwd": record.cwd, "program": record.command[0], **visible},
            tool_metadata={"execution": {key: result[key] for key in ("wall_time_seconds", "total_output_bytes", "retained_start")}},
            is_error=result["state"] in {"timed_out", "terminated"} or result["exit_code"] not in (None, 0)
                or bool(result["error"]) or bool(result.get("log_error")))

    async def _manage(self, request, context):
        action = request["action"]
        if action == "list":
            records = self.manager.records(context.session_id, str(self.root))
            start, rows, size = request.get("cursor", 0), [], 0
            for r in records[start:start + request.get("limit", 20)]:
                display = shlex.join(r.command)
                finished = getattr(r.handle, "finished", None)
                row = dict(process_id=r.id, command=display[:512], cwd=r.cwd,
                    state=r.handle.state if r.handle else "starting",
                    elapsed_seconds=round(max(0, (finished or time.monotonic()) - r.started), 3))
                code = getattr(getattr(r.handle, "process", None), "returncode", None)
                if code is not None:
                    row["exit_code"] = code
                if len(display) > 512:
                    row["command_truncated"] = True
                size += len(json.dumps(row, ensure_ascii=False).encode("utf-8"))
                if size > self.config.max_output_bytes:
                    if not rows:
                        raise ToolBusinessError("Output limit is too small for a process summary.", code="output_limit_exceeded")
                    break
                rows.append(row)
            next_cursor = start + len(rows)
            return ToolOutput(structured_content={"processes": rows,
                "next_cursor": next_cursor if next_cursor < len(records) else None})
        record = self.manager.get(request["process_id"], context.session_id, str(self.root))
        if record.handle is None:
            raise ToolBusinessError("Process has not finished starting.", code="process_starting")
        if action in {"read", "wait"}:
            seconds = request.get("wait_seconds")
            if seconds is None:
                seconds = self.config.default_wait_seconds
            self._wait_limit(seconds)
            self._output_limit(request)
        if action == "terminate":
            await record.stop()
        elif action == "wait":
            seconds = self.config.default_wait_seconds if request.get("wait_seconds") is None else request["wait_seconds"]
            try:
                await asyncio.wait_for(record.handle.wait(), seconds)
            except TimeoutError:
                pass
        elif action == "write":
            try:
                data = request.get("text", "").encode("utf-8")
            except UnicodeError as error:
                raise ToolBusinessError("Input must contain valid UTF-8 text.", code="invalid_input") from error
            if len(data) > self.config.max_input_bytes:
                raise ToolBusinessError("Input exceeds its size limit.", code="input_limit_exceeded")
            try:
                async with asyncio.timeout(5):
                    await record.handle.write(data, close_stdin=request.get("close_stdin", False))
            except (BrokenPipeError, ConnectionResetError, TimeoutError) as error:
                raise ToolBusinessError(str(error) or "Process did not accept input.", code="process_input_unavailable") from error
        try:
            logged = getattr(getattr(record.handle, "spec", None), "output_log", None) is not None
            result = await record.handle.read(request.get("cursor"), max_bytes=self._output_limit(request, logged=logged),
                wait_seconds=0 if action == "wait" else request.get("wait_seconds", 0),
                max_line_bytes=self.config.max_line_bytes if request.get("max_line_bytes") is None else request["max_line_bytes"])
        except ValueError as error:
            raise ToolBusinessError(str(error), code="invalid_cursor") from error
        if action == "terminate":
            return self._output(record, result).model_copy(update={"is_error": bool(result["error"])})
        return self._output(record, result)

    async def aclose(self):
        await self.manager.aclose()


__all__ = ["ExecutionService", "LocalExecutionService"]
