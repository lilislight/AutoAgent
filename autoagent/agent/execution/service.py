"""Workspace execution plans, shared command policy and Tool observations."""

import asyncio
from dataclasses import replace
from fnmatch import fnmatchcase
import hashlib
import inspect
import json
import os
from pathlib import Path
import shlex
import shutil
import time
from typing import Protocol, runtime_checkable

from ..models import ToolOutput
from ..tools import ToolBusinessError, ToolDecision
from ..tools.operations import ToolOperation
from .analyzer import TreeSitterShellAnalyzer
from .backend import ResolvedExecution
from .local import LocalExecutionBackend, settle
from .manager import ProcessManager
from .models import ExecutionToolsConfig


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def identity(path):
    stat = path.stat()
    return f"{stat.st_dev}:{stat.st_ino}:{stat.st_size}:{stat.st_mtime_ns}"


@runtime_checkable
class ExecutionService(Protocol):
    """Resolved operations are authorized by ToolSet before execute is called."""

    config: ExecutionToolsConfig
    version: str
    def operations(self, name, arguments, context) -> tuple[ToolOperation, ...]: ...
    def evaluate(self, operation, context): ...
    async def execute(self, name, arguments, context) -> ToolOutput: ...
    async def aclose(self): ...


class LocalExecutionService:
    """Local execution with application-owned adapters and runtime environment.

    policy(operation, context) may return a decision or None to use rules.
    Custom adapters/policies must supply a new adapter_version when behavior
    changes. No environment values or live handles enter persisted operations.
    """

    def __init__(self, root, *, working_directory=".", config=None, backend=None,
                 manager=None, analyzer=None, policy=None, environment=None, adapter_version="1"):
        self.root = Path(root).resolve(strict=True)
        self.cwd = (self.root / working_directory).resolve(strict=True)
        if not self.root.is_dir() or not self.cwd.is_dir() or not self.cwd.is_relative_to(self.root):
            raise ValueError("Execution working directory must be inside its Workspace.")
        self.config = ExecutionToolsConfig.model_validate(config or ExecutionToolsConfig())
        self.backend = backend or LocalExecutionBackend()
        self.manager = manager or ProcessManager()
        self.analyzer = analyzer or TreeSitterShellAnalyzer()
        self.policy = policy
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
            if os.name == "nt" and selected.suffix.lower() in {".bat", ".cmd"}:
                raise OSError("Batch files require an explicit supported Shell adapter.")
            return selected
        except OSError as error:
            raise ToolBusinessError(str(error), code="program_unavailable") from error

    def _commands(self, argv, *, cwd, depth=0):
        try:
            argv = (str(self._binary(argv[0], cwd)), *argv[1:])
        except ToolBusinessError:
            pass  # A Shell builtin/cmdlet has no external executable.
        name = Path(argv[0]).stem.lower()
        if depth > 4:
            return (argv,), False, "Nested Shell wrapper limit reached."
        if name not in {"bash", "pwsh", "powershell", "sh", "zsh", "cmd"}:
            return (argv,), True, None
        # Only known, profile-free wrappers share command rules. Other options
        # remain a whole-invocation policy check rather than an unsafe unwrap.
        args = list(argv[1:])
        shell = None
        script = None
        if name == "bash":
            while args and args[0] in {"--noprofile", "--norc"}:
                args.pop(0)
            if len(args) == 2 and args[0] == "-c":
                shell, script = "bash", args[1]
        elif name in {"pwsh", "powershell"}:
            lowered = [value.lower() for value in args]
            if "-noprofile" in lowered and "-command" in lowered:
                index = lowered.index("-command")
                if index == len(args) - 2 and all(value in {"-nologo", "-noprofile", "-noninteractive"} for value in lowered[:index]):
                    shell, script = "pwsh", args[-1]
        if script is None:
            return (argv,), False, "Shell invocation requires whole-command approval."
        analysis = self.analyzer.analyze(script, shell)
        if not analysis.complete or not analysis.commands:
            return (argv,), False, analysis.reason
        result = []
        for child in analysis.commands:
            nested, complete, reason = self._commands(child, cwd=cwd, depth=depth + 1)
            if not complete:
                return (argv,), False, reason
            result.extend(nested)
        return tuple(result), True, None

    def _managed_operation(self, requested, context):
        from ..tools import ToolContext
        operations = self.files.operations("write_file", {"path": requested}, context or ToolContext())
        value = operations[0]
        if not Path(value.resource).is_relative_to(self.root):
            raise ToolBusinessError("Logs, caches and temporary directories must be inside the Workspace.", code="invalid_output_path")
        return value

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

    def _plan(self, arguments, context=None):
        command = arguments["command"]
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
            operation = self._managed_operation(requested, context)
            managed.append(operation)
            directory = operation.resource
            if name == "cache_directory":
                environment.update(XDG_CACHE_HOME=directory, PIP_CACHE_DIR=str(Path(directory) / "pip"),
                    HF_HOME=str(Path(directory) / "huggingface"), HF_HUB_CACHE=str(Path(directory) / "huggingface" / "hub"),
                    UV_CACHE_DIR=str(Path(directory) / "uv"))
            else:
                environment.update(TMPDIR=directory, TMP=directory, TEMP=directory)
        log = self._managed_operation(arguments["log_path"], context) if arguments.get("log_path") is not None else None
        if log is not None and not self.backend.capabilities.output_logs:
            raise ToolBusinessError("This execution backend does not support output logs.", code="logging_unavailable")
        if log is not None and any(Path(value.resource).is_relative_to(Path(log.resource)) for value in managed):
            raise ToolBusinessError("The log file cannot be a cache/temp directory or its ancestor.", code="invalid_output_path")
        if command["mode"] == "exec":
            program, args = command["program"], tuple(command.get("args", ()))
        elif command["shell"] == "bash":
            program, args = "bash", ("--noprofile", "--norc", "-c", command["script"])
        else:
            program, args = "pwsh", ("-NoLogo", "-NoProfile", "-NonInteractive", "-Command", command["script"])
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
        commands, complete, reason = self._commands(spec.argv, cwd=cwd)
        stat = cwd.stat()
        plan = digest(dict(argv=spec.argv, executable=identity(executable), cwd=str(cwd),
            directory_identity=f"{stat.st_dev}:{stat.st_ino}", environment=digest(environment),
            stdin=digest(stdin), log_path=log.resource if log is not None else None,
            keep_stdin_open=spec.keep_stdin_open, timeout=timeout, service=self.version))
        operations = []
        for child in commands:
            try:
                target = self._binary(child[0], cwd)
                resolved, stamp = str(target), identity(target)
            except ToolBusinessError:
                if command["mode"] != "shell" and child == spec.argv:
                    raise
                # Shell builtins/cmdlets need not be external executables.
                resolved, stamp = child[0], "shell_command"
            operations.append(ToolOperation(action="command.execute", resource=resolved,
                attributes=dict(program=Path(child[0]).name, args=json.dumps(child[1:], ensure_ascii=False),
                    cwd=str(cwd), executable_identity=stamp, plan_digest=plan,
                    analysis_complete=str(complete).lower(), analysis_reason=reason or "")))
        return spec, (*operations, *managed, *((log,) if log is not None else ()))

    def _session(self, context):
        if not context.session_id:
            raise ToolBusinessError("Execution requires a Session identity in ToolContext.", code="session_required")
        return context.session_id

    def operations(self, name, arguments, context):
        self._session(context)
        if name == "execute_command":
            return self._plan(arguments, context)[1]
        request = arguments["request"]
        action = request["action"]
        if action in {"read", "wait"}:
            self._wait_limit(self.config.default_wait_seconds if request.get("wait_seconds") is None else request["wait_seconds"])
            self._output_limit(request)
        process_id = request.get("process_id")
        if process_id:
            self.manager.get(process_id, context.session_id, str(self.root))
        return (ToolOperation(action="process." + action, resource=process_id or str(self.root),
            attributes={"session_id": context.session_id, "root": str(self.root)}),)

    def evaluate(self, operation, context):
        if operation.action.startswith("file."):
            return self.files.evaluate(operation, context)
        if self.policy is not None:
            result = self.policy(operation, context)
            if inspect.isawaitable(result):
                async def finish():
                    value = await result
                    return self._rule(operation) if value is None else value
                return finish()
            if result is not None:
                return result
        return self._rule(operation)

    def _rule(self, operation):
        if operation.action.startswith("process."):
            return ToolDecision(action="ask" if operation.action == "process.write" else "allow")
        attributes = operation.attributes
        args = tuple(json.loads(attributes["args"]))
        selected = ToolDecision(action=self.config.default_decision, reason=attributes.get("analysis_reason") or "Unmatched command")
        for rule in self.config.rules:
            actual = attributes["program"]
            try:
                expected = self._binary(rule.program, Path(attributes["cwd"]))
                matched = os.path.normcase(str(expected)) == os.path.normcase(operation.resource)
            except ToolBusinessError:
                matched = attributes["executable_identity"] == "shell_command" and rule.program == actual
                if os.name == "nt" and attributes["executable_identity"] == "shell_command":
                    matched = rule.program.lower() == actual.lower()
            if not matched or rule.cwd is not None and not fnmatchcase(attributes["cwd"], rule.cwd):
                continue
            if rule.args is not None and args != rule.args:
                continue
            if rule.args_prefix is not None and args[:len(rule.args_prefix)] != rule.args_prefix:
                continue
            if rule.args_regex is not None:
                import regex
                try:
                    if regex.fullmatch(rule.args_regex, shlex.join(args), timeout=self.config.regex_timeout_seconds) is None:
                        continue
                except TimeoutError:
                    return ToolDecision(action="deny", reason="Command regex exceeded its evaluation limit.")
            selected = ToolDecision(action=rule.decision, reason=rule.reason or "Command rule")
        return selected

    async def execute(self, name, arguments, context):
        session_id = self._session(context)
        if name != "execute_command":
            return await self._manage(arguments["request"], context)
        spec, operations = self._plan(arguments, context)
        if operations != context.operations:
            raise ToolBusinessError("Execution target changed after authorization; request approval again.", code="execution_plan_changed")
        from autoagent.core import OperatorCallContext
        current = OperatorCallContext.current()
        invocation_id = current.invocation_id if current else (context.approval_scope or {}).get("invocation_id")
        sink = None
        def prepare(spec):
            nonlocal sink
            for operation in operations:
                if not operation.action.startswith("file."):
                    continue
                if arguments.get("log_path") is not None and operation.attributes["requested"] == arguments["log_path"]:
                    from .logs import OutputLog
                    sink = OutputLog(operation.resource, self.files.open_output(operation), self.config.max_log_bytes)
                    spec = replace(spec, output_log=sink)
                else:
                    self.files.create_directory(operation)
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
            result = await record.handle.read(None, max_bytes=self._output_limit(arguments, logged=sink is not None), wait_seconds=0)
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
        return ToolOutput(structured_content={"process_id": record.id, "cwd": record.cwd, **visible},
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
                wait_seconds=0 if action == "wait" else request.get("wait_seconds", 0))
        except ValueError as error:
            raise ToolBusinessError(str(error), code="invalid_cursor") from error
        if action == "terminate":
            return self._output(record, result).model_copy(update={"is_error": bool(result["error"])})
        return self._output(record, result)

    async def aclose(self):
        await self.manager.aclose()


__all__ = ["ExecutionService", "LocalExecutionService"]
