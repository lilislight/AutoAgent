"""Two ordinary Tools with the existing ToolSet authorization boundary."""

import json
import os
from typing import Annotated, Literal
from pydantic import Field, ValidationError

from ..models import LLMToolResult, ToolOutput
from ..tools import Tool, ToolContext, ToolSet
from ..tools.operations import ToolOperationPolicy
from .models import ExecCommand, ShellCommand, ProcessRequest
from .service import ExecutionService


class _ExecutionTool(Tool):
    def __init__(self, handler, service, **kwargs):
        super().__init__(handler, **kwargs)
        self.service = service

    def _invalid_arguments(self, call, message, *, details=()):
        result = super()._invalid_arguments(call, message, details=details)
        if self.metadata.name == "execute_command":
            try:
                decoded = json.loads(call.arguments)
                nested = decoded.get("command")
                shell = decoded.get("mode") == "shell" or "script" in decoded or (
                    isinstance(nested, dict) and nested.get("mode") == "shell")
            except (ValueError, AttributeError):
                shell = False
            result.structured_content["error"].update(
                hint="Put mode, program/args or script/shell, cwd and output limits at the top level. Do not wrap them in command or add undeclared fields.",
                expected_shape={"mode": "shell", "script": "echo hi"} if shell else
                    {"mode": "exec", "program": "git", "args": ["status", "--short"]})
        else:
            result.structured_content["error"].update(
                hint="Put the process operation inside request. Use a returned process_id and next_cursor when reading output.",
                expected_shape={"request": {"action": "list"}})
        return result

    def _prepare(self, call):
        prepared = super()._prepare(call)
        if isinstance(prepared, LLMToolResult) or self.metadata.name != "execute_command":
            return prepared
        arguments, record = prepared
        try:
            if record["mode"] == "exec":
                if record.get("script") is not None or record.get("shell") is not None:
                    raise ValueError("Exec mode accepts program and args; omit script and shell.")
                command = ExecCommand.model_validate({key: record[key] for key in ("mode", "program", "args")
                    if record.get(key) is not None}, strict=True)
            else:
                if record.get("program") is not None or record.get("args") is not None:
                    raise ValueError("Shell mode accepts script and optional shell; omit program and args.")
                command = ShellCommand.model_validate({"mode": "shell", "script": record.get("script"),
                    "shell": record.get("shell") or ("pwsh" if os.name == "nt" else "bash")}, strict=True)
        except ValidationError as error:
            details = error.errors(include_url=False, include_input=False, include_context=False)
            return self._invalid_arguments(call, "; ".join(
                f"{'.'.join(map(str, item['loc']))}: {item['msg']}" for item in details), details=details)
        except ValueError as error:
            return self._invalid_arguments(call, str(error))
        return arguments, {**record, **command.model_dump(mode="json")}

    @property
    def definition(self):
        value = super().definition
        config = self.service.config
        if value.name == "execute_command":
            props = value.parameters["properties"]
            props["args"]["description"] = "Literal argument array for exec mode. Do not repeat program; no shell expansion or splitting."
            props["program"]["description"] = "Executable name or path for exec mode, not an entire command line."
            props["shell"]["description"] = "Shell interpreter for shell mode. Omit to use bash on POSIX or pwsh on Windows."
            for name, description in {
                "cwd": "Working directory relative to this Workspace's working directory, or an absolute directory subject to policy. Omitted '.' does not mean the repository root; the result reports resolved cwd.",
                "yield_seconds": "Maximum initial wait, returning sooner on exit. 0 immediately returns a process handle. A running result is not a failure: continue with manage_process rather than starting the command again.",
                "timeout_seconds": "Hard runtime deadline, independent of yield/read/wait. Reaching it stops the process and reports timed_out; increasing a later wait cannot extend this deadline.",
                "log_path": "Optional new Workspace log file for longer output. Existing files are never overwritten; choose a unique path for each launch. Log capture has its own size cap.",
                "stdin": "Initial UTF-8 input. Use keep_stdin_open only if later writes are needed; otherwise stdin closes after this input.",
            }.items():
                props[name]["description"] = description
            for name, maximum, default in (("yield_seconds", config.max_wait_seconds, config.default_yield_seconds),
                    ("timeout_seconds", config.max_timeout_seconds, config.default_timeout_seconds)):
                props[name]["anyOf"][0]["maximum"] = maximum
                props[name]["default"] = default
            for name in ("cache_directory", "temp_directory"):
                props[name]["default"] = getattr(config, name)
            props["max_output_bytes"]["anyOf"][0]["maximum"] = config.max_output_bytes
            props["stdin"]["anyOf"][0]["maxLength"] = config.max_input_bytes
            description = f" Waits are capped at {config.max_wait_seconds:g}s; process runtime at {config.max_timeout_seconds:g}s. Logged previews default to {min(config.log_preview_bytes, config.max_output_bytes)} bytes; logs are capped at {config.max_log_bytes} bytes."
            description += (f" Command/program arguments together are capped at {config.max_command_bytes} UTF-8 bytes;"
                            f" stdin at {config.max_input_bytes} UTF-8 bytes. At most {config.max_running_processes} concurrent processes per Session; wait for or terminate existing handles before starting more.")
        else:
            for name in ("ProcessRead", "ProcessWait"):
                prop = value.parameters["$defs"][name]["properties"]["wait_seconds"]
                (prop["anyOf"][0] if "anyOf" in prop else prop)["maximum"] = config.max_wait_seconds
                if name == "ProcessWait":
                    prop["default"] = config.default_wait_seconds
                value.parameters["$defs"][name]["properties"]["max_output_bytes"]["anyOf"][0]["maximum"] = config.max_output_bytes
                value.parameters["$defs"][name]["properties"]["max_line_bytes"]["description"] = (
                    f"Per-line preview cap, default {config.max_line_bytes or 'unlimited'} bytes. Set 0 for raw lines; byte cursors and log files are unchanged.")
            value.parameters["$defs"]["ProcessWrite"]["properties"]["text"]["maxLength"] = config.max_input_bytes
            description = (f" Waits are capped at {config.max_wait_seconds:g}s; wait defaults to {config.default_wait_seconds:g}s."
                           f" Each stdin write is capped at {config.max_input_bytes} UTF-8 bytes; split larger input into multiple writes."
                           f" Retained output is capped at {config.max_retained_bytes} bytes; use log_path when launching to preserve longer output.")
        return value.model_copy(update={"description": value.description + description})


def execution_tools(service, *, context=None):
    if not isinstance(service, ExecutionService):
        raise TypeError("Execution Tools require an ExecutionService.")
    async def execute_command(mode: Literal["exec", "shell"], program: str | None = None,
            args: Annotated[list[str] | None, Field(max_length=1024)] = None,
            script: str | None = None, shell: Literal["bash", "pwsh"] | None = None,
            cwd: str = ".", stdin: str | None = None,
            keep_stdin_open: bool = False,
            yield_seconds: Annotated[float | None, Field(ge=0)] = None,
            timeout_seconds: Annotated[float | None, Field(gt=0)] = None,
            log_path: str | None = None, cache_directory: str | None = None,
            temp_directory: str | None = None,
            max_output_bytes: Annotated[int | None, Field(ge=4)] = None) -> ToolOutput:
        """Run a program or shell script in the Workspace. Exec example: {"mode":"exec","program":"git","args":["status","--short"]}. Shell example: {"mode":"shell","script":"echo hi"}. All options are top-level. Use cwd instead of cd and the project's interpreter when running code. yield_seconds returns a running handle; timeout_seconds kills on deadline. Continue running processes with manage_process, never restart to read output. log_path saves longer output; previews shorten long lines automatically. cache_directory/temp_directory redirect conventional caches/temp (not a sandbox). stdin closes unless keep_stdin_open=true."""
        current = ToolContext.current()
        return await service.execute("execute_command", current.arguments, current)

    async def manage_process(request: ProcessRequest) -> ToolOutput:
        """Manage this Session's processes. Example: {"request":{"action":"list"}}. wait awaits exit or wait_seconds; expiry leaves the process running. read with cursor=previous next_cursor waits for output/exit/expiry; omitted cursor repeats a latest preview. Cursors are raw non-consuming UTF-8 byte offsets. has_more describes buffered output after next_cursor, not process completion. Check state and exit_code. max_line_bytes=0 retrieves raw lines. write sends stdin; terminate stops the owned process tree where supported. Never restart a process to read its output. Handles do not survive Host restart."""
        current = ToolContext.current()
        return await service.execute("manage_process", current.arguments, current)

    tools = []
    for handler in (execute_command, manage_process):
        name = handler.__name__
        def resolve(arguments, context, name=name):
            from ..tools import ToolBusinessError
            try:
                return service.operations(name, arguments, context)
            except OSError as error:
                raise ToolBusinessError("Execution target is unavailable: " + str(error), code="execution_target_unavailable") from error
        tools.append(_ExecutionTool(handler, service, operations=resolve))
    return ToolSet(tools, context=context, policy=ToolOperationPolicy(service.evaluate))


__all__ = ["execution_tools"]
