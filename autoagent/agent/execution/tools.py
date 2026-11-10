"""Two ordinary Tools with the existing ToolSet authorization boundary."""

from typing import Annotated
from pydantic import Field

from ..models import ToolOutput
from ..tools import Tool, ToolContext, ToolSet
from ..tools.operations import ToolOperationPolicy
from .models import ExecutionCommand, ProcessRequest
from .service import ExecutionService


class _ExecutionTool(Tool):
    def __init__(self, handler, service, **kwargs):
        super().__init__(handler, **kwargs)
        self.service = service

    @property
    def definition(self):
        value = super().definition
        config = self.service.config
        if value.name == "execute_command":
            props = value.parameters["properties"]
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
            value.parameters["$defs"]["ProcessWrite"]["properties"]["text"]["maxLength"] = config.max_input_bytes
            description = (f" Waits are capped at {config.max_wait_seconds:g}s; wait defaults to {config.default_wait_seconds:g}s."
                           f" Each stdin write is capped at {config.max_input_bytes} UTF-8 bytes; split larger input into multiple writes."
                           f" Retained output is capped at {config.max_retained_bytes} bytes; use log_path when launching to preserve longer output.")
        return value.model_copy(update={"description": value.description + description})


def execution_tools(service, *, context=None):
    if not isinstance(service, ExecutionService):
        raise TypeError("Execution Tools require an ExecutionService.")
    async def execute_command(command: ExecutionCommand, cwd: str = ".", stdin: str | None = None,
            keep_stdin_open: bool = False,
            yield_seconds: Annotated[float | None, Field(ge=0)] = None,
            timeout_seconds: Annotated[float | None, Field(gt=0)] = None,
            log_path: str | None = None, cache_directory: str | None = None,
            temp_directory: str | None = None,
            max_output_bytes: Annotated[int | None, Field(ge=4)] = None) -> ToolOutput:
        """Run in the Workspace. Prefer exec with program/args and cwd; use the interpreter required by the project, not an assumed python/python3. Shell is for scripts/pipes; its exit_code is the Shell's final status. yield_seconds limits the initial wait; timeout_seconds stops the process. Running results retain a process_id and next_cursor: continue that handle instead of restarting. has_more describes output buffered now, not future output. Check both state and exit_code. log_path saves combined output to a new bounded Workspace file without overwriting; stdout/stderr remain bounded previews. cache_directory/temp_directory redirect conventional library caches/temp into Workspace directories (not a sandbox). stdin closes unless keep_stdin_open=true."""
        current = ToolContext.current()
        return await service.execute("execute_command", current.arguments, current)

    async def manage_process(request: ProcessRequest) -> ToolOutput:
        """Manage this Session's processes. wait awaits exit or wait_seconds even when new logs arrive; expiry returns running without stopping the process. read(cursor=previous next_cursor, wait_seconds=30) returns buffered new output immediately or waits for output/exit/expiry; omitted cursor repeats a latest-output preview. Cursors are non-consuming UTF-8 byte offsets, not line numbers. has_more=false means no more buffered output now, not that the process has exited. Check state and exit_code. write can close stdin; terminate stops the owned process tree where supported; list enumerates handles. Neither read nor wait restarts commands. Handles do not survive Host restart. max_output_bytes sets the returned preview size up to the configured cap. Logs/previews are bounded."""
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
