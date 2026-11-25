"""Execution configuration and portable, discriminated Tool arguments."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from ..models._base import Model, NonEmpty


class ExecutionToolsConfig(Model):
    default_timeout_seconds: float = Field(default=120, gt=0)
    max_timeout_seconds: float = Field(default=600, gt=0)
    default_yield_seconds: float = Field(default=10, ge=0)
    default_wait_seconds: float = Field(default=30, gt=0)
    max_wait_seconds: float = Field(default=600, gt=0, le=3600)
    max_output_bytes: int = Field(default=64 * 1024, ge=4, le=1024 * 1024)
    max_retained_bytes: int = Field(default=1024 * 1024, ge=4, le=16 * 1024 * 1024)
    max_line_bytes: int | None = Field(default=2048, ge=4, le=1024 * 1024)
    max_input_bytes: int = Field(default=64 * 1024, gt=0)
    max_command_bytes: int = Field(default=64 * 1024, gt=0)
    max_log_bytes: int = Field(default=16 * 1024 * 1024, ge=4)
    log_preview_bytes: int = Field(default=4096, ge=4)
    cache_directory: str | None = None
    temp_directory: str | None = None
    max_running_processes: int = Field(default=4, gt=0, le=64)

    @model_validator(mode="after")
    def limits(self):
        if self.default_timeout_seconds > self.max_timeout_seconds:
            raise ValueError("Default timeout exceeds the execution timeout cap.")
        if max(self.default_yield_seconds, self.default_wait_seconds) > self.max_wait_seconds:
            raise ValueError("Default wait exceeds max_wait_seconds.")
        return self


class ExecCommand(Model):
    mode: Literal["exec"]
    program: NonEmpty = Field(description="Executable name or path only, not a command line. Relative paths resolve against cwd.", examples=["git"])
    args: list[str] = Field(default_factory=list, max_length=1024,
        description="One literal argument per element. No shell splitting, expansion or pipes. Do not repeat the program name.", examples=[["status", "--short"]])


class ShellCommand(Model):
    mode: Literal["shell"]
    shell: Literal["bash", "pwsh", "powershell"]
    script: NonEmpty = Field(description="Script interpreted by the selected shell without startup profiles. Use exec for a program with literal arguments.")


ExecutionCommand = Annotated[ExecCommand | ShellCommand, Field(discriminator="mode")]


class ProcessRead(Model):
    action: Literal["read"]
    process_id: NonEmpty
    cursor: int | None = Field(default=None, ge=0, description="Pass the previous next_cursor for new output only. 0 reads earliest retained output; omitted/null previews the latest output again. Reading never consumes output.")
    wait_seconds: float = Field(default=0, ge=0, description="If no unread output exists at cursor, wait for output, exit, or this many seconds. Expiry leaves the process running; do not restart it.")
    max_output_bytes: int | None = Field(default=None, ge=4)
    max_line_bytes: int | None = Field(default=None, ge=0, description="Optional per-line preview cap. Omit for the configured default; 0 returns raw lines. Raw byte cursors and saved logs are unchanged.")


class ProcessWait(Model):
    action: Literal["wait"]
    process_id: NonEmpty
    cursor: int | None = Field(default=None, ge=0, description="Pass the previous next_cursor to avoid repeating output. Omitted/null returns a latest-output preview; reading never consumes output.")
    wait_seconds: float | None = Field(default=None, gt=0, description="Wait for exit or this duration, ignoring intermediate output. Expiry returns running without killing the process. Use read with cursor and wait_seconds to wake on output.")
    max_output_bytes: int | None = Field(default=None, ge=4)
    max_line_bytes: int | None = Field(default=None, ge=0, description="Optional per-line preview cap; 0 returns raw lines. Defaults to the configured limit.")


class ProcessWrite(Model):
    action: Literal["write"]
    process_id: NonEmpty
    text: str = ""
    close_stdin: bool = False


class ProcessTerminate(Model):
    action: Literal["terminate"]
    process_id: NonEmpty


class ProcessList(Model):
    action: Literal["list"]
    cursor: int = Field(default=0, ge=0)
    limit: int = Field(default=20, ge=1, le=128)


ProcessRequest = Annotated[ProcessRead | ProcessWait | ProcessWrite | ProcessTerminate | ProcessList, Field(discriminator="action")]

__all__ = ["ExecutionToolsConfig", "ExecCommand", "ShellCommand", "ExecutionCommand", "ProcessRequest"]
