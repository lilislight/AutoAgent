"""Execution configuration and portable, discriminated Tool arguments."""

from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from ..models._base import Model, NonEmpty

Decision = Literal["allow", "ask", "deny"]


class CommandRule(Model):
    """Match one resolved command, independent of its exec/Shell entry point.

    Omitted argument selectors match every argument list; args=() matches no
    arguments. Regex uses fullmatch on shlex.join(args), a display format only.
    Rules are ordered: the last match wins within this configuration.
    """

    program: NonEmpty
    decision: Decision
    args: tuple[str, ...] | None = None
    args_prefix: tuple[str, ...] | None = None
    args_regex: NonEmpty | None = None
    cwd: str | None = None
    reason: str | None = None

    @field_validator("args", "args_prefix", mode="before")
    @classmethod
    def tuples(cls, value):
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def selectors(self):
        if sum(value is not None for value in (self.args, self.args_prefix, self.args_regex)) > 1:
            raise ValueError("Use one argument selector per CommandRule.")
        if self.args_regex is not None:
            import regex
            try:
                regex.compile(self.args_regex)
            except regex.error as error:
                raise ValueError("Invalid command argument regex: " + str(error)) from error
        return self


class ExecutionToolsConfig(Model):
    default_decision: Decision = "ask"
    rules: tuple[CommandRule, ...] = ()
    default_timeout_seconds: float = Field(default=120, gt=0)
    max_timeout_seconds: float = Field(default=600, gt=0)
    default_yield_seconds: float = Field(default=10, ge=0)
    default_wait_seconds: float = Field(default=30, gt=0)
    max_wait_seconds: float = Field(default=600, gt=0, le=3600)
    max_output_bytes: int = Field(default=64 * 1024, ge=4, le=1024 * 1024)
    max_retained_bytes: int = Field(default=1024 * 1024, ge=4, le=16 * 1024 * 1024)
    max_input_bytes: int = Field(default=64 * 1024, gt=0)
    max_command_bytes: int = Field(default=64 * 1024, gt=0)
    max_log_bytes: int = Field(default=16 * 1024 * 1024, ge=4)
    log_preview_bytes: int = Field(default=4096, ge=4)
    cache_directory: str | None = None
    temp_directory: str | None = None
    max_running_processes: int = Field(default=4, gt=0, le=64)
    regex_timeout_seconds: float = Field(default=0.05, gt=0, le=1)

    @field_validator("rules", mode="before")
    @classmethod
    def tuples(cls, value):
        return tuple(value) if isinstance(value, list) else value

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
    shell: Literal["bash", "pwsh"]
    script: NonEmpty = Field(description="Script interpreted by the selected shell without startup profiles. Use exec for a program with literal arguments.")


ExecutionCommand = Annotated[ExecCommand | ShellCommand, Field(discriminator="mode")]


class ProcessRead(Model):
    action: Literal["read"]
    process_id: NonEmpty
    cursor: int | None = Field(default=None, ge=0, description="Pass the previous next_cursor for new output only. 0 reads earliest retained output; omitted/null previews the latest output again. Reading never consumes output.")
    wait_seconds: float = Field(default=0, ge=0, description="If no unread output exists at cursor, wait for output, exit, or this many seconds. Expiry leaves the process running; do not restart it.")
    max_output_bytes: int | None = Field(default=None, ge=4)


class ProcessWait(Model):
    action: Literal["wait"]
    process_id: NonEmpty
    cursor: int | None = Field(default=None, ge=0, description="Pass the previous next_cursor to avoid repeating output. Omitted/null returns a latest-output preview; reading never consumes output.")
    wait_seconds: float | None = Field(default=None, gt=0, description="Wait for exit or this duration, ignoring intermediate output. Expiry returns running without killing the process. Use read with cursor and wait_seconds to wake on output.")
    max_output_bytes: int | None = Field(default=None, ge=4)


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

__all__ = ["CommandRule", "ExecutionToolsConfig", "ExecCommand", "ShellCommand", "ExecutionCommand", "ProcessRequest"]
