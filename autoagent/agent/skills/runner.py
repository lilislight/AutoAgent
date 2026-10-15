"""Opt-in script execution, bound to application-selected commands and files."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
import hashlib
import math
import os
from pathlib import Path
import signal
import tempfile
from typing import Protocol, runtime_checkable

from .errors import SkillLimitError, SkillScriptError
from .files import resource_path
from .models import SkillScriptResult, SkillScriptSpec
from .skill import Skill


@runtime_checkable
class SkillScriptRunner(Protocol):
    async def arun(self, skill: Skill, path: str, *, arguments: tuple[str, ...] = (),
                   stdin: str | None = None, timeout_seconds: float | None = None) -> SkillScriptResult: ...


async def _settle(task, *, propagate_cancellation=False):
    """Retain ownership through repeated cancellation until cleanup has finished."""
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
            continue
        except Exception:
            break
    if cancelled and propagate_cancellation:
        task.exception() if not task.cancelled() else None
        raise asyncio.CancelledError
    return task.result()


class SubprocessSkillRunner:
    """Execute only explicit bindings, with staged inputs and bounded process I/O.

    This runs with the application's OS permissions. Configure its deployment
    isolation as needed. Metadata never creates bindings or grants execution.
    The temporary working directory contains only the bound script/resources.
    Environment configuration is runtime-only; omitted env inherits the process
    environment. POSIX children are reclaimed through their process group.
    """

    def __init__(
        self, scripts: Iterable[SkillScriptSpec], *, max_timeout_seconds: float = 30,
        max_output_bytes: int = 128 * 1024, max_input_bytes: int = 1024 * 1024,
        max_arguments: int = 128,
        max_staging_bytes: int = 16 * 1024 * 1024,
        env: Mapping[str, str] | None = None,
    ):
        if not math.isfinite(max_timeout_seconds) or max_timeout_seconds <= 0:
            raise ValueError("Script timeout must be positive and finite.")
        if any(type(value) is not int or value <= 0 for value in (max_output_bytes, max_input_bytes, max_staging_bytes, max_arguments)):
            raise ValueError("Script byte limits must be positive integers.")
        self.max_timeout_seconds = max_timeout_seconds
        self.max_output_bytes = max_output_bytes
        self.max_input_bytes = max_input_bytes
        self.max_arguments = max_arguments
        self.max_staging_bytes = max_staging_bytes
        self._env = None if env is None else dict(env)
        self._scripts = {}
        for value in scripts:
            value = SkillScriptSpec.model_validate(value.model_dump(mode="python"))
            key = (value.skill_name, value.path)
            if key in self._scripts:
                raise ValueError("Duplicate Skill script binding.")
            self._scripts[key] = value

    def _stage(self, skill: Skill, spec: SkillScriptSpec, directory: Path) -> tuple[str, str]:
        # Live documents, scripts and companion resources are captured together
        # before staging. Already frozen Skills keep their complete version ID.
        if skill.snapshot_revision is None:
            skill = skill.freeze(paths=(spec.path, *spec.resources))
        total = 0
        revision = None
        for path in dict.fromkeys((spec.path, *spec.resources)):
            data = skill.read_bytes(path)
            total += len(data)
            if total > self.max_staging_bytes:
                raise SkillLimitError("Script inputs exceed the staging byte limit.")
            destination = directory.joinpath(*path.split("/"))
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(data)
            if path == spec.path:
                revision = hashlib.sha256(data).hexdigest()
        return revision, skill.snapshot_revision

    async def arun(self, skill: Skill, path: str, *, arguments: tuple[str, ...] = (),
                   stdin: str | None = None, timeout_seconds: float | None = None) -> SkillScriptResult:
        resource_path(path)
        spec = self._scripts.get((skill.metadata.name, path))
        if spec is None:
            raise SkillScriptError("Script has no application-provided execution binding.")
        if not isinstance(arguments, tuple) or any(not isinstance(value, str) or "\x00" in value for value in arguments):
            raise SkillScriptError("Script arguments must be strings without NUL characters.")
        if len(arguments) > self.max_arguments:
            raise SkillLimitError("Script arguments exceed their configured count limit.")
        timeout = self.max_timeout_seconds if timeout_seconds is None else timeout_seconds
        if isinstance(timeout, bool) or not math.isfinite(timeout) or not 0 < timeout <= self.max_timeout_seconds:
            raise SkillScriptError("Script timeout exceeds its configured limit.")
        try:
            input_bytes = b"" if stdin is None else stdin.encode("utf-8")
            argument_bytes = sum(len(value.encode("utf-8")) for value in arguments)
        except UnicodeError as error:
            raise SkillScriptError("Script inputs must be valid UTF-8.") from error
        if len(input_bytes) + argument_bytes > self.max_input_bytes:
            raise SkillLimitError("Script stdin and arguments exceed their configured byte limit.")
        with tempfile.TemporaryDirectory(prefix="autoagent-skill-") as temporary:
            directory = Path(temporary)
            staging = asyncio.create_task(asyncio.to_thread(self._stage, skill, spec, directory))
            try:
                revision, snapshot_revision = await asyncio.shield(staging)
            except asyncio.CancelledError as cancelled:
                try:
                    await _settle(staging)
                except (Exception, asyncio.CancelledError):
                    pass
                raise cancelled
            process = None
            tasks = []

            async def cleanup():
                if process is not None:
                    try:
                        if os.name == "posix":
                            os.killpg(process.pid, signal.SIGKILL)
                        elif process.returncode is None:
                            process.kill()
                    except ProcessLookupError:
                        pass
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    await process.wait()

            spawn = asyncio.create_task(asyncio.create_subprocess_exec(
                *spec.command, str(directory.joinpath(*path.split("/"))), *arguments,
                cwd=directory, env=self._env, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                start_new_session=os.name == "posix",
            ))
            try:
                process = await asyncio.shield(spawn)
                stdout, stderr = bytearray(), bytearray()
                output_size = 0

                async def collect(stream, target):
                    nonlocal output_size
                    while data := await stream.read(8192):
                        output_size += len(data)
                        if output_size > self.max_output_bytes:
                            raise SkillLimitError("Script output exceeds its configured byte limit.")
                        target.extend(data)

                async def feed():
                    try:
                        process.stdin.write(input_bytes)
                        await process.stdin.drain()
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    finally:
                        process.stdin.close()
                        try:
                            await process.stdin.wait_closed()
                        except (BrokenPipeError, ConnectionResetError):
                            pass

                tasks = [asyncio.create_task(collect(process.stdout, stdout)),
                         asyncio.create_task(collect(process.stderr, stderr)),
                         asyncio.create_task(feed()), asyncio.create_task(process.wait())]
                async with asyncio.timeout(timeout):
                    await asyncio.gather(*tasks)
                return SkillScriptResult(
                    skill_name=skill.metadata.name, path=path, revision=revision, snapshot_revision=snapshot_revision,
                    returncode=process.returncode,
                    stdout=stdout.decode("utf-8", errors="replace"),
                    stderr=stderr.decode("utf-8", errors="replace"),
                )
            except asyncio.CancelledError:
                if process is None:
                    try:
                        process = await _settle(spawn)
                    except OSError:
                        pass
                raise
            except TimeoutError as error:
                raise SkillScriptError("Skill script timed out.") from error
            except OSError as error:
                raise SkillScriptError("Skill script could not be launched.") from error
            finally:
                closing = asyncio.create_task(cleanup())
                await _settle(closing, propagate_cancellation=True)


__all__ = ["SkillScriptRunner", "SubprocessSkillRunner"]
