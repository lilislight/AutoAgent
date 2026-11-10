"""Local asynchronous pipes, bounded logs, deadlines and owned cleanup."""

import asyncio
from collections import deque
import codecs
import os
import signal
import time

from .backend import ExecutionCapabilities


async def settle(task):
    """Finish cleanup even if its caller receives repeated cancellation."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


class LocalProcess:
    def __init__(self, process, spec):
        self.process, self.spec = process, spec
        self.started = time.monotonic()
        self.finished = None
        self.state = "running"
        self.reason = None
        self.error = None
        self._chunks = deque()
        self._start = self._end = 0
        self._changed = asyncio.Condition()
        self._input_lock = asyncio.Lock()
        self._stop_task = None
        self._readers = [asyncio.create_task(self._collect(name, stream)) for name, stream in
                         (("stdout", process.stdout), ("stderr", process.stderr))]
        self._feed = asyncio.create_task(self._initial_input())
        self._done = asyncio.create_task(self._monitor())

    async def _notify(self):
        async with self._changed:
            self._changed.notify_all()

    async def _collect(self, name, stream):
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        try:
            while raw := await stream.read(8192):
                await self._append(name, decoder.decode(raw).encode("utf-8"))
        finally:
            await self._append(name, decoder.decode(b"", final=True).encode("utf-8"))

    async def _append(self, name, data):
        if not data:
            return
        if self.spec.output_log is not None:
            self.spec.output_log.write(data)
        # Logs are normalized UTF-8 text. Each retained chunk starts/ends at a
        # character boundary, including when an OS read split a codepoint.
        self._chunks.append((self._end, name, data))
        self._end += len(data)
        excess = self._end - self._start - self.spec.max_retained_bytes
        while excess > 0 and self._chunks:
            offset, kind, first = self._chunks.popleft()
            discarded = min(excess, len(first))
            while discarded < len(first) and first[discarded] & 0xC0 == 0x80:
                discarded += 1
            self._start += discarded
            excess -= discarded
            if discarded < len(first):
                self._chunks.appendleft((offset + discarded, kind, first[discarded:]))
        await self._notify()

    def _boundary(self, position, *, forward):
        for offset, _, data in self._chunks:
            if offset <= position < offset + len(data):
                index = position - offset
                while 0 <= index < len(data) and data[index] & 0xC0 == 0x80:
                    index += 1 if forward else -1
                return offset + index
        return position

    async def _initial_input(self):
        try:
            if self.spec.stdin is not None:
                await self.write(self.spec.stdin, close_stdin=not self.spec.keep_stdin_open)
            elif self.process.stdin is not None and not self.spec.keep_stdin_open:
                await self.write(b"", close_stdin=True)
        except (BrokenPipeError, ConnectionResetError):
            pass

    async def write(self, data, *, close_stdin=False):
        async with self._input_lock:
            stream = self.process.stdin
            if stream is None or stream.is_closing() or self.process.returncode is not None:
                raise BrokenPipeError("Process stdin is closed.")
            stream.write(data)
            await stream.drain()
            if close_stdin:
                stream.close()
                try:
                    await stream.wait_closed()
                except (BrokenPipeError, ConnectionResetError):
                    pass

    async def _stop(self):
        if os.name == "posix":
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                return
            await asyncio.sleep(0.2)
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        elif self.process.returncode is None:
            # Windows fallback is explicitly best effort. A future native Job
            # backend can supply stronger ownership without changing Tools.
            killer = None
            try:
                killer = await asyncio.create_subprocess_exec("taskkill", "/PID", str(self.process.pid), "/T", "/F",
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
                code = await asyncio.wait_for(killer.wait(), 5)
                if code and self.process.returncode is None:
                    self.process.kill()
            except (OSError, TimeoutError):
                if killer is not None and killer.returncode is None:
                    killer.kill()
                    await killer.wait()
                if self.process.returncode is None:
                    self.process.kill()

    async def _stop_once(self):
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._stop())
        await asyncio.shield(self._stop_task)

    async def _monitor(self):
        try:
            # Process.wait can wait for inherited pipe EOF. The deadline bounds
            # both the direct child and descendants retaining those pipes.
            async with asyncio.timeout(self.spec.timeout_seconds):
                while self.process.returncode is None:
                    await asyncio.sleep(0.01)
                await self._stop_once()
                await asyncio.gather(*self._readers, self._feed)
        except TimeoutError:
            self.reason = self.reason or "timeout"
        except Exception as error:
            self.reason = self.reason or "io_error"
            self.error = str(error)
        finally:
            try:
                # Reclaim ordinary background descendants even after root exit.
                await self._stop_once()
                async with asyncio.timeout(2):
                    await self.process.wait()
                    await asyncio.gather(*self._readers, self._feed, return_exceptions=True)
            except (TimeoutError, OSError) as error:
                self.error = self.error or str(error) or "Process output did not settle during cleanup."
            finally:
                for task in (*self._readers, self._feed):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*self._readers, self._feed, return_exceptions=True)
                # asyncio.Process has no public close method. The local adapter
                # owns its CPython subprocess transport: close pipe descriptors
                # even if an escaped descendant retains the other ends. This
                # does not claim that an escaped process was terminated.
                self.process._transport.close()
                if self.spec.output_log is not None:
                    try:
                        self.spec.output_log.close()
                    except OSError as error:
                        self.error = self.error or str(error)
                self.state = {"timeout": "timed_out", "terminated": "terminated"}.get(self.reason, "exited")
                self.finished = time.monotonic()
                await self._notify()

    async def wait(self):
        await asyncio.shield(self._done)

    async def terminate(self):
        if self._done.done():
            return
        self.reason = self.reason or "terminated"
        await self._stop_once()
        await self.wait()

    async def read(self, cursor, *, max_bytes, wait_seconds):
        if cursor is not None and cursor > self._end:
            raise ValueError(f"Cursor {cursor} is past current output position {self._end}. "
                "Use the previous next_cursor for this process; cursors are byte offsets, not timestamps or line numbers.")
        position = max(self._start, self._end - max_bytes) if cursor is None else cursor
        deadline = time.monotonic() + wait_seconds
        async with self._changed:
            while position >= self._end and self.state == "running":
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    await asyncio.wait_for(self._changed.wait(), remaining)
                except TimeoutError:
                    break
        requested = position
        position = max(position, self._start)
        position = self._boundary(position, forward=True)
        end = self._boundary(min(self._end, position + max_bytes), forward=False)
        streams = {"stdout": bytearray(), "stderr": bytearray()}
        for offset, name, data in self._chunks:
            left, right = max(position, offset), min(end, offset + len(data))
            if left < right:
                streams[name].extend(data[left - offset:right - offset])
        result = {name: bytes(data).decode("utf-8") for name, data in streams.items()}
        result.update(state=self.state, exit_code=self.process.returncode,
            wall_time_seconds=round((self.finished or time.monotonic()) - self.started, 3),
            next_cursor=end, retained_start=self._start, total_output_bytes=self._end,
            truncated=requested < position or cursor is None and position > 0,
            has_more=end < self._end, reason=self.reason, error=self.error)
        omitted = position - (cursor if cursor is not None else 0)
        if omitted > 0:
            result["skipped_bytes"] = omitted
        if self.spec.output_log is not None:
            result.update(self.spec.output_log.summary())
        return result


class LocalExecutionBackend:
    capabilities = ExecutionCapabilities(output_logs=True)

    async def spawn(self, spec):
        try:
            process = await asyncio.create_subprocess_exec(*spec.argv, cwd=spec.cwd, env=dict(spec.environment),
                stdin=asyncio.subprocess.PIPE if spec.keep_stdin_open or spec.stdin is not None else asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                start_new_session=os.name == "posix",
                **({"creationflags": 0x08000000} if os.name == "nt" else {}))
        except BaseException:
            if spec.output_log is not None:
                spec.output_log.close()
            raise
        return LocalProcess(process, spec)


__all__ = ["LocalExecutionBackend"]
