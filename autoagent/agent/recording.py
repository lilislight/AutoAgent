"""Optional model-call metadata recording; no SDK, database or payload dependency."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
import logging
import time
from typing import Literal, Protocol, runtime_checkable
from uuid import uuid4
from weakref import WeakKeyDictionary

from pydantic import Field, model_validator

from autoagent.core.operators.context import OperatorCallContext

from .models._base import Model, NonEmpty

_logger = logging.getLogger(__name__)


class LLMCallContext(Model):
    workflow_id: NonEmpty | None = None
    workflow_revision_id: NonEmpty | None = None
    workflow_version: NonEmpty | None = None
    session_id: NonEmpty | None = None
    invocation_id: NonEmpty | None = None
    occurrence_id: NonEmpty | None = None
    node_id: NonEmpty | None = None
    operator_id: NonEmpty | None = None
    operator_call_id: NonEmpty | None = None
    operator_definition_id: NonEmpty | None = None
    unit_index: int | None = Field(default=None, ge=0)
    attempt: int = Field(default=1, ge=1)


class LLMCallRecord(LLMCallContext):
    """One Provider invocation; IDs and counts only, never prompts/media/secrets.

    A running row survives interrupted processes. Only reported token counts are
    retained; absent usage remains unknown. SDK-internal transport retries on
    borrowed clients are part of this one Provider invocation.
    """

    llm_call_id: NonEmpty
    kind: Literal["generation", "embedding"]
    provider_id: NonEmpty
    protocol: NonEmpty
    requested_model: NonEmpty
    response_model: NonEmpty | None = None
    streaming: bool = False
    status: Literal["running", "succeeded", "failed", "cancelled"]
    started_at_us: int = Field(ge=0)
    completed_at_us: int | None = Field(default=None, ge=0)
    duration_ns: int | None = Field(default=None, ge=0)
    usage_reported: bool = False
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)
    reasoning_tokens: int | None = Field(default=None, ge=0)
    cache_read_tokens: int | None = Field(default=None, ge=0)
    provider_response_id: NonEmpty | None = None
    provider_request_id: NonEmpty | None = None
    finish_reason: NonEmpty | None = None
    error_type: NonEmpty | None = None
    error_code: NonEmpty | None = None
    status_code: int | None = None

    @model_validator(mode="after")
    def _lifecycle(self):
        terminal = self.status != "running"
        if terminal != (self.completed_at_us is not None and self.duration_ns is not None):
            raise ValueError("Terminal call records require completion time and duration.")
        if not terminal and (self.completed_at_us is not None or self.duration_ns is not None):
            raise ValueError("Running calls cannot carry terminal timing.")
        if not self.usage_reported and any(value is not None for value in (
            self.input_tokens, self.output_tokens, self.total_tokens, self.reasoning_tokens, self.cache_read_tokens,
        )):
            raise ValueError("Unreported token usage must remain unknown.")
        if self.status in {"running", "succeeded"} and self.error_type is not None:
            raise ValueError("Only failed/cancelled calls carry failure details.")
        return self


@runtime_checkable
class LLMCallRecorder(Protocol):
    async def record_llm_call(self, record: LLMCallRecord) -> None: ...


_context: ContextVar[LLMCallContext | None] = ContextVar("autoagent_llm_call_context", default=None)
_recorder: ContextVar[LLMCallRecorder | None] = ContextVar("autoagent_llm_call_recorder", default=None)
_runtime_recorders = WeakKeyDictionary()


@contextmanager
def llm_recording_context(context: LLMCallContext | None = None, *, recorder: LLMCallRecorder | None = None):
    """Explicit standalone context; actual Core identities take precedence in a Workflow."""
    if recorder is not None and not isinstance(recorder, LLMCallRecorder):
        raise TypeError("Recorder must implement record_llm_call.")
    context_token = _context.set(_context.get() if context is None else LLMCallContext.model_validate(context))
    recorder_token = _recorder.set(recorder if recorder is not None else _recorder.get())
    try:
        yield
    finally:
        _recorder.reset(recorder_token)
        _context.reset(context_token)


@contextmanager
def runtime_llm_recorder(recorder: LLMCallRecorder | None):
    """Bind one recorder to an owned Runtime loop, including its existing Tasks.

    This registration is loop-local, since lifespan Tasks and invocation Tasks
    need not share ContextVars. The application retains recorder ownership.
    """
    if recorder is None:
        yield
        return
    if not isinstance(recorder, LLMCallRecorder):
        raise TypeError("Recorder must implement record_llm_call.")
    loop = asyncio.get_running_loop()
    if loop in _runtime_recorders:
        raise RuntimeError("A model-call recorder is already registered on this Runtime loop.")
    _runtime_recorders[loop] = recorder
    try:
        yield
    finally:
        _runtime_recorders.pop(loop, None)


async def _write(recorder, record):
    try:
        await recorder.record_llm_call(record)
    except asyncio.CancelledError:
        if asyncio.current_task().cancelling():
            raise
        _logger.warning("LLM call recorder cancelled its write: call_id=%s phase=%s", record.llm_call_id, record.status)
    except Exception as error:
        # Observational storage failure must not trigger another remote call.
        _logger.warning("LLM call recording failed: call_id=%s phase=%s error_type=%s",
                        record.llm_call_id, record.status, type(error).__name__)


class _CallRecording:
    def __init__(self, recorder, record):
        self.recorder, self.record = recorder, record
        self.started_ns = time.perf_counter_ns()

    async def finish(self, *, response=None, error=None, cancelled=False, request_id=None):
        usage = None if response is None else response.usage
        data = self.record.model_dump(mode="python")
        data.update(status="cancelled" if cancelled else "failed" if error is not None else "succeeded",
                    completed_at_us=time.time_ns() // 1000, duration_ns=time.perf_counter_ns() - self.started_ns,
                    usage_reported=usage is not None)
        if response is not None:
            data.update(response_model=response.model, provider_response_id=getattr(response, "provider_response_id", None),
                        finish_reason=getattr(response, "finish_reason", None))
            request_id = request_id or response.provider_metadata.get("_request_id")
        for key in ("input_tokens", "output_tokens", "total_tokens", "reasoning_tokens", "cache_read_tokens"):
            data[key] = None if usage is None else getattr(usage, key, None)
        if error is not None:
            cause = error.__cause__ or error
            code = getattr(error, "kind", None) or getattr(cause, "code", None)
            data.update(error_type=type(cause).__name__, error_code=None if code is None else str(code),
                        status_code=getattr(error, "status_code", None) or getattr(cause, "status_code", None))
            request_id = request_id or getattr(error, "request_id", None) or getattr(cause, "request_id", None)
        data["provider_request_id"] = request_id
        terminal = LLMCallRecord.model_validate(data)
        # Settle the write even if cancellation arrives while recording, retaining
        # caller cancellation and avoiding a detached database operation.
        task = asyncio.create_task(_write(self.recorder, terminal))
        cancelled_during_write = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                if asyncio.current_task().cancelling():
                    cancelled_during_write = True
        if task.cancelled():
            _logger.warning("LLM call recorder cancelled its write: call_id=%s phase=%s", terminal.llm_call_id, terminal.status)
        else:
            task.result()
        if cancelled_during_write:
            raise asyncio.CancelledError


async def start_llm_call(owner, request, *, kind="generation", streaming=False):
    if not owner.record_calls:
        return None
    recorder = owner.recorder if owner.recorder is not None else _recorder.get()
    if recorder is None:
        recorder = _runtime_recorders.get(asyncio.get_running_loop())
    if recorder is None:
        return None
    manual, core = _context.get(), OperatorCallContext.current()
    data = (manual or LLMCallContext()).model_dump(mode="python")
    if core is not None:
        data.update({key: getattr(core, key, None) for key in LLMCallContext.model_fields})
    record = LLMCallRecord(**data, llm_call_id=str(uuid4()), kind=kind, provider_id=owner.provider_id,
                           protocol=owner.protocol, requested_model=request.model, streaming=streaming,
                           status="running", started_at_us=time.time_ns() // 1000)
    recording = _CallRecording(recorder, record)
    try:
        await _write(recorder, record)
    except asyncio.CancelledError as error:
        await recording.finish(error=error, cancelled=True)
        raise
    return recording


async def finish_llm_call(recording, *, response=None, error=None, request_id=None):
    if recording is None:
        return
    response = response or getattr(error, "response", None)
    if response is not None and isinstance(error, GeneratorExit):
        error = None
    try:
        await recording.finish(response=response, error=error,
                               cancelled=isinstance(error, (asyncio.CancelledError, GeneratorExit)), request_id=request_id)
    except Exception as failure:
        _logger.warning("LLM call terminal recording failed: call_id=%s error_type=%s",
                        recording.record.llm_call_id, type(failure).__name__)


__all__ = ["LLMCallContext", "LLMCallRecord", "LLMCallRecorder", "llm_recording_context", "runtime_llm_recorder"]
