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

from pydantic import Field, JsonValue, model_validator
from autoagent.core.money import MAX_COST_UNITS, units_usd

from autoagent.core.operators.context import OperatorCallContext

from .models._base import Model, NonEmpty
from .models import LLMMessage, LLMProviderItem, LLMRequest, LLMResponse, LLMToolResult

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
    billing_details: dict[str, JsonValue] = Field(default_factory=dict)
    cost_units: int | None = Field(default=None, ge=0, le=MAX_COST_UNITS)
    cost_metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @property
    def cost_usd(self) -> str | None:
        return None if self.cost_units is None else units_usd(self.cost_units)

    @model_validator(mode="after")
    def _lifecycle(self):
        terminal = self.status != "running"
        if terminal != (self.completed_at_us is not None and self.duration_ns is not None):
            raise ValueError("Terminal call records require completion time and duration.")
        if not terminal and (self.completed_at_us is not None or self.duration_ns is not None):
            raise ValueError("Running calls cannot carry terminal timing.")
        if not terminal and (self.cost_units is not None or self.cost_metadata):
            raise ValueError("Running calls cannot carry settled cost.")
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


@runtime_checkable
class LLMUsageLedger(LLMCallRecorder, Protocol):
    """Mandatory token admission and settlement, independent of observation."""

    async def admit_llm_call(self, record: LLMCallRecord, checks) -> None: ...
    async def mark_llm_call_unsettled(self, llm_call_id: str) -> None: ...


_context: ContextVar[LLMCallContext | None] = ContextVar("autoagent_llm_call_context", default=None)
_recorder: ContextVar[LLMCallRecorder | None] = ContextVar("autoagent_llm_call_recorder", default=None)
_runtime_recorders = WeakKeyDictionary()
_runtime_usage_policies = WeakKeyDictionary()
_accounting_failures = WeakKeyDictionary()


async def _mark_unsettled(ledger, call_id):
    failures = _accounting_failures.setdefault(asyncio.get_running_loop(), set())
    failures.add(call_id)
    try:
        await ledger.mark_llm_call_unsettled(call_id)
    except Exception:
        _logger.warning("Cannot mark token settlement unresolved: call_id=%s", call_id)
    else:
        # The durable running row now supplies scope-specific unknown handling.
        # Block the whole Runtime only if even this transition could not run.
        failures.discard(call_id)


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
def runtime_llm_recorder(recorder: LLMCallRecorder | None, *, usage_policy=None):
    """Bind one recorder to an owned Runtime loop, including its existing Tasks.

    This registration is loop-local, since lifespan Tasks and invocation Tasks
    need not share ContextVars. The application retains recorder ownership.
    """
    from .usage import ProjectUsagePolicy
    if usage_policy is not None and not isinstance(usage_policy, ProjectUsagePolicy):
        raise TypeError("Runtime usage policy must be ProjectUsagePolicy.")
    if recorder is None and usage_policy is not None:
        raise ValueError("Usage limits require a recorder with admission support.")
    if recorder is None:
        yield
        return
    if not isinstance(recorder, LLMCallRecorder):
        raise TypeError("Recorder must implement record_llm_call.")
    loop = asyncio.get_running_loop()
    if loop in _runtime_recorders:
        raise RuntimeError("A model-call recorder is already registered on this Runtime loop.")
    _runtime_recorders[loop] = recorder
    _runtime_usage_policies[loop] = usage_policy
    _accounting_failures.pop(loop, None)
    try:
        yield
    finally:
        _runtime_recorders.pop(loop, None)
        _runtime_usage_policies.pop(loop, None)
        _accounting_failures.pop(loop, None)


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
    def __init__(self, recorder, record, *, strict=False, observer=None):
        self.recorder, self.record = recorder, record
        self.strict, self.observer = strict, observer
        self.started_ns = time.perf_counter_ns()

    async def finish(self, *, response=None, error=None, cancelled=False, request_id=None):
        usage = None if response is None else response.usage
        data = self.record.model_dump(mode="python")
        data.update(status="cancelled" if cancelled else "failed" if error is not None else "succeeded",
                    completed_at_us=time.time_ns() // 1000, duration_ns=time.perf_counter_ns() - self.started_ns,
                    usage_reported=usage is not None)
        if response is not None:
            data["response_model"] = response.model
            if isinstance(response, LLMResponse):
                data.update(provider_response_id=response.provider_response_id,
                            finish_reason=response.finish_reason)
            request_id = request_id or response.provider_metadata.get("_request_id")
            billing = dict(self.record.billing_details)
            if usage is not None:
                native = usage.native
                billing["usage"] = {key: native[key] for key in (
                    "cache_creation_input_tokens", "cache_read_input_tokens", "cache_creation",
                    "prompt_tokens_details", "completion_tokens_details", "input_tokens_details", "output_tokens_details",
                ) if key in native}
                # Both OpenAI protocols and LiteLLM normalize input counts to
                # include cache tokens, even when vendor extension keys remain.
                details = billing["usage"]
                if any(isinstance(value, dict) and value.get("audio_tokens", 0) for value in details.values()):
                    billing["unsupported_modalities"] = True
                server_tools = native.get("server_tool_use")
                if isinstance(server_tools, dict) and any(server_tools.values()):
                    billing["billable_tools"] = True
                if native.get("speed") not in (None, "standard") or native.get("inference_geo") is not None:
                    billing["unsupported_billing"] = True
            tier = response.provider_metadata.get("service_tier")
            if tier is not None:
                billing["service_tier"] = tier
            if isinstance(response, LLMResponse):
                if any(isinstance(item, LLMProviderItem) and item.provider_type in (
                    "web_search_call", "file_search_call", "code_interpreter_call", "image_generation_call",
                ) for item in response.output):
                    billing["billable_tools"] = True
                if any(part.type not in ("text", "refusal") for item in response.output
                       if isinstance(item, LLMMessage) for part in item.content):
                    billing["unsupported_modalities"] = True
            data["billing_details"] = billing
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
        async def write():
            if self.strict:
                await self.recorder.record_llm_call(terminal)
            else:
                await _write(self.recorder, terminal)
            if self.observer is not None:
                await _write(self.observer, terminal)
        task = asyncio.create_task(write())
        cancelled_during_write = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                if asyncio.current_task().cancelling():
                    cancelled_during_write = True
        if task.cancelled():
            if self.strict:
                await _mark_unsettled(self.recorder, self.record.llm_call_id)
            _logger.warning("LLM call recorder cancelled its write: call_id=%s phase=%s", terminal.llm_call_id, terminal.status)
        else:
            task.result()
        if cancelled_during_write:
            raise asyncio.CancelledError


async def start_llm_call(owner, request, *, kind="generation", streaming=False):
    from autoagent.core.usage import UsageAccountingError
    from .usage import usage_checks
    loop = asyncio.get_running_loop()
    runtime_recorder = _runtime_recorders.get(loop)
    recorder = owner.recorder if owner.recorder is not None else _recorder.get()
    if recorder is None:
        recorder = runtime_recorder
    manual, core = _context.get(), OperatorCallContext.current()
    project = _runtime_usage_policies.get(loop)
    has_core_limits = core is not None and any(
        policy is not None and policy.limited for policy in (core.node_usage, core.invocation_usage, core.session_usage)
    )
    if not has_core_limits and project is None and (not owner.record_calls or recorder is None):
        return None
    data = (manual or LLMCallContext()).model_dump(mode="python")
    if core is not None:
        data.update({key: getattr(core, key, None) for key in LLMCallContext.model_fields})
    config = getattr(owner, "config", None)
    billing = {"pricing_provider": getattr(config, "pricing_provider", None)
               or getattr(config, "model_prefix", None) or owner.provider_id}
    from .cost import current_token_pricing
    if (pricing := current_token_pricing()) is not None:
        billing["token_pricing"] = pricing
    options = request.provider_options
    if options is not None and options.values.get("service_tier") is not None:
        billing["service_tier"] = options.values["service_tier"]
    if isinstance(request, LLMRequest) and any(
        part.type not in ("text", "refusal") for item in request.input
        if isinstance(item, (LLMMessage, LLMToolResult)) for part in item.content
    ):
        billing["unsupported_modalities"] = True
    record = LLMCallRecord(**data, llm_call_id=str(uuid4()), kind=kind, provider_id=owner.provider_id,
                           protocol=owner.protocol, requested_model=request.model, streaming=streaming,
                           status="running", started_at_us=time.time_ns() // 1000,
                           billing_details=billing)
    checks = usage_checks(core, project, record)
    if checks:
        if _accounting_failures.get(loop) and any(check.policy.on_unknown == "block" for check in checks):
            raise UsageAccountingError("A previous token settlement is unresolved; admission blocked until recovery.")
        ledger = runtime_recorder or _recorder.get() or owner.recorder
        if not isinstance(ledger, LLMUsageLedger):
            raise UsageAccountingError("Token limits require a usage ledger with admission support.")
        try:
            await ledger.admit_llm_call(record, checks)
        except asyncio.CancelledError:
            # A cancelled admission may already be durable. Keep its unknown
            # liability; never release it as an assumed zero-token settlement.
            await asyncio.shield(_mark_unsettled(ledger, record.llm_call_id))
            raise
        except UsageAccountingError:
            raise
        except Exception as error:
            raise UsageAccountingError("Token admission failed; no LLM request was sent.") from error
        observer = recorder if owner.record_calls and recorder is not ledger else None
        recording = _CallRecording(ledger, record, strict=True, observer=observer)
        if observer is not None:
            try:
                await _write(observer, record)
            except asyncio.CancelledError:
                await asyncio.shield(_mark_unsettled(ledger, record.llm_call_id))
                raise
        return recording
    if not owner.record_calls or recorder is None:
        return None
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
        if recording.strict:
            await _mark_unsettled(recording.recorder, recording.record.llm_call_id)
        _logger.warning("LLM call terminal recording failed: call_id=%s error_type=%s",
                        recording.record.llm_call_id, type(failure).__name__)


__all__ = ["LLMCallContext", "LLMCallRecord", "LLMCallRecorder", "LLMUsageLedger", "llm_recording_context", "runtime_llm_recorder"]
