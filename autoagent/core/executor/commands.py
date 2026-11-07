"""Per-unit native Commands; Python tasks stop at durable wait registration."""

from __future__ import annotations

import asyncio
from functools import lru_cache
from uuid import uuid4

from pydantic import ValidationError

from ..commands import (
    Wait, RuntimeHandle, RuntimeObservation, ResumeReceipt, CancelReceipt,
    TimerResult, SignalReceipt, SignalBatch, SelectResult, SelfHandle, OwnerHandle,
    SignalCase, TimerCase,
)
from ..commands.waits import CommandSuspended
from ..commands.models import TimerRequest
from ..operators import ValueContract
from ..runtime import OperatorCallStarted, OperatorCallCompleted, OperatorCallFailed, RuntimeErrorInfo, thaw
from ..workflow import InputMappingContext
from ..commands import ResumeRequest, CancelRequest, SendSignalRequest, ReceiveSignalRequest, HasSignalRequest, HasSignalResult
from ..errors import NodeTimeoutError, WaitTimeoutError, RuntimeInfrastructureError, RuntimeTransitionError
from ..runtime.events import CommandRetryStarted, CommandsPending


WAITING_COMMANDS = frozenset({'system_command:wait', 'system_command:await', 'system_command:await_any', 'system_command:await_signal', 'system_command:select'})


class CommandCallError(Exception):
    """Carry a recorded failure across durable suspension without changing its type."""

    def __init__(self, error):
        self.error = error
        super().__init__(error.message)


def retry_at(node, call, error, now):
    policy = node.retry
    if policy is None or call.retry_attempt >= policy.max_attempts:
        return None
    if isinstance(error, (RuntimeInfrastructureError, ValidationError)):
        return None
    if isinstance(error, RuntimeTransitionError) and error.code not in {'MAILBOX_FULL', 'SIGNAL_RECEIPTS_FULL'}:
        return None
    if isinstance(error, WaitTimeoutError) and 'WaitTimeoutError' not in policy.retry_on:
        return None
    if not any(base.__name__ in policy.retry_on for base in type(error).__mro__):
        return None
    return now + int(policy.backoff.delay(call.retry_attempt + 1) * 1_000_000)


@lru_cache(maxsize=None)
def _contract(typ):
    return ValueContract.create(typ, location='Command output')


def output_contract(node):
    if isinstance(node.executable, Wait):
        return node.executable.output_contract
    typ = {
        'spawn': RuntimeHandle, 'await': RuntimeObservation, 'await_any': RuntimeObservation,
        'has_signal': HasSignalResult, 'status': RuntimeObservation, 'resume': ResumeReceipt, 'cancel': CancelReceipt,
        'timer': TimerResult, 'send_signal': SignalReceipt,
        'receive_signal': SignalBatch, 'await_signal': SignalBatch, 'select': SelectResult,
    }[node.executable.id.removeprefix('system_command:')]
    return _contract(typ)


class CommandExecutor:
    def __init__(self, owner, node, occurrence_id, mapped, session_id):
        self.owner, self.node, self.oid, self.sid = owner, node, occurrence_id, session_id
        self.iid = self.state().invocation.id
        self.inputs = list(mapped) if node.map is not None else [mapped]
        self.contract = output_contract(node)
        self.command = node.executable
        self.plan = None
        self._calls = {call.unit_index: call for call in owner._occurrence_calls(session_id, occurrence_id)}

    def state(self):
        return self.owner._repository.state(self.sid)

    async def emit(self, event):
        accepted = await self.owner._emit(self.sid, self.iid, event)
        if isinstance(event, OperatorCallStarted):
            self._calls[event.unit_index] = self.state().invocation.scheduler.operator_calls[event.call_id]
        return accepted

    def call(self, index):
        previous = self._calls.get(index)
        return self.state().invocation.scheduler.operator_calls.get(previous.id, previous) if previous else None

    def deadline(self):
        return self.owner._clock_us() + int(self.node.timeout.seconds * 1_000_000) if self.node.timeout else None

    async def run(self):
        if self.command.id == 'system_command:await' and self.command.workflow is not None:
            for index, value in enumerate(self.inputs):
                if self.call(index) is None:
                    record = await self.prepare(value)
                    await self.emit(OperatorCallStarted(str(uuid4()), self.oid, self.command.id, index, record, deadline_at_us=self.deadline()))
        if getattr(self.command, 'workflow', None) is not None and self.inputs:
            mapped = self.inputs if self.node.map is not None else self.inputs[0]
            self.plan = await self.owner._plan_children(self.node, self.oid, mapped, self.state())
        results, pending = [None] * len(self.inputs), set()
        if not self.inputs:
            return await self.owner._command_output(self.node, self.oid, self.inputs, results, self.sid)
        capacity = self.owner._node_executor.max_operator_concurrency
        limit = min(len(self.inputs), self.node.map.max_parallelism if self.node.map and self.node.map.max_parallelism else capacity, capacity)
        # Consumption uses input-order registration within this occurrence.
        if self.command.id in {'system_command:receive_signal', 'system_command:await_signal', 'system_command:select'}:
            limit = 1
        queue, deferred = asyncio.Queue(), set()
        remaining = len(self.inputs)
        for index in range(remaining):
            queue.put_nowait(index)

        async def enqueue_later(index, delay):
            await asyncio.sleep(delay)
            queue.put_nowait(index)

        async def worker():
            nonlocal remaining
            while (index := await queue.get()) is not None:
                call = self.call(index)
                if call is not None and call.status == 'failed' and call.retry_at_us is not None:
                    delay = (call.retry_at_us - self.owner._clock_us()) / 1_000_000
                    if delay > 0:
                        task = asyncio.create_task(enqueue_later(index, delay))
                        deferred.add(task)
                        task.add_done_callback(deferred.discard)
                        continue
                ready, result, retrying = await self.attempt(index)
                if retrying:
                    queue.put_nowait(index)
                    continue
                if ready:
                    results[index] = result
                else:
                    pending.add(index)
                remaining -= 1
                if not remaining:
                    for _ in range(limit):
                        queue.put_nowait(None)

        tasks = [asyncio.create_task(worker()) for _ in range(limit)]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            for task in deferred:
                task.cancel()
            await asyncio.gather(*deferred, return_exceptions=True)
        if pending:
            await self.emit(CommandsPending(self.oid))
            raise CommandSuspended()
        return await self.owner._command_output(self.node, self.oid, self.inputs, results, self.sid)

    async def prepare(self, value):
        command = self.command
        if isinstance(command, Wait):
            return command.input_contract.to_record(value)
        record = self.node.input_contract.to_record(value) if self.node.input_contract else value
        if command.id == 'system_command:timer' and value.delay_us is not None:
            return TimerRequest(deadline_at_us=self.owner._clock_us() + value.delay_us).model_dump(mode='python')
        if command.id == 'system_command:send_signal' and command.handle is not None:
            handle = command.handle
            if isinstance(handle, (SelfHandle, OwnerHandle)):
                handle = self.owner._runtime_identity(self.sid)[0 if isinstance(handle, SelfHandle) else 1]
            elif callable(handle):
                handle = await self.owner._node_executor.call_hook(handle, self.context())
            return SendSignalRequest(handle=handle, endpoint=command.endpoint, payload=record).model_dump(mode='python')
        if command.id in {'system_command:receive_signal', 'system_command:await_signal'} and command.endpoint is not None:
            return {'endpoint': command.endpoint, 'limit': command.limit}
        if command.id == 'system_command:select':
            cases = []
            values = command.cases.items() if command.cases is not None else value.cases.items()
            for name, case in values:
                kind = getattr(case, 'kind', None)
                if isinstance(case, SignalCase) or kind == 'signal':
                    kind, condition = 'signal', {'endpoint': case.endpoint, 'limit': case.limit}
                    if not case.consume:
                        condition['consume'] = False
                elif isinstance(case, TimerCase) or kind == 'timer':
                    kind, condition = 'timer', {'deadline_at_us': case.deadline_at_us if case.deadline_at_us is not None else self.owner._clock_us() + case.delay_us}
                else:
                    handle = await self.owner._node_executor.call_hook(case.handle, self.context()) if callable(case.handle) else case.handle
                    after = await self.owner._node_executor.call_hook(case.after, self.context()) if callable(case.after) else case.after
                    kind, condition = 'child', {'handle': RuntimeHandle.model_validate(handle).model_dump(), 'after': after}
                    if case.scope != 'direct':
                        condition['scope'] = case.scope
                cases.append({'name': name, 'kind': kind, 'condition': condition})
            return {'cases': cases, 'select': True}
        return record

    def context(self):
        state = self.state()
        return InputMappingContext(invocation_input=thaw(state.invocation.input), incoming=self.owner._incoming_values(state.invocation, self.oid),
            invocation_context=state.invocation.context, session_context=state.session.context)

    async def attempt(self, index):
        call = self.call(index)
        if call is not None and call.status == 'completed':
            return True, self.contract.restore(thaw(call.output)), False
        if call is not None and call.status == 'failed':
            if call.retry_at_us is None:
                raise CommandCallError(call.error)
            await asyncio.sleep(max(0, (call.retry_at_us - self.owner._clock_us()) / 1_000_000))
            if self.command.id in WAITING_COMMANDS:
                await self.emit(OperatorCallStarted(str(uuid4()), self.oid, self.command.id, index, thaw(call.input),
                    retry_attempt=call.retry_attempt + 1, deadline_at_us=self.deadline()))
            else:
                await self.emit(CommandRetryStarted(call.id, self.deadline()))
            call = self.call(index)
        if call is None:
            cached = self.state().invocation.scheduler.occurrences[self.oid].execution.command_inputs
            record = thaw(cached[str(index)]) if str(index) in cached else await self.prepare(self.inputs[index])
            await self.emit(OperatorCallStarted(str(uuid4()), self.oid, self.command.id, index, record, deadline_at_us=self.deadline()))
            call = self.call(index)
        wait = self.state().invocation.scheduler.waits.get(call.id)
        if wait is not None:
            if wait.status == 'waiting':
                return False, None, False
            if wait.status == 'resumed' and isinstance(self.command, Wait):
                result = self.contract.restore(thaw(wait.response))
                await self.emit(OperatorCallCompleted(call.id, self.contract.to_record(result)))
                return True, result, False
        try:
            if self.command.id in WAITING_COMMANDS:
                if call.deadline_at_us is not None and self.owner._clock_us() >= call.deadline_at_us:
                    raise WaitTimeoutError(self.node.id)
                result = await self.perform(call, index)
            elif self.command.id == 'system_command:spawn':
                unit = self.plan.units[index]
                if (call.deadline_at_us is not None and self.owner._clock_us() >= call.deadline_at_us
                        and self.owner._repository.state(unit.session_id).invocation is None):
                    raise NodeTimeoutError(self.node.id, self.node.timeout.seconds)
                # Opening a Child is an atomic durable admission. Do not abort
                # halfway through publishing its ownership/acceptance markers.
                result = await self.perform(call, index)
            elif call.deadline_at_us is None:
                result = await self.perform(call, index)
            else:
                remaining = (call.deadline_at_us - self.owner._clock_us()) / 1_000_000
                timer = asyncio.timeout(max(0, remaining))
                try:
                    async with timer:
                        result = await self.perform(call, index)
                except TimeoutError:
                    if not timer.expired():
                        raise
                    if self.command.id in {'system_command:send_signal', 'system_command:receive_signal', 'system_command:resume', 'system_command:cancel'}:
                        # Resolve a possibly accepted effect before reporting a
                        # timeout; Runtime checks the deadline before new work.
                        result = await self.perform(call, index)
                    else:
                        raise NodeTimeoutError(self.node.id, self.node.timeout.seconds)
        except (asyncio.CancelledError, CommandSuspended):
            raise
        except Exception as error:
            if isinstance(error, RuntimeInfrastructureError):
                raise
            accepted = self.call(index)
            if accepted.status == 'completed':
                # Receive commits consumption and its output in one event.
                # A failed transport acknowledgement cannot invalidate it.
                return True, self.contract.restore(thaw(accepted.output)), False
            due = retry_at(self.node, call, error, self.owner._clock_us())
            await self.emit(OperatorCallFailed(call.id, RuntimeErrorInfo(type(error).__name__, str(error) or type(error).__name__), retry_at_us=due))
            if due is not None:
                return False, None, True
            raise
        if result is _PENDING:
            return False, None, False
        if self.call(index).status != 'completed':
            await self.emit(OperatorCallCompleted(call.id, self.contract.to_record(result)))
        if self.command.id == 'system_command:send_signal':
            await self.owner._release_signal(self.sid, self.iid, call.id, call.input['handle']['session_id'])
        if self.command.id == 'system_command:resume':
            await self.owner._release_resume(self.sid, self.iid, call.id, call.input['handle']['session_id'])
        return True, result, False

    async def perform(self, call, index):
        owner, command, record = self.owner, self.command, thaw(call.input)
        if isinstance(command, Wait):
            from ..runtime import WaitRequested
            await self.emit(WaitRequested(self.oid, call.id, record))
            return _PENDING
        handle = None
        if command.workflow is not None:
            await owner._ensure_child_unit(command.workflow, self.sid, self.iid, self.plan.creation_id, index, None)
            handle = owner._child_handle(command.workflow, self.plan.units[index])
        if command.id == 'system_command:spawn':
            return handle
        if command.id in {'system_command:await', 'system_command:await_any', 'system_command:timer', 'system_command:await_signal', 'system_command:select'}:
            if command.id == 'system_command:await':
                kind, condition = 'child', {'handle': (handle or RuntimeHandle.model_validate(record)).model_dump(), 'after': None}
            elif command.id == 'system_command:await_any':
                kind, condition = 'any', {'select': False, 'cases': [{'name': str(i), 'kind': 'child', 'condition': {'handle': target, 'after': None}} for i, target in enumerate(record['handles'])]}
            elif command.id == 'system_command:timer':
                kind, condition = 'timer', {'deadline_at_us': record['deadline_at_us']}
            elif command.id == 'system_command:await_signal':
                kind, condition = 'signal', record
            else:
                kind, condition = 'any', record
            await owner._suspend_command(self.sid, self.iid, call.id, kind, condition)
            return _PENDING
        if command.id == 'system_command:status':
            return await owner._status_runtime(self.sid, RuntimeHandle.model_validate(record))
        if command.id == 'system_command:resume':
            return await owner._resume_runtime(self.sid, self.iid, call.id, ResumeRequest.model_validate(record))
        if command.id == 'system_command:cancel':
            return await owner._cancel_runtime(self.sid, self.iid, call.id, CancelRequest.model_validate(record))
        if command.id == 'system_command:send_signal':
            return await owner._send_signal_runtime(self.sid, self.iid, call.id, SendSignalRequest.model_validate(record))
        if command.id == 'system_command:has_signal':
            return await owner._has_signal_runtime(self.sid, self.iid, call.id, HasSignalRequest.model_validate(record))
        if command.id == 'system_command:receive_signal':
            return await owner._receive_signal_runtime(self.sid, self.iid, call.id, ReceiveSignalRequest.model_validate(record))
        raise RuntimeError('Unsupported System Command.')


_PENDING = object()
