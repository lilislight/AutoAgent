"""Borrowed, durable non-waiting Commands inside ordinary Operator calls."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import replace
import hashlib
import json
from urllib.parse import quote

from .models import Spawn, Status, Cancel, Resume, RuntimeHandle, CommandIR, CancelRequest, ResumeRequest
from .signals import SendSignal, ReceiveSignal, SendSignalRequest, ReceiveSignalRequest, HasSignal, HasSignalRequest, HasSignalResult
from ..operators import OperatorCallContext, ValueContract
from ..errors import RuntimeTransitionError
from ..runtime.events import CommandInvoked, CommandReturned
from ..runtime.inline import runtime_call
from ..runtime.values import thaw


_current = ContextVar("autoagent_command_invoker", default=None)


class CommandInvoker:
    """A Command executes under its Operator unit and an explicit stable key.

    Completed effects are reused across that unit's retries. Suspending
    Commands belong to native Workflow Nodes and are deliberately rejected.
    """

    def __init__(self, owner, session_id, occurrence_id):
        self.owner, self.sid, self.oid = owner, session_id, occurrence_id
        self.loop = asyncio.get_running_loop()
        self.locks = {}

    @classmethod
    def current(cls):
        value = _current.get()
        if value is None:
            raise RuntimeError("CommandInvoker is available only inside a Runtime Operator call.")
        return value

    def _context(self):
        context = OperatorCallContext.current()
        state = self.owner._repository.state(self.sid)
        inv = state.invocation
        call = inv.scheduler.operator_calls.get(context.operator_call_id) if context is not None else None
        if (context is None or context.session_id != self.sid or context.occurrence_id != self.oid
                or call is None or call.status != "running" or inv.stopping):
            raise RuntimeTransitionError("COMMAND_CALL_INVALID", "Command requires its active Operator unit.")
        return context, inv, call

    def clock_us(self):
        return self.owner._clock_us()

    def identity(self):
        self._context()
        return self.owner._runtime_identity(self.sid)

    def workflow(self, identity):
        self._context()
        return self.owner._workflow_lookup(identity)

    def visible_handles(self, *, include_parent=True, include_siblings=False):
        """Only local ownership neighbors, without exposing Runtime State."""
        _, inv, _ = self._context()
        own, parent = self.owner._runtime_identity(self.sid)
        result = []
        def children(current, relation):
            for plan in current.child_plans.values():
                for unit in plan.units:
                    handle = RuntimeHandle(session_id=unit.session_id, invocation_id=unit.invocation_id,
                        workflow_id=plan.workflow_id, workflow_revision_id=plan.workflow_revision_id)
                    if handle != own:
                        result.append((relation, handle))
        if parent is not None:
            if include_parent:
                result.append(("parent", parent))
            if include_siblings:
                children(self.owner._repository.state(parent.session_id).invocation, "sibling")
        children(inv, "child")
        return tuple(result)

    async def execute(self, command, value, *, key):
        if not isinstance(command, (Spawn, Status, Cancel, Resume, SendSignal, ReceiveSignal, HasSignal)):
            raise TypeError("Only non-waiting Commands can execute inside an Operator.")
        if not isinstance(key, str) or not key.strip():
            raise ValueError("Command key must be nonempty and stable within an Operator unit.")
        if asyncio.get_running_loop() is not self.loop:
            return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(self.execute(command, value, key=key), self.loop))
        context, inv, parent = self._context()
        slot = (context.unit_index, key)
        lock = self.locks.setdefault(slot, asyncio.Lock())
        async with lock:
            context, inv, parent = self._context()
            child = None
            if isinstance(command, Spawn):
                child = self.workflow(command.workflow)
                contract = child.node(command.entry_node_id).input_contract
                record = {"workflow_revision_id": child.workflow_revision_id, "entry_node_id": command.entry_node_id,
                    "value": contract.to_record(value) if contract is not None else value}
            else:
                typ = {Status: RuntimeHandle, Cancel: CancelRequest, Resume: ResumeRequest,
                    SendSignal: SendSignalRequest, ReceiveSignal: ReceiveSignalRequest, HasSignal: HasSignalRequest}[type(command)]
                record = ValueContract.create(typ, location="Nested Command input").to_record(value)
                if isinstance(command, (SendSignal, ReceiveSignal)) and getattr(command, "endpoint", None) is not None:
                    raise ValueError("Nested Signal Commands use explicit request contracts.")
            # Canonical JSON detaches the input, including strict model fields.
            record = json.loads(json.dumps(record, allow_nan=False))
            records = inv.scheduler.occurrences[self.oid].execution.inline_commands
            saved = next((call for call in records.values()
                if call["unit_index"] == context.unit_index and call["key"] == key), None)
            if saved is not None:
                if saved["operator_id"] != command.id or thaw(saved["input"]) != record:
                    raise RuntimeTransitionError("COMMAND_KEY_CONFLICT", "Command key was reused with different input.")
                call_id = saved["id"]
            else:
                digest = hashlib.sha256(json.dumps([inv.id, context.unit_index, key]).encode()).hexdigest()
                call_id = "inline:" + quote(self.oid, safe="") + ":" + digest
                await self.owner._emit(self.sid, inv.id, CommandInvoked(self.oid, call_id,
                    parent.id, context.unit_index, key, command.id, record, parent.deadline_at_us))
            call = runtime_call(self.owner._repository.state(self.sid).invocation, call_id)
            if call.status == "completed":
                result = thaw(call.output)
            else:
                if isinstance(command, Spawn):
                    node = self.owner._workflow_lookup(inv.workflow_revision_id).node(context.node_id)
                    if (call_id + ":child" not in self.owner._repository.state(self.sid).invocation.child_plans
                            and call.deadline_at_us is not None and self.clock_us() >= call.deadline_at_us):
                        from ..errors import NodeTimeoutError
                        raise NodeTimeoutError(node.id, node.timeout.seconds)
                    node = replace(node, executable=CommandIR(command.id, child, command.entry_node_id),
                        map=None, input_contract=child.node(command.entry_node_id).input_contract)
                    plan = await self.owner._plan_children(node, self.oid,
                        node.input_contract.restore(record["value"]) if node.input_contract else record["value"],
                        self.owner._repository.state(self.sid), creation_id=call_id + ":child")
                    await self.owner._ensure_child_unit(child, self.sid, inv.id, plan.creation_id, 0, None)
                    result = self.owner._child_handle(child, plan.units[0]).model_dump(mode="json")
                elif isinstance(command, Status):
                    result = (await self.owner._status_runtime(self.sid, RuntimeHandle.model_validate(record))).model_dump(mode="json")
                else:
                    handler, typ = {
                        Cancel: (self.owner._cancel_runtime, CancelRequest),
                        Resume: (self.owner._resume_runtime, ResumeRequest),
                        SendSignal: (self.owner._send_signal_runtime, SendSignalRequest),
                        ReceiveSignal: (self.owner._receive_signal_runtime, ReceiveSignalRequest),
                        HasSignal: (self.owner._has_signal_runtime, HasSignalRequest),
                    }[type(command)]
                    returned = await handler(self.sid, inv.id, call_id, typ.model_validate(record))
                    result = returned.model_dump(mode="json")
                latest = runtime_call(self.owner._repository.state(self.sid).invocation, call_id)
                if latest.status != "completed":
                    await self.owner._emit(self.sid, inv.id, CommandReturned(self.oid, call_id, result))
            if isinstance(command, SendSignal):
                await self._release(self.owner._release_signal, call_id, record["handle"]["session_id"])
            elif isinstance(command, Resume):
                await self._release(self.owner._release_resume, call_id, record["handle"]["session_id"])
            from .models import RuntimeObservation, CancelReceipt, ResumeReceipt
            from .signals import SignalReceipt, SignalBatch
            output = {Spawn: RuntimeHandle, Status: RuntimeObservation, Cancel: CancelReceipt,
                Resume: ResumeReceipt, SendSignal: SignalReceipt, ReceiveSignal: SignalBatch, HasSignal: HasSignalResult}[type(command)]
            return output.model_validate_json(json.dumps(result))

    async def _release(self, handler, call_id, target):
        # A completed result can be replayed after its receipt was retired.
        try:
            await handler(self.sid, self._context()[1].id, call_id, target)
        except RuntimeTransitionError as error:
            if error.code not in {"SIGNAL_RECEIPT_MISSING"}:
                raise


@contextmanager
def command_invoker_context(invoker):
    token = _current.set(invoker)
    try:
        yield
    finally:
        _current.reset(token)
