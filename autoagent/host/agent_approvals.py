"""Reevaluate Agent approval views while preserving the original Core Wait."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import Context
from dataclasses import replace
import hashlib
import json
import logging
from weakref import WeakValueDictionary

from autoagent.agent.context import plain, restore
from autoagent.agent.workflow.models import (
    AgentApprovalRequest, AgentApprovalResolution, AgentApprovalResponse, ToolTask,
)
from autoagent.agent.workflow.tools import authorization_binding, binding_error
from autoagent.agent.tools.execution import error_result
from autoagent.agent.tools.context import tool_context

from .models import HostRunConflict


class AgentApprovals:
    def __init__(self, service):
        self.service = service
        self.loop = None
        self.locks = WeakValueDictionary()
        self.tasks = set()

    @asynccontextmanager
    async def runtime_scope(self):
        self.loop = asyncio.get_running_loop()
        try:
            yield
        finally:
            for task in tuple(self.tasks):
                task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)
            self.tasks.clear()
            self.loop = None
            self.locks.clear()

    async def changed(self, session_id=None):
        """Reevaluate live Waits after committed Session or resource changes."""
        if self.loop is None:
            return
        async def schedule():
            async def update():
                runtime = self.service.host._runtime
                owners = {sid: sid for sid in tuple(runtime.resident)}
                owners.update(runtime.owners)
                refs = {root: runtime.resident[root] for sid, root in tuple(owners.items())
                    if (session_id is None or sid == session_id) and root in runtime.resident}
                async def refresh(ref):
                    await self.reconcile(ref, await runtime.app.astatus(ref), join=False)
                await asyncio.gather(*(refresh(ref) for ref in refs.values()))
            # This Host-owned task can outlive the notifying Operator. Do not
            # borrow its executor, Tool context, or LLM recording scope.
            task = asyncio.create_task(update(), context=Context())
            self.tasks.add(task)
            def finished(task):
                self.tasks.discard(task)
                if not task.cancelled() and task.exception() is not None:
                    error = task.exception()
                    logging.getLogger(__name__).error("Agent approval reevaluation failed", exc_info=(type(error), error, error.__traceback__))
            task.add_done_callback(finished)
        await self._run(schedule)

    async def _run(self, operation):
        # Policies may borrow loop-bound clients created by Host lifespan. Keep
        # reevaluation and submission on that same loop, without Core internals.
        if asyncio.get_running_loop() is self.loop:
            return await operation()
        if self.loop is None:
            raise RuntimeError("Agent approval resources are not running.")
        return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(operation(), self.loop))

    @staticmethod
    def _batch(wait):
        value = plain(wait.request)
        if wait.kind == "external" and isinstance(value, dict) and value.get("kind") == "agent.tool_approval":
            return restore(AgentApprovalRequest, value)
        return None

    async def _evaluate(self, wait):
        batch = self._batch(wait)
        if batch is None:
            return None
        service = await self.service.for_invocation(batch.invocation_id)
        snapshot = await service._get_snapshot(batch.invocation_id, cache=False)
        if snapshot is None:
            raise RuntimeError("This approval batch has no accepted Agent execution snapshot.")
        if snapshot.settings.workspace is not None:
            await service.prepare_workspace(snapshot)
        revision = service.resource_revision(snapshot)
        from autoagent.agent.workflow.builtins import workflow_catalog
        catalog, _ = workflow_catalog(service, snapshot)
        asks, bindings, automatic = [], [], []
        for original, binding in zip(batch.requests, batch.bindings):
            task = ToolTask(request_id=batch.request_id, call=original.call, binding=binding,
                operations=original.operations)
            result = binding_error(service, snapshot, task, catalog=catalog)
            checked = None
            if result is None:
                tools = service.toolset(snapshot, binding=authorization_binding(task), catalog=catalog)
                with tool_context(tools.context):
                    checked = await tools.apreflight(task.call)
                result = checked.result
            if result is None and checked.decision.action == "deny":
                result = error_result(task.call, "tool_access_denied", checked.decision.reason or "Tool access denied.")
            if result is not None:
                automatic.append(AgentApprovalResolution(request_id=original.id, source="policy", result=result))
            elif checked.decision.action == "ask":
                if snapshot.settings.approval_mode == "auto_approve":
                    automatic.append(AgentApprovalResolution(request_id=original.id, source="system",
                        approval=checked.decision.approval_request.respond(approved=True, reason="Automatically approved by approval mode.")))
                else:
                    asks.append(checked.decision.approval_request)
                    bindings.append(binding)
            else:
                automatic.append(AgentApprovalResolution(request_id=original.id, source="policy"))
        latest = await service._get_snapshot(batch.invocation_id, cache=False)
        if (revision != service.resource_revision(snapshot) or latest is None or latest.snapshot_id != snapshot.snapshot_id
                or latest.settings.approval_mode != snapshot.settings.approval_mode):
            raise HostRunConflict("approval_changed", "Agent resources or settings changed during approval evaluation; refresh.")
        identity = [wait.id, snapshot.snapshot_id, revision,
            [value.model_dump(mode="json") for value in asks],
            [value.model_dump(mode="json") for value in automatic]]
        token = hashlib.sha256(json.dumps(identity, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        return batch, AgentApprovalRequest(request_id=batch.request_id, invocation_id=batch.invocation_id,
            snapshot_id=snapshot.snapshot_id, requests=tuple(asks), bindings=tuple(bindings),
            automatic=tuple(automatic), revision=token)

    async def action_view(self, ref, wait_id):
        """Read one current external request without consuming or resuming it."""
        async def read():
            status = await self.service.host._runtime.app.astatus(ref)
            wait = next((value for value in status.waits if value.id == wait_id and value.kind == "external"), None)
            if wait is None:
                raise HostRunConflict("wait_unavailable", "The external Wait is no longer pending.")
            evaluated = await self._evaluate(wait)
            if evaluated is None:
                return dict(request=plain(wait.request), response_example=None)
            view = evaluated[1]
            reply = AgentApprovalResponse(decisions=tuple(item.respond(approved=False) for item in view.requests),
                revision=view.revision)
            return dict(request=view.model_dump(mode="json"), response_example=reply.model_dump(mode="json"))
        return await self._run(read)

    async def project(self, result):
        if not result.waits or self.loop is None:
            return result
        async def project():
            waits = []
            for wait in result.waits:
                evaluated = await self._evaluate(wait)
                waits.append(replace(wait, request=evaluated[1].model_dump(mode="json")) if evaluated else wait)
            return replace(result, waits=tuple(waits))
        return await self._run(project)

    @staticmethod
    def _resolved(original, view, decisions):
        approved = {value.request_id: value for value in decisions}
        resolutions = {value.request_id: value for value in view.automatic}
        # Pair the fresh request back to the original durable batch by call ID.
        old_ids = {value.call.call_id: value.id for value in original.requests}
        for request in view.requests:
            old_id = old_ids[request.call.call_id]
            resolutions[old_id] = AgentApprovalResolution(request_id=old_id, source="user",
                approval=approved[request.id])
        source = "user" if decisions else "system" if any(value.source == "system" for value in resolutions.values()) else "policy"
        return AgentApprovalResponse(decisions=(), source=source,
            revision=view.revision, resolutions=tuple(resolutions[value.id] for value in original.requests))

    async def respond(self, ref, wait_id, response):
        async def respond():
            lock = self.locks.setdefault(ref.invocation_id, asyncio.Lock())
            async with lock:
                app = self.service.host._runtime.app
                status = await app.astatus(ref)
                wait = next((value for value in status.waits if value.id == wait_id), None)
                if wait is None:
                    raise HostRunConflict("wait_unavailable", "The approval Wait is no longer pending; refresh the conversation.")
                evaluated = await self._evaluate(wait) if wait is not None else None
                value = response
                if wait is not None:
                    from autoagent.agent.workflow.questions import AskUserRequest, AskUserResponse
                    request = plain(wait.request)
                    if wait.kind == "external" and isinstance(request, dict) and request.get("kind") == "agent.ask_user":
                        question_request = restore(AskUserRequest, request)
                        reply = restore(AskUserResponse, response)
                        reply.validate_request(question_request)
                        value = reply
                if evaluated is not None:
                    original, view = evaluated
                    reply = restore(AgentApprovalResponse, response)
                    if reply.source != "user" or reply.resolutions:
                        raise ValueError("Policy resolutions are produced by Host, not external approval replies.")
                    # Ordinary initial replies may omit a revision. A refreshed
                    # execution stage or changed approval identity always needs it.
                    unchanged = (original.snapshot_id == view.snapshot_id and not view.automatic
                        and tuple(item.id for item in original.requests) == tuple(item.id for item in view.requests))
                    if reply.revision != view.revision and not (reply.revision is None and unchanged):
                        raise HostRunConflict("approval_changed", "Approval view changed; fetch the current batch before replying.")
                    reply.validate_request(view)
                    value = self._resolved(original, view, reply.decisions)
                    value.validate_request(original)
                await app.asubmit_resume(ref, wait_id, value)
        return await self._run(respond)

    async def reconcile(self, ref, result, *, join=True):
        """After recovery, consume fully automatic batches through native Wait.

        Mixed batches keep the same Wait; projection exposes only current Ask
        items. Interrupted reevaluation can be derived again from the durable
        original request and accepted execution snapshot after restart.
        """
        if not result.waits or result.control_state != "active":
            return result
        async def reconcile():
            lock = self.locks.setdefault(ref.invocation_id, asyncio.Lock())
            async with lock:
                app = self.service.host._runtime.app
                changed = False
                current = await app.astatus(ref)
                if current.control_state != "active":
                    return current
                for wait in current.waits:
                    evaluated = await self._evaluate(wait)
                    if evaluated is None or evaluated[1].requests:
                        continue
                    original, view = evaluated
                    reply = self._resolved(original, view, ())
                    reply.validate_request(original)
                    await app.asubmit_resume(ref, wait.id, reply)
                    changed = True
                if join and (changed or current.status == "running"):
                    return await app.ajoin(ref)
                return await app.astatus(ref) if changed else current
        return await self._run(reconcile)
