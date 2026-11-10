"""Host-owned FIFO chat submissions; browser connections never drive execution."""

import asyncio
import logging
from weakref import WeakValueDictionary

from autoagent.core import RuntimeTransitionError
from .models import HostNotFoundError, HostRunConflict


class AgentMessageQueue:
    def __init__(self, service):
        self.service, self.host = service, service.host
        self.locks = WeakValueDictionary()
        self.wake = asyncio.Event()
        self.task = None
        self.closed = False

    def lock(self, sid):
        lock = self.locks.get(sid)
        if lock is None:
            lock = self.locks[sid] = asyncio.Lock()
        return lock

    async def storage(self, method, *args, **kwargs):
        return await self.host._persistence.agent_queue_operation(method, *args, **kwargs)

    async def start(self):
        self.wake.set()
        self.task = asyncio.create_task(self._work())

    async def close(self):
        self.closed = True
        self.wake.set()
        if self.task is not None:
            await self.task

    async def _work(self):
        while not self.closed:
            self.wake.clear()
            try:
                sessions = await self.storage("pending_sessions")
                for sid in sessions:
                    if self.closed:
                        return
                    try:
                        async with self.lock(sid):
                            await self._advance(sid)
                    except Exception:
                        logging.getLogger(__name__).exception("Could not advance queued messages for Session %s", sid)
            except Exception:
                logging.getLogger(__name__).exception("Could not advance the Agent message queue")
                sessions = [True]
            try:
                await asyncio.wait_for(self.wake.wait(), 1 if sessions else 30)
            except TimeoutError:
                pass

    async def latest(self, sid):
        row = await self.host._persistence.get_session(sid, view="accepted")
        return await self.host._persistence.get_invocation(row["latest_invocation_id"], view="accepted") if row and row["latest_invocation_id"] else None

    async def _advance(self, sid):
        rows = await self.storage("rows", sid)
        if not rows:
            return
        item = rows[0]
        for sending in rows:
            if sending["state"] == "sending":
                await self._signal(sending)
        if item["state"] == "sending":
            return
        latest = await self.latest(sid)
        iid = latest["invocation_id"] if latest else None
        if item["state"] == "blocked":
            if iid != item["blocked_on_id"] or not latest or latest["status"] != "completed":
                return
            await self.storage("change", sid, item["id"], ("blocked",), state="queued", error=None, blocked_on_id=None)
        if item["state"] == "starting":
            if iid and await self.storage("request_for", iid) == item["id"]:
                await self.host._persistence.flush()
                await self.storage("change", sid, item["id"], ("starting",), state="started", invocation_id=iid, error=None)
                self.wake.set()
                return
            await self.storage("change", sid, item["id"], ("starting",), state="queued")
        if latest and latest["status"] not in ("completed", "failed", "cancelled"):
            return
        if latest and latest["status"] != "completed" and item["continue_after_id"] != iid:
            await self.storage("change", sid, item["id"], ("queued",), state="blocked",
                blocked_on_id=iid, error="The previous task did not complete. Resume it or continue the queue.")
            return
        await self.storage("change", sid, item["id"], ("queued",), state="starting", previous_invocation_id=iid, error=None)
        try:
            ref = await self.service.start_conversation(sid, item["message"], request_id=item["id"])
            # The accepted Invocation identity must be durable before retiring its submission.
            await self.host._persistence.flush()
            await self.storage("change", sid, item["id"], ("starting",), state="started", invocation_id=ref.invocation_id)
            self.wake.set()
        except Exception as error:
            if self.closed:
                return
            # Admission can briefly wait on child settlement or a delivered result.
            if getattr(error, "code", "") in ("SESSION_CHILDREN_ACTIVE", "SESSION_RESULT_PENDING", "session_busy", "SESSION_INVOCATION_ACTIVE"):
                return
            latest = await self.latest(sid)
            if latest and await self.storage("request_for", latest["invocation_id"]) == item["id"]:
                return  # Reconcile an ambiguous admission; never resubmit blindly.
            await self.storage("change", sid, item["id"], ("starting",), state="blocked", error=str(error))

    async def submit(self, sid, message, request_id, workspace_id=None, *, send_to=None):
        if not message.strip() or not request_id or len(request_id) > 64 or not all(c.isascii() and (c.isalnum() or c in "_-") for c in request_id):
            raise ValueError("A message and a unique request ID (up to 64 letters, digits, '_' or '-') are required.")
        async def operation():
            async with self.lock(sid):
                await self.service.workspace_session(sid, allow_new=True)
                catalog = await self.service.workspace_catalog()
                if catalog["agent"] is None:
                    from .models import HostNotFoundError
                    raise HostNotFoundError("The SDK Agent is disabled for this project.")
                record = await self.service.conversation_settings(sid, workspace_id)
                await self.service.approvals._run(lambda: self.service._workspaces.validate(record.settings.workspace))
                await self.host._persistence.agent_context_operation("initialize_settings", sid, record.settings)
                previous = await self.storage("rows", sid, item_id=request_id)
                if previous and previous[0]["state"] in ("started", "sent", "cancelled"):
                    return await self.storage("add", sid, request_id, message)
                if send_to is not None:
                    latest = await self.latest(sid)
                    if not latest or latest["invocation_id"] != send_to:
                        raise HostRunConflict("queue_target_changed", "The current task changed; your message was not sent.")
                item = await self.storage("add", sid, request_id, message)
                if send_to is not None:
                    if item["state"] in ("queued", "blocked"):
                        await self.storage("change", sid, request_id, (item["state"],), state="sending", invocation_id=send_to, error=None)
                    if item["state"] in ("queued", "blocked", "sending"):
                        await self._signal((await self.storage("rows", sid, item_id=request_id))[0])
                else:
                    await self._advance(sid)
                self.wake.set()
                return (await self.storage("rows", sid, item_id=request_id))[0]
        return await self.host._perform(operation, payload=message, control=True)

    async def view(self, sid):
        async def read():
            await self.service.workspace_session(sid, allow_new=True)
            rows = await self.storage("rows", sid)
            latest = await self.latest(sid)
            return dict(items=rows, invocation_id=latest["invocation_id"] if latest else None)
        return await self.host._perform(read)

    async def act(self, sid, item_id, action, invocation_id=None):
        async def operation():
            async with self.lock(sid):
                await self.service.workspace_session(sid, allow_new=True)
                rows = await self.storage("rows", sid, item_id=item_id)
                if not rows:
                    raise HostRunConflict("queue_changed", "Queued message is unavailable.")
                item = rows[0]
                if action == "cancel":
                    if item["state"] != "cancelled":
                        await self.storage("change", sid, item_id, ("queued", "blocked"), state="cancelled", error=None)
                elif action == "continue":
                    latest = await self.latest(sid)
                    await self.storage("change", sid, item_id, ("blocked",), state="queued",
                        continue_after_id=latest["invocation_id"] if latest else None, error=None)
                elif action == "send_now":
                    if item["state"] == "sent":
                        return item
                    latest = await self.latest(sid)
                    if not latest or latest["invocation_id"] != invocation_id:
                        raise HostRunConflict("queue_target_changed", "The current task changed; this message remains queued.")
                    if item["state"] != "sending":
                        await self.storage("change", sid, item_id, ("queued", "blocked"), state="sending", invocation_id=invocation_id, error=None)
                    item = (await self.storage("rows", sid, item_id=item_id))[0]
                    await self._signal(item)
                else:
                    raise ValueError("Unknown queue action.")
                self.wake.set()
                return (await self.storage("rows", sid, item_id=item_id))[0]
        return await self.host._perform(operation, control=True)

    async def _signal(self, item):
        try:
            family = await self.service.workspace_invocation(item["invocation_id"])
            revision = family["current"]["workflow_revision_id"]
            options = self.service.options_for_workflow(self.host._runtime.workflow_irs[revision])
            await self.host.signal(item["invocation_id"], options.user_signal_endpoint,
                dict(message=item["message"], reply_requested=False, reply_to=None, wait_timeout_seconds=None),
                source_id="queue_" + item["id"], sequence=1)
            await self.host._persistence.flush()
            await self.storage("change", item["session_id"], item["id"], ("sending",), state="sent", error=None)
        except Exception as error:
            # Business rejection is definitive. Transport/storage failures are
            # retried against the same Signal source/sequence, never as a new turn.
            rejected = isinstance(error, (RuntimeTransitionError, HostNotFoundError, HostRunConflict, ValueError, KeyError))
            await self.storage("change", item["session_id"], item["id"], ("sending",), state="blocked" if rejected else "sending",
                error="Could not deliver to that task; message retained. " + str(error))
