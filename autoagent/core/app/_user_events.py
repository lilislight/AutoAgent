"""Project-owned User Event mapping at confirmed Runtime lifecycle boundaries."""

import asyncio
from collections import defaultdict, deque
from contextvars import ContextVar

from ..operators.context import OperatorCallContext
from ..runtime.events import (
    CommandAwakened, InvocationCancelled, InvocationFailed, InvocationSettling,
    NodeFailed, OperatorCallFailed, WaitRequested, WaitResumed,
)


_mapping = ContextVar("autoagent_user_event_mapping", default=False)


class UserEventRuntime:
    def _init_user_events(self):
        self._user_event_pending = defaultdict(deque)
        self._user_event_tasks = {}
        self._user_event_retired = set()
        self._user_event_delivery_locks = defaultdict(asyncio.Lock)

    def _user_event_before(self, sid, payload):
        """Capture only opted-in, affected entities before State releases values."""
        if not isinstance(payload, (WaitRequested, WaitResumed, CommandAwakened,
                OperatorCallFailed, InvocationCancelled, InvocationFailed, InvocationSettling, NodeFailed)):
            return None
        state = self._repository.state(sid)
        inv = state.invocation
        if inv is None:
            return None
        workflow = self._workflows.get(inv.workflow_revision_id)
        if workflow is None:
            # Cancelling an unopened/recovered Child does not require loading
            # its Workflow implementation merely to generate observations.
            return None
        if isinstance(payload, NodeFailed):
            occurrence = inv.scheduler.occurrences[payload.occurrence_id]
            node = workflow.node(occurrence.node_id)
            if node.user_events is None:
                return None
            selected = {item.edge_id for item in occurrence.execution.routing if item.selected}
            handled = any(edge.on == "error" and (edge.condition is None or edge.id in selected)
                          for edge in workflow.outgoing(node.id))
            return ("error", node, occurrence.id, handled)
        if isinstance(payload, WaitRequested):
            node = workflow.node(inv.scheduler.occurrences[payload.occurrence_id].node_id)
            return ("waiting", node, payload.wait_id) if node.user_events is not None else None
        if isinstance(payload, (WaitResumed, CommandAwakened)):
            ids = (payload.wait_id,)
        elif isinstance(payload, OperatorCallFailed):
            ids = (payload.call_id,)
        else:
            ids = tuple(wait.id for wait in inv.scheduler.waits.values() if wait.status == "waiting")
        entries = []
        for identity in ids:
            wait = inv.scheduler.waits.get(identity)
            if wait is None or wait.status != "waiting":
                continue
            node = workflow.node(inv.scheduler.occurrences[wait.occurrence_id].node_id)
            if node.user_events is not None:
                entries.append((node, wait, inv.scheduler.operator_calls.get(wait.id)))
        return ("resolved", entries) if entries else None

    def _queue_user_event_transition(self, sid, event, before):
        if before is None:
            return
        state = self._repository.state(sid)
        inv = state.invocation
        payload = event.payload

        def enqueue(node, oid, phase, value, call=None):
            context = OperatorCallContext(operator_id=call.operator_id, operator_call_id=call.id,
                unit_index=call.unit_index) if call is not None else None
            self._user_event_pending[inv.id].append((node, dict(session_id=sid, invocation_id=inv.id,
                occurrence_id=oid, phase=phase, value=value, invocation_context=inv.context,
                session_context=state.session.context, call=context, handles=self._runtime_identity(sid))))

        if before[0] == "error":
            _, node, oid, handled = before
            error = payload.error
            enqueue(node, oid, "error", {"stage": error.phase, "error": {
                "type": error.type, "message": error.message}, "handled": handled})
        elif before[0] == "waiting":
            _, node, identity = before
            wait = inv.scheduler.waits[identity]
            enqueue(node, wait.occurrence_id, "waiting", self._user_wait_value(wait),
                    inv.scheduler.operator_calls.get(identity))
        else:
            for node, wait, call in before[1]:
                after = inv.scheduler.waits.get(wait.id)
                if after is None or after.status == "waiting":
                    continue
                value = self._user_wait_value(wait)
                value.update(outcome="cancelled", response=None, error=None)
                if isinstance(payload, WaitResumed):
                    value.update(outcome="answered", response=payload.response)
                elif isinstance(payload, CommandAwakened):
                    value.update(outcome="awakened", response=payload.output)
                elif isinstance(payload, OperatorCallFailed):
                    value.update(outcome="timed_out" if payload.error.type == "WaitTimeoutError" else "cancelled",
                        error={"type": payload.error.type, "message": payload.error.message})
                else:
                    value["reason"] = getattr(payload, "reason", None)
                enqueue(node, wait.occurrence_id, "wait_resolved", value, call)
        if self._user_event_pending.get(inv.id) and inv.id not in self._user_event_tasks:
            self._user_event_tasks[inv.id] = asyncio.create_task(self._deliver_user_event_transitions(inv.id))

    @staticmethod
    def _user_wait_value(wait):
        return dict(wait_id=wait.id, kind=wait.kind, request=wait.request,
                    deadline_at_us=wait.deadline_at_us)

    async def _deliver_user_event_transitions(self, iid):
        # Always execute user code in its own task, outside transition/control
        # locks. Frozen snapshots survive cleanup and partial Map completion.
        token = _mapping.set(True)
        try:
            queue = self._user_event_pending[iid]
            while queue:
                node, context = queue.popleft()
                await self._workflow_executor._emit_mapped_user_events(node, **context)
        finally:
            _mapping.reset(token)
            self._user_event_pending.pop(iid, None)
            self._user_event_tasks.pop(iid, None)
            if iid in self._user_event_retired:
                self._user_event_retired.remove(iid)
                self._discard_user_events(iid)

    async def _flush_user_events(self, invocation_ids=None):
        if _mapping.get():
            return  # A Mapper may inspect status without waiting on itself.
        while True:
            tasks = tuple(task for iid, task in self._user_event_tasks.items()
                          if invocation_ids is None or iid in invocation_ids)
            if not tasks:
                return
            await asyncio.gather(*(asyncio.shield(task) for task in tasks))

    def _discard_user_events(self, iid):
        if iid in self._user_event_tasks:
            self._user_event_retired.add(iid)
            return
        self._user_event_journal.discard(iid)
        self._user_event_sink_errors.pop(iid, None)
        self._user_event_delivery_locks.pop(iid, None)
