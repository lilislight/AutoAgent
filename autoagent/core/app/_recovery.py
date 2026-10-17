"""Admission of terminal recovery, shared with interrupted graph continuation."""
from uuid import uuid4

from ..errors import RuntimeTransitionError
from ..runtime.events import RecoveryPrepared, InvocationReopened
from ..runtime.recovery import prepare_recovery
from ..runtime.state import RuntimeState, validate_runtime_state
from ..runtime.graph_checkpoint import validate_graph
from ..runtime.values import thaw


class TerminalRecoveryRuntime:
    def _recovery_not_dispatched(self, root, request_id):
        for sid in (root, *self._descendant_sessions(root)):
            inv = self._repository.state(sid).invocation
            if inv is None or inv.last_recovery_id != request_id:
                continue
            suffix = f'~recover:{inv.recovery_count}'
            if any(oid.endswith(suffix) and item.started_sequence is not None
                   for oid,item in inv.scheduler.occurrences.items()):
                return False
        return True

    async def _prepare_recovery(self, ref, source, expected_version, request_id):
        """Run inside exclusive Root admission, before any Child or Operator dispatch."""
        root = self._root_session_id(ref.session_id)
        state = self._repository.state(root)
        inv = state.invocation
        if request_id is not None and (not isinstance(request_id, str) or not request_id.strip()):
            raise ValueError('request_id must be nonempty.')
        if expected_version is not None and (type(expected_version) is not int or expected_version < 0):
            raise ValueError('expected_version must be a nonnegative Session sequence.')
        if request_id and inv.last_recovery_id == request_id:
            undispatched = self._recovery_not_dispatched(root, request_id)
            if undispatched:
                self._pause_fences.discard(root)
            # The last reopen ACK may have been lost before any business Task
            # was started. Continue that accepted round, never prepare another.
            return inv.status != 'running' or inv.control_state != 'active' or not undispatched

        if inv.pending_recovery is None and expected_version is not None and state.sequence != expected_version:
            raise RuntimeTransitionError('RECOVERY_VERSION_CONFLICT', 'Execution changed since the recovery request was prepared.')
        if inv.pending_recovery is None and inv.status in {'failed', 'cancelled'} and source is not None:
            if ref.session_id != root:
                raise RuntimeTransitionError('RECOVERY_ROOT_REQUIRED', 'Recover the owning Root graph.')
            states = {sid:state for sid in (root, *self._descendant_sessions(root))
                      if (state := self._repository.state(sid)).session is not None}
            identity = request_id or str(uuid4())
            prepared = prepare_recovery(root, states, source, self._workflow_for_state, identity, self._clock_us())
            combined = {**states, **prepared}
            for candidate in combined.values():
                validate_runtime_state(candidate)
            validate_graph(root, combined)
            plan = {'request_id':identity, 'states':{sid:s.to_record() for sid,s in prepared.items()}}
            event = await self._repository.commit(session_id=root, invocation_id=inv.id,
                payload=RecoveryPrepared(identity, inv.status), recovery_plan=plan, occurred_at_us=self._clock_us())
            self._publish_ownership(event)
            inv = self._repository.state(root).invocation
        if inv.pending_recovery is not None:
            plan = thaw(inv.pending_recovery)
            identity = plan['request_id']
            if request_id and request_id != identity:
                raise RuntimeTransitionError('RECOVERY_IN_PROGRESS', 'Another recovery preparation must finish first.')
            self._pause_fences.add(root)
            # The Root marker remains until every prepared Child is accepted.
            # Lost ACKs are retried by Repository.settle before this is called.
            order = [sid for sid in plan['states'] if sid != root] + [root]
            for sid in order:
                current = self._repository.state(sid)
                if current.invocation.last_recovery_id == identity:
                    continue
                candidate = RuntimeState.from_record(plan['states'][sid])
                event = await self._repository.commit(session_id=sid, invocation_id=current.invocation.id,
                    payload=InvocationReopened(identity, current.invocation.status, candidate.invocation.recovery_count),
                    recovery_state=candidate, occurred_at_us=self._clock_us())
                self._publish_ownership(event)
                self._drive_errors.pop(sid, None)
            self._pause_fences.discard(root)
        return False
