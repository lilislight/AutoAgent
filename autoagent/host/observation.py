"""Observable waits that neither retain payloads nor activate stored executions."""

import asyncio
import math

from .models import HostError, HostNotFoundError
from autoagent.core.errors import RuntimeTransitionError


class ExecutionObservation:
    def _resident_observation_ref(self, invocation_id):
        if self._runtime is None or invocation_id in self._runtime.needs_recovery:
            return None
        return next((ref for ref in self._runtime.resident.values()
                     if ref.invocation_id == invocation_id), None)

    async def _wait_resident(self, ref, until, deadline, revision, changed):
        """Wait on payload-free Core notifications before building a SQL summary."""
        loop = asyncio.get_running_loop()
        while True:
            try:
                snapshot = await self._runtime.app.acontrol_snapshot(ref)
            except RuntimeTransitionError as error:
                if error.code != 'INVOCATION_REF_STALE':
                    raise
                return revision
            if snapshot.settled or snapshot.needs_recovery or until == 'boundary' and snapshot.at_boundary:
                return revision
            remaining = None if deadline is None else deadline - loop.time()
            if remaining is not None and remaining <= 0:
                return revision
            duration = min(1., remaining) if remaining is not None else 1.
            persistence_wait = asyncio.create_task(changed(revision, duration, ref.invocation_id))
            core_wait = asyncio.create_task(self._runtime.app.await_control_change(
                ref, snapshot.revision, timeout=duration, until=until))
            try:
                done, _ = await asyncio.wait((persistence_wait, core_wait), return_when=asyncio.FIRST_COMPLETED)
                if persistence_wait in done:
                    revision = persistence_wait.result()
                if core_wait in done:
                    core_wait.result()
            finally:
                for task in (persistence_wait, core_wait):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(persistence_wait, core_wait, return_exceptions=True)

    async def get_execution_state(self, invocation_id):
        """Summarize the Root tree selected by an Invocation ID without loading values."""
        async def query():
            resident_snapshot = None
            resident_ref = self._resident_observation_ref(invocation_id)
            if resident_ref is not None:
                try:
                    resident_snapshot = await self._runtime.app.acontrol_snapshot(resident_ref)
                except RuntimeTransitionError as error:
                    if error.code != 'INVOCATION_REF_STALE':
                        raise
            version = resident_snapshot.graph_version if resident_snapshot and resident_snapshot.settled else None
            state = await self._persistence.execution_state(invocation_id, settled_version=version)
            if state is None:
                raise HostNotFoundError("Unknown execution.")
            ref = self._runtime.resident.get(state["session_id"]) if self._runtime else None
            state["resident"] = ref is not None and ref.invocation_id == state["invocation_id"]
            state["needs_recovery"] = self._runtime is not None and state["invocation_id"] in self._runtime.needs_recovery
            candidate=state.pop('_waiting_boundary')
            if state['resident'] and not state['needs_recovery'] and (candidate or state['at_boundary'] or state['settled']):
                try:
                    snapshot = (resident_snapshot if version is not None and version == state['_graph_version']
                                else await self._runtime.app.acontrol_snapshot(ref))
                except RuntimeTransitionError as error:
                    if error.code!='INVOCATION_REF_STALE':raise
                    state['resident']=False
                    snapshot=None
                if snapshot is not None:
                    # Child transitions can advance without changing the Root's
                    # sequence. Only combine exactly matching graph versions.
                    if snapshot.graph_version != state['_graph_version']:
                        state = await self._persistence.execution_state(invocation_id)
                        state['resident'] = True
                        state['needs_recovery'] = snapshot.needs_recovery
                        state.pop('_waiting_boundary', None)
                    if snapshot.graph_version == state['_graph_version']:
                        state['at_boundary'] = snapshot.at_boundary
                        state['settled'] = snapshot.settled
            state.pop('_graph_version',None)
            return state
        return await self._perform(query)

    async def wait_execution(self, invocation_id, *, until="boundary", timeout=None):
        """Observe a boundary/terminal result; cancelling this await only stops observation."""
        if until not in ("none", "boundary", "terminal"):
            raise ValueError("until must be none, boundary or terminal.")
        if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, (float, int))
                                    or not math.isfinite(timeout) or timeout < 0):
            raise ValueError("timeout must be finite and nonnegative.")
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        self._check_loop()
        if self._state != "ready":
            raise HostError("Host is not open.")
        # Register before the first snapshot so a commit during a query is
        # remembered. Waiting holds neither a DB connection nor an operation slot.
        async with self._persistence.execution_notifications(invocation_id) as (revision, changed):
            ref = self._resident_observation_ref(invocation_id)
            if ref is not None and until != 'none':
                revision = await self._wait_resident(ref, until, deadline, revision, changed)
            while True:
                state = await self.get_execution_state(invocation_id)
                pending_fork = self._runtime.fork_tasks.get(state['invocation_id']) if self._runtime else None
                if until == "none" or state["settled"] or until == "boundary" and state["at_boundary"]:
                    return dict(state, observation="accepted" if until == "none" else "terminal" if state["settled"] else "boundary")
                # Fork admission owns activation already. A cut inside Map
                # registration is runnable, not a request for manual recovery.
                if state["needs_recovery"] and pending_fork is None:
                    return dict(state, observation="interrupted")
                remaining = None if deadline is None else deadline - loop.time()
                if remaining is not None and remaining <= 0:
                    return dict(state, observation="timeout")
                # Bounded fallback also observes Runtime-only recovery changes,
                # buffered accepted state and commits made by another process.
                duration=min(1.,remaining) if remaining is not None else 1.
                ref=self._runtime.resident.get(state['session_id']) if self._runtime else None
                if state['resident'] and ref and pending_fork is None:
                    revision = await self._wait_resident(ref, until, deadline, revision, changed)
                else:
                    revision=await changed(revision,min(.1,duration),state['invocation_id'])
