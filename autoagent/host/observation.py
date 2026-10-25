"""Observable waits that neither retain payloads nor activate stored executions."""

import asyncio
import math

from .models import HostError, HostNotFoundError


class ExecutionObservation:
    async def get_execution_state(self, invocation_id):
        """Summarize the Root tree selected by an Invocation ID without loading values."""
        async def query():
            state = await self._persistence.execution_state(invocation_id)
            if state is None:
                raise HostNotFoundError("Unknown execution.")
            ref = self._runtime.resident.get(state["session_id"]) if self._runtime else None
            state["resident"] = ref is not None and ref.invocation_id == state["invocation_id"]
            state["needs_recovery"] = self._runtime is not None and state["invocation_id"] in self._runtime.needs_recovery
            candidate = state.pop("_waiting_boundary")
            if candidate and state["resident"] and not state["at_boundary"]:
                # Select's SQL row intentionally stores no conditions. Inspect
                # the immutable acknowledged Core State without thawing Wait
                # requests or serializing business values, including any Wait
                # beyond the summary's first page.
                sessions = (ref.session_id, *(sid for sid, owner in self._runtime.owners.items() if owner == ref.session_id))
                for sid in sessions:
                    invocation = self._runtime.repository.state(sid).invocation
                    if invocation is None or invocation.stopping:
                        continue
                    if any(wait.status == "waiting" and wait.kind == "any" and
                           any(case["kind"] != "child" for case in wait.request["cases"])
                           for wait in invocation.scheduler.waits.values()):
                        state["at_boundary"] = True
                        break
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
            while True:
                state = await self.get_execution_state(invocation_id)
                if until == "none" or state["settled"] or until == "boundary" and state["at_boundary"]:
                    return dict(state, observation="accepted" if until == "none" else "terminal" if state["settled"] else "boundary")
                if state["needs_recovery"]:
                    return dict(state, observation="interrupted")
                remaining = None if deadline is None else deadline - loop.time()
                if remaining is not None and remaining <= 0:
                    return dict(state, observation="timeout")
                # Bounded fallback also observes Runtime-only recovery changes,
                # buffered accepted state and commits made by another process.
                revision = await changed(revision, min(0.1, remaining) if remaining is not None else 0.1,
                                         state["invocation_id"])
