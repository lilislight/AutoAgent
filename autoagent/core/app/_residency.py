"""Consistent graph capture and bounded-lifetime memory estimates."""

from .models import GraphResidencyInfo
from ..errors import RuntimeTransitionError
from ..runtime._memory import retained_entries


def retained_size(values):
    """Count shared State objects once without serializing business values."""
    return sum(size for _, _, size in retained_entries(values))


class ResidencyRuntime:
    def _graph_version(self, root):
        return tuple(sorted(
            (sid, state.invocation.id if state.invocation else None, state.sequence)
            for sid in (root, *self._descendant_sessions(root))
            if (state := self._repository.state(sid)).session is not None
        ))

    def capture_checkpoint(self, ref):
        """Capture a quiescent Root graph without discarding its State."""
        return self._run(self._capture_checkpoint(ref))

    async def acapture_checkpoint(self, ref):
        return await self._await(self._submit(self._capture_checkpoint(ref)))

    async def _capture_checkpoint(self, ref):
        self._state_for_ref(ref)
        async with self._graph_gate(ref.session_id):
            await self._check_unloadable(ref)
            return await self._capture_graph_locked(ref.session_id)

    async def aresidency_info(self, ref):
        """Estimate acknowledged State, caching only the current graph version."""
        return await self._await(self._submit(self._residency_info(ref)))

    async def _residency_info(self, ref):
        suspension = await self._suspension_info(ref)
        inv = self._state_for_ref(ref).invocation
        version = self._graph_version(ref.session_id)
        cached = self._residency_sizes.get(ref.session_id)
        if cached is None or cached[0] != version:
            size = retained_size(self._repository.state(sid) for sid, _, _ in version)
            self._residency_sizes[ref.session_id] = version, size
        else:
            size = cached[1]
        return GraphResidencyInfo(
            ref, version, size, suspension.can_unload,
            suspension.next_deadline_at_us, inv.status, inv.control_state,
        )

    async def _check_unloadable(self, ref):
        """Called under exclusive graph admission; settle and then recheck."""
        root = ref.session_id
        self._state_for_ref(ref)
        sessions = (root, *self._descendant_sessions(root))
        if self._graph_controls.get(root) or any(self._task_runtime.is_live(s) for s in sessions):
            raise RuntimeTransitionError("RELATED_INVOCATION_NOT_UNLOADABLE", "Graph has live tasks or control operations.")
        await self._settle_runtime_commits(sessions)
        sessions = (root, *self._descendant_sessions(root))
        if root in self._recovering or self._result_leases.get(root, 0):
            raise RuntimeTransitionError("INVOCATION_RESULT_PENDING", "Graph still has an active reader or recovery.")
        paused = self._state_for_ref(ref).invocation.control_state == "paused"
        for sid in sessions:
            inv = self._repository.state(sid).invocation
            if inv is not None and ((not paused and inv.status not in {'waiting', 'settling', 'completed', 'failed', 'cancelled'}) or self._task_runtime.is_live(sid)):
                raise RuntimeTransitionError("RELATED_INVOCATION_NOT_UNLOADABLE", "Graph is not quiescent.")
            if inv is not None and inv.terminal and self._remaining_children(sid):
                raise RuntimeTransitionError("RELATED_INVOCATION_NOT_UNLOADABLE", "Terminal graph has unsettled ownership.")
