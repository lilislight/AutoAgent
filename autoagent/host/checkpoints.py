"""Explicit debugging controls layered over automatic Checkpoint residency."""

import asyncio

from .models import HostNotFoundError, HostRunConflict
from .persistence.models import ConflictError


class CheckpointControls:
    async def list_checkpoints(self, session_id, *, limit=100, cursor=None):
        """List graph metadata containing this Session, without reading Context."""
        async def query():
            if await self._persistence.get_session(session_id, view="accepted") is None:
                raise HostNotFoundError("Session is unavailable.")
            return await self._persistence.list_checkpoints(session_id, limit=limit, cursor=cursor)
        return await self._perform(query)

    async def get_checkpoint(self, checkpoint_id):
        """Return metadata, graph positions and system deletion dependencies."""
        async def query():
            info = await self._persistence.checkpoint_info(checkpoint_id)
            if info is None:
                raise HostNotFoundError("Checkpoint is unavailable.")
            return info
        return await self._perform(query)

    async def create_checkpoint(self, session_id, *, sequence=None, pinned=False):
        """Capture now or immediately after a Session Event; never rewind live execution."""
        if type(pinned) is not bool:
            raise ValueError("pinned must be a boolean.")
        if sequence is not None and (type(sequence) is not int or not 1 <= sequence <= 9007199254740991):
            raise ValueError('sequence must be a positive safe integer.')

        async def create():
            if sequence is not None:
                await self._persistence.flush()
                bundle = await self._persistence.read_checkpoint_position(session_id, sequence)
                try:
                    graph = await asyncio.to_thread(self._runtime._restore_graph, bundle)
                    identity = await self._persistence.save_checkpoint(graph, reason='manual', pinned=pinned)
                except (ValueError, KeyError) as error:
                    raise HostRunConflict('checkpoint_position_unavailable',
                        'This Event prefix cannot form a complete recoverable graph: ' + str(error)) from error
                # Historical creation cannot evict the current recovery snapshot.
                # Normal retention applies at the next automatic capture; Pin to keep.
                return await self._persistence.checkpoint_info(identity)
            session = await self._persistence.get_session(session_id, view="accepted")
            if session is None or session['latest_invocation_id'] is None:
                raise HostNotFoundError("Session has no Invocation.")
            invocation = await self._persistence.get_invocation(session['latest_invocation_id'], view="accepted")
            root = await self._persistence.get_invocation(invocation['root_invocation_id'], view="accepted")
            sid = root['session_id']
            item = self._runtime.residency.touch(sid)
            item.users += 1
            try:
                async with self._runtime.coordinated(sid):
                    # Re-read under Root coordination: a new Invocation may have
                    # been admitted while this request was finding its owner.
                    current = await self._persistence.get_session(sid, view="accepted")
                    if current['latest_invocation_id'] != root['invocation_id']:
                        raise HostRunConflict('checkpoint_graph_changed', 'Session changed; retry against its current Invocation.')
                    ref = self._runtime.resident.get(sid)
                    if ref is not None:
                        graph = await self._runtime.app.acapture_checkpoint(ref)
                    else:
                        await self._persistence.flush()
                        bundle = await self._persistence.read_recovery_bundle(root['invocation_id'])
                        graph = await asyncio.to_thread(self._runtime._restore_graph, bundle)
                    identity = await self._runtime.save_checkpoint(graph, reason="manual", pinned=pinned)
                    # The Writer's version guard serializes concurrent capture,
                    # Pin and retention. Metadata contains no snapshot payload.
                    return await self._persistence.checkpoint_info(identity)
            finally:
                item.users -= 1
                self._runtime.residency.trim()
        return await self._perform(create, control=True)

    async def delete_checkpoint(self, checkpoint_id):
        """Delete even a pinned snapshot, unless a system dependency requires it."""
        async def remove():
            try:
                deleted = await self._persistence.delete_checkpoint(checkpoint_id)
            except ConflictError as error:
                raise HostRunConflict('checkpoint_required', str(error)) from error
            if not deleted:
                raise HostNotFoundError("Checkpoint is unavailable.")
        return await self._perform(remove, control=True)

    async def set_checkpoint_pinned(self, checkpoint_id, pinned):
        """Pin prevents automatic cleanup; it does not override system safety."""
        async def change():
            info = await self._persistence.set_checkpoint_pinned(checkpoint_id, pinned)
            if info is None:
                raise HostNotFoundError("Checkpoint is unavailable.")
            return info
        return await self._perform(change, control=True)
