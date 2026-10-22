"""Persisted execution evidence shared by local CLI and Server transports."""

from .models import HostNotFoundError, HostRunConflict
from .persistence.models import InspectionError


class HistoryQueries:
    async def inspect_execution_state(self, invocation_id, *, sequence):
        """Read the persisted state immediately after an exact Invocation Event."""
        return await self._inspect_state(invocation_id, sequence=sequence)

    async def read_execution_context(self, invocation_id, *, sequence, scope, pointer=None):
        """Read a historical Session/Invocation Context subtree, with bounded decoding."""
        if scope not in ("session", "invocation"):
            raise ValueError("scope must be session or invocation.")
        return await self._inspect_state(invocation_id, sequence=sequence, scope=scope, pointer=pointer)

    async def _inspect_state(self, invocation_id, **options):
        async def query():
            try:
                result = await self._persistence.inspect_execution_state(invocation_id, **options)
            except InspectionError as error:
                raise HostRunConflict(error.code, str(error)) from error
            if result is None:
                raise HostNotFoundError("Unknown persisted Invocation.")
            return result
        return await self._perform(query)

    async def diagnose_execution(self, invocation_id, *, limit=20):
        """Summarize saved failure and wait evidence for the selected subtree."""
        async def query():
            result = await self._persistence.execution_diagnosis(invocation_id, limit=limit)
            if result is None:
                raise HostNotFoundError('Unknown persisted Invocation.')
            return result
        return await self._perform(query)

    async def get_history_identity(self):
        async def query():
            return dict(store_id=await self._persistence.history_identity(), view="durable",
                registration_checked=self._runtime is not None)
        return await self._perform(query)

    async def get_invocation_summary(self, invocation_id):
        async def query():
            result = await self._persistence.invocation_summary(invocation_id)
            if result is None:
                raise HostNotFoundError("Unknown persisted Invocation.")
            return result
        return await self._perform(query)

    async def list_execution_events(self, invocation_id, *, after=0, names=(), limit=50, cursor=None):
        async def query():
            result = await self._persistence.execution_events(invocation_id, after=after, names=names, limit=limit, cursor=cursor)
            if result is None:
                raise HostNotFoundError("Unknown persisted Invocation.")
            return result
        return await self._perform(query)

    async def get_execution_event(self, invocation_id, event_id, *, field=None, pointer=None):
        async def query():
            result = await self._persistence.execution_event(invocation_id, event_id, field=field, pointer=pointer)
            if result is None:
                raise HostNotFoundError("Unknown Event in this Invocation.")
            return result
        return await self._perform(query)
