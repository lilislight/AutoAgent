"""Read-only Workflow comparison and Session information for Server and CLI."""

from .models import HostNotFoundError
from .persistence.models import Page


class Exploration:
    async def compare_workflow_revisions(self, base_revision_id, target_revision_id, *, limit=50, cursor=None):
        async def query():
            result = await self._persistence.compare_workflow_revisions(base_revision_id, target_revision_id, limit=limit, cursor=cursor)
            if result is None:
                raise HostNotFoundError('Workflow Revision is unavailable.')
            return result
        return await self._perform(query)

    async def get_revision_comparison_object(self, base_revision_id, target_revision_id, kind, identity):
        """Read changed fields; complete definitions use the ordinary object reader."""
        async def query():
            result = await self._persistence.revision_comparison_object(base_revision_id, target_revision_id, kind, identity)
            if result is None:
                raise HostNotFoundError('Comparison object is unavailable.')
            return result
        return await self._perform(query)

    async def get_session_information(self, session_id):
        async def query():
            result = await self._persistence.session_information(session_id)
            if result is None:
                raise HostNotFoundError('Session is unavailable.')
            return result
        return await self._perform(query)

    async def list_session_history(self, session_id, *, limit=50, cursor=None,
        search='', workflow_ids=(), revision_ids=(), statuses=(), started_after_us=None, started_before_us=None):
        """Read causal Invocation order; filtering retains the original Session ordinal."""
        if not isinstance(search, str) or len(search) > 512:
            raise ValueError('search must have at most 512 characters.')
        async def query():
            result = await self._persistence.session_history(session_id, limit=limit, cursor=cursor,
                search=search.strip().lower(), workflow_ids=workflow_ids, revision_ids=revision_ids,
                statuses=statuses, started_after_us=started_after_us, started_before_us=started_before_us)
            if result is None:
                raise HostNotFoundError('Session is unavailable.')
            return result
        return await self._perform(query)

    async def list_session_workflows(self, session_id, *, limit=50, cursor=None):
        async def query():
            page = await self._persistence.session_workflows(session_id, limit=limit, cursor=cursor)
            if page is None:
                raise HostNotFoundError('Session is unavailable.')
            return Page(tuple(dict(row, runnable=self._runtime is not None and row['workflow_revision_id'] in self._runtime.revisions) for row in page.items),
                        page.next_cursor, page.frontier, page.total_count)
        return await self._perform(query)
