"""Small committed heads for a selected Invocation and its execution family."""
import time
from sqlalchemy import and_, func, or_, select
from .schema import meta
from .trace_cursor import encode_trace_cursor


class TraceHeads:
    def __init__(self, queries):
        self.queries = queries

    async def read(self, iid):
        # Capture this before the SQL snapshot: a concurrent commit must wake the
        # waiter even when it happened while the snapshot was being read.
        revision = self.queries.writer.commit_revision
        async with self.queries.view(invocation_id=iid) as (conn, _, _, frontier):
            invs, metadata = meta.tables['invocations'], meta.tables['persistence_meta']
            inv = (await conn.execute(select(invs.c.root_invocation_id, invs.c.status,
                invs.c.updated_store_order).where(invs.c.invocation_id == iid))).mappings().first()
            if inv is None:
                return None
            store_id = await conn.scalar(select(metadata.c.store_id))
            family_version, active = (await conn.execute(select(func.max(invs.c.updated_store_order),
                func.count().filter(or_(invs.c.status.not_in(('completed', 'failed', 'cancelled')),
                    and_(invs.c.status.is_(None), or_(invs.c.ownership_phase.is_(None), invs.c.ownership_phase.not_in(('abandoned', 'terminal'))))))
                ).where(invs.c.root_invocation_id == inv['root_invocation_id']))).one()
            return dict(invocation_id=iid, root_invocation_id=inv['root_invocation_id'],
                invocation_version=inv['updated_store_order'], family_version=family_version or 0,
                status=inv['status'], active=bool(active), store_id=store_id,
                cursor=encode_trace_cursor(store_id, iid, frontier), frontier=frontier,
                server_now_us=time.time_ns() // 1000, revision=revision)
