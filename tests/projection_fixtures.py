"""SQL fixture access for Event-backed read relations.

Timing fixtures alter the underlying test Events, so cache eviction/rebuild sees
exactly the same values. Production has no Event mutation API.
"""
import json
from sqlalchemy import select, update, insert, func
from sqlalchemy.sql.dml import Delete, Update, Insert

from autoagent.host.persistence.read_models import ProjectionConnection
from autoagent.host.persistence.schema import DETAIL_TABLES, meta


class EventFixtureConnection(ProjectionConnection):
    async def _invalidate_projections(self):
        from autoagent.host.persistence.query_projections import reset
        await reset(self.raw)
        self.persisted_order = None
        self.cache.clear()
        self.loaded.clear()
        self.relations.clear()
        self.sql_scopes.clear()
        self.merged = None
        self.working_bytes = self.relation_bytes = 0

    async def execute(self, statement, *args, **kwargs):
        if isinstance(statement,Insert) and statement.table.name=='node_executions':
            from uuid import uuid4
            values={getattr(key,'name',key):getattr(value,'value',value) for key,value in statement._values.items()}
            events, invs, metadata=(meta.tables[name] for name in ('runtime_events','invocations','persistence_meta'))
            inv=(await self.raw.execute(select(invs).where(invs.c.invocation_id==values['invocation_id']))).mappings().one()
            previous=(await self.raw.execute(select(events).where(events.c.session_id==inv['session_id']).order_by(events.c.sequence.desc()).limit(1))).mappings().one()
            order=await self.raw.scalar(select(metadata.c.last_store_order))
            fields=('node_id','scope','status','ready_at_us','started_at_us','completed_at_us','recovery_attempts')
            node={key:values[key] for key in fields}
            node['execution']={'phase':values.get('phase','none')}
            delta={'operations':[dict(op='add',path=['invocation','scheduler','occurrences',values['occurrence_id']],value=node)]}
            await self.raw.execute(insert(events).values(event_id=uuid4().hex,store_order=order+1,
                session_id=inv['session_id'],invocation_id=values['invocation_id'],sequence=previous['sequence']+1,
                event_name='scheduler.updated',occurred_at_us=values['started_at_us'],schema_version=previous['schema_version'],
                payload_text='{}',delta_text=json.dumps(delta)))
            await self.raw.execute(update(metadata).values(last_store_order=order+1))
            self.frontier=order+1
            await self._invalidate_projections()
            return None
        if isinstance(statement,(Update,Delete)) and statement.table.name in DETAIL_TABLES:
            if isinstance(statement,Delete):
                # Deleting optional cached projection rows cannot delete facts.
                await self._invalidate_projections()
                return None
            table=statement.table
            records=(await super().execute(select(table).where(*statement._where_criteria))).mappings().all()
            changes={getattr(key,'name',key):getattr(value,'value',value) for key,value in statement._values.items()}
            field={'node_executions':'occurrences','operator_calls':'operator_calls','waits':'waits'}[table.name]
            identity_field={'node_executions':'occurrence_id','operator_calls':'call_id','waits':'wait_id'}[table.name]
            targets={(r['invocation_id'],r[identity_field]) for r in records}
            events=meta.tables['runtime_events']
            rows=(await self.raw.execute(select(events).where(events.c.invocation_id.in_({iid for iid,_ in targets})))).mappings().all()
            for row in rows:
                delta=json.loads(row['delta_text']) if row['delta_text'] else None
                if delta is None: continue
                touched=False
                for op in delta['operations']:
                    path,value=op['path'],op.get('value')
                    if path==['invocation'] and isinstance(value,dict):
                        for key,item in value.get('scheduler',{}).get(field,{}).items():
                            if (row['invocation_id'],key) in targets:
                                item.update(changes);touched=True
                    elif path[:3]==['invocation','scheduler',field] and len(path)>=4 and (row['invocation_id'],path[3]) in targets:
                        if len(path)==4 and isinstance(value,dict):value.update(changes);touched=True
                        elif len(path)==5 and path[4] in changes:op['value']=changes[path[4]];touched=True
                if touched:
                    await self.raw.execute(update(events).where(events.c.event_id==row['event_id']).values(delta_text=json.dumps(delta)))
            await self._invalidate_projections()
            return None
        if isinstance(statement, Delete) and statement.table.name == 'runtime_events':
            # Optional SQL projections can reference the deliberately truncated
            # Event. Invalidate them before removing its canonical test fact.
            await self._invalidate_projections()
        return await super().execute(statement,*args,**kwargs)
