"""Minimal durable execution directory and active-work indexes.

This reducer never builds historical Node/Call/Wait, routing or graph views.
Rows in active_* exist only while the corresponding work can affect control.
"""
from sqlalchemy import select

from .schema import meta

OPERATIONAL_TABLES = ('sessions', 'invocations', 'active_nodes', 'active_waits')


def reference(kind, identity, *path):
    return dict(kind=kind, id=identity, path=list(path))


class OperationalBatch:
    def __init__(self, connection, *, metadata_cache=None):
        self.connection = connection
        self.metadata_cache = metadata_cache
        self.rows = {name: {} for name in OPERATIONAL_TABLES}
        self.dirty = {name: set() for name in OPERATIONAL_TABLES}
        self.order = 0
        self.labels = {}
        self.ended_waits = {}
        self.created = {name: set() for name in ('active_nodes', 'active_waits')}

    async def get(self, name, **identity):
        table = meta.tables[name]
        key = tuple(identity[c.name] for c in table.primary_key)
        if key not in self.rows[name]:
            cached = self.metadata_cache.get((name, key)) if self.metadata_cache else None
            if cached is None:
                found = (await self.connection.execute(select(table).filter_by(**identity))).mappings().first()
                cached = dict(found) if found else None
            self.rows[name][key] = cached
        return self.rows[name][key]

    async def put(self, name, identity, values):
        table = meta.tables[name]
        key = tuple(identity[c.name] for c in table.primary_key)
        # Complete active-Node rows never need a read before their upsert.
        old = self.rows[name].get(key) if name == 'active_nodes' else await self.get(name, **identity)
        row = dict(old or identity, **values)
        if name in ('invocations', 'active_waits'):
            row['updated_store_order'] = self.order
        self.rows[name][key] = row
        if row != old:
            self.dirty[name].add(key)
        return row

    async def remove(self, name, **identity):
        table = meta.tables[name]
        key = tuple(identity[c.name] for c in table.primary_key)
        # A tombstone must also hide an earlier buffered insertion.
        self.rows[name][key] = None
        if key in self.created[name]:
            # Canonically added and removed in this batch: SQL never saw it.
            self.dirty[name].discard(key)
        else:
            self.dirty[name].add(key)

    def changed(self, name):
        return [self.rows[name][key] for key in self.dirty[name] if self.rows[name][key] is not None]

    async def prefetch(self, records):
        for name, field in (('sessions', 'session_id'), ('invocations', 'invocation_id')):
            table = meta.tables[name]
            missing = []
            for identity in {r[field] for r in records if r.get(field)}:
                key = (identity,)
                row = self.metadata_cache.get((name, key)) if self.metadata_cache else None
                self.rows[name][key] = row
                if row is None:
                    missing.append(identity)
            for start in range(0, len(missing), 500):
                for row in (await self.connection.execute(select(table).where(table.c[field].in_(missing[start:start+500])))).mappings():
                    self.rows[name][(row[field],)] = dict(row)

    def publish(self):
        if self.metadata_cache is None:
            return
        for name in OPERATIONAL_TABLES:
            for key in self.dirty[name]:
                row = self.rows[name][key]
                if row is None:
                    self.metadata_cache.discard((name, key))
                else:
                    self.metadata_cache.put((name, key), {c.name: row.get(c.name) for c in meta.tables[name].c})

    async def _scheduler(self, iid, scheduler, kind, identity, path, *, added=False):
        labels = self.labels.setdefault(iid, {})
        occurrences = scheduler.get('occurrences', {})
        for oid, value in occurrences.items():
            labels[oid] = value['node_id']
            if value['status'] in ('ready', 'running'):
                if added:
                    self.created['active_nodes'].add((iid, oid))
                await self.put('active_nodes', dict(invocation_id=iid, occurrence_id=oid),
                               dict(node_id=value['node_id'], status=value['status']))
        for wid, value in scheduler.get('waits', {}).items():
            if value['status'] != 'waiting':
                self.ended_waits[(iid, wid)] = dict(invocation_id=iid, wait_id=wid,
                    occurrence_id=value['occurrence_id'], kind=value['kind'], status=value['status'])
                await self.remove('active_waits', invocation_id=iid, wait_id=wid)
                continue
            oid = value['occurrence_id']
            node_id = labels.get(oid)
            if node_id is None:
                node = await self.get('active_nodes', invocation_id=iid, occurrence_id=oid)
                old = await self.get('active_waits', invocation_id=iid, wait_id=wid)
                node_id = (node or old or {}).get('node_id')
            if node_id is None:
                raise ValueError('An active Wait must identify its Node.')
            request = value.get('request')
            # Normal Event metadata carries this flag before stripping requests.
            independent = value.get('has_independent_case', value['kind'] not in ('child', 'any'))
            if isinstance(request, dict) and value['kind'] == 'any':
                independent = any(c['kind'] != 'child' for c in request.get('cases', ()))
            values = {k: value.get(k) for k in ('occurrence_id','kind','status','created_at_us','deadline_at_us')}
            values.update(node_id=node_id, has_independent_case=bool(independent),
                          request_ref=reference(kind, identity, *path, 'waits', wid, 'request'))
            if added:
                self.created['active_waits'].add((iid, wid))
            await self.put('active_waits', dict(invocation_id=iid, wait_id=wid), values)
        # Keep labels available until all Waits in this transition are indexed.
        for oid, value in occurrences.items():
            if value['status'] not in ('ready', 'running'):
                await self.remove('active_nodes', invocation_id=iid, occurrence_id=oid)

    async def _plan(self, iid, plan):
        parent = await self.get('invocations', invocation_id=iid)
        for unit in plan['units']:
            csid, ciid = unit['session_id'], unit['invocation_id']
            child = await self.get('invocations', invocation_id=ciid)
            if await self.get('sessions', session_id=csid) is None:
                await self.put('sessions', dict(session_id=csid), dict(created_at_us=None,
                    durable_sequence=0, history_start_sequence=1, latest_invocation_id=None))
            values = dict(session_id=csid, workflow_revision_id=plan['workflow_revision_id'],
                entry_node_id=plan['entry_node_id'], root_invocation_id=parent['root_invocation_id'],
                parent_invocation_id=iid, parent_occurrence_id=plan['parent_occurrence_id'],
                creation_id=plan['creation_id'], unit_index=unit['unit_index'], ownership_phase=unit['phase'])
            if child is None:
                values.update(status=None, last_sequence=0)
            await self.put('invocations', dict(invocation_id=ciid), values)

    async def runtime(self, event, order):
        self.order = order
        sid, iid, seq = event['session_id'], event['invocation_id'], event['sequence']
        name, payload, stamp = event['event_name'], event['payload'], event['occurred_at_us']
        ref = lambda *path: reference('runtime_event', event['id'], *path)
        session = await self.get('sessions', session_id=sid)
        await self.put('sessions', dict(session_id=sid), dict(
            created_at_us=stamp if name in ('session.opened','invocation.forked') else (session or {}).get('created_at_us'),
            latest_invocation_id=iid if name in ('invocation.started','invocation.forked') else (session or {}).get('latest_invocation_id'),
            durable_sequence=seq, history_start_sequence=(session or {}).get('history_start_sequence',1)))
        if iid is None:
            return
        fresh = False
        if name in ('invocation.started','invocation.forked'):
            old = await self.get('invocations', invocation_id=iid)
            fresh = name == 'invocation.started' and old is None
            await self.put('invocations', dict(invocation_id=iid), dict(session_id=sid,
                workflow_revision_id=payload['workflow_revision_id'], entry_node_id=payload['entry_node_id'],
                root_invocation_id=(old or {}).get('root_invocation_id',iid), status='running',
                control_state='active', recovery_count=0, recovery_pending=False,
                started_at_us=stamp, last_sequence=seq,
                input_ref=ref('payload','input') if name=='invocation.started' else ref('delta','operations',1,'value','input')))
        operations = (event.get('delta') or {}).get('operations', ())
        for index, operation in enumerate(operations):
            path, value = operation['path'], operation.get('value')
            if operation['op'] == 'remove':
                if path[:2] == ['invocation','scheduler'] and len(path)==4:
                    table = {'occurrences':'active_nodes','waits':'active_waits'}.get(path[2])
                    if table:
                        await self.remove(table, invocation_id=iid, **{('occurrence_id' if table=='active_nodes' else 'wait_id'):path[3]})
                continue
            if path == ['invocation']:
                fields = ('status','pending_outcome','control_state','recovery_count','last_recovery_id','started_at_us','completed_at_us')
                values = {k:value[k] for k in fields if k in value}
                values['recovery_pending'] = bool(value.get('pending_recovery'))
                if name in ('invocation.forked','fork.state_imported'):
                    for field in ('input','output','error'):
                        if value.get(field) is not None:
                            values[field+'_ref'] = ref('delta','operations',index,'value',field)
                if name == 'invocation.reopened':
                    values.update(error_ref=None,error_type=None,error_code=None,output_ref=None)
                await self.put('invocations', dict(invocation_id=iid), values)
                # Whole-State replacements (recovery/fork) replace active work.
                for table in ('active_nodes','active_waits'):
                    source = meta.tables[table]
                    # Foreign keys prove these tables empty for a new Invocation.
                    if not fresh:
                        for row in (await self.connection.execute(select(source).where(source.c.invocation_id==iid))).mappings():
                            key = {c.name:row[c.name] for c in source.primary_key}
                            await self.remove(table, **key)
                    for key in list(self.rows[table]):
                        if key[0] == iid:
                            await self.remove(table, **dict(zip(source.primary_key.columns.keys(),key)))
                await self._scheduler(iid,value.get('scheduler',{}),'runtime_event',event['id'],
                    ('delta','operations',index,'value','scheduler'), added=name=='invocation.started')
                for plan in value.get('child_plans',{}).values():
                    await self._plan(iid,plan)
            elif path[:2] == ['invocation','child_plans'] and len(path)==3:
                await self._plan(iid,value)
            elif path[:2] == ['invocation','scheduler'] and len(path)==4:
                field, key = path[2:]
                if field in ('occurrences','waits'):
                    await self._scheduler(iid,{field:{key:value}},'runtime_event',event['id'],
                        ('delta','operations',index,'value'), added=operation['op']=='add')
                    if field == 'waits' and value['status']=='waiting':
                        await self.put('active_waits',dict(invocation_id=iid,wait_id=key),
                            dict(request_ref=ref('delta','operations',index,'value','request')))
            elif len(path)==2 and path[0]=='invocation':
                if path[1]=='pending_recovery':
                    await self.put('invocations',dict(invocation_id=iid),dict(recovery_pending=bool(value)))
                elif path[1] in ('status','pending_outcome','control_state','started_at_us','completed_at_us'):
                    values = {path[1]:value}
                    if path[1]=='pending_outcome' and value in ('failed','cancelled'):
                        values['output_ref']=None
                    await self.put('invocations',dict(invocation_id=iid),values)
        await self.put('invocations',dict(invocation_id=iid),dict(last_sequence=seq))
        if name in ('invocation.completed','invocation.settling','invocation.failed'):
            values = dict(output_ref=ref('payload','output') if 'output' in payload and
                (name!='invocation.settling' or payload['outcome']=='completed') else None)
            if payload.get('error'):
                values.update(error_ref=ref('payload','error'),error_type=payload['error']['type'],error_code=payload['error'].get('code'))
            await self.put('invocations',dict(invocation_id=iid),values)
        elif name=='invocation.cancelled':
            await self.put('invocations',dict(invocation_id=iid),dict(output_ref=None))
        elif name=='node_occurrence.waiting':
            await self.put('active_waits',dict(invocation_id=iid,wait_id=payload['wait_id']),
                dict(request_ref=ref('payload','request')))

    async def checkpoint(self, record, checkpoint_id, *, anchor):
        if not anchor:
            return
        root_state = next(s for s in record['sessions'] if s['session_id']==record['root_session_id'])['state']
        root = root_state['invocation']['id']
        for index, session in enumerate(record['sessions']):
            sid, state = session['session_id'], session['state']
            old = await self.get('sessions',session_id=sid)
            if old and old['durable_sequence']>=state['sequence']:
                continue
            await self.put('sessions',dict(session_id=sid),dict(created_at_us=state['session']['created_at_us'],
                latest_invocation_id=state['session']['latest_invocation_id'],durable_sequence=state['sequence'],
                history_start_sequence=state['sequence']+1))
            inv = state['invocation']
            if inv is None:
                continue
            iid = inv['id']; path=('sessions',index,'state','invocation')
            fields=('status','pending_outcome','control_state','recovery_count','last_recovery_id','started_at_us','completed_at_us')
            values={k:inv.get(k) for k in fields}
            values.update(session_id=sid,workflow_revision_id=inv['workflow_revision_id'],entry_node_id=inv['entry_node_id'],
                root_invocation_id=root,last_sequence=state['sequence'],recovery_pending=bool(inv.get('pending_recovery')))
            for field in ('input','output','error'):
                if inv.get(field) is not None and inv.get('kind')!='child_result':
                    values[field+'_ref']=reference('checkpoint',checkpoint_id,*path,field)
            if inv.get('error'):
                values.update(error_type=inv['error']['type'],error_code=inv['error'].get('code'))
            await self.put('invocations',dict(invocation_id=iid),values)
            await self._scheduler(iid,inv.get('scheduler',{}),'checkpoint',checkpoint_id,(*path,'scheduler'))
            for plan in inv.get('child_plans',{}).values():
                await self._plan(iid,plan)
