"""Bounded, as-of business history. Runtime storage details stop at this boundary."""

from copy import deepcopy
import json

from sqlalchemy import JSON, LargeBinary, cast, func, select, type_coerce

from .inspection import BoundedReader, ReadBudget
from .models import InspectionError
from .reading import pointer_tokens
from .schema import meta


class BusinessHistory:
    def __init__(self, queries, config):
        self.queries, self.config = queries, config

    def _body(self, conn, column):
        return type_coerce(column, JSON) if conn.dialect.name == 'sqlite' else cast(column, JSON)

    async def _validate(self, conn, member):
        events = meta.tables['runtime_events']
        upper = member.get('position')
        if upper is None:
            return
        actual = (await conn.execute(select(events.c.event_id).where(events.c.invocation_id == member['invocation_id'],
            events.c.sequence == upper['sequence']))).scalar_one_or_none()
        if actual != upper['event_id']:
            raise InspectionError('HISTORY_UNAVAILABLE', 'The fixed business observation is no longer available.')
        # Session sequences, including facts outside the selected Invocation, must form a full prefix.
        count, first, last = (await conn.execute(select(func.count(), func.min(events.c.sequence), func.max(events.c.sequence))
            .where(events.c.session_id == member['session_id'], events.c.sequence <= upper['sequence']))).one()
        if first != 1 or last != upper['sequence'] or count != last:
            raise InspectionError('HISTORY_UNAVAILABLE', 'Business history has a missing prefix or gap; absence cannot be established.')

    async def _rows(self, conn, statement, limit):
        rows = (await conn.execute(statement.limit(limit + 1))).mappings().all()
        if len(rows) > limit:
            raise InspectionError('INSPECTION_LIMIT', 'Business history exceeds the read limit; narrow the selection or increase read_limit.')
        return [dict(row) for row in rows]

    async def _definition(self, conn, member):
        reader = await BoundedReader.open(self.queries.storage, conn, 'workflow_revision', member['workflow_revision_id'], ReadBudget(self.config))
        nodes = await reader.read(('definition', 'nodes'), deep=False)
        edges = await reader.read(('definition', 'edges'), deep=False)
        return {await reader.read(('definition', 'nodes', index, 'id')) for index in range(len(nodes))}, {
            await reader.read(('definition', 'edges', index, 'id')):
                await reader.read(('definition', 'edges', index, 'source')) for index in range(len(edges))}

    async def _facts(self, conn, member, limit, *, occurrence_id=None, call_ids=None):
        e = meta.tables['runtime_events']
        body = self._body(conn, e.c.payload_text)
        fields = ('occurrence_id', 'call_id', 'unit_index', 'retry_attempt', 'wait_id', 'wait_kind', 'creation_id')
        query = select(e.c.event_id, e.c.sequence, e.c.store_order, e.c.event_name,
            *(body[field].label(field) for field in fields)).where(e.c.invocation_id == member['invocation_id'],
                e.c.sequence <= member['position']['sequence'])
        if occurrence_id is not None:
            query = query.where(body['occurrence_id'].as_string() == occurrence_id)
        if call_ids is not None:
            query = query.where(body['call_id'].as_string().in_(call_ids))
        return await self._rows(conn, query.order_by(e.c.sequence), limit)

    @staticmethod
    def _position(member, row, label):
        return dict(invocation_id=member['invocation_id'], session_id=member['session_id'],
            event_id=row['event_id'], sequence=row['sequence'], store_order=row['store_order'], label=label)

    async def records(self, member, kind, *, selector=None, occurrence_id=None, limit=100000):
        q, g = self.queries, meta.tables['graph_changes']
        async with q.view(invocation_id=member['invocation_id']) as (conn, _, _, _):
            await self._validate(conn, member)
            nodes, edges = await self._definition(conn, member)
            if kind == 'nodes' and selector not in nodes or kind in ('routes', 'route_source') and selector not in edges:
                raise ValueError(f'Unknown {"Node" if kind == "nodes" else "route"}: {selector}')
            if member.get('position') is None:
                return []
            iid, upper = member['invocation_id'], member['position']['sequence']
            if kind == 'route_source':
                n = meta.tables['node_executions']
                ids = select(n.c.occurrence_id).where(n.c.invocation_id == iid, n.c.node_id == edges[selector])
                return bool(await conn.scalar(select(func.count()).select_from(g).where(g.c.invocation_id == iid,
                    g.c.entity_type == 'node', g.c.entity_id.in_(ids), g.c.sequence <= upper,
                    g.c.change['status'].as_string().in_(('ready', 'running', 'waiting', 'completed', 'failed', 'cancelled')))))
            if kind == 'nodes':
                n = meta.tables['node_executions']
                ids = select(n.c.occurrence_id).where(n.c.invocation_id == iid, n.c.node_id == selector)
                rows = await self._rows(conn, select(g.c.event_id, g.c.entity_id, g.c.sequence, g.c.store_order, g.c.change)
                    .where(g.c.invocation_id == iid, g.c.entity_type == 'node', g.c.entity_id.in_(ids), g.c.sequence <= upper)
                    .order_by(g.c.sequence, g.c.change_index), limit)
                grouped = {}
                for row in rows:
                    item = grouped.setdefault(row['entity_id'], dict(occurrence_id=row['entity_id'], node_id=selector,
                        status='ready', started=False, transitions=[], positions={}, recovery_attempts=0))
                    change = row['change']
                    if 'status' in change and (not item['transitions'] or item['status'] != change['status']):
                        position = self._position(member, row, 'node.' + change['status'])
                        item['transitions'].append(dict(status=change['status'], position=position))
                        item['positions'][change['status']] = position
                    item.update({key: value for key, value in change.items() if key in ('status', 'phase', 'scope', 'recovery_attempts')})
                    if change.get('status') == 'running':
                        item['started'] = True
                return list(grouped.values())
            if kind == 'routes':
                rows = await self._rows(conn, select(g.c.event_id, g.c.sequence, g.c.store_order, g.c.entity_id, g.c.change)
                    .where(g.c.invocation_id == iid, g.c.entity_type == 'edge', g.c.sequence <= upper,
                        g.c.change['edge_id'].as_string() == selector).order_by(g.c.sequence, g.c.change_index), limit)
                decisions = {}
                for row in rows:
                    key = row['change'].get('source_occurrence_id')
                    source_status = (await conn.execute(select(g.c.change['status'].as_string()).where(
                        g.c.invocation_id == iid, g.c.entity_type == 'node', g.c.entity_id == key,
                        g.c.sequence <= row['sequence'], g.c.change['status'].as_string().is_not(None))
                        .order_by(g.c.sequence.desc(), g.c.change_index.desc()).limit(1))).scalar_one_or_none()
                    decisions[key] = dict(route_id=selector, source_occurrence_id=key,
                        status='not_reached' if source_status == 'skipped' else 'taken' if row['change'].get('selected') else 'not_taken',
                        position=self._position(member, row, 'route.decided'))
                return list(decisions.values())
            if kind == 'transitions':
                entity = 'node' if occurrence_id else 'invocation'
                query = select(g.c.event_id, g.c.sequence, g.c.store_order, g.c.change).where(
                    g.c.invocation_id == iid, g.c.entity_type == entity, g.c.entity_id == (occurrence_id or iid), g.c.sequence <= upper)
                rows = await self._rows(conn, query.order_by(g.c.sequence, g.c.change_index), limit)
                changes, state = [], {}
                for row in rows:
                    for key in ('status', 'control_state'):
                        if key in row['change'] and state.get(key) != row['change'][key]:
                            state[key] = row['change'][key]
                            changes.append(dict(field=key, value=state[key], position=self._position(member, row, f'{entity}.{state[key]}')))
                return changes
            if kind in ('items', 'values'):
                facts = await self._facts(conn, member, limit, occurrence_id=occurrence_id)
                calls = {}
                for row in facts:
                    if row['event_name'] == 'operator_call.started':
                        calls[row['call_id']] = dict(call_id=row['call_id'], unit_index=row['unit_index'],
                            retry_attempt=row['retry_attempt'] or 0, status='running',
                            position=self._position(member, row, 'item.started'), input=self._ref(row, 'input'))
                if calls:
                    ends = await self._facts(conn, member, limit, call_ids=list(calls))
                    for row in ends:
                        item = calls.get(row['call_id'])
                        if item is None:
                            continue
                        if row['event_name'] in ('operator_call.completed', 'operator_call.failed'):
                            field = 'output' if row['event_name'].endswith('completed') else 'error'
                            item.update(status='completed' if field == 'output' else 'failed',
                                position=self._position(member, row, 'item.' + ('completed' if field == 'output' else 'failed')))
                            item[field] = self._ref(row, field)
                if kind == 'items':
                    # Each unit's effective attempt is explicit; all attempts remain available separately.
                    effective = {}
                    for item in calls.values():
                        effective[item['unit_index']] = item
                    return [dict(item, attempts=[attempt for attempt in calls.values() if attempt['unit_index'] == index])
                        for index, item in sorted(effective.items())]
                values = {}
                for row in facts:
                    field = {'input.mapped': ('input', 'mapped_input'), 'node.aggregated': ('aggregate', 'output'),
                        'node_occurrence.completed': ('output', 'output'), 'node_occurrence.failed': ('error', 'error'),
                        'node.faulted': ('error', 'error')}.get(row['event_name'])
                    if field:
                        values[field[0]] = self._ref(row, field[1])
                if 'input' not in values and calls:
                    units = {}
                    for item in calls.values():
                        units[item['unit_index']] = item['input']
                    values['input'] = units[0] if len(units) == 1 else dict(parts=[value for _, value in sorted(units.items())])
                return values
            if kind == 'interactions':
                rows = await self._rows(conn, select(g.c.event_id, g.c.sequence, g.c.store_order, g.c.entity_id, g.c.change)
                    .where(g.c.invocation_id == iid, g.c.entity_type == 'wait', g.c.sequence <= upper)
                    .order_by(g.c.sequence, g.c.change_index), limit)
                waits = {}
                for row in rows:
                    item = waits.setdefault(row['entity_id'], dict(wait_id=row['entity_id'], positions={}))
                    item.update(row['change'])
                    if 'status' in row['change']:
                        item['positions'][row['change']['status']] = self._position(member, row, 'interaction.' + row['change']['status'])
                facts = await self._facts(conn, member, limit)
                for row in facts:
                    if row['wait_id'] in waits:
                        item = waits[row['wait_id']]
                        if row['event_name'] == 'node_occurrence.waiting':
                            item['request'] = self._ref(row, 'request')
                        elif row['event_name'] in ('wait.resumed', 'command.awakened'):
                            item['response'] = self._ref(row, 'response' if row['event_name'] == 'wait.resumed' else 'output')
                n = meta.tables['node_executions']
                node_ids = dict((await conn.execute(select(n.c.occurrence_id, n.c.node_id).where(n.c.invocation_id == iid))).all())
                return [dict(item, node_id=node_ids.get(item.get('occurrence_id'))) for item in waits.values()]
            if kind == 'messages':
                upper = member.get('user_upper_sequence')
                if upper is None:
                    raise InspectionError('HISTORY_UNAVAILABLE', 'This historical position has no recorded business-message boundary.')
                users = meta.tables['user_events']
                count = await conn.scalar(select(func.count()).select_from(users).where(users.c.invocation_id == iid, users.c.sequence <= upper))
                if count != upper:
                    raise InspectionError('HISTORY_UNAVAILABLE', 'Business-message history is incomplete.')
                statement = select(users.c.event_id, users.c.kind, users.c.occurrence_id, users.c.sequence).where(users.c.invocation_id == iid, users.c.sequence <= upper)
                if selector is not None: statement = statement.where(users.c.kind == selector)
                rows = await self._rows(conn, statement.order_by(users.c.sequence), limit)
                return [dict(kind=row['kind'], occurrence_id=row['occurrence_id'],
                    body=dict(kind='user_event', id=row['event_id'], path=['payload'])) for row in rows]
            raise ValueError('Unknown business history query.')

    @staticmethod
    def _ref(row, field):
        return dict(kind='runtime_event', id=row['event_id'], path=['payload', field])

    async def order(self, snapshot, earlier, later, *, limit):
        """Prove cross-Session order through recorded creation, settlement or Signal delivery."""
        links, facts = [], {}
        e = meta.tables['runtime_events']
        async with self.queries.view(invocation_id=snapshot['invocation_id']) as (conn, _, _, _):
            body = self._body(conn, e.c.payload_text)
            remaining = limit
            for member in snapshot['members']:
                await self._validate(conn, member)
                if not member.get('position'): continue
                rows = await self._rows(conn, select(e.c.event_name, e.c.sequence,
                    body['creation_id'].label('creation_id'), body['unit_index'].label('unit_index'),
                    body['phase'].label('phase'), body['call_id'].label('call_id'),
                    body['origin'].label('origin')).where(e.c.invocation_id == member['invocation_id'],
                    e.c.sequence <= member['position']['sequence'], e.c.event_name.in_(
                        ('child_invocation.planned', 'child_invocation.phase_changed', 'signal.accepted',
                         'wait.resumed', 'operator_call.started', 'operator_call.completed',
                         'invocation.completed', 'invocation.failed', 'invocation.cancelled')))
                    .order_by(e.c.sequence), remaining)
                remaining -= len(rows)
                facts[member['invocation_id']] = (member, rows)
            for member, rows in facts.values():
                sid = member['session_id']
                parent = facts.get(member.get('parent_invocation_id'))
                if parent:
                    owner, parent_rows = parent
                    first = await conn.scalar(select(func.min(e.c.sequence)).where(e.c.invocation_id == member['invocation_id']))
                    terminal = next((row['sequence'] for row in rows if row['event_name'] in
                        ('invocation.completed', 'invocation.failed', 'invocation.cancelled')), None)
                    for row in parent_rows:
                        if row['creation_id'] != member['creation_id']: continue
                        if row['event_name'] == 'child_invocation.planned':
                            links.append(((owner['session_id'], row['sequence']), (sid, first)))
                        elif row['phase'] == 'terminal' and row['unit_index'] == member['unit_index'] and terminal is not None:
                            links.append(((sid, terminal), (owner['session_id'], row['sequence'])))
                for row in rows:
                    origin = row['origin']
                    if row['event_name'] not in ('signal.accepted', 'wait.resumed') or not origin: continue
                    sender = facts.get(origin.get('invocation_id'))
                    if sender is None or sender[0]['session_id'] != origin.get('session_id'): continue
                    for call in sender[1]:
                        if call['call_id'] != origin.get('call_id'): continue
                        if call['event_name'] == 'operator_call.started':
                            links.append(((origin['session_id'], call['sequence']), (sid, row['sequence'])))
                        elif call['event_name'] == 'operator_call.completed':
                            links.append(((sid, row['sequence']), (origin['session_id'], call['sequence'])))
        def reachable(start, end):
            reached = {start['session_id']: start['sequence']}
            for _ in range(len(links) + 1):
                changed = False
                for source, target in links:
                    if source[0] in reached and reached[source[0]] <= source[1] and target[1] < reached.get(target[0], float('inf')):
                        reached[target[0]] = target[1]
                        changed = True
                if end['session_id'] in reached and reached[end['session_id']] <= end['sequence']: return True
                if not changed: return False
            return False
        if reachable(earlier, later): return True
        if reachable(later, earlier): return False
        raise InspectionError('ORDER_UNKNOWN', 'No recorded causal path connects these business positions.')

    async def value(self, member, reference, *, pointer=None):
        pointer_tokens(pointer)
        if 'parts' in reference:
            tokens = pointer_tokens(pointer)
            if tokens:
                if not tokens[0].isdigit() or str(int(tokens[0])) != tokens[0] or int(tokens[0]) >= len(reference['parts']):
                    raise InspectionError('VALUE_UNAVAILABLE', 'The selected input item is unavailable.')
                rest = ''.join('/' + token.replace('~', '~0').replace('/', '~1') for token in tokens[1:])
                return await self.value(member, reference['parts'][int(tokens[0])], pointer=rest)
            result = [await self.value(member, item) for item in reference['parts']]
            ReadBudget(self.config).check_result(result)
            return result
        async with self.queries.view(invocation_id=member['invocation_id']) as (conn, _, _, _):
            await self._validate(conn, member)
            table = meta.tables['runtime_events' if reference['kind'] == 'runtime_event' else 'user_events']
            position = (await conn.execute(select(table.c.sequence).where(table.c.event_id == reference['id'],
                table.c.invocation_id == member['invocation_id']))).scalar_one_or_none()
            upper = member['position']['sequence'] if reference['kind'] == 'runtime_event' else member.get('user_upper_sequence')
            if position is None or upper is None or position > upper:
                raise InspectionError('HISTORY_UNAVAILABLE', 'Business value is outside the fixed observation.')
            budget = ReadBudget(self.config)
            column = table.c.payload_text
            length = func.length(cast(column, LargeBinary)) if conn.dialect.name == 'sqlite' else func.octet_length(column)
            budget.consume(await conn.scalar(select(length).where(table.c.event_id == reference['id'])) or 0)
            text = await conn.scalar(select(column).where(table.c.event_id == reference['id']))
            refs = meta.tables['artifact_refs']
            references = {row.path: row.artifact_hash for row in (await conn.execute(
                select(refs.c.path, refs.c.artifact_hash).where(refs.c.owner_kind == reference['kind'],
                    refs.c.owner_id == reference['id'], (refs.c.path == '/payload') | refs.c.path.startswith('/payload/', autoescape=True))))}
            reader = BoundedReader(self.queries.storage, conn, {'payload': json.loads(text)}, references)
            reader.budget = budget
            try:
                value = await reader.read_pointer(pointer, base=reference['path'])
            except (KeyError, IndexError, TypeError) as error:
                raise InspectionError('VALUE_UNAVAILABLE', 'Selected business field is missing (distinct from JSON null).') from error
            budget.check_result(value)
            return value

    async def at(self, snapshot, position):
        from .inspection import StateInspection
        result = deepcopy(snapshot)
        members = []
        async with self.queries.view(invocation_id=position['invocation_id']) as (conn, _, _, _):
            e = meta.tables['runtime_events']
            target = (await conn.execute(select(e.c.store_order).where(e.c.event_id == position['event_id'],
                e.c.invocation_id == position['invocation_id'], e.c.sequence == position['sequence']))).scalar_one_or_none()
            if target is None or target > snapshot['frontier']:
                raise ValueError('Position is outside this observation.')
            for item in snapshot['members']:
                row = (await conn.execute(select(e.c.event_id, e.c.event_name, e.c.sequence, e.c.store_order).where(
                    e.c.invocation_id == item['invocation_id'], e.c.store_order <= target,
                    e.c.sequence <= (item['position'] or {}).get('sequence', 0)).order_by(e.c.sequence.desc()).limit(1))).mappings().first()
                if row:
                    member = dict(item, position=dict(item['position'], **dict(row)), user_upper_sequence=None)
                    # Activity metadata must describe the selected cut, never today's projection.
                    g = meta.tables['graph_changes']
                    activity = await self._rows(conn, select(g.c.entity_type, g.c.entity_id, g.c.change).where(
                        g.c.invocation_id == item['invocation_id'], g.c.sequence <= row['sequence'],
                        g.c.entity_type.in_(('node', 'wait'))).order_by(g.c.sequence, g.c.change_index),
                        self.config.inspection_scan_events)
                    nodes, waits = {}, {}
                    for change in activity:
                        group = nodes if change['entity_type'] == 'node' else waits
                        group.setdefault(change['entity_id'], {}).update(change['change'])
                    member['active_nodes'] = sum(node.get('status') in ('ready', 'running') for node in nodes.values())
                    member['waits'] = [dict(wait, wait_id=identity) for identity, wait in waits.items() if wait.get('status') == 'waiting']
                    members.append(member)
        for member in members:
            state = await StateInspection(self.queries, self.config).read(member['invocation_id'], sequence=member['position']['sequence'])
            member.update(state['state'])
        result.update(members=members, invocation_id=position['invocation_id'], frontier=target,
            settled=all(member['status'] in ('completed', 'failed', 'cancelled') for member in members))
        root = next((member for member in members if member['invocation_id'] == result['root_invocation_id']), None)
        inactive = not any(member['active_nodes'] for member in members)
        result['at_boundary'] = bool(result['settled'] or root and inactive and
            (root.get('control_state') == 'paused' or any(member['waits'] for member in members)))
        return result
