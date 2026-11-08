"""Unified read-only business execution views, independent of Evaluation.

History adapters, pagination and source references stay inside Host. A new
observation never mutates an earlier view. Values are JSON and read on demand.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass


_reads = ContextVar('autoagent_business_reads', default=None)


@contextmanager
def observe_reads():
    """Collect bounded source references for one business rule, never payloads."""
    reads = []
    token = _reads.set(reads)
    try:
        yield reads
    finally:
        _reads.reset(token)


class ViewUnavailable(RuntimeError):
    """A query cannot establish a business fact; this is never evidence of absence."""
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class Position:
    """An opaque business milestone, such as Node completion or approval request."""
    _store_id: str
    _record: dict

    @property
    def label(self):
        return self._record['label']

    @property
    def invocation_id(self):
        return self._record['invocation_id']


class ExecutionView:
    def __init__(self, host, snapshot, *, read_limit=100000, invocation_id=None):
        self.__host = host
        self.__snapshot = deepcopy(snapshot)
        self.__iid = invocation_id or snapshot['invocation_id']
        self.__limit = read_limit
        self.__member = next((item for item in self.__snapshot['members'] if item['invocation_id'] == self.__iid), None)
        if self.__member is None:
            raise ValueError('Invocation is outside this observation.')

    @property
    def invocation_id(self):
        return self.__iid

    @property
    def session_id(self):
        return self.__member['session_id']

    @property
    def workflow_id(self):
        return self.__member['workflow_id']

    @property
    def status(self):
        self._record('execution.status')
        return self.__member['status']

    @property
    def control_state(self):
        self._record('execution.control_state')
        return self.__member.get('control_state') or 'active'

    @property
    def settled(self):
        return bool(self.__snapshot['settled'])

    @property
    def root_invocation_id(self):
        return self.__snapshot['root_invocation_id']

    @property
    def parent_invocation_id(self):
        return self.__member.get('parent_invocation_id')

    def to_record(self):
        """Serializable observation for reports; callers cannot change the live view."""
        return deepcopy(self.__snapshot | {'invocation_id': self.__iid})

    def _record(self, operation, **details):
        reference = dict(store_id=self.__snapshot['store_id'], invocation_id=self.__iid,
            session_id=self.session_id, operation=operation, **details)
        if self.__member.get('position'):
            reference['position'] = deepcopy(self.__member['position'])
        reads = _reads.get()
        if reads is not None and len(reads) < 64 and reference not in reads:
            reads.append(reference)
        return reference

    def node(self, node_id):
        return NodeSelection(self, node_id)

    def route(self, route_id):
        return RouteSelection(self, route_id)

    def children(self, *, workflow_id=None, recursive=False):
        members = self.__snapshot['members']
        ids = {self.__iid}
        if recursive:
            while True:
                expanded = ids | {item['invocation_id'] for item in members if item.get('parent_invocation_id') in ids}
                if expanded == ids:
                    break
                ids = expanded
        selected = [item for item in members if item['invocation_id'] != self.__iid and
            (item['invocation_id'] in ids if recursive else item.get('parent_invocation_id') == self.__iid)
            and (workflow_id is None or item['workflow_id'] == workflow_id)]
        self._record('execution.children', workflow_id=workflow_id, recursive=recursive)
        return tuple(ExecutionView(self.__host, self.__snapshot, read_limit=self.__limit,
            invocation_id=item['invocation_id']) for item in selected)

    async def _records(self, kind, **options):
        self._record(kind, **options)
        result = await self.__host._business_query('business_history', self.__member, kind, limit=self.__limit, **options)
        return result

    async def _value(self, reference, *, pointer=None):
        if reference is None:
            raise ViewUnavailable('VALUE_UNAVAILABLE', 'Business value is not available at this observation.')
        self._record('value', source=reference, pointer=pointer or '')
        result = await self.__host._business_query('business_value', self.__member, reference, pointer=pointer)
        return result

    async def output(self, *, pointer=None):
        return await self._execution_value('output', pointer=pointer)

    async def error(self, *, pointer=None):
        return await self._execution_value('error', pointer=pointer)

    async def _execution_value(self, field, *, pointer=None):
        if not self.__member.get('position'):
            raise ViewUnavailable('VALUE_UNAVAILABLE', 'Execution has not opened at this observation.')
        result = await self._history_value(field=field, pointer=pointer)
        if not result['available']:
            raise ViewUnavailable('VALUE_UNAVAILABLE', 'Selected business result is missing.')
        return result['value']

    async def context(self, *, scope='session', pointer=None):
        if not self.__member.get('position'):
            raise ViewUnavailable('VALUE_UNAVAILABLE', 'Execution has not opened at this observation.')
        result = await self._history_value(scope=scope, pointer=pointer)
        if not result['available']:
            raise ViewUnavailable('VALUE_UNAVAILABLE', 'Selected Context field is missing.')
        return result['value']

    async def _history_value(self, *, scope=None, field=None, pointer=None):
        from .models import HostRunConflict
        self._record('context' if scope else 'execution.' + field, scope=scope, pointer=pointer or '')
        try:
            options = dict(sequence=self.__member['position']['sequence'], pointer=pointer)
            if scope is not None:
                return await self.__host.read_execution_context(self.__iid, scope=scope, **options)
            return await self.__host.read_execution_value_at(self.__iid, field=field, **options)
        except HostRunConflict as error:
            raise ViewUnavailable(error.code, str(error)) from error

    async def transitions(self):
        return tuple(Transition(self, row) for row in await self._records('transitions'))

    async def interactions(self, *, node_id=None, pending=True, family=False):
        views = (self, *self.children(recursive=True)) if family else (self,)
        result = []
        valid = node_id is None
        for view in views:
            if node_id is not None:
                try:
                    await view.node(node_id).all()
                    valid = True
                except ValueError:
                    continue
            for row in await view._records('interactions'):
                if (not pending or row.get('status') == 'waiting') and (node_id is None or row.get('node_id') == node_id):
                    result.append(Interaction(view, row))
        if not valid:
            raise ValueError(f'Unknown Node in the selected execution family: {node_id}')
        return tuple(result)

    async def messages(self, *, kind=None):
        return tuple(BusinessMessage(self, row) for row in await self._records('messages', selector=kind))

    async def at(self, position):
        if not isinstance(position, Position) or position._store_id != self.__snapshot['store_id']:
            raise ValueError('Select a business position from this store.')
        member = next((item for item in self.__snapshot['members'] if item['invocation_id'] == position.invocation_id), None)
        if member is None or not member.get('position') or position._record['sequence'] > member['position']['sequence']:
            raise ValueError('Position is outside this observation.')
        snapshot = await self.__host._business_query('business_position', self.__snapshot, position._record)
        return ExecutionView(self.__host, snapshot, read_limit=self.__limit, invocation_id=position.invocation_id)

    def _position(self, row):
        return Position(self.__snapshot['store_id'], deepcopy(row))

    async def precedes(self, earlier, later):
        """A stable order is known within a Session. Unproven cross-Session order is explicit."""
        for position in (earlier, later):
            if not isinstance(position, Position) or position._store_id != self.__snapshot['store_id']:
                raise ValueError('Use positions from this execution store.')
            covered = any(item['session_id'] == position._record['session_id'] and item.get('position')
                and item['position']['sequence'] >= position._record['sequence'] for item in self.__snapshot['members'])
            if not covered or position._record['store_order'] > self.__snapshot['frontier']:
                raise ValueError('Position is outside this observation.')
        if earlier._record['session_id'] == later._record['session_id']:
            self._record('precedes', earlier=earlier._record, later=later._record)
            return earlier._record['sequence'] < later._record['sequence']
        self._record('precedes', earlier=earlier._record, later=later._record)
        return await self.__host._business_query('business_order', self.__snapshot,
            earlier._record, later._record, limit=self.__limit)


class NodeSelection:
    def __init__(self, view, node_id):
        self.view, self.node_id = view, node_id

    async def all(self):
        return tuple(NodeRun(self.view, row) for row in await self.view._records('nodes', selector=self.node_id))

    async def one(self):
        rows = await self.all()
        if len(rows) != 1:
            raise ValueError(f'Node {self.node_id} has {len(rows)} occurrences; select first(), last(), all() or by_id().')
        return rows[0]

    async def first(self):
        rows = await self.all()
        if not rows:
            raise ViewUnavailable('NO_OCCURRENCE', 'Node has no occurrence at this observation.')
        return rows[0]

    async def last(self):
        rows = await self.all()
        if not rows:
            raise ViewUnavailable('NO_OCCURRENCE', 'Node has no occurrence at this observation.')
        return rows[-1]

    async def by_id(self, occurrence_id):
        for row in await self.all():
            if row.occurrence_id == occurrence_id:
                return row
        raise ViewUnavailable('NO_OCCURRENCE', 'Selected occurrence is not in this observation.')

    async def was_executed(self):
        return any(row.started for row in await self.all())


class NodeRun:
    def __init__(self, view, row):
        self._view, self._row = view, deepcopy(row)

    @property
    def node_id(self): return self._row['node_id']
    @property
    def occurrence_id(self): return self._row['occurrence_id']
    @property
    def status(self): return self._row['status']
    @property
    def started(self): return self._row['started']
    @property
    def recovery_attempts(self): return self._row['recovery_attempts']

    def position(self, state):
        row = self._row['positions'].get(state)
        if row is None:
            raise ViewUnavailable('POSITION_UNAVAILABLE', f'Node has not reached {state}.')
        return self._view._position(row)

    @property
    def completed(self): return self.position('completed')
    @property
    def waiting(self): return self.position('waiting')

    async def _value(self, field, pointer):
        if field == 'output' and self.status != 'completed':
            raise ViewUnavailable('VALUE_UNAVAILABLE', 'Node has no committed output at this observation.')
        refs = await self._view._records('values', occurrence_id=self.occurrence_id)
        return await self._view._value(refs.get(field), pointer=pointer)

    async def input(self, *, pointer=None): return await self._value('input', pointer)
    async def output(self, *, pointer=None): return await self._value('output', pointer)
    async def aggregate(self, *, pointer=None): return await self._value('aggregate', pointer)
    async def error(self, *, pointer=None): return await self._value('error', pointer)
    async def items(self):
        return tuple(MapItem(self._view, row) for row in await self._view._records('items', occurrence_id=self.occurrence_id))
    async def transitions(self):
        return tuple(Transition(self._view, row) for row in await self._view._records('transitions', occurrence_id=self.occurrence_id))


class MapItem:
    def __init__(self, view, row): self._view, self._row = view, deepcopy(row)
    @property
    def index(self): return self._row['unit_index']
    @property
    def status(self): return self._row['status']
    @property
    def attempts(self): return tuple(MapItem(self._view, row) for row in self._row.get('attempts', ()))
    async def input(self, *, pointer=None): return await self._view._value(self._row.get('input'), pointer=pointer)
    async def output(self, *, pointer=None): return await self._view._value(self._row.get('output'), pointer=pointer)
    async def error(self, *, pointer=None): return await self._view._value(self._row.get('error'), pointer=pointer)


class RouteSelection:
    def __init__(self, view, route_id): self.view, self.route_id = view, route_id
    async def decisions(self): return tuple(RouteDecision(self.view, row) for row in await self.view._records('routes', selector=self.route_id))
    async def was_taken(self): return any(row.status == 'taken' for row in await self.decisions())
    async def state(self):
        rows = await self.decisions()
        if not rows:
            reached = await self.view._records('route_source', selector=self.route_id)
            return 'undecided' if reached else 'not_reached'
        if len(rows) != 1: raise ValueError('Route was decided repeatedly; select an explicit decision from decisions().')
        return rows[0].status


class RouteDecision:
    def __init__(self, view, row): self._view, self._row = view, deepcopy(row)
    @property
    def status(self): return self._row['status']
    @property
    def source_occurrence_id(self): return self._row['source_occurrence_id']
    @property
    def position(self): return self._view._position(self._row['position'])


class Transition:
    def __init__(self, view, row): self._view, self._row = view, deepcopy(row)
    @property
    def field(self): return self._row['field']
    @property
    def value(self): return self._row['value']
    @property
    def position(self): return self._view._position(self._row['position'])


class Interaction:
    def __init__(self, view, row): self.execution, self._row = view, deepcopy(row)
    @property
    def id(self): return self._row['wait_id']
    @property
    def node_id(self): return self._row['node_id']
    @property
    def kind(self): return self._row['kind']
    @property
    def status(self): return self._row['status']
    @property
    def requested(self): return self.execution._position(self._row['positions']['waiting'])
    async def request(self, *, pointer=None): return await self.execution._value(self._row.get('request'), pointer=pointer)
    async def response(self, *, pointer=None): return await self.execution._value(self._row.get('response'), pointer=pointer)


class BusinessMessage:
    def __init__(self, view, row): self._view, self._row = view, deepcopy(row)
    @property
    def kind(self): return self._row['kind']
    async def body(self, *, pointer=None): return await self._view._value(self._row['body'], pointer=pointer)
