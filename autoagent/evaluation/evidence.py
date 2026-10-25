"""Lazy, read-only evidence pinned to each member's own Session Event sequence."""

from copy import deepcopy
import json

from .models import EvidenceIncomplete


class Evidence:
    def __init__(self, host, snapshot, event_limit):
        self.__host = host
        self.__snapshot = deepcopy(snapshot)
        self.__limit = event_limit

    @property
    def snapshot(self):
        return deepcopy(self.__snapshot)

    @property
    def invocation_id(self):
        return self.__snapshot['invocation_id']

    def member(self, invocation_id=None):
        iid = invocation_id or self.invocation_id
        for item in self.__snapshot['members']:
            if item['invocation_id'] == iid:
                return deepcopy(item)
        raise ValueError('Invocation is outside this evidence cut.')

    def reference(self, invocation_id=None):
        member = self.member(invocation_id)
        return dict(store_id=self.__snapshot['store_id'], **(member['position'] or
            dict(session_id=member['session_id'], invocation_id=member['invocation_id'], sequence=None)))

    async def value(self, field='output', *, pointer=None, invocation_id=None):
        member = self.member(invocation_id)
        if member['position'] is None:
            raise EvidenceIncomplete('Invocation has no persisted Runtime Event at this cut.')
        result = await self.__host.read_execution_value_at(member['invocation_id'],
            sequence=member['position']['sequence'], field=field, pointer=pointer)
        if not result['available']:
            raise EvidenceIncomplete(f'{field} is unavailable at the selected position.')
        return result['value']

    async def context(self, *, scope, pointer=None, invocation_id=None):
        member = self.member(invocation_id)
        if member['position'] is None:
            raise EvidenceIncomplete('Invocation has not opened at this cut.')
        result = await self.__host.read_execution_context(member['invocation_id'],
            sequence=member['position']['sequence'], scope=scope, pointer=pointer)
        if not result['available']:
            raise EvidenceIncomplete('Selected Context is unavailable at this position.')
        return result['value']

    async def events(self, *, names=(), invocation_id=None):
        """Yield all matching headers through the cut, or explicitly fail on budget exhaustion."""
        member = self.member(invocation_id)
        if member['position'] is None:
            return
        upper = member['position']['sequence']
        cursor, count = None, 0
        while True:
            page = await self.__host.list_execution_events(member['invocation_id'], names=names,
                limit=min(1000, self.__limit + 1), cursor=cursor)
            for item in page.items:
                if item['sequence'] > upper:
                    return
                count += 1
                if count > self.__limit:
                    raise EvidenceIncomplete('Event read budget exceeded; a partial history cannot prove absence.')
                yield item
            if page.next_cursor is None:
                return
            cursor = page.next_cursor

    async def event_value(self, event, *, field='payload', pointer=None):
        member = self.member(event['invocation_id'])
        position = member['position']
        if position is None or event['sequence'] > position['sequence']:
            raise ValueError('Event is outside this evidence cut.')
        header = await self.__host.get_execution_event(member['invocation_id'], event['event_id'])
        if header['sequence'] != event['sequence']:
            raise ValueError('Event identity does not match its claimed position.')
        return await self.__host.get_execution_event(member['invocation_id'], event['event_id'], field=field, pointer=pointer)

    async def wait_request(self, wait):
        member = self.member(wait['invocation_id'])
        selected = next((item for item in member['waits'] if item['wait_id'] == wait['wait_id']), None)
        if selected is None:
            raise ValueError('Wait is outside this evidence cut.')
        reference = selected.get('value_keys', {}).get('request')
        if reference is None:
            raise EvidenceIncomplete('Wait request has no immutable source reference.')
        kind, event_id, path = json.loads(reference)
        if kind != 'runtime_event' or not path or path[0] not in ('payload', 'delta'):
            raise EvidenceIncomplete('Unsupported Wait request source.')
        event = await self.__host.get_execution_event(member['invocation_id'], event_id)
        pointer = ''.join('/' + str(part).replace('~', '~0').replace('/', '~1') for part in path[1:])
        value = await self.event_value(event, field=path[0], pointer=pointer)
        if not value['available']:
            raise EvidenceIncomplete('Wait request is unavailable.')
        return value['value']
