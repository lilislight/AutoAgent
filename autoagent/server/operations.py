"""Bounded operation receipts; HTTP disconnects never cancel a Host command."""
import asyncio
import hashlib
import json
import time
from collections import OrderedDict
from autoagent.host import HostCapacityError, HostNotFoundError, HostRunConflict


class ExecutionOperations:
    def __init__(self,host,settings):
        self.host,self.settings=host,settings
        self.entries=OrderedDict()
        self.tasks=set()
        self.closed=False

    def expire(self, *, reserve=False):
        now=time.monotonic()
        for key,entry in list(self.entries.items()):
            if entry['status']!='running' and now-entry['_finished']>self.settings.operation_ttl_seconds:
                self.entries.pop(key)
        for key,entry in list(self.entries.items()):
            if not reserve or len(self.entries)<self.settings.operation_receipts:
                break
            if entry['status']!='running':
                self.entries.pop(key)

    @staticmethod
    def public(entry):
        return {key:value for key,value in entry.items() if not key.startswith('_')}

    async def submit(self,body):
        self.expire()
        fingerprint=hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
        existing=self.entries.get(body['request_id'])
        if existing:
            if existing['_fingerprint']!=fingerprint:
                raise HostRunConflict('request_conflict','This request_id was already used with different content.')
            return self.public(existing)
        if self.closed or len(self.tasks)>=self.settings.operation_concurrency:
            raise HostCapacityError('Execution operation capacity is exhausted.')
        options={key:value for key,value in body.items() if key not in ('request_id','invocation_id','action')}
        await self.host.validate_execution_action(body['invocation_id'],body['action'],**options)
        # Preflight awaited: another submit may have acquired this key or slot.
        if body['request_id'] in self.entries:
            existing=self.entries[body['request_id']]
            if existing['_fingerprint']!=fingerprint:
                raise HostRunConflict('request_conflict','This request_id was already used with different content.')
            return self.public(existing)
        if self.closed or len(self.tasks)>=self.settings.operation_concurrency:
            raise HostCapacityError('Execution operation capacity is exhausted.')
        self.expire(reserve=True)
        entry=dict(operation_id=body['request_id'],invocation_id=body['invocation_id'],action=body['action'],status='running',
            result=None,error=None,_fingerprint=fingerprint,_finished=0)
        self.entries[body['request_id']]=entry
        task=asyncio.create_task(self.run(entry,body))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return self.public(entry)

    async def run(self,entry,body):
        try:
            options={key:value for key,value in body.items() if key not in ('request_id','invocation_id','action')}
            entry['result']=await self.host.perform_execution_action(body['invocation_id'],body['action'],**options)
            entry['status']='succeeded'
        except asyncio.CancelledError:
            entry['status']='unobserved'
            entry['error']=dict(code='observer_closed',message='Operation observer closed. Check execution state before retrying.')
            raise
        except Exception as error:
            entry['status']='failed'
            entry['error']=dict(code=getattr(error,'code','operation_failed'),message=str(error)[:8192])
        finally:
            entry['_finished']=time.monotonic()

    def get(self,operation_id):
        self.expire()
        entry=self.entries.get(operation_id)
        if entry is None:
            raise HostNotFoundError('Operation receipt is unavailable or expired. Check execution state before retrying.')
        return self.public(entry)

    async def close(self):
        self.closed=True
        tasks=tuple(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        self.entries.clear()
