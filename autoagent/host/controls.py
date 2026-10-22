"""High-level Root controls and JSON adapters shared by Server and CLI."""
import json
from pydantic import TypeAdapter
from .models import HostNotFoundError, HostRunConflict

TERMINAL = ('completed','failed','cancelled')


def decode_input(contract, value):
    return contract.validate(value) if contract.annotation is None else TypeAdapter(contract.annotation).validate_json(json.dumps(value,allow_nan=False),strict=True)


class ExecutionControls:
    async def _control_view(self, iid, **options):
        if self._read_only:
            raise HostRunConflict('read_only', 'Execution controls require a running Host.')
        data=await self._persistence.control_metadata(iid,tuple(self._runtime.revisions),**options)
        if data is None:
            raise HostNotFoundError('Unknown execution.')
        root, selected=data['root'],data['selected']
        registered=root['workflow_revision_id'] in self._runtime.revisions
        active=root['status'] not in TERMINAL
        reason='This Root Revision is not registered in this Host.' if not registered else 'This is not the Session\'s latest execution.' if not data['latest'] else 'A required Child Revision is not registered in this Host.' if data['missing'] else None
        base=reason is None
        resident=self._runtime.resident.get(root['session_id'])
        interrupted=active and (resident is None or root['invocation_id'] in self._runtime.needs_recovery)
        recovering=root['invocation_id'] in self._runtime.recovery_tasks
        control=root.get('control_state') or 'active'
        stopping=root.get('pending_outcome') in ('failed','cancelled')
        ir=self._runtime.workflow_irs.get(root['workflow_revision_id'])
        endpoints=[name for name,_ in ir.signal_endpoints] if ir else []
        def action(available,message):
            return dict(available=bool(base and available),reason=reason or (None if available else message))
        actions=dict(
            pause=action(active and not interrupted and not stopping and control=='active','Execution is paused, interrupted, stopping or already finished.'),
            recover=action(not recovering and not stopping and (control=='paused' or interrupted or root['status'] in ('failed','cancelled') or root.get('recovery_pending')),'Execution is already active or has completed successfully.'),
            cancel=action(active,'Execution has already finished.'),
            respond=action(active and not stopping and bool(data['waits']),'There are no pending external responses.'),
            signal=action(root['status'] in ('running','waiting') and not stopping and bool(endpoints),'No declared Signal Endpoint is currently available.'),
            run_again=dict(available=selected['workflow_revision_id'] in self._runtime.revisions,reason=None if selected['workflow_revision_id'] in self._runtime.revisions else 'This exact Revision is not registered in this Host.'))
        return data,dict(invocation_id=iid,root_invocation_id=root['invocation_id'],root_session_id=root['session_id'],
            status=root['status'],control_state=control,recovery_reason='paused' if control=='paused' else 'interrupted' if interrupted else root['status'] if root['status'] in ('failed','cancelled') else None,
            expected_version=root['last_sequence'],recovery_count=root.get('recovery_count') or 0,
            last_recovery_id=root.get('last_recovery_id'),
            response_target=(dict(invocation_id=data['wait']['invocation_id'],wait_id=data['wait']['wait_id'],status=data['wait']['status']) if data['wait'] else None),
            actions=actions,endpoints=endpoints,
            pending_responses=[dict({k:row[k] for k in ('invocation_id','wait_id','occurrence_id','created_at_us','node_id')}, store_order=row['updated_store_order']) for row in data['waits']],
            next_cursor=data['next_cursor'],frontier=data['frontier'])

    async def get_execution_controls(self, invocation_id, *, cursor=None, target_invocation_id=None, wait_id=None):
        async def query():
            _,controls=await self._control_view(invocation_id,cursor=cursor,target_iid=target_invocation_id,wait_id=wait_id)
            return controls
        return await self._perform(query)

    async def _action_spec(self, iid, action, *, target_invocation_id=None, wait_id=None, endpoint=None, source_id=None,request_id=None):
        data,controls=await self._control_view(iid,target_iid=target_invocation_id,wait_id=wait_id,source_id=source_id)
        key=action
        if key not in controls['actions']:
            raise ValueError('Unknown execution action.')
        permission=controls['actions'][key]
        duplicate=action=='recover' and request_id and request_id==data['root'].get('last_recovery_id')
        if not permission['available'] and not duplicate:
            raise HostRunConflict('action_unavailable',permission['reason'])
        contract=None
        if action=='respond':
            wait,target,node=data['wait'],data['target'],data['node']
            if not wait or not target or not node or wait['kind']!='external' or wait['status']!='waiting':
                raise HostRunConflict('wait_unavailable','Choose a pending external Wait in this execution tree.')
            ir=self._runtime.workflow_irs.get(target['workflow_revision_id'])
            if ir is None:
                raise HostRunConflict('workflow_unregistered','The Wait Revision is not registered in this Host.')
            contract=ir.node(node['node_id']).executable.output_contract
        elif action=='signal':
            ir=self._runtime.workflow_irs[data['root']['workflow_revision_id']]
            if endpoint:
                contract=dict(ir.signal_endpoints).get(endpoint)
                if contract is None:
                    raise ValueError('Choose a declared Signal Endpoint.')
        return data,controls,contract

    async def get_execution_action_spec(self, invocation_id, action, **options):
        async def query():
            data,controls,contract=await self._action_spec(invocation_id,action,**options)
            return dict(root_invocation_id=controls['root_invocation_id'],action=action,
                input_schema=json.loads(contract.schema) if contract else None,
                endpoints=controls['endpoints'],next_sequence=data['source_sequence']+1)
        return await self._perform(query)

    async def validate_execution_action(self, invocation_id, action, *, payload=None, target_invocation_id=None,
        wait_id=None, endpoint=None, source_id=None, sequence=None, reason=None, expected_version=None, request_id=None):
        """Reject malformed payloads before asynchronous admission, without changing Runtime state."""
        async def validate():
            data,_,contract=await self._action_spec(invocation_id,action,target_invocation_id=target_invocation_id,
                wait_id=wait_id,endpoint=endpoint,source_id=source_id,request_id=request_id)
            if action=='recover' and expected_version is not None and data['root']['last_sequence'] != expected_version and data['root'].get('last_recovery_id') != request_id and not data['root'].get('recovery_pending'):
                raise HostRunConflict('recovery_version_conflict','Execution changed; refresh it before recovering.')
            if action in ('respond','signal'):
                if contract is None:
                    raise ValueError('Choose a declared action contract.')
                decode_input(contract,payload)
        return await self._perform(validate,payload=payload)

    async def perform_execution_action(self, invocation_id, action, *, payload=None, target_invocation_id=None,
        wait_id=None, endpoint=None, source_id=None, sequence=None, reason=None, expected_version=None, request_id=None):
        """Apply an exact Root-scoped command; callers may observe this awaitable asynchronously."""
        async def apply():
            data,controls,contract=await self._action_spec(invocation_id,action,target_invocation_id=target_invocation_id,
                wait_id=wait_id,endpoint=endpoint,source_id=source_id,request_id=request_id)
            root_id=controls['root_invocation_id']
            async with self._execution(root_id,load=True,touch=True) as ref:
                if action=='cancel':
                    result=await self._runtime.app.acancel(ref,reason)
                elif action=='recover':
                    result=await self._runtime.activate(ref,force=True,request_id=request_id,expected_version=expected_version)
                    # A finished recovery receipt must be visible to the
                    # durable-only trace endpoint used by the UI refresh.
                    await self._persistence.flush()
                elif action=='pause':
                    result=await self._runtime.app.apause(ref)
                elif action=='respond':
                    value=decode_input(contract,payload)
                    await self._runtime.activate(ref)
                    await self._runtime.app.asubmit_resume(ref,wait_id,value)
                    return dict(root_invocation_id=root_id,accepted=True)
                elif action=='signal':
                    if contract is None or not source_id or type(sequence) is not int:
                        raise ValueError('Signal requires an Endpoint, source_id and sequence.')
                    value=contract.to_record(decode_input(contract,payload))
                    await self._runtime.activate(ref)
                    receipt=await self._runtime.app.asignal(ref,endpoint,value,source_id=source_id,sequence=sequence)
                    return dict(root_invocation_id=root_id,accepted=True,message_id=receipt.message_id,accepted_sequence=receipt.accepted_sequence)
                else:
                    raise ValueError('Unknown execution action.')
                return dict(root_invocation_id=root_id,status=result.status,control_state=result.control_state)
        return await self._perform(apply,payload=payload,control=True)
