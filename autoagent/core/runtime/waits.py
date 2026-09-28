"""Validation and atomic completion of durable Command waits."""
from collections.abc import Mapping
from dataclasses import replace
from .operations import StateDelta, StateOperation
from .values import freeze, thaw
from ..commands.models import RuntimeHandle
from ..errors import RuntimeTransitionError


def validate_condition(kind, condition):
    if not isinstance(condition, Mapping):
        raise ValueError('Wait condition must be a mapping.')
    if kind == 'signal':
        if set(condition) != {'endpoint', 'limit'} or not isinstance(condition['endpoint'], str) or not condition['endpoint'].strip() or type(condition['limit']) is not int or condition['limit'] < 1:
            raise ValueError('Invalid Signal wait condition.')
    elif kind == 'timer':
        if set(condition) != {'deadline_at_us'} or type(condition['deadline_at_us']) is not int or condition['deadline_at_us'] < 0:
            raise ValueError('Invalid Timer wait condition.')
    elif kind == 'child':
        if set(condition) != {'handle', 'after'} or (condition['after'] is not None and not isinstance(condition['after'], str)):
            raise ValueError('Invalid Child wait condition.')
        RuntimeHandle.model_validate(thaw(condition['handle']))
    elif kind == 'any':
        if set(condition) != {'cases', 'select'} or type(condition['select']) is not bool or not condition['cases']:
            raise ValueError('Invalid composite wait condition.')
        names = set()
        for case in condition['cases']:
            if not isinstance(case, Mapping) or set(case) != {'name', 'kind', 'condition'} or not isinstance(case['name'], str) or not case['name'] or case['name'] in names or case['kind'] not in {'signal', 'timer', 'child'}:
                raise ValueError('Invalid wait case.')
            names.add(case['name'])
            validate_condition(case['kind'], case['condition'])
    else:
        raise ValueError('Unsupported internal wait kind.')


def awakening_delta(planner, state, payload, now, sid, iid, index):
    from .events import OperatorCallCompleted
    inv = state.invocation
    wait = inv.scheduler.waits.get(payload.wait_id)
    call = inv.scheduler.operator_calls.get(payload.wait_id)
    if inv.stopping or wait is None or wait.status != 'waiting' or wait.kind == 'external' or call is None or call.status != 'running':
        raise RuntimeTransitionError('COMMAND_NOT_WAITING', 'Command awakening requires an active internal wait.')
    condition, kind = wait.request, wait.kind
    output = payload.output
    if kind == 'any':
        if condition['select']:
            case = next((c for c in condition['cases'] if c['name'] == output['case']), None)
            if case is None:
                raise ValueError('Unknown Select winner.')
            output = output['value']
        else:
            case = next((c for c in condition['cases'] if c['condition']['handle'] == output['handle']), None)
            if case is None:
                raise ValueError('Unknown AwaitAny winner.')
        kind, condition = case['kind'], case['condition']
    if kind == 'signal':
        selected = tuple(mid for mid, entry in sorted(inv.signals['messages'].items(), key=lambda item: item[1]['message']['accepted_sequence']) if entry['message']['endpoint'] == condition['endpoint'])[:condition['limit']]
        if not selected or payload.message_ids != selected or output != freeze({'messages': [inv.signals['messages'][mid]['message'] for mid in selected]}):
            raise ValueError('Signal awakening must consume the next matching messages.')
    elif payload.message_ids:
        raise ValueError('Only Signal waits may consume messages.')
    elif kind == 'timer':
        if output != {'deadline_at_us': condition['deadline_at_us']}:
            raise ValueError('Timer result does not match its deadline.')
    elif kind == 'child':
        if not isinstance(output, Mapping) or output.get('handle') != condition['handle'] or output.get('status') not in {'waiting', 'settling', 'completed', 'failed', 'cancelled'}:
            raise ValueError('Child result does not match its Handle.')
    delta = planner.plan(state, OperatorCallCompleted(call.id, payload.output), occurred_at_us=now,
        session_id=sid, invocation_id=iid, _execution_index=index)
    ops = list(delta.operations)
    def put(path, value, op='replace'):
        ops.append(StateOperation._from_owned(op, ('invocation', *path), value))
    put(('scheduler', 'waits', wait.id), replace(wait, status='resumed', resumed_at_us=now))
    occ = inv.scheduler.occurrences[wait.occurrence_id]
    put(('scheduler', 'occurrences', occ.id), replace(occ, status='running', ready_at_us=now))
    put(('status',), 'running')
    size = inv.signals['bytes']
    for mid in payload.message_ids:
        size -= inv.signals['messages'][mid]['size_bytes']
        put(('signals', 'messages', mid), None, 'remove')
    if payload.message_ids:
        put(('signals', 'bytes'), size)
    return StateDelta(tuple(ops))


def validate_wait_call(wait, calls):
    """Internal waiting records must belong to the corresponding recorded Command."""
    call = calls.get(wait.id)
    allowed = {'signal': {'system_command:await_signal'}, 'timer': {'system_command:timer'},
        'child': {'system_command:await'}, 'any': {'system_command:await_any', 'system_command:select'}}
    if call is None or call.occurrence_id != wait.occurrence_id or call.operator_id not in allowed[wait.kind]:
        raise ValueError('Internal Wait must reference its Command Call.')
    if wait.status == 'waiting' and call.status != 'running':
        raise ValueError('Waiting Command requires a running Call.')
    if wait.status == 'resumed' and call.status != 'completed':
        raise ValueError('Awakened Command requires a completed Call.')
    if wait.status != 'waiting':
        return
    condition = wait.request
    if condition is None:
        raise ValueError('Waiting Command requires its condition.')
    if wait.registered_sequence < 1:
        raise ValueError('Waiting Command requires its registration sequence.')
    if wait.kind == 'signal' and condition != call.input:
        raise ValueError('Signal wait condition does not match its Call.')
    if wait.kind == 'timer' and condition['deadline_at_us'] != call.input['deadline_at_us']:
        raise ValueError('Timer deadline does not match its Call.')
    if wait.kind == 'any':
        if condition['select'] != (call.operator_id == 'system_command:select'):
            raise ValueError('Composite wait kind does not match its Call.')
        if condition['select'] and condition != call.input:
            raise ValueError('Select conditions do not match their recorded Call.')
        if not condition['select'] and [thaw(c['condition']['handle']) for c in condition['cases'] if c['kind'] == 'child'] != thaw(call.input['handles']):
            raise ValueError('AwaitAny handles do not match their recorded Call.')


def validate_wait_graph(states, owners):
    for sid, state in states.items():
        if state.invocation is None:
            continue
        for wait in state.invocation.scheduler.waits.values():
            if wait.status != 'waiting' or wait.kind == 'external':
                continue
            cases = wait.request['cases'] if wait.kind == 'any' else ({'kind': wait.kind, 'condition': wait.request},)
            for case in cases:
                if case['kind'] != 'child':
                    continue
                handle = case['condition']['handle']
                child_state = states.get(handle['session_id'])
                child = child_state.invocation if child_state else None
                if owners.get(handle['session_id']) != sid or child is None or (child.id, child.workflow_id, child.workflow_revision_id) != (handle['invocation_id'], handle['workflow_id'], handle['workflow_revision_id']):
                    raise ValueError('Child Wait must reference its exact directly owned Child.')
