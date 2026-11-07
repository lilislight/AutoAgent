"""Prepare an immutable Root fork without installing State or invoking user code."""
from dataclasses import dataclass, replace
from uuid import uuid4
from urllib.parse import quote

from ..errors import RuntimeTransitionError
from .checkpoint import SessionCheckpoint
from .events import InvocationForked, ForkStateImported, ForkInitialized, SignalInherited, RuntimeEvent
from .graph_checkpoint import RuntimeGraphCheckpoint
from .operations import StateDelta, StateOperation
from .recovery import prepare_recovery
from .state import RuntimeState, ChildResult, SchedulerState, child_input_digest
from .signals import empty_signals, payload_size
from .values import freeze, thaw


@dataclass(frozen=True, slots=True)
class ForkPlan:
    graph: RuntimeGraphCheckpoint
    events: tuple[RuntimeEvent, ...]
    members: tuple[dict, ...]
    replay_calls: tuple[dict, ...]


def prepare_fork(graph, workflows, *, source=None, now, session_id=None):
    """Use exact historical State; preserve facts and replay only permitted unfinished work."""
    from ..app.app import AutoAgentApp
    states = {s.session_id: s.state for s in graph.sessions}
    root = states[graph.root_session_id].invocation
    if root is None or root.status == 'completed':
        raise RuntimeTransitionError('FORK_NO_WORK', 'Choose an earlier Event with unfinished work.')
    if any(s.invocation is not None and (s.invocation.pending_recovery is not None or
            s.invocation.control_state == 'pausing') for s in states.values()):
        raise RuntimeTransitionError('FORK_TRANSITION_IN_PROGRESS', 'Choose a position after the graph transition completes.')
    for state in states.values():
        if state.invocation is not None and state.invocation.workflow_revision_id not in workflows:
            raise RuntimeTransitionError('FORK_REVISION_UNAVAILABLE', 'Every inherited Workflow Revision must be registered.')
    original = dict(states)
    if root.status in {'failed', 'cancelled'}:
        if source is None:
            raise RuntimeTransitionError('FORK_HISTORY_MISSING', 'Terminal Fork requires canonical execution history.')
        states.update(prepare_recovery(graph.root_session_id, states, source,
            lambda s: workflows[s.invocation.workflow_revision_id], str(uuid4()), now))
    replay = []
    for sid, state in states.items():
        inv = state.invocation
        if inv is None or isinstance(inv, ChildResult) or inv.terminal:
            continue
        error = AutoAgentApp._recovery_error(workflows[inv.workflow_revision_id], state)
        if error:
            raise RuntimeTransitionError('FORK_REPLAY_NOT_ALLOWED', error.message)
        for call in inv.scheduler.operator_calls.values():
            if call.status in {'running', 'lost'} and not call.operator_id.startswith('system_command:'):
                replay.append(dict(session_id=sid, invocation_id=inv.id, call_id=call.id,
                    occurrence_id=call.occurrence_id, operator_id=call.operator_id))
        # No source Operator task runs in the new graph. Preserve its uncertain
        # attempt as lost before recovery starts a distinct Call, never as live.
        pending_calls = {cid:replace(call,status='lost',completed_at_us=now)
            if call.status == 'running' and not call.operator_id.startswith('system_command:') else call
            for cid,call in inv.scheduler.operator_calls.items()}
        states[sid] = replace(state,invocation=replace(inv,scheduler=replace(inv.scheduler,operator_calls=pending_calls)))

    # Expand compacted Children for inspection, retaining the current receipt and
    # ownership frontiers. Historical values are rehydrated only for this cut.
    if source:
        for sid, state in list(states.items()):
            inv = state.invocation
            if inv is None:
                continue
            if isinstance(inv, ChildResult) and sid in source.full_states:
                rich = source.full_states[sid].invocation
                inv = replace(rich, signals=inv.signals, child_plans=inv.child_plans,
                    resume_receipts=inv.resume_receipts, resume_receipt_frontiers=inv.resume_receipt_frontiers)
            if not isinstance(inv, ChildResult):
                nodes = {key:replace(item, output=source.outputs.get(sid, {}).get(key, item.output),
                    execution=source.workspaces.get(sid, {}).get(key,item.execution) if item.status == 'completed' else item.execution)
                    for key,item in inv.scheduler.occurrences.items()}
                calls = dict(inv.scheduler.operator_calls)
                for key,call in source.calls.get(sid, {}).items():
                    if call.occurrence_id in nodes:
                        if key not in calls:
                            calls[key] = call
                        elif calls[key].status == 'completed':
                            calls[key] = replace(calls[key],input=call.input,output=call.output)
                inv = replace(inv, scheduler=replace(inv.scheduler, occurrences=nodes, operator_calls=calls))
            states[sid] = replace(state, invocation=inv)

    sessions = {sid:str(uuid4()) for sid in states}
    if session_id is not None:
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError('session_id must be nonempty.')
        sessions[graph.root_session_id] = session_id
    invocations, calls, messages, creations = {}, {}, {}, {}
    boundaries, accepted = {}, {}
    for state in states.values():
        inv = state.invocation
        if inv is None:
            continue
        invocations.setdefault(inv.id, str(uuid4()))
        count = len(inv.signals['messages'])
        boundaries[state.session.id] = count+2 if count > 1 else 1
        for key in (*inv.scheduler.operator_calls, *inv.scheduler.waits):
            calls.setdefault(key, str(uuid4()))
        for occurrence in inv.scheduler.occurrences.values():
            for key in occurrence.execution.inline_commands:
                calls.setdefault(key, 'inline:' + quote(occurrence.id, safe='') + ':' + uuid4().hex)
        for index,(key,entry) in enumerate(sorted(inv.signals['messages'].items(), key=lambda pair:pair[1]['message']['accepted_sequence'])):
            # Multiple queued Signals get distinct bootstrap positions so FIFO
            # order survives canonical JSON key sorting and restart validation.
            messages.setdefault(key, str(uuid4()))
            accepted[key] = index+2 if count > 1 else 1
        for plan in inv.child_plans.values():
            inline_id = plan.creation_id.removesuffix(':child')
            creations[plan.creation_id] = (calls[inline_id] + ':child' if inline_id in calls
                else f'{invocations[inv.id]}:{plan.parent_occurrence_id}:child')
            for unit in plan.units:
                sessions.setdefault(unit.session_id, str(uuid4()))
                invocations.setdefault(unit.invocation_id, str(uuid4()))
    for state in states.values():
        inv = state.invocation
        if inv is None:
            continue
        for cid in inv.signals['receipts']:
            messages['internal:'+cid] = 'internal:'+calls[cid]
        for key in list(messages):
            if key.startswith('internal:') and key[len('internal:'):] in calls:
                messages[key] = 'internal:'+calls[key[len('internal:'):]]

    versions = {}
    for sid,state in states.items():
        inv = state.invocation
        if inv is None:
            continue
        handle = dict(session_id=sid,invocation_id=inv.id,workflow_id=inv.workflow_id,workflow_revision_id=inv.workflow_revision_id)
        waits, pending = [], [sid]
        while pending:
            current_sid = pending.pop()
            current = states[current_sid].invocation
            if current is None or current.stopping:
                continue
            waits.extend((current_sid,w.id,w.kind)
                for w in current.scheduler.waits.values() if w.status == 'waiting')
            pending.extend(u.session_id for p in current.child_plans.values() for u in p.units if u.session_id in states)
        old = child_input_digest(dict(handle=handle,status=inv.status,pending_outcome=inv.pending_outcome,waits=sorted(waits)))
        new = child_input_digest(dict(handle=dict(handle,session_id=sessions[sid],invocation_id=invocations[inv.id]),
            status=inv.status,pending_outcome=inv.pending_outcome,
            waits=sorted((sessions[s],calls[w],k) for s,w,k in waits)))
        versions[old] = new

    def values(value):
        """Only structurally typed Handles/RuntimeWaits are identities in business JSON."""
        if isinstance(value, list):
            return [values(v) for v in value]
        if not isinstance(value, dict):
            return value
        if set(value) == {'session_id','invocation_id','workflow_id','workflow_revision_id'}:
            if value['session_id'] not in sessions or value['invocation_id'] not in invocations:
                raise RuntimeTransitionError('FORK_EXTERNAL_HANDLE', 'The selected state refers to a Handle outside its Root graph.')
            return dict(value, session_id=sessions[value['session_id']], invocation_id=invocations[value['invocation_id']])
        result = {k:values(v) for k,v in value.items()}
        if set(value) == {'message_id','endpoint','accepted_sequence'}:
            result['message_id'] = messages.get(value['message_id'],value['message_id'])
            result['accepted_sequence'] = accepted.get(value['message_id'],1)
        if set(value) == {'message_id','endpoint','payload','accepted_sequence','sender_id'}:
            result['message_id'] = messages.get(value['message_id'],value['message_id'])
            result['accepted_sequence'] = accepted.get(value['message_id'],1)
            result['sender_id'] = invocations.get(value['sender_id'], value['sender_id'])
        if 'handle' in value and 'wait_id' in value and value['wait_id'] in calls:
            result['wait_id'] = calls[value['wait_id']]
        if 'handle' in value:
            for field in ('after','version'):
                if isinstance(value.get(field),str):
                    result[field] = versions.get(value[field],value[field])
        return result

    business = {'input','output','context','raw_input','mapped_input','aggregate_output',
        'request','response','payload','metrics','error','fault','pending_context_patch','command_inputs','reused_calls'}
    stamps = {'created_at_us','updated_at_us','started_at_us','completed_at_us','ready_at_us','resumed_at_us'}
    requests = {calls[c.id]:values(thaw(c.input)) for s in states.values() if s.invocation is not None
        for c in s.invocation.scheduler.operator_calls.values() if c.id in calls and c.input is not None}
    requests.update({calls[c['id']]:values(thaw(c['input'])) for s in states.values() if s.invocation is not None
        for occurrence in s.invocation.scheduler.occurrences.values()
        for c in occurrence.execution.inline_commands.values()})
    def structural(value, key=''):
        if isinstance(value, list):
            return [structural(v) for v in value]
        if not isinstance(value, dict):
            return value
        result = {}
        for k,v in value.items():
            nk = calls.get(k, messages.get(k, creations.get(k, k)))
            if k in {'session_id','parent_session_id'} and v in sessions:
                result[nk] = sessions[v]
            elif k in {'id','invocation_id','parent_invocation_id','latest_invocation_id','sender_id'} and isinstance(v,str) and v in invocations:
                result[nk] = invocations[v]
            elif k in {'id','call_id','wait_id','parent_call_id'} and isinstance(v,str) and v in calls:
                result[nk] = calls[v]
            elif k in {'id','message_id'} and isinstance(v,str) and v in messages:
                result[nk] = messages[v]
            elif k == 'creation_id':
                result[nk] = creations.get(v,v)
            elif k == 'context_path_revisions':
                result[nk] = {p:active_sequence for p in v}
            elif k == 'sources':
                result[nk] = {name:dict(structural(entry),sequence=entry['sequence']) for name,entry in v.items()}
            elif k == 'external' and isinstance(v,dict):
                result[nk] = dict(structural(v),sequence=v['sequence'])
            elif k in {'registered_sequence','started_sequence','accepted_sequence','caller_sequence','sequence'} and isinstance(v,int):
                result[nk] = accepted.get(value.get('message_id'),1) if k == 'accepted_sequence' else active_sequence if v else 0
            elif k in stamps and v is not None:
                result[nk] = now
            elif k in business:
                result[nk] = values(v)
                if k == 'reused_calls':
                    for cached in result[nk].values():
                        cached['id'] = calls.get(cached['id'],cached['id'])
            else:
                result[nk] = structural(v,k)
        return result

    events, members, snapshots = [], [], []
    # The source graph is parent ordered, so durable admission can seed every
    # Child relationship before its initialization Event is projected.
    pending = [graph.root_session_id]
    while pending:
        sid = pending.pop(0)
        state = states[sid]
        inv = state.invocation
        if inv is None:
            continue
        active_sequence = boundaries[sid]
        record = structural(state.to_record())
        record['session']['id'] = sessions[sid]
        record['sequence'], record['last_event_id'] = active_sequence, str(uuid4())
        if record['invocation'].get('control_state') in {'paused','pausing'}:
            record['invocation']['control_state'] = 'active'
        # Frontiers use Session IDs as mapping keys, unlike business JSON.
        for field in ('frontiers',):
            frontier = record['invocation']['signals'][field]
            record['invocation']['signals'][field] = {sessions.get(k,k):dict(v,sequence=boundaries.get(k,1)) for k,v in frontier.items()}
        rf = record['invocation'].get('resume_receipt_frontiers', {})
        record['invocation']['resume_receipt_frontiers'] = {sessions.get(k,k):dict(v,sequence=boundaries.get(k,1)) for k,v in rf.items()}
        mailbox = record['invocation']['signals']
        for entry in mailbox['messages'].values():
            entry['size_bytes'] = payload_size(entry['message']['payload'])
        mailbox['bytes'] = sum(e['size_bytes'] for e in mailbox['messages'].values())
        for proof in mailbox['receipts'].values():
            request = requests.get(proof['origin']['call_id'])
            if isinstance(request,dict):
                proof['origin']['digest'] = child_input_digest({'endpoint':request['endpoint'],'payload':request['payload']})
        for cid,receipt in record['invocation'].get('resume_receipts',{}).items():
            request = requests.get(cid)
            if isinstance(request,dict):
                receipt['response_digest'] = child_input_digest(request.get('response'))
        for plan in record['invocation']['child_plans'].values():
            for unit in plan['units']:
                pending.extend(old for old,new in sessions.items() if new == unit['session_id'] and old in states)
        branch = RuntimeState.from_record(record)
        payload = InvocationForked(inv.workflow_id, inv.workflow_revision_id, inv.entry_node_id,
            sid, original[sid].invocation.id, original[sid].sequence, original[sid].last_event_id,
            root.id, invocations[root.id],
            {calls[cid]:cid for cid,c in inv.scheduler.operator_calls.items() if cid in calls and c.status == 'completed'},
            {oid:item.status for oid,item in original[sid].invocation.scheduler.occurrences.items()}, active_sequence)
        def delta(candidate):
            return StateDelta((StateOperation('replace',('session',),candidate.session),
                StateOperation('replace',('invocation',),candidate.invocation)))
        if active_sequence > 1:
            initial = replace(branch,sequence=1,last_event_id=str(uuid4()),
                session=replace(branch.session,context_path_revisions={p:1 for p in branch.session.context_path_revisions}),
                invocation=replace(branch.invocation,status='running',pending_outcome=None,output=None,error=None,
                    context_path_revisions={p:1 for p in branch.invocation.context_path_revisions},
                    scheduler=SchedulerState(),child_plans={},signals=empty_signals(),resume_receipts=freeze({}),resume_receipt_frontiers=freeze({})))
            initial = RuntimeState.from_record(initial.to_record())
            events.append(RuntimeEvent(sessions[sid],1,payload,initial.invocation.id,delta(initial),now,initial.last_event_id))
            from .reducer import StateReducer
            current = initial
            for old_id,entry in sorted(inv.signals['messages'].items(),key=lambda pair:pair[1]['message']['accepted_sequence']):
                new_id = messages[old_id]
                seq = accepted[old_id]
                item = mailbox['messages'][new_id]
                signal = RuntimeEvent(sessions[sid],seq,SignalInherited(new_id,old_id,item['message']['endpoint']),branch.invocation.id,
                    StateDelta((StateOperation('add',('invocation','signals','messages',new_id),freeze(item)),
                        StateOperation('replace',('invocation','signals','bytes'),current.invocation.signals['bytes']+item['size_bytes']))),now)
                events.append(signal)
                current = StateReducer().apply(current,signal)
            events.append(RuntimeEvent(sessions[sid],active_sequence,ForkStateImported(**{f:getattr(payload,f) for f in payload.__dataclass_fields__ if f != 'kind'}),
                branch.invocation.id,delta(branch),now,record['last_event_id']))
        else:
            events.append(RuntimeEvent(sessions[sid], 1, payload, branch.invocation.id,
            StateDelta((StateOperation('replace', ('session',), branch.session),
                StateOperation('replace', ('invocation',), branch.invocation))), now, record['last_event_id']))
        snapshots.append(SessionCheckpoint.from_state(branch))
        members.append(dict(source_session_id=sid, source_invocation_id=original[sid].invocation.id,
            source_sequence=original[sid].sequence, session_id=sessions[sid], invocation_id=branch.invocation.id))
    if len(snapshots) > 1:
        # A Root needs a position after all Child admissions, even when no
        # business Event follows (for example, a family entirely in Wait).
        root_state = snapshots[0].state
        boundary = RuntimeEvent(root_state.session.id,root_state.sequence+1,
            ForkInitialized(root_state.invocation.id,len(snapshots)),root_state.invocation.id,occurred_at_us=now)
        events.append(boundary)
        snapshots[0] = SessionCheckpoint.from_state(replace(root_state,sequence=boundary.sequence,last_event_id=boundary.id))
    result = RuntimeGraphCheckpoint(sessions[graph.root_session_id], tuple(snapshots))
    return ForkPlan(result, tuple(events), tuple(members), tuple(replay))
