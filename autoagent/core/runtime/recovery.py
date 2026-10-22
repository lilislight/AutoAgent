"""Reconstruct released execution inputs from canonical history, without rerunning code."""
from __future__ import annotations

from dataclasses import dataclass, replace
from types import MappingProxyType
from collections.abc import Iterable, Mapping

from ..errors import RuntimeTransitionError
from .events import RuntimeEvent, NodeCompleted
from .reducer import StateReducer
from .state import (RuntimeState, ChildResult, NodeExecutionState, OperatorCallState, ChildInvocationPlan, child_units, runtime_mapping)
from .scheduling import occurrence_key
from .values import freeze, DurableValue


@dataclass(frozen=True, slots=True)
class RecoverySource:
    """Caller-owned canonical history. Core never keeps this history after admission."""
    states: Mapping[str, RuntimeState]
    full_states: Mapping[str, RuntimeState]
    workspaces: Mapping[str, Mapping[str, NodeExecutionState]]
    calls: Mapping[str, Mapping[str, OperatorCallState]]
    outputs: Mapping[str, Mapping[str, DurableValue]]
    plans: Mapping[str, Mapping[str, ChildInvocationPlan]]
    resolutions: Mapping

    @classmethod
    def from_events(cls, events: Iterable[RuntimeEvent], *, initial_states=None) -> RecoverySource:
        """Consume complete Session prefixes without retaining the Event collection itself."""
        reducer = StateReducer()
        states, full, work, calls, outputs, plans, resolutions = dict(initial_states or {}), {}, {}, {}, {}, {}, {}
        for sid,state in states.items():
            inv = state.invocation
            if inv is not None and not isinstance(inv, ChildResult):
                full[sid] = state
                work[sid] = {key:item.execution for key,item in inv.scheduler.occurrences.items()}
                calls[sid] = dict(inv.scheduler.operator_calls)
                outputs[sid] = {key:item.output for key,item in inv.scheduler.occurrences.items() if item.output is not None}
                plans[sid] = dict(inv.child_plans)
                resolutions[sid] = dict(inv.scheduler.resolutions)
        for event in events:
            if not isinstance(event, RuntimeEvent):
                raise TypeError('RecoverySource requires Runtime Events.')
            sid = event.session_id
            before = states.get(sid, RuntimeState())
            after = reducer.apply(before, event)
            inv = after.invocation
            if event.event_name == 'invocation.started':
                work[sid], calls[sid], outputs[sid], plans[sid], resolutions[sid] = {}, {}, {}, {}, {}
            if inv is not None and not isinstance(inv, ChildResult):
                full[sid] = after
                touched = {name:set() for name in ('occurrences','operator_calls','child_plans','resolutions')}
                whole = False
                for op in event.delta.operations if event.delta else ():
                    path = op.path
                    if path in (('invocation',), ('invocation','scheduler')):
                        whole = True
                        break
                    if len(path) >= 3 and path[:2] == ('invocation','child_plans'):
                        touched['child_plans'].add(path[2])
                    elif len(path) >= 4 and path[:2] == ('invocation','scheduler') and path[2] in touched:
                        touched[path[2]].add(path[3])
                for oid in inv.scheduler.occurrences if whole else touched['occurrences']:
                    item = inv.scheduler.occurrences.get(oid)
                    if item is not None and item.execution != NodeExecutionState():
                        work.setdefault(sid, {})[oid] = item.execution
                    if item is not None and item.status == 'completed' and item.output is not None:
                        outputs.setdefault(sid, {})[oid] = item.output
                for cid in inv.scheduler.operator_calls if whole else touched['operator_calls']:
                    item = inv.scheduler.operator_calls.get(cid)
                    if item is not None and (item.input is not None or item.output is not None or
                            event.event_name == 'operator_call.completed' and event.payload.call_id == cid):
                        calls.setdefault(sid, {})[cid] = item
                for key in inv.child_plans if whole else touched['child_plans']:
                    plan = inv.child_plans.get(key)
                    if plan is None:
                        continue
                    # Child planning records every input before any unit may
                    # release it. Keep that immutable original once; walking
                    # every unit after each phase change would be quadratic.
                    plans.setdefault(sid, {}).setdefault(key, plan)
                for key in inv.scheduler.resolutions if whole else touched['resolutions']:
                    if key in inv.scheduler.resolutions:
                        resolutions.setdefault(sid, {})[key] = inv.scheduler.resolutions[key]
            if isinstance(event.payload, NodeCompleted):
                outputs.setdefault(sid, {})[event.payload.occurrence_id] = event.payload.output
            states[sid] = after
        return cls(MappingProxyType(states), MappingProxyType(full), *(
            MappingProxyType({sid: MappingProxyType(items) for sid, items in value.items()})
            for value in (work, calls, outputs, plans, resolutions)))


def prepare_recovery(root, current, source, workflow_for_state, request_id, now):
    """Create one Root-owned plan; all execution identities and results remain auditable."""
    if not isinstance(source, RecoverySource):
        raise TypeError('Terminal recovery requires a RecoverySource.')
    pending = [root]
    selected = {}
    while pending:
        sid = pending.pop()
        state = current[sid]
        inv = state.invocation
        original = source.states.get(sid)
        if original is None or original.sequence != state.sequence or original.last_event_id != state.last_event_id:
            raise RuntimeTransitionError('RECOVERY_SOURCE_STALE', 'Recovery history does not match the acknowledged graph.')
        if inv is None or inv.status == 'completed':
            continue
        if inv.status not in {'failed', 'cancelled'}:
            raise RuntimeTransitionError('RECOVERY_NOT_SETTLED', 'The complete failed or cancelled graph must settle before recovery.')
        selected[sid] = state
        pending.extend(u.session_id for p in inv.child_plans.values() for u in p.units if u.session_id in current)

    from ..executor.workflow_executor import WorkflowExecutor
    prepared = {}
    for sid, state in selected.items():
        final = state.invocation
        rich = source.full_states.get(sid)
        if rich is None or rich.invocation.id != final.id:
            raise RuntimeTransitionError('RECOVERY_HISTORY_MISSING', 'Released Child execution history is unavailable.')
        inv = rich.invocation
        workflow = workflow_for_state(rich)
        sched = inv.scheduler
        targets = {oid for oid, item in sched.occurrences.items() if item.status == 'cancelled' or
                   item.status == 'failed' and not WorkflowExecutor._failure_handled(workflow, sched, oid)}
        if not targets and sid == root:
            # A detached Child can fail after the Parent body has completed.
            # Recovery of an already successful Root is intentionally excluded.
            if not any(u.session_id in selected for p in inv.child_plans.values() for u in p.units):
                raise RuntimeTransitionError('RECOVERY_NO_WORK', 'No failed or cancelled execution can be recovered.')
        # Await results are observations, including failures. Refresh only the
        # units whose owned Child is reopened; successful sibling results stand.
        invalid_units = {}
        for plan in inv.child_plans.values():
            indices = {u.unit_index for u in plan.units if u.session_id in selected}
            item = sched.occurrences.get(plan.parent_occurrence_id)
            if indices and item is not None and workflow.node(item.node_id).executable.id == 'system_command:await':
                targets.add(item.id)
                invalid_units[item.id] = indices
        reachable = {sched.occurrences[oid].node_id for oid in targets}
        queue = list(reachable)
        while queue:
            for edge in workflow.outgoing(queue.pop()):
                if edge.target not in reachable:
                    reachable.add(edge.target)
                    queue.append(edge.target)
        def comparable(left, right):
            count = min(len(left), len(right))
            return left[:count] == right[:count]
        removed = {oid for oid, item in sched.occurrences.items() if item.status == 'skipped' and item.node_id in reachable
                   and any(comparable(item.scope, sched.occurrences[target].scope) for target in targets)}
        deferred = {oid for oid in targets if any(a.source_occurrence_id in targets for a in sched.occurrences[oid].activations)}
        removed |= deferred
        reset = targets | removed
        for oid in targets:
            item = sched.occurrences[oid]
            if item.started_at_us is not None and workflow.node(item.node_id).recovery_mode.mode == 'never':
                raise RuntimeTransitionError('RECOVERY_NOT_ALLOWED', f'Node {item.node_id!r} does not permit recomputation.')
        identities = dict(sched.occurrence_ids)
        renamed = {}
        for oid in sched.occurrences:
            if oid not in reset:
                continue
            item = sched.occurrences[oid]
            key = occurrence_key(item.node_id, item.scope)
            renamed[oid] = identities[key] = f'{key}~recover:{final.recovery_count + 1}'
        nodes = {}
        retired = dict(sched.retired_occurrences)
        for oid, item in sched.occurrences.items():
            if oid in removed:
                continue
            if oid not in targets:
                nodes[oid] = replace(item, output=source.outputs.get(sid, {}).get(oid, item.output))
                continue
            node = workflow.node(item.node_id)
            if item.started_at_us is not None and node.recovery_mode.mode == 'never':
                raise RuntimeTransitionError('RECOVERY_NOT_ALLOWED', f'Node {item.node_id!r} does not permit recomputation.')
            upstream_changed = any(a.source_occurrence_id in targets for a in item.activations)
            workspace = (NodeExecutionState() if upstream_changed else
                         source.workspaces.get(sid, {}).get(oid, NodeExecutionState()))
            cached = {key:value for key,value in workspace.reused_calls.items() if int(key) not in invalid_units.get(oid, ())}
            command_inputs = dict(workspace.command_inputs)
            for call in source.calls.get(sid, {}).values():
                if call.occurrence_id != oid or upstream_changed:
                    continue
                if call.status == 'completed' and call.unit_index not in invalid_units.get(oid, ()):
                    cached[str(call.unit_index)] = freeze({'id': call.id, 'operator_id': call.operator_id, 'output': call.output})
                elif call.operator_id.startswith('system_command:') and call.input is not None:
                    command_inputs[str(call.unit_index)] = call.input
            workspace = replace(workspace, phase='started', fault=None,
                completed_stages=tuple(x for x in workspace.completed_stages if x not in ({'routing_resolved', 'aggregated', 'output_bound'} if oid in invalid_units else {'routing_resolved'})),
                routing=(), routing_source_status=None, reused_calls=freeze(cached), command_inputs=freeze(command_inputs))
            nodes[renamed[oid]] = replace(item, id=renamed[oid], status='ready', error=None, output=None,
                started_at_us=None, completed_at_us=None, started_sequence=None, ready_at_us=now,
                recovery_attempts=0, metrics=freeze({}), execution=workspace)
        for item in nodes.values():
            if item.status != 'completed':
                continue
            for activation in item.activations:
                source_item = sched.occurrences.get(activation.source_occurrence_id)
                if activation.source_occurrence_id in targets and source_item.status == 'completed':
                    retired[source_item.id] = replace(source_item, output=None, metrics=None, activations=(), execution=NodeExecutionState())
        nodes = {oid: replace(item, activations=tuple(
            a if item.status == 'completed' and a.source_occurrence_id in retired else
            replace(a, source_occurrence_id=renamed.get(a.source_occurrence_id, a.source_occurrence_id))
            for a in item.activations)) for oid,item in nodes.items()}
        # Discard obsolete routing decisions. Reconstitute only independent
        # incoming routes that a skipped join consumed before the failure.
        reset_node_ids = {sched.occurrences[oid].node_id for oid in reset}
        resolutions = {k:v for k,v in sched.resolutions.items() if workflow.edge(v.edge_id).source not in reset_node_ids}
        for key, value in source.resolutions.get(sid, {}).items():
            if workflow.edge(value.edge_id).source not in reset_node_ids and any(
                sched.occurrences[oid].node_id == value.target_node_id and sched.occurrences[oid].scope == value.target_scope for oid in removed):
                resolutions.setdefault(key, value)
        plans = {}
        for key, plan in inv.child_plans.items():
            parent_id = renamed.get(plan.parent_occurrence_id, plan.parent_occurrence_id)
            units = []
            original_plan = source.plans.get(sid, {}).get(key, plan)
            for index, unit in enumerate(plan.units):
                child = current.get(unit.session_id)
                phase = 'accepted' if unit.session_id in selected else 'planned' if child is None or child.invocation is None else unit.phase
                if plan.parent_occurrence_id in targets:
                    original_input = original_plan.units[index].input
                    units.append(replace(unit, phase=phase, input=original_input, input_released=False))
                else:
                    units.append(replace(unit, phase=phase))
            plans[key] = replace(plan, parent_occurrence_id=parent_id, units=child_units(tuple(units)))
        scheduler = replace(sched, retired_occurrences=runtime_mapping(retired), occurrence_ids=MappingProxyType(identities), occurrences=runtime_mapping(nodes),
            ready=tuple(renamed[oid] for oid in sched.occurrences if oid in targets and oid not in removed), resolutions=runtime_mapping(resolutions),
            operator_calls=runtime_mapping({k:v for k,v in sched.operator_calls.items() if v.occurrence_id not in reset}),
            waits=runtime_mapping({k:v for k,v in sched.waits.items() if v.occurrence_id not in reset}))
        reopened = replace(inv, status='running', scheduler=scheduler, child_plans=runtime_mapping(plans),
            error=None, cancel_reason=None, cancel_origin=None, output=None, pending_outcome=None,
            completed_at_us=None, control_state='active', recovery_count=final.recovery_count + 1,
            last_recovery_id=request_id, pending_recovery=None, signals=final.signals,
            resume_receipts=final.resume_receipts, resume_receipt_frontiers=final.resume_receipt_frontiers)
        session = rich.session if isinstance(final, ChildResult) else state.session
        prepared[sid] = replace(state, session=replace(session, updated_at_us=now), invocation=reopened)
    return prepared
