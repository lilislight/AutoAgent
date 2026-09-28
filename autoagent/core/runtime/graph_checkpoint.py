"""Complete ownership bundles; each Session keeps its independent sequence."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
from .checkpoint import SessionCheckpoint
from .state import RuntimeState, ChildResult, child_input_digest


def validate_graph(root_session_id: str, states: Mapping[str, RuntimeState]) -> None:
    """Validate closure, causal phases, identities and external Wait uniqueness."""
    if not isinstance(root_session_id, str) or not root_session_id.strip() or root_session_id not in states:
        raise ValueError("Graph root must identify a bundled Session.")
    owners = {}
    if isinstance(states[root_session_id].invocation, ChildResult):
        raise ValueError("A compacted Child cannot become a Root.")
    if states[root_session_id].invocation is None:
        raise ValueError("Graph root requires an Invocation.")
    waits = set()
    for sid, state in states.items():
        inv = state.invocation
        if state.session is None or state.session.id != sid:
            raise ValueError("Graph Session identity mismatch.")
        if inv is None:
            continue  # An acknowledged SessionOpened can precede Child admission.
        for wait in inv.scheduler.waits.values():
            if wait.status == 'waiting':
                if wait.id in waits:
                    raise ValueError("Graph external Wait ids must be unique.")
                waits.add(wait.id)
        for plan in inv.child_plans.values():
            for unit in plan.units:
                if unit.session_id in owners:
                    raise ValueError("Child has multiple ownership claims.")
                owners[unit.session_id] = sid
                child_state = states.get(unit.session_id)
                child = child_state.invocation if child_state is not None else None
                if child is None:
                    if unit.phase == 'planned':
                        continue
                    if unit.phase == 'abandoned' and inv.stopping:
                        continue  # A cancelled plan need never create its Child.
                    raise ValueError("Graph is missing an admitted Child.")
                if unit.phase == "abandoned":
                    raise ValueError("An abandoned plan cannot have a Child Session.")
                if (child.id != unit.invocation_id or child.workflow_id != plan.workflow_id
                        or child.workflow_revision_id != plan.workflow_revision_id
                        or child.entry_node_id != plan.entry_node_id):
                    raise ValueError("Child identity does not match its ownership plan.")
                if isinstance(child, ChildResult):
                    if (child.parent_session_id != sid or child.parent_invocation_id != inv.id
                            or child.creation_id != plan.creation_id or child.unit_index != unit.unit_index):
                        raise ValueError("ChildResult ownership does not match Parent.")
                    if not unit.input_released and child.input_digest != child_input_digest(unit.input):
                        raise ValueError("ChildResult input does not match Parent admission.")
                elif not unit.input_released and child.input != unit.input:
                    raise ValueError("Child input does not match its ownership plan.")
                if unit.phase == 'terminal' and (not child.terminal or any(
                        u.phase not in {'terminal', 'abandoned'} for p in child.child_plans.values() for u in p.units)):
                    raise ValueError("Parent terminal marker precedes Child subtree settlement.")
        if inv.terminal and any(u.phase not in {'terminal', 'abandoned'} for p in inv.child_plans.values() for u in p.units):
            raise ValueError("Completed Invocation contains unsettled Children.")
    # Receipt retirement is causally after the caller's acknowledged result.
    # Independent Session prefixes must not erase that acknowledgement while
    # retaining the target's later retirement or compaction.
    from .waits import validate_wait_graph
    validate_wait_graph(states, owners)
    for target, state in states.items():
        inv = state.invocation
        if inv is None:
            continue
        for sid, dependency in inv.signals['frontiers'].items():
            caller_state = states.get(sid)
            caller = caller_state.invocation if caller_state else None
            if caller is None or caller.id != dependency['invocation_id'] or caller_state.sequence < dependency['sequence']:
                raise ValueError('Signal receipt retirement precedes caller acknowledgement.')
        for call_id, proof in inv.signals['receipts'].items():
            origin = proof['origin']
            caller_state = states.get(origin['session_id'])
            caller = caller_state.invocation if caller_state else None
            if caller is None or caller.id != origin['invocation_id']:
                raise ValueError('Signal receipt references a missing caller.')
            if not isinstance(caller, ChildResult):
                call = caller.scheduler.operator_calls.get(call_id)
                if call is None or call.operator_id != 'system_command:send_signal':
                    raise ValueError('Signal receipt references a missing SendSignal Call.')
                if call.input is not None:
                    request = call.input
                    handle = request.get('handle', {})
                    if (handle.get('session_id') != target or handle.get('invocation_id') != inv.id
                            or handle.get('workflow_id') != inv.workflow_id
                            or handle.get('workflow_revision_id') != inv.workflow_revision_id
                            or child_input_digest({'endpoint': request.get('endpoint'), 'payload': request.get('payload')}) != origin['digest']):
                        raise ValueError('Signal receipt does not match its Command input.')
        for entry in inv.signals['messages'].values():
            if entry['message']['accepted_sequence'] > state.sequence:
                raise ValueError('Signal message is ahead of its target state.')
        for receipt in [p['receipt'] for p in inv.signals['receipts'].values()] + [p['receipt'] for p in inv.signals['sources'].values()]:
            if receipt['accepted_sequence'] > state.sequence:
                raise ValueError('Signal receipt is ahead of its target state.')
        if inv.cancel_origin is not None:
            origin = inv.cancel_origin
            caller_state = states.get(origin['session_id'])
            caller = caller_state.invocation if caller_state else None
            if caller is None or caller.id != origin['invocation_id']:
                raise ValueError('Cancellation origin references a missing caller.')
            if not isinstance(caller, ChildResult):
                call = caller.scheduler.operator_calls.get(origin['call_id'])
                if call is None or call.operator_id != 'system_command:cancel':
                    raise ValueError('Cancellation origin references a missing Cancel Call.')
                if call.input is not None:
                    request = call.input
                    handle = request.get('handle', {})
                    if (handle.get('session_id') != target or handle.get('invocation_id') != inv.id
                            or handle.get('workflow_id') != inv.workflow_id
                            or handle.get('workflow_revision_id') != inv.workflow_revision_id
                            or child_input_digest(request.get('reason')) != origin['reason_digest']):
                        raise ValueError('Cancellation origin does not match its Command input.')
        for sid, dependency in inv.resume_receipt_frontiers.items():
            caller_state = states.get(sid)
            caller = caller_state.invocation if caller_state else None
            if (caller is None or caller.id != dependency['invocation_id']
                    or caller_state.sequence < dependency['sequence']):
                raise ValueError("Resume receipt retirement precedes caller acknowledgement.")
        for call_id, receipt in inv.resume_receipts.items():
            caller_state = states.get(receipt['session_id'])
            caller = caller_state.invocation if caller_state else None
            if caller is None or caller.id != receipt['invocation_id']:
                raise ValueError("Resume receipt references a missing caller Invocation.")
            if isinstance(caller, ChildResult):
                continue
            call = caller.scheduler.operator_calls.get(call_id)
            if call is None or call.operator_id != 'system_command:resume':
                raise ValueError("Resume receipt references a missing Command Call.")
            if call.input is not None:
                request = call.input
                handle = request.get('handle', {})
                if (handle.get('session_id') != target or handle.get('invocation_id') != inv.id
                        or handle.get('workflow_id') != inv.workflow_id
                        or handle.get('workflow_revision_id') != inv.workflow_revision_id
                        or request.get('wait_id') != receipt['wait_id']
                        or child_input_digest(request.get('response')) != receipt['response_digest']):
                    raise ValueError("Resume receipt does not match its Command input.")
    if root_session_id in owners:
        raise ValueError("Graph root is owned by another Session.")
    visited = set()
    pending = [root_session_id]
    while pending:
        sid = pending.pop()
        if sid in visited:
            raise ValueError("Graph contains a cycle.")
        visited.add(sid)
        inv = states[sid].invocation
        if inv is not None:
            pending.extend(u.session_id for p in inv.child_plans.values() for u in p.units if u.session_id in states)
    if visited != set(states):
        raise ValueError("Graph contains orphan Sessions or a disconnected cycle.")


@dataclass(frozen=True, slots=True)
class RuntimeGraphCheckpoint:
    """Immutable graph snapshot, including durable plans for unopened Children."""
    root_session_id: str
    sessions: tuple[SessionCheckpoint, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.sessions, tuple) or not all(isinstance(s, SessionCheckpoint) for s in self.sessions):
            raise TypeError("Graph sessions must be a tuple of SessionCheckpoint values.")
        states = {s.session_id: s.state for s in self.sessions}
        if len(states) != len(self.sessions):
            raise ValueError("Duplicate Graph Session.")
        validate_graph(self.root_session_id, states)

    def to_record(self) -> dict[str, object]:
        record = {'schema_version': 1, 'root_session_id': self.root_session_id,
                  'sessions': [s.to_record() for s in sorted(self.sessions, key=lambda s: s.session_id)]}
        record['digest'] = _digest(record)
        return record

    @classmethod
    def from_record(cls, record: dict[str, object]) -> RuntimeGraphCheckpoint:
        if not isinstance(record, dict) or set(record) != {'schema_version', 'root_session_id', 'sessions', 'digest'}:
            raise TypeError("Invalid Graph Checkpoint schema.")
        if type(record['schema_version']) is not int or record['schema_version'] != 1:
            raise ValueError("Unsupported Graph Checkpoint version.")
        if not isinstance(record['sessions'], list):
            raise TypeError("Graph sessions must be a list.")
        if record['digest'] != _digest({k: v for k, v in record.items() if k != 'digest'}):
            raise ValueError("Graph Checkpoint digest mismatch.")
        return cls(record['root_session_id'], tuple(SessionCheckpoint.from_record(s) for s in record['sessions']))


def _digest(record: dict[str, object]) -> str:
    return hashlib.sha256(json.dumps(record, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
