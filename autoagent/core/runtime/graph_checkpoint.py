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
                        or child.workflow_revision_id != plan.workflow_revision_id):
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
