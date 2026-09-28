"""Bounded Signal state and atomic delivery/receive transition planning."""
from collections.abc import Mapping
from dataclasses import dataclass
import json
from typing import ClassVar

from .values import freeze, thaw
from .operations import StateDelta, StateOperation
from ..errors import RuntimeTransitionError


_EMPTY_SIGNALS = freeze({'messages': {}, 'bytes': 0, 'receipts': {}, 'frontiers': {}, 'sources': {}})


def empty_signals():
    # Immutable path-copy updates let invocations without Signals share this.
    return _EMPTY_SIGNALS


def payload_size(payload):
    return len(json.dumps(thaw(payload), ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8'))


@dataclass(frozen=True, slots=True)
class SignalAccepted:
    kind: ClassVar[str] = 'signal.accepted'
    message_id: str
    endpoint: str
    payload: object
    size_bytes: int
    origin: object = None
    external: object = None

    def __post_init__(self):
        for name in ('payload', 'origin', 'external'):
            object.__setattr__(self, name, freeze(getattr(self, name)))
        if not isinstance(self.message_id, str) or not self.message_id or not isinstance(self.endpoint, str) or not self.endpoint:
            raise ValueError('Signal requires message and endpoint identity.')
        if type(self.size_bytes) is not int or self.size_bytes < 0 or self.size_bytes != payload_size(self.payload):
            raise ValueError('Signal payload byte count is invalid.')
        if (self.origin is None) == (self.external is None):
            raise ValueError('Signal requires exactly one delivery origin.')
        if self.origin is not None:
            validate_origin(self.origin)
        if self.external is not None:
            if not isinstance(self.external, Mapping) or set(self.external) != {'source_id', 'sequence', 'digest'}:
                raise ValueError('Invalid external Signal origin.')
            nonempty(self.external['source_id'])
            positive(self.external['sequence'])
            digest(self.external['digest'])


@dataclass(frozen=True, slots=True)
class SignalsReceived:
    kind: ClassVar[str] = 'signal.received'
    call_id: str
    message_ids: tuple[str, ...]

    def __post_init__(self):
        nonempty(self.call_id)
        if not isinstance(self.message_ids, tuple) or len(set(self.message_ids)) != len(self.message_ids):
            raise ValueError('Received messages must be a unique tuple.')
        for mid in self.message_ids:
            nonempty(mid)


@dataclass(frozen=True, slots=True)
class SignalReceiptReleased:
    kind: ClassVar[str] = 'signal.receipt_released'
    call_id: str
    caller_sequence: int

    def __post_init__(self):
        nonempty(self.call_id)
        positive(self.caller_sequence)


def nonempty(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError('Signal identity must be a nonempty string.')


def positive(value):
    if type(value) is not int or value < 1:
        raise ValueError('Signal sequence must be a positive integer.')


def digest(value):
    if not isinstance(value, str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('Invalid Signal digest.')


def validate_origin(origin):
    if not isinstance(origin, Mapping) or set(origin) != {'session_id', 'invocation_id', 'call_id', 'digest'}:
        raise ValueError('Invalid Signal origin.')
    for key in ('session_id', 'invocation_id', 'call_id'):
        nonempty(origin[key])
    digest(origin['digest'])


def signal_delta(planner, state, payload, occurred_at_us, session_id, invocation_id, execution_index):
    """One target lane owns all acceptance, consumption and receipt retirement."""
    from .events import OperatorCallCompleted
    inv = state.invocation
    signals = inv.signals
    ops = []
    def put(path, value, op='replace'):
        ops.append(StateOperation._from_owned(op, ('invocation', 'signals', *path), value))
    if isinstance(payload, SignalAccepted):
        if inv.status not in {'running', 'waiting'} or payload.message_id in signals['messages']:
            raise RuntimeTransitionError('SIGNAL_TARGET_UNAVAILABLE', 'Signal target is stopping, terminal or delivery is duplicated.')
        receipt = freeze({'message_id': payload.message_id, 'endpoint': payload.endpoint, 'accepted_sequence': state.sequence + 1})
        message = freeze({**receipt, 'accepted_at_us': occurred_at_us, 'payload': payload.payload})
        put(('messages', payload.message_id), freeze({'message': message, 'size_bytes': payload.size_bytes}), 'add')
        put(('bytes',), signals['bytes'] + payload.size_bytes)
        if payload.origin is not None:
            call_id = payload.origin['call_id']
            if call_id in signals['receipts']:
                raise RuntimeTransitionError('SIGNAL_DUPLICATE', 'Signal Call already accepted.')
            put(('receipts', call_id), freeze({'origin': payload.origin, 'receipt': receipt}), 'add')
        else:
            source = payload.external['source_id']
            previous = signals['sources'].get(source)
            if payload.external['sequence'] != (previous['sequence'] + 1 if previous else 1):
                raise RuntimeTransitionError('SIGNAL_SEQUENCE_INVALID', 'Signal sequence is not next.')
            put(('sources', source), freeze({**payload.external, 'receipt': receipt}), 'replace' if previous else 'add')
    elif isinstance(payload, SignalsReceived):
        call = inv.scheduler.operator_calls.get(payload.call_id)
        if inv.stopping or call is None or call.status != 'running' or call.operator_id != 'system_command:receive_signal':
            raise RuntimeTransitionError('SIGNAL_RECEIVE_CALL_INVALID', 'Receive requires its active Call.')
        endpoint, limit = call.input['endpoint'], call.input['limit']
        selected = tuple(mid for mid, entry in sorted(signals['messages'].items(), key=lambda item: item[1]['message']['accepted_sequence']) if entry['message']['endpoint'] == endpoint)[:limit]
        if payload.message_ids != selected:
            raise RuntimeTransitionError('SIGNAL_RECEIVE_ORDER_INVALID', 'Receive must take the next Endpoint messages.')
        messages = [signals['messages'][mid]['message'] for mid in selected]
        delta = planner.plan(state, OperatorCallCompleted(call.id, freeze({'messages': messages})),
            occurred_at_us=occurred_at_us, session_id=session_id, invocation_id=invocation_id,
            _execution_index=execution_index)
        ops.extend(delta.operations)
        size = signals['bytes']
        for mid in selected:
            size -= signals['messages'][mid]['size_bytes']
            put(('messages', mid), None, 'remove')
        put(('bytes',), size)
    else:
        proof = signals['receipts'].get(payload.call_id)
        if proof is None:
            raise RuntimeTransitionError('SIGNAL_RECEIPT_MISSING', 'Signal receipt was already retired.')
        origin = proof['origin']
        previous = signals['frontiers'].get(origin['session_id'])
        if previous and previous['invocation_id'] != origin['invocation_id']:
            raise RuntimeTransitionError('SIGNAL_CALLER_REPLACED', 'Signal caller changed.')
        put(('receipts', payload.call_id), None, 'remove')
        put(('frontiers', origin['session_id']), freeze({'invocation_id': origin['invocation_id'],
            'sequence': max(payload.caller_sequence, previous['sequence'] if previous else 0)}), 'replace' if previous else 'add')
    return StateDelta(tuple(ops))


def validate_signal_state(inv):
    from ..commands.signals import SignalMessage
    s = inv.signals
    if not isinstance(s, Mapping) or set(s) != {'messages', 'bytes', 'receipts', 'frontiers', 'sources'}:
        raise ValueError('Invalid Signal state fields.')
    if any(not isinstance(s[key], Mapping) for key in ('messages', 'receipts', 'frontiers', 'sources')):
        raise ValueError('Signal collections must be mappings.')
    if type(s['bytes']) is not int or s['bytes'] < 0:
        raise ValueError('Invalid Signal byte count.')
    total = 0
    sequences = set()
    for mid, entry in s['messages'].items():
        if set(entry) != {'message', 'size_bytes'}:
            raise ValueError('Invalid mailbox entry.')
        message = entry['message']
        SignalMessage.model_validate(thaw(message))
        if mid != message['message_id'] or message['accepted_sequence'] in sequences:
            raise ValueError('Invalid Signal message identity or order.')
        positive(message['accepted_sequence'])
        sequences.add(message['accepted_sequence'])
        if type(entry['size_bytes']) is not int or entry['size_bytes'] != payload_size(message['payload']):
            raise ValueError('Invalid Signal payload size.')
        total += entry['size_bytes']
    if total != s['bytes'] or (inv.terminal and s['messages']):
        raise ValueError('Mailbox accounting or terminal retention is invalid.')
    for call_id, proof in s['receipts'].items():
        if set(proof) != {'origin', 'receipt'}:
            raise ValueError('Invalid Signal receipt fields.')
        validate_origin(proof['origin'])
        if call_id != proof['origin']['call_id']:
            raise ValueError('Signal receipt Call identity mismatch.')
        validate_receipt(proof['receipt'])
        if proof['receipt']['message_id'] != 'internal:' + call_id:
            raise ValueError('Signal message identity does not match its Call.')
    for sid, frontier in s['frontiers'].items():
        nonempty(sid)
        if set(frontier) != {'invocation_id', 'sequence'}:
            raise ValueError('Invalid Signal frontier.')
        nonempty(frontier['invocation_id'])
        positive(frontier['sequence'])
    for source, entry in s['sources'].items():
        if set(entry) != {'source_id', 'sequence', 'digest', 'receipt'} or source != entry['source_id']:
            raise ValueError('Invalid external Signal source.')
        nonempty(source)
        positive(entry['sequence'])
        digest(entry['digest'])
        validate_receipt(entry['receipt'])


def validate_receipt(receipt):
    if set(receipt) != {'message_id', 'endpoint', 'accepted_sequence'}:
        raise ValueError('Invalid Signal receipt.')
    nonempty(receipt['message_id'])
    nonempty(receipt['endpoint'])
    positive(receipt['accepted_sequence'])


def validate_signal_workflow(inv, workflow):
    """Code-dependent bounds and Endpoint contracts for checkpoint recovery."""
    s, limits = inv.signals, workflow.signal_limits
    sources = set(s['frontiers']) | {p['origin']['session_id'] for p in s['receipts'].values()}
    if (len(s['messages']) > limits.max_messages or s['bytes'] > limits.max_mailbox_bytes
            or len(s['receipts']) > limits.max_pending_receipts or len(sources) > limits.max_internal_sources
            or len(s['sources']) > limits.max_external_sources):
        raise ValueError('Checkpoint Signal state exceeds Workflow limits.')
    contracts = dict(workflow.signal_endpoints)
    for entry in s['messages'].values():
        message = entry['message']
        contract = contracts.get(message['endpoint'])
        if contract is None or entry['size_bytes'] > limits.max_message_bytes:
            raise ValueError('Checkpoint message violates its Endpoint or byte limit.')
        if contract.to_record(thaw(message['payload'])) != message['payload']:
            raise ValueError('Checkpoint Signal payload is not canonical.')
    for proof in (*s['receipts'].values(), *s['sources'].values()):
        if proof['receipt']['endpoint'] not in contracts:
            raise ValueError('Checkpoint receipt refers to an unknown Endpoint.')
