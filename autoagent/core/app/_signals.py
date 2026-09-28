"""Signal delivery through the existing graph and Session commit lanes."""
import json
from ..commands import RuntimeHandle, SignalReceipt, SignalBatch
from ..commands.signals import SendSignalRequest, ReceiveSignalRequest
from ..errors import RuntimeTransitionError
from ..runtime.signals import (SignalAccepted, SignalsReceived, SignalReceiptReleased,
    payload_size, nonempty, positive)
from ..runtime.state import child_input_digest
from ..runtime.values import thaw


def fail(code, message):
    raise RuntimeTransitionError(code, message)


class SignalRuntime:
    async def _send_signal_runtime(self, caller_sid, caller_iid, call_id, request):
        request = SendSignalRequest.model_validate(request)
        target = request.handle.session_id
        root = self._root_session_id(caller_sid)
        async with self._graph_gate(root).shared(), self._session_transition_lock(target):
            caller = self._repository.state(caller_sid).invocation
            call = caller.scheduler.operator_calls.get(call_id) if caller else None
            if (caller is None or caller.id != caller_iid or call is None or call.status != 'running'
                    or call.operator_id != 'system_command:send_signal'
                    or thaw(call.input) != request.model_dump(mode='python')):
                fail('SIGNAL_CALL_MISMATCH', 'SendSignal requires its active recorded Call.')
            target_inv = self._repository.state(target).invocation
            if (target_inv is None or self._root_session_id(target) != root
                    or self._runtime_identity(target)[0] != request.handle):
                fail('RUNTIME_HANDLE_MISMATCH', 'SendSignal requires an exact Handle in the caller graph.')
            origin = {'session_id': caller_sid, 'invocation_id': caller_iid, 'call_id': call_id,
                'digest': child_input_digest({'endpoint': request.endpoint, 'payload': request.payload})}
            await self._settle_runtime_commits((target,))
            return await self._accept_signal(request, origin=origin)

    async def _signal(self, ref, request, source_id, sequence):
        self._state_for_ref(ref)
        nonempty(source_id)
        if len(source_id) > 128:
            raise ValueError('Signal source_id cannot exceed 128 characters.')
        positive(sequence)
        root = ref.session_id
        async with self._graph_gate(root).shared(), self._session_transition_lock(root):
            self._state_for_ref(ref)
            await self._settle_runtime_commits((root,))
            external = {'source_id': source_id, 'sequence': sequence,
                'digest': child_input_digest({'endpoint': request.endpoint, 'payload': request.payload})}
            return await self._accept_signal(request, external=external)

    async def _accept_signal(self, request, *, origin=None, external=None):
        """Caller holds the target lane; duplicate lookup precedes lifecycle checks."""
        target = request.handle.session_id
        state = self._repository.state(target)
        inv = state.invocation
        signals = inv.signals
        if origin is not None:
            existing = signals['receipts'].get(origin['call_id'])
            if existing is not None:
                if existing['origin'] != origin:
                    fail('SIGNAL_CALL_MISMATCH', 'Signal Call was reused with different content.')
                return SignalReceipt(handle=request.handle, **thaw(existing['receipt']))
            message_id = 'internal:' + origin['call_id']
        else:
            existing = signals['sources'].get(external['source_id'])
            if existing:
                if external['sequence'] < existing['sequence']:
                    fail('SIGNAL_SEQUENCE_RETIRED', 'Historical Signal sequence is retired; it will not be delivered again.')
                if external['sequence'] == existing['sequence']:
                    if external['digest'] != existing['digest']:
                        fail('SIGNAL_SEQUENCE_CONFLICT', 'Signal sequence was reused with different content.')
                    return SignalReceipt(handle=request.handle, **thaw(existing['receipt']))
            if external['sequence'] != (existing['sequence'] + 1 if existing else 1):
                fail('SIGNAL_SEQUENCE_GAP', 'Signal sequence must be the next sequence for this source.')
            message_id = 'external:' + json.dumps([external['source_id'], external['sequence']], separators=(',', ':'))
        if inv.status not in {'running', 'waiting'} or self._branch_stopping(target):
            fail('SIGNAL_TARGET_UNAVAILABLE', 'Signals require a running or waiting target.')
        workflow = self._workflow_for_state(state)
        contract = dict(workflow.signal_endpoints).get(request.endpoint)
        if contract is None:
            fail('SIGNAL_ENDPOINT_UNKNOWN', 'Target Workflow does not declare this Signal Endpoint.')
        payload = contract.to_record(request.payload)
        size = payload_size(payload)
        limits = workflow.signal_limits
        if size > limits.max_message_bytes:
            fail('SIGNAL_TOO_LARGE', 'Signal exceeds the single-message byte limit.')
        if len(signals['messages']) >= limits.max_messages or signals['bytes'] + size > limits.max_mailbox_bytes:
            fail('MAILBOX_FULL', 'Signal mailbox capacity is exhausted.')
        if origin is not None:
            if len(signals['receipts']) >= limits.max_pending_receipts:
                fail('SIGNAL_RECEIPTS_FULL', 'Pending Signal receipt capacity is exhausted.')
            sources = set(signals['frontiers']) | {p['origin']['session_id'] for p in signals['receipts'].values()}
            if origin['session_id'] not in sources and len(sources) >= limits.max_internal_sources:
                fail('SIGNAL_SOURCES_FULL', 'Internal Signal source capacity is exhausted.')
        elif external['source_id'] not in signals['sources'] and len(signals['sources']) >= limits.max_external_sources:
            fail('SIGNAL_SOURCES_FULL', 'External Signal source capacity is exhausted.')
        event = await self._emit_locked(target, inv.id,
            SignalAccepted(message_id, request.endpoint, payload, size, origin, external))
        return SignalReceipt(handle=request.handle, endpoint=request.endpoint,
            message_id=message_id, accepted_sequence=event.sequence)

    async def _receive_signal_runtime(self, sid, iid, call_id, request):
        request = ReceiveSignalRequest.model_validate(request)
        async with self._graph_gate(self._root_session_id(sid)).shared(), self._session_transition_lock(sid):
            await self._settle_runtime_commits((sid,))
            state = self._repository.state(sid)
            inv = state.invocation
            call = inv.scheduler.operator_calls.get(call_id) if inv else None
            if (inv is None or inv.id != iid or call is None or call.operator_id != 'system_command:receive_signal'
                    or thaw(call.input) != request.model_dump(mode='python')):
                fail('SIGNAL_RECEIVE_CALL_INVALID', 'ReceiveSignal requires its recorded Call.')
            if call.status == 'completed':
                return SignalBatch.model_validate(thaw(call.output))
            if self._branch_stopping(sid):
                fail('INVOCATION_STOPPING', 'Cannot receive from a stopping Invocation.')
            if request.endpoint not in dict(self._workflow_for_state(state).signal_endpoints):
                fail('SIGNAL_ENDPOINT_UNKNOWN', 'Workflow does not declare this Signal Endpoint.')
            if sid in self._wait_sessions:
                await self._service_waits_locked(sid)
                inv = self._repository.state(sid).invocation
            selected = tuple(mid for mid, entry in sorted(inv.signals['messages'].items(), key=lambda item: item[1]['message']['accepted_sequence'])
                if entry['message']['endpoint'] == request.endpoint)[:request.limit]
            await self._emit_locked(sid, iid, SignalsReceived(call_id, selected))
            return SignalBatch.model_validate(thaw(self._repository.state(sid).invocation.scheduler.operator_calls[call_id].output))

    async def _release_signal(self, caller_sid, caller_iid, call_id, target):
        async with self._graph_gate(self._root_session_id(caller_sid)).shared(), self._session_transition_lock(target):
            await self._settle_runtime_commits((target,))
            inv = self._repository.state(target).invocation
            proof = inv.signals['receipts'].get(call_id)
            if proof is None:
                return
            state = self._repository.state(caller_sid)
            caller = state.invocation
            call = caller.scheduler.operator_calls.get(call_id) if caller else None
            if (caller is None or caller.id != caller_iid or proof['origin']['session_id'] != caller_sid
                    or proof['origin']['invocation_id'] != caller_iid
                    or not (caller.terminal or (call and call.status in {'completed', 'failed', 'cancelled'}))):
                fail('SIGNAL_RECEIPT_ACTIVE', 'Signal caller has not settled.')
            await self._emit_locked(target, inv.id, SignalReceiptReleased(call_id, state.sequence))

    async def _collect_signal_receipts(self, root):
        for target in tuple(self._signal_receipt_targets):
            if self._root_session_id(target) != root:
                continue
            inv = self._repository.state(target).invocation
            for call_id, proof in tuple(inv.signals['receipts'].items()):
                origin = proof['origin']
                caller = self._repository.state(origin['session_id']).invocation
                call = caller.scheduler.operator_calls.get(call_id) if caller else None
                if caller and (caller.terminal or (call and call.status in {'completed', 'failed', 'cancelled'})):
                    await self._release_signal(origin['session_id'], origin['invocation_id'], call_id, target)
