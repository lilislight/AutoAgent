"""Run with: .venv/bin/python -m examples.signal_workflow_demo

Demonstrates internal Parent -> Child delivery, explicit Wait/Resume, external
source sequence retries, checkpoint JSON round-trip and atomic message receive.
Signal acceptance never resumes a Wait. The receiver deliberately consumes the
messages after its explicit resume. No external service or API key is needed.
"""
import json

from pydantic import BaseModel, ConfigDict
from autoagent import (
    AutoAgentApp, Await, ContextOperation, ContextPatch, Edge, InputMappingContext,
    Node, OutputBindingContext, ReceiveSignal, Resume,
    ResumeRequest, RuntimeGraphCheckpoint, RuntimeHandle, RuntimeObservation,
    SendSignal, SignalBatch, SignalEndpoint, SignalLimits,
    Wait, Workflow,
)


class Gate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    ready: bool


class Message(BaseModel):
    model_config = ConfigDict(extra='forbid')
    text: str


def receiver_workflow() -> Workflow:
    return Workflow('signal_receiver',
        nodes=[Node('gate', Wait(Gate, Gate)),
               Node('receive', ReceiveSignal(endpoint='user_message', limit=10))],
        edges=[Edge('gate', 'receive')],
        signal_endpoints=[SignalEndpoint('user_message', Message)],
        signal_limits=SignalLimits(max_messages=16, max_message_bytes=64 * 1024,
                                   max_mailbox_bytes=256 * 1024))


def remember_wait(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(invocation=(ContextOperation.set('wait_id', context.output['waits'][0]['wait_id']),))


def send_to_child(context: InputMappingContext) -> Message:
    return Message(text='Hello from the Parent')


def resume_child(context: InputMappingContext) -> ResumeRequest:
    receipt = next(iter(context.incoming.values()))
    return ResumeRequest(handle=RuntimeHandle.model_validate(receipt['handle']),
                         wait_id=context.invocation_context['wait_id'], response={'ready': True})


def target_child(context: InputMappingContext) -> RuntimeHandle:
    return RuntimeHandle.model_validate(next(iter(context.incoming.values()))['handle'])


def parent_workflow() -> Workflow:
    return Workflow('signal_parent', nodes=[
        Node('child', Await(receiver_workflow(), 'gate'), output_binding=remember_wait),
        Node('send', SendSignal(handle=target_child, endpoint='user_message'), input_mapping=send_to_child),
        Node('resume', Resume(), input_mapping=resume_child),
        Node('result', Await(), input_mapping=target_child),
    ], edges=[Edge('child', 'send'), Edge('send', 'resume'), Edge('resume', 'result')])


def run_demo() -> dict:
    app = AutoAgentApp()
    try:
        internal = app.invoke(parent_workflow(), {'ready': False})
        assert internal.status == 'completed', internal
        assert internal.output.status == 'completed'
        internal_batch = SignalBatch.model_validate(internal.output.output)

        workflow = receiver_workflow()
        waiting = app.invoke(workflow, {'ready': False})
        assert waiting.status == 'waiting'
        first = app.signal(waiting.ref, 'user_message', {'text': 'First external message'},
                           source_id='demo-client', sequence=1)
        retry = app.signal(waiting.ref, 'user_message', {'text': 'First external message'},
                           source_id='demo-client', sequence=1)
        assert retry == first
        app.signal(waiting.ref, 'user_message', {'text': 'Second external message'},
                   source_id='demo-client', sequence=2)

        # The mailbox and source watermark survive a portable checkpoint.
        checkpoint = app.unload_session(waiting.ref, capture_checkpoint=True)
        record = json.loads(json.dumps(checkpoint.to_record(), sort_keys=True))
        app.load_checkpoint(RuntimeGraphCheckpoint.from_record(record))
        external = app.resume(waiting.ref, waiting.waits[0].id, {'ready': True})
        assert external.status == 'completed', external
        assert [m.payload['text'] for m in external.output.messages] == [
            'First external message', 'Second external message']
        return {
            'internal_messages': [m.payload for m in internal_batch.messages],
            'external_messages': [m.payload for m in external.output.messages],
            'duplicate_returned_same_receipt': retry == first,
            'checkpoint_roundtrip': 'ok',
        }
    finally:
        app.close()


if __name__ == '__main__':
    print(json.dumps(run_demo(), ensure_ascii=False, indent=2))
