"""Run with .venv/bin/python -m examples.durable_waits_demo."""
from pydantic import BaseModel, ConfigDict
from autoagent import (
    AutoAgentApp, AwaitSignal, Node, Select, SignalCase, SignalEndpoint,
    TimerCase, Workflow, RuntimeGraphCheckpoint,
)


class Feedback(BaseModel):
    model_config = ConfigDict(extra="forbid")
    text: str


def run_demo():
    app = AutoAgentApp()
    try:
        inbox = Workflow('durable_inbox', nodes=[Node('feedback', AwaitSignal('feedback'))],
            signal_endpoints=[SignalEndpoint('feedback', Feedback)])
        waiting = app.invoke(inbox, None)
        print('Signal wait:', app.suspension_info(waiting.ref).model_dump())
        checkpoint = app.unload_session(waiting.ref, capture_checkpoint=True)
        app.load_checkpoint(RuntimeGraphCheckpoint.from_record(checkpoint.to_record()))
        app.recover(waiting.ref)
        app.signal(waiting.ref, 'feedback', {'text': 'Continue with the new requirements'}, source_id='client', sequence=1)
        received = app.join(waiting.ref)
        assert received.status == 'completed', received
        print('Received:', received.output.model_dump())

        choice = Workflow('message_or_timeout', nodes=[Node('choose', Select({
            'message': SignalCase('feedback'),
            'timeout': TimerCase(delay_us=30_000_000),
        }))], signal_endpoints=[SignalEndpoint('feedback', Feedback)])
        waiting = app.invoke(choice, None)
        print('Next deadline:', app.suspension_info(waiting.ref).next_deadline_at_us)
        app.signal(waiting.ref, 'feedback', {'text': 'New instruction'}, source_id='client', sequence=1)
        selected = app.join(waiting.ref)
        assert selected.status == 'completed', selected
        assert selected.output.case == 'message'
        print('Selected:', selected.output.model_dump())
        return received, selected
    finally:
        app.close()


if __name__ == '__main__':
    run_demo()
