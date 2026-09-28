"""Runtime Resume acceptance, identity, and crash-boundary contracts."""
import unittest
from pydantic import BaseModel, ConfigDict
from autoagent import (AutoAgentApp, Workflow, Node, Edge, Wait, Await, Resume, ResumeRequest,
    ResumeReceipt, RuntimeHandle, InputMappingContext)
from tests.test_system_commands import Collector


class Value(BaseModel):
    model_config = ConfigDict(extra='forbid')
    value: int


def response(context: InputMappingContext) -> ResumeRequest:
    observation = next(iter(context.incoming.values()))
    wait = observation['waits'][0]
    return ResumeRequest(handle=RuntimeHandle.model_validate(wait['handle']), wait_id=wait['wait_id'], response={'value': 9})


def target(context: InputMappingContext) -> RuntimeHandle:
    return RuntimeHandle.model_validate(next(iter(context.incoming.values()))['handle'])


def workflow():
    child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
    return Workflow('root', nodes=[Node('await', Await(child, 'wait')),
        Node('resume', Resume(), input_mapping=response),
        Node('result', Await(), input_mapping=target)],
        edges=[Edge('await', 'resume'), Edge('resume', 'result')])


class ResumeCommandTests(unittest.TestCase):
    def test_resume_origin_validation(self):
        """Malformed effect origins are rejected before replay can consume a Wait."""
        from autoagent.core.runtime.events import WaitResumed
        origin = {'session_id': 'caller', 'invocation_id': 'invocation',
                  'call_id': 'call', 'response_digest': 'a' * 64}
        self.assertEqual(dict(WaitResumed('wait', None, origin).origin), origin)
        for invalid in (1, [], 'origin', {}, {**origin, 'response_digest': 'invalid'}):
            with self.subTest(origin=invalid), self.assertRaises(ValueError):
                WaitResumed('wait', None, invalid)

    def test_resume_child_and_await_final_result(self):
        """A Parent resumes its Child and separately awaits the final output."""
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(workflow(), {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output.status, 'completed')
            self.assertEqual(result.output.output, {'value': 9})
            calls = [e.payload for e in sink.events if e.event_name == 'operator_call.started'
                and e.payload.operator_id == 'system_command:resume']
            self.assertEqual(len(calls), 1)
            resumed = [e.payload for e in sink.events if e.event_name == 'wait.resumed']
            self.assertEqual(len(resumed), 1)
            self.assertEqual(resumed[0].origin['call_id'], calls[0].call_id)
            self.assertTrue(all(not app._repository.state(sid).invocation.resume_receipts
                for sid in app._repository.session_ids()))
            cp = app.unload_session(result.ref, capture_checkpoint=True)
            self.assertIsNotNone(cp)
        finally:
            app.close()

    def test_invalid_response_and_wait_identity_do_not_consume_wait(self):
        """Malformed responses and mismatched Wait IDs fail before a response is accepted."""
        for invalid in ('response', 'wait_id', 'handle'):
            def bad_response(context: InputMappingContext) -> ResumeRequest:
                request = response(context)
                if invalid == 'response':
                    return request.model_copy(update={'response': {'value': 'wrong'}})
                if invalid == 'wait_id':
                    return request.model_copy(update={'wait_id': 'missing'})
                return request.model_copy(update={'handle': request.handle.model_copy(update={'workflow_id': 'forged'})})
            child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
            root = Workflow('root', nodes=[Node('await', Await(child, 'wait')),
                Node('resume', Resume(), input_mapping=bad_response)], edges=[Edge('await', 'resume')])
            sink = Collector()
            app = AutoAgentApp(runtime_event_sink=sink)
            try:
                result = app.invoke(root, {'value': 1})
                self.assertEqual(result.status, 'failed', result)
                self.assertFalse(any(e.event_name == 'wait.resumed' for e in sink.events))
            finally:
                app.close()

    def test_cross_graph_handle_is_rejected(self):
        """A valid external Handle does not grant another Runtime Graph mutation access."""
        app = AutoAgentApp()
        try:
            waiting = app.invoke(Workflow('target', nodes=[Node('wait', Wait(Value, Value))]), {'value': 1})
            request = ResumeRequest(handle=RuntimeHandle(**waiting.ref.model_dump()), wait_id=waiting.waits[0].id, response={'value': 2})
            result = app.invoke(Workflow('caller', nodes=[Node('resume', Resume())]), request.model_dump())
            self.assertEqual(result.status, 'failed')
            self.assertEqual(app.status(waiting.ref).status, 'waiting')
        finally:
            app.close()

    def test_concurrent_commands_only_consume_one_response(self):
        """Distinct Resume calls racing for one Wait have exactly one accepted response."""
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        root = Workflow('root', nodes=[Node('await', Await(child, 'wait')),
            Node('first', Resume(), input_mapping=response), Node('second', Resume(), input_mapping=response)],
            edges=[Edge('await', 'first'), Edge('await', 'second')], failure_mode='continue_active_branches')
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(root, {'value': 1})
            self.assertEqual(result.status, 'failed')
            self.assertEqual(sum(e.event_name == 'wait.resumed' for e in sink.events), 1)
            resume_ids = {e.payload.call_id for e in sink.events if e.event_name == 'operator_call.started'
                and e.payload.operator_id == 'system_command:resume'}
            completed = [e for e in sink.events if e.event_name == 'operator_call.completed' and e.payload.call_id in resume_ids]
            failed = [e for e in sink.events if e.event_name == 'operator_call.failed' and e.payload.call_id in resume_ids]
            self.assertEqual((len(completed), len(failed)), (1, 1))
        finally:
            app.close()

    def test_resume_sibling_with_explicit_handle(self):
        """A Child can consume a sibling Wait when explicitly given its exact Handle."""
        from autoagent import Spawn
        responder = Workflow('responder', nodes=[Node('resume', Resume())])
        waiter = Workflow('waiter', nodes=[Node('wait', Wait(Value, Value))])
        root = Workflow('root', nodes=[Node('await', Await(waiter, 'wait')),
            Node('respond', Spawn(responder, 'resume'), input_mapping=response)], edges=[Edge('await', 'respond')])
        app = AutoAgentApp()
        try:
            result = app.invoke(root, {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            outputs = {app._repository.state(sid).invocation.workflow_id: app._repository.state(sid).invocation.output
                for sid in app._repository.session_ids()}
            self.assertEqual(outputs['waiter'], {'value': 9})
            self.assertIsNotNone(app.unload_session(result.ref, capture_checkpoint=True))
        finally:
            app.close()

    def test_self_and_child_to_parent_resume_never_join_the_root(self):
        """Self and Child-to-Parent Resume acknowledge a response without a graph join deadlock."""
        import asyncio
        from autoagent import Spawn
        from autoagent.core.runtime.events import WaitRequested
        def identity(value: Value) -> Value:
            return value
        for use_child in (False, True):
            ready = asyncio.Event()
            holder = {}
            class Sink(Collector):
                async def append(self, event):
                    await super().append(event)
                    if isinstance(event.payload, WaitRequested):
                        holder['wait_id'] = event.payload.wait_id
                        ready.set()
            async def make_request(context: InputMappingContext) -> ResumeRequest:
                await ready.wait()
                return ResumeRequest(handle=context.self_handle, wait_id=holder['wait_id'], response={'value': 12})
            executable = Spawn(Workflow('child', nodes=[Node('resume', Resume())]), 'resume') if use_child else Resume()
            root = Workflow('root', nodes=[Node('start', identity), Node('wait', Wait(Value, Value)),
                Node('respond', executable, input_mapping=make_request)], edges=[Edge('start', 'wait'), Edge('start', 'respond')])
            app = AutoAgentApp(runtime_event_sink=Sink())
            try:
                ref = app.submit_invoke(root, {'value': 1}).ref
                result = app.join(ref, timeout=2)
                self.assertEqual(result.status, 'completed', result)
                self.assertEqual(result.output['wait'], {'value': 12})
                self.assertIsNotNone(app.unload_session(ref, capture_checkpoint=True))
            finally:
                app.close()

    def test_every_resume_prefix_recovers_once_including_compacted_target(self):
        """A compact target retains the pending acceptance proof until caller completion ACK."""
        import asyncio
        from autoagent.core import RuntimeEvent, ChildResult
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph, session_checkpoints
        compacted = asyncio.Event()
        class Sink(Collector):
            resume_id = None
            async def append(self, event):
                if event.event_name == 'operator_call.started' and event.payload.operator_id == 'system_command:resume':
                    self.resume_id = event.payload.call_id
                if event.event_name == 'operator_call.completed' and event.payload.call_id == self.resume_id:
                    await compacted.wait()
                await super().append(event)
                if event.event_name == 'child_invocation.compacted':
                    compacted.set()
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(workflow(), {'value': 1}, session_id='root')
            self.assertEqual(result.status, 'completed', result)
            events = tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)
        finally:
            app.close()
        checked_compact = False
        start = next(i for i, e in enumerate(events) if e.event_name == 'operator_call.started'
            and e.payload.operator_id == 'system_command:resume')
        for index in range(start, len(events)):
            with self.subTest(cut=events[index].event_name, index=index):
                prefix = events[:index+1]
                checkpoint = _checkpoint_from_prefix(prefix)
                if events[index].event_name == 'child_invocation.compacted':
                    target_state = next(s.state for s in session_checkpoints(checkpoint) if s.session_id != 'root')
                    self.assertIsInstance(target_state.invocation, ChildResult)
                    self.assertTrue(target_state.invocation.resume_receipts)
                    checked_compact = True
                recovered_sink = Collector()
                restored = AutoAgentApp(runtime_event_sink=recovered_sink)
                try:
                    restored.register_workflow(workflow())
                    ref = load_graph(restored, checkpoint).invocations[0]
                    result = restored.recover(ref)
                    self.assertEqual(result.status, 'completed', result)
                    self.assertEqual(result.output.output, {'value': 9})
                    self.assertEqual(sum(e.event_name == 'wait.resumed' for e in (*prefix, *recovered_sink.events)), 1)
                    self.assertFalse(any(e.event_name == 'operator_call.started' and
                        e.payload.operator_id == 'system_command:resume' for e in recovered_sink.events))
                finally:
                    restored.close()
        self.assertTrue(checked_compact)

    def test_lost_ack_retries_exact_event_in_same_app(self):
        """Lost target, caller-result, and receipt-release ACKs retry their exact event once."""
        import asyncio
        from autoagent import RuntimeInfrastructureError
        for boundary in ('wait.resumed', 'operator_call.completed', 'resume_receipt.released'):
            class Sink(Collector):
                failed = None
                resume_id = None
                retries = 0
                async def append(self, event):
                    if event.event_name == 'operator_call.started' and event.payload.operator_id == 'system_command:resume':
                        self.resume_id = event.payload.call_id
                    match = event.event_name == boundary and (boundary != 'operator_call.completed' or event.payload.call_id == self.resume_id)
                    if self.failed is event:
                        self.retries += 1
                    if all(previous.id != event.id for previous in self.events):
                        self.events.append(event)
                    if match and self.failed is None:
                        self.failed = event
                        raise OSError('committed but acknowledgement lost')
            sink = Sink()
            app = AutoAgentApp(runtime_event_sink=sink)
            try:
                with self.assertRaises(RuntimeInfrastructureError):
                    app.invoke(workflow(), {'value': 1}, session_id='root')
                async def settle_tasks():
                    tasks = [app._task_runtime.task(sid) for sid in app._repository.session_ids()]
                    await asyncio.gather(*(t for t in tasks if t is not None), return_exceptions=True)
                app._runtime_loop.run(settle_tasks())
                inv = app._repository.state('root').invocation
                result = app.recover(app._ref_for_invocation('root', inv))
                self.assertEqual(result.status, 'completed', (boundary, result))
                self.assertEqual(result.output.output, {'value': 9})
                self.assertEqual(sink.retries, 1, boundary)
                self.assertEqual(sum(e.event_name == 'wait.resumed' for e in sink.events), 1)
                self.assertFalse(app._resume_receipt_targets)
            finally:
                app.close()

    def test_checkpoint_rejects_receipt_retirement_ahead_of_caller(self):
        """A later target checkpoint cannot erase the proof against an older caller prefix."""
        from autoagent.core import StateReducer, SessionCheckpoint, RuntimeGraphCheckpoint
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(workflow(), {'value': 1}, session_id='root')
            events = tuple(sink.events)
            resume_id = next(e.payload.call_id for e in events if e.event_name == 'operator_call.started'
                and e.payload.operator_id == 'system_command:resume')
            cut = next(i for i, e in enumerate(events) if e.event_name == 'operator_call.completed' and e.payload.call_id == resume_id)
            parent = StateReducer().reduce(tuple(e for e in events[:cut] if e.session_id == 'root'))
            children = [app._repository.capture_checkpoint(sid) for sid in app._repository.session_ids() if sid != 'root']
            with self.assertRaisesRegex(ValueError, 'retirement precedes caller acknowledgement'):
                RuntimeGraphCheckpoint('root', (SessionCheckpoint.from_state(parent), *children))
            self.assertEqual(result.status, 'completed')
        finally:
            app.close()

    def test_cancel_after_accepted_response_retires_pending_receipt(self):
        """Cancelling a caller after response acceptance frees proof metadata without replay."""
        import asyncio
        import threading
        entered = threading.Event()
        class Sink(Collector):
            resume_id = None
            async def append(self, event):
                if event.event_name == 'operator_call.started' and event.payload.operator_id == 'system_command:resume':
                    self.resume_id = event.payload.call_id
                if event.event_name == 'operator_call.completed' and event.payload.call_id == self.resume_id:
                    entered.set()
                    await asyncio.sleep(.05)
                await super().append(event)
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            ref = app.submit_invoke(workflow(), {'value': 1}).ref
            self.assertTrue(entered.wait(2))
            result = app.cancel(ref, reason='cancel command graph')
            self.assertIn(result.status, {'cancelled', 'completed'})
            self.assertFalse(app._resume_receipt_targets)
            checkpoint = app.unload_session(ref, capture_checkpoint=True)
            self.assertTrue(all(not s.state.invocation.resume_receipts for s in checkpoint.sessions))
        finally:
            app.close()

    def test_resume_receipt_returns_before_target_operator_finishes(self):
        """Resume acceptance does not wait for target business work or hold its execution slot."""
        import asyncio
        import threading
        from autoagent import OutputBindingContext
        accepted, release = threading.Event(), threading.Event()
        async def work(value: Value) -> Value:
            while not release.is_set():
                await asyncio.sleep(.001)
            return value
        def received(context: OutputBindingContext) -> None:
            self.assertEqual(context.output['wait_id'], holder['wait_id'])
            accepted.set()
        holder = {}
        def request(context: InputMappingContext) -> ResumeRequest:
            r = response(context)
            holder['wait_id'] = r.wait_id
            return r
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value)), Node('work', work)], edges=[Edge('wait', 'work')])
        root = Workflow('root', nodes=[Node('await', Await(child, 'wait')),
            Node('resume', Resume(), input_mapping=request, output_binding=received)], edges=[Edge('await', 'resume')])
        app = AutoAgentApp(max_operator_concurrency=1)
        try:
            ref = app.submit_invoke(root, {'value': 1}).ref
            self.assertTrue(accepted.wait(2))
            self.assertNotEqual(app.status(ref).status, 'completed')
            release.set()
            self.assertEqual(app.join(ref, timeout=2).status, 'completed')
        finally:
            release.set()
            app.close()

    def test_sequential_resumes_keep_one_caller_frontier(self):
        """Repeated accepted responses retain one causal watermark, not every response receipt."""
        child = Workflow('child', nodes=[Node('first', Wait(Value, Value)), Node('second', Wait(Value, Value))],
            edges=[Edge('first', 'second')])
        root = Workflow('root', nodes=[Node('one', Await(child, 'first')),
            Node('resume_one', Resume(), input_mapping=response), Node('two', Await(), input_mapping=target),
            Node('resume_two', Resume(), input_mapping=response), Node('result', Await(), input_mapping=target)],
            edges=[Edge('one', 'resume_one'), Edge('resume_one', 'two'), Edge('two', 'resume_two'), Edge('resume_two', 'result')])
        app = AutoAgentApp()
        try:
            result = app.invoke(root, {'value': 1}, session_id='root')
            self.assertEqual(result.status, 'completed', result)
            child_state = app._repository.state(result.output.handle.session_id).invocation
            self.assertFalse(child_state.resume_receipts)
            self.assertEqual(list(child_state.resume_receipt_frontiers), ['root'])
            self.assertEqual(set(child_state.resume_receipt_frontiers['root']), {'invocation_id', 'sequence'})
        finally:
            app.close()

    def test_map_resume_and_empty_input(self):
        """Map Resume returns ordered receipts and empty Maps create no response effects."""
        from autoagent import Map
        def values(context: InputMappingContext) -> list[Value]:
            return [Value(value=i) for i in range(context.invocation_input['value'])]
        def responses(context: InputMappingContext) -> list[ResumeRequest]:
            observations = next(iter(context.incoming.values()))
            return [ResumeRequest(handle=RuntimeHandle.model_validate(o['handle']), wait_id=o['waits'][0]['wait_id'],
                response={'value': i + 10}) for i, o in enumerate(observations)]
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        root = Workflow('root', nodes=[Node('await', Await(child, 'wait'), input_mapping=values, map=Map()),
            Node('resume', Resume(), input_mapping=responses, map=Map())], edges=[Edge('await', 'resume')])
        for count in (0, 3):
            app = AutoAgentApp()
            try:
                result = app.invoke(root, {'value': count})
                self.assertEqual(result.status, 'completed', result)
                self.assertEqual(len(result.output), count)
                for i, receipt in enumerate(result.output):
                    self.assertIsInstance(receipt, ResumeReceipt)
                    child_result = app._repository.state(receipt.handle.session_id).invocation
                    self.assertEqual(child_result.output, {'value': i + 10})
                    self.assertFalse(child_result.resume_receipts)
            finally:
                app.close()

    def test_terminal_target_is_rejected(self):
        """A completed target cannot accept a new Resume even with an otherwise valid Handle."""
        def identity(value: Value) -> Value:
            return value
        def request(context: InputMappingContext) -> ResumeRequest:
            observation = next(iter(context.incoming.values()))
            return ResumeRequest(handle=RuntimeHandle.model_validate(observation['handle']), wait_id='finished', response={'value': 2})
        child = Workflow('child', nodes=[Node('work', identity)])
        root = Workflow('root', nodes=[Node('await', Await(child, 'work')),
            Node('resume', Resume(), input_mapping=request)], edges=[Edge('await', 'resume')])
        app = AutoAgentApp()
        try:
            result = app.invoke(root, {'value': 1})
            self.assertEqual(result.status, 'failed')
            self.assertIn('terminal', result.error.message)
        finally:
            app.close()

    def test_cross_graph_rejection_does_not_settle_target_pending_event(self):
        """Rejected cross-graph Resume cannot acknowledge an unrelated pending target event."""
        from autoagent import RuntimeInfrastructureError
        class Sink(Collector):
            pending = None
            attempts = 0
            async def append(self, event):
                if event.event_name == 'node_occurrence.waiting':
                    self.attempts += 1
                    if self.pending is None:
                        self.pending = event
                        raise OSError('target Wait acknowledgement lost')
                await super().append(event)
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            with self.assertRaises(RuntimeInfrastructureError):
                app.invoke(Workflow('target', nodes=[Node('wait', Wait(Value, Value))]), {'value': 1}, session_id='target')
            state = app._repository.state('target')
            handle = RuntimeHandle(session_id='target', invocation_id=state.invocation.id,
                workflow_id=state.invocation.workflow_id, workflow_revision_id=state.invocation.workflow_revision_id)
            request = ResumeRequest(handle=handle, wait_id=sink.pending.payload.wait_id, response={'value': 2})
            result = app.invoke(Workflow('caller', nodes=[Node('resume', Resume())]), request.model_dump())
            self.assertEqual(result.status, 'failed')
            self.assertEqual(sink.attempts, 1)
            self.assertEqual(app._repository.state('target').sequence, state.sequence)
        finally:
            app.close()
