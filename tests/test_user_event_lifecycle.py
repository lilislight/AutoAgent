"""Generic Wait/error observations and transient/persistent User Event routing."""

import asyncio
from collections import Counter
from collections.abc import AsyncIterator
from contextlib import aclosing
import unittest
import warnings

from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp, Await, AwaitAny, AwaitAnyRequest, AwaitSignal, BackoffPolicy,
    Edge, InputMappingContext, InvocationRef, Map, RuntimeHandle, Select, SelectRequest,
    SignalEndpoint, SignalRequestCase, Spawn, TimerRequestCase,
    Node, RetryPolicy, Stream, StreamContext, TimeoutPolicy, Timer, TimerRequest,
    UserEventContext, UserEventSpec, Wait, Workflow,
)
from autoagent.core.runtime import UserEvent


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


def fail(value: Value) -> Value:
    raise ValueError("failed")


def mapped(context: InputMappingContext) -> list[Value]:
    return [{"value": index} for index in range(context.invocation_input["value"])]


def repaired(context: InputMappingContext) -> Value:
    return {"value": 42}


def first_group(context: InputMappingContext) -> AwaitAnyRequest:
    return AwaitAnyRequest(handles=[RuntimeHandle.model_validate(next(iter(context.incoming.values())))])


def observe(context: UserEventContext) -> UserEventSpec:
    return UserEventSpec(kind=context.phase, payload={"value": context.value,
        "unit_index": context.unit_index, "call_id": context.operator_call_id},
        persist=context.phase != "chunk")


class Sink:
    def __init__(self):
        self.events = []

    async def append_user_event(self, event):
        self.events.append(event)


async def pieces(value: Value) -> AsyncIterator[Value]:
    for number in range(value["value"]):
        yield {"value": number}


class Sum:
    def initial(self, context: StreamContext) -> Value:
        return {"value": 0}

    def add(self, context: StreamContext, state: Value, chunk: Value) -> Value:
        return {"value": state["value"] + chunk["value"]}

    def finish(self, context: StreamContext, state: Value) -> Value:
        return state


class UserEventLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_persistent_sink_failure_does_not_suppress_later_live_chunks(self):
        """An incomplete saved prefix does not disable the independent transient channel."""
        class BrokenHistory(Sink):
            async def append_user_event(self, event):
                if event.persist:
                    raise RuntimeError("history unavailable")
                await super().append_user_event(event)
        sink = BrokenHistory()
        workflow = Workflow("history-failure-live", nodes=[Node("first", identity, user_events=observe),
            Node("sum", pieces, stream=Stream(Sum()), user_events=observe)], edges=[Edge("first", "sum")])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
                result = await app.ainvoke(workflow, {"value": 3})
                self.assertIsInstance(app.user_event_sink_error, RuntimeError)
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual([e.sequence for e in sink.events], [None] * 3)
        self.assertEqual(len(caught), 1)

    async def test_native_signal_and_select_use_the_same_wait_contract(self):
        """Signal and Select waits publish their registered condition and selected outcome."""
        for command, value in (
            (AwaitSignal("messages"), None),
            (Select(), SelectRequest(cases={"signal": SignalRequestCase(endpoint="messages"),
                "later": TimerRequestCase(delay_us=10_000_000)}).model_dump()),
        ):
            with self.subTest(command=type(command).__name__):
                sink = Sink()
                workflow = Workflow("native-wait-events", nodes=[Node("wait", command, user_events=observe)],
                    signal_endpoints=[SignalEndpoint("messages", Value)])
                async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
                    result = await app.ainvoke(workflow, value)
                    await app.asignal(result.ref, "messages", {"value": 7}, source_id="test", sequence=1)
                    result = await app.ajoin(result.ref, timeout=2)
                    self.assertEqual(result.status, "completed", result.error)
                self.assertEqual([e.kind for e in sink.events], ["waiting", "wait_resolved", "output"])
                self.assertEqual(sink.events[1].payload["value"]["outcome"], "awakened")

    async def test_child_await_and_await_any_publish_wait_resolution(self):
        """Await and AwaitAny observe Child completion through the same per-Wait hook."""
        child = Workflow("event-child", nodes=[Node("ask", Wait(Value, Value))])
        for any_child in (False, True):
            with self.subTest(any_child=any_child):
                sink = Sink()
                nodes = ([Node("spawn", Spawn(child, "ask")), Node("await", AwaitAny(),
                    input_mapping=first_group, user_events=observe)] if any_child else
                    [Node("await", Await(child, "ask"), user_events=observe)])
                workflow = Workflow("event-parent", nodes=nodes,
                    edges=[Edge("spawn", "await")] if any_child else [])
                async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
                    result = await app.ainvoke(workflow, {"value": 1})
                    # Await also returns when a Child needs external input;
                    # the parent settles while that Child remains resumable.
                    self.assertEqual([e.kind for e in sink.events], ["waiting", "wait_resolved", "output"])
                    result = await app.aresume(result.ref, result.waits[0].id, {"value": 8})
                    self.assertEqual(result.status, "completed", result.error)
                self.assertEqual([e.kind for e in sink.events], ["waiting", "wait_resolved", "output"])
                self.assertEqual(sink.events[1].payload["value"]["outcome"], "awakened")

    async def test_successful_operator_retry_has_no_final_error_event(self):
        """Intermediate failed attempts do not masquerade as final Node failures."""
        calls = 0
        def flaky(value: Value) -> Value:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise ValueError("retry")
            return value
        sink = Sink()
        async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
            result = await app.ainvoke(Workflow("retry-observations", nodes=[Node("work", flaky,
                retry=RetryPolicy(max_attempts=2, backoff=BackoffPolicy(initial_seconds=0)), user_events=observe)]), {"value": 1})
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(calls, 3)
        self.assertEqual([e.kind for e in sink.events], ["output"])

    async def test_branch_failure_cancels_other_wait_without_reporting_it_as_error(self):
        """Fail-fast closure resolves pending interactions even outside their Node task."""
        async def delayed_failure(value: Value) -> Value:
            await asyncio.sleep(.02)
            raise ValueError("branch failed")
        sink = Sink()
        workflow = Workflow("parallel-wait-failure", nodes=[Node("start", identity),
            Node("ask", Wait(Value, Value), user_events=observe),
            Node("fail", delayed_failure, user_events=observe)],
            edges=[Edge("start", "ask"), Edge("start", "fail")])
        async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
            result = await app.ainvoke(workflow, {"value": 1})
            self.assertEqual(result.status, "failed", result.error)
        self.assertEqual(Counter(e.kind for e in sink.events), {"waiting": 1, "wait_resolved": 1, "error": 1})
        closed = next(e for e in sink.events if e.kind == "wait_resolved")
        self.assertEqual(closed.payload["value"]["outcome"], "cancelled")

    async def test_live_chunks_do_not_consume_history_sequence(self):
        """A complete result is history event 1 even after many transient chunks."""
        sink = Sink()
        workflow = Workflow("mixed-events", nodes=[Node("sum", pieces, stream=Stream(Sum()), user_events=observe)])
        async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
            result = await app.ainvoke(workflow, {"value": 20})
        self.assertEqual(result.output, {"value": 190})
        self.assertEqual([e.sequence for e in sink.events], [None] * 20 + [1])
        self.assertEqual([e.persist for e in sink.events], [False] * 20 + [True])
        for event in sink.events:
            self.assertEqual(UserEvent.from_record(event.to_record()), event)

    async def test_map_waits_publish_individually_before_aggregate(self):
        """Answering one unit closes its interaction while other units still wait."""
        sink = Sink()
        workflow = Workflow("mapped-questions", nodes=[Node("ask", Wait(Value, Value),
            map=Map(max_parallelism=1), input_mapping=mapped, user_events=observe)])
        async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
            result = await app.ainvoke(workflow, {"value": 3})
            self.assertEqual(Counter(e.kind for e in sink.events), {"waiting": 3})
            units = {e.payload["value"]["wait_id"]: e.payload["unit_index"] for e in sink.events}
            for wait in reversed(result.waits):
                result = await app.aresume(result.ref, wait.id, {"value": units[wait.id] + 10})
                resolved = [e for e in sink.events if e.kind == "wait_resolved"]
                self.assertEqual(resolved[-1].payload["value"]["wait_id"], wait.id)
                self.assertEqual(resolved[-1].payload["value"]["outcome"], "answered")
            self.assertEqual(result.output, [{"value": 10}, {"value": 11}, {"value": 12}])
            self.assertEqual(Counter(e.kind for e in sink.events), {"waiting": 3, "wait_resolved": 3, "output": 1})
            self.assertEqual([e.sequence for e in sink.events], list(range(1, 8)))

    async def test_invalid_response_does_not_resolve_wait(self):
        """A rejected reply cannot emit an answered interaction."""
        sink = Sink()
        async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
            result = await app.ainvoke(Workflow("invalid-reply", nodes=[Node("ask", Wait(Value, Value),
                user_events=observe)]), {"value": 1})
            with self.assertRaises((TypeError, ValueError)):
                await app.aresume(result.ref, result.waits[0].id, {"value": "invalid"})
            self.assertEqual([e.kind for e in sink.events], ["waiting"])
            await app.acancel(result.ref)
            self.assertEqual(sink.events[-1].payload["value"]["outcome"], "cancelled")

    async def test_timer_completion_is_awakening_not_timeout(self):
        """A timer's successful deadline is a normal Wait resolution."""
        sink = Sink()
        async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
            result = await app.ainvoke(Workflow("timer-events", nodes=[Node("timer", Timer(),
                user_events=observe)]), TimerRequest(delay_us=1000).model_dump())
            if result.status != "completed":
                result = await app.ajoin(result.ref, timeout=2)
            self.assertEqual(result.status, "completed", result.error)
        self.assertEqual([e.kind for e in sink.events], ["waiting", "wait_resolved", "output"])
        self.assertEqual(sink.events[1].payload["value"]["outcome"], "awakened")

    async def test_timeout_retry_closes_old_wait_and_registers_new_wait(self):
        """Retry timeouts create new Wait identities; error is only final Node failure."""
        sink = Sink()
        workflow = Workflow("wait-retry-events", nodes=[Node("ask", Wait(Value, Value),
            timeout=TimeoutPolicy(seconds=.02), retry=RetryPolicy(max_attempts=1,
                retry_on=("WaitTimeoutError",), backoff=BackoffPolicy(initial_seconds=0)), user_events=observe)])
        async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
            result = await app.ainvoke(workflow, {"value": 1})
            await asyncio.sleep(.08)
            result = await app.ajoin(result.ref, timeout=2)
            self.assertEqual(result.status, "failed", result.error)
        self.assertEqual(Counter(e.kind for e in sink.events), {"waiting": 2, "wait_resolved": 2, "error": 1})
        waits = [e.payload["value"]["wait_id"] for e in sink.events if e.kind == "waiting"]
        self.assertEqual(len(set(waits)), 2)
        self.assertTrue(all(e.payload["value"]["outcome"] == "timed_out" for e in sink.events if e.kind == "wait_resolved"))

    async def test_error_edge_is_reported_as_handled_node_error(self):
        """A selected Error Edge does not turn a handled Node failure into Invocation failure."""
        sink = Sink()
        workflow = Workflow("handled-error-events", nodes=[Node("bad", fail, user_events=observe),
            Node("repair", identity, input_mapping=repaired)], edges=[Edge("bad", "repair", on="error")])
        async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
            result = await app.ainvoke(workflow, {"value": 1})
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(len(sink.events), 1)
        self.assertEqual(sink.events[0].kind, "error")
        self.assertTrue(sink.events[0].payload["value"]["handled"])
        self.assertEqual(sink.events[0].payload["value"]["stage"], "operator")

    async def test_wait_mapper_can_inspect_state_outside_control_locks(self):
        """A lifecycle Mapper can reenter status without a lock or self-flush deadlock."""
        sink = Sink()
        async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
            async def observer(context: UserEventContext) -> UserEventSpec:
                handle = context.self_handle
                self.assertIsNotNone(handle)
                status = await asyncio.wait_for(app.astatus(InvocationRef(**handle.model_dump())), 1)
                return UserEventSpec(kind=context.phase, payload={"status": status.status})
            result = await asyncio.wait_for(app.ainvoke(Workflow("inspect-in-mapper", nodes=[
                Node("ask", Wait(Value, Value), user_events=observer)]), {"value": 1}), 2)
            self.assertEqual(sink.events[0].payload["status"], "waiting")
            await app.acancel(result.ref)

    async def test_recovery_does_not_register_existing_wait_again(self):
        """Loading and recovering a pending Wait reuses it without a new waiting event."""
        sink = Sink()
        workflow = Workflow("wait-observation-recovery", nodes=[Node("ask", Wait(Value, Value), user_events=observe)])
        async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
            result = await app.ainvoke(workflow, {"value": 1})
            checkpoint = await app.aunload_session(result.ref, capture_checkpoint=True)
            await app.aload_checkpoint(checkpoint)
            recovered = await app.arecover(result.ref)
            self.assertEqual([w.id for w in recovered.waits], [w.id for w in result.waits])
            self.assertEqual([e.kind for e in sink.events], ["waiting"])
            await app.aresume(result.ref, result.waits[0].id, {"value": 2})
            self.assertEqual(Counter(e.kind for e in sink.events), {"waiting": 1, "wait_resolved": 1, "output": 1})

    async def test_transient_sink_failure_does_not_stop_persistent_delivery(self):
        """A dropped live observation does not poison the ordered history stream."""
        class FailingLive(Sink):
            async def append_user_event(self, event):
                if not event.persist:
                    raise RuntimeError("live failed")
                await super().append_user_event(event)
        sink = FailingLive()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
                result = await app.ainvoke(Workflow("live-failure", nodes=[Node("sum", pieces,
                    stream=Stream(Sum()), user_events=observe)]), {"value": 2})
                self.assertIsNone(app.user_event_sink_error)
        self.assertEqual(result.status, "completed")
        self.assertEqual([e.sequence for e in sink.events], [1])
        self.assertEqual(len(caught), 2)


class UserEventPersistenceContractTests(unittest.TestCase):
    def test_history_sequence_and_persistence_intent_cannot_contradict(self):
        """Only persistent events have positive sequence numbers; bool flags stay strict."""
        for sequence, persist in ((1, False), (None, True), (0, True), (True, True)):
            with self.subTest(sequence=sequence, persist=persist), self.assertRaises(ValueError):
                UserEvent("session", "invocation", sequence, "event", {}, persist=persist)
        for value in (1, "false", None):
            with self.subTest(persist=value), self.assertRaises(ValueError):
                UserEventSpec(kind="event", payload={}, persist=value)
