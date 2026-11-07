"""Opt-in User Event mapping is independent of streaming reduction and State."""

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import aclosing
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from pydantic import ValidationError
from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp, ContextOperation, ContextPatch, InputMappingContext, Map, Node, OutputBindingContext,
    Stream, StreamContext, UserEventContext, UserEventSpec, ValueContract,
    Workflow, WorkflowCompiler,
)


class Input(TypedDict):
    count: int


class Chunk(TypedDict):
    number: int


class Total(TypedDict):
    total: int


async def numbers(value: Input) -> AsyncIterator[Chunk]:
    for number in range(1, value["count"] + 1):
        await asyncio.sleep(0)
        yield {"number": number}


async def broken(value: Input) -> AsyncIterator[Chunk]:
    yield {"number": 1}
    raise ValueError("source failed")


def ordinary(value: Input) -> Total:
    return {"total": value["count"]}


def map_input(context: InputMappingContext) -> list[Input]:
    return [{"count": item["count"]} for item in context.invocation_input]


class Sum:
    def initial(self, context: StreamContext) -> Total:
        return {"total": 0}

    def add(self, context: StreamContext, state: Total, chunk: Chunk) -> Total:
        return {"total": state["total"] + chunk["number"]}

    def finish(self, context: StreamContext, state: Total) -> Total:
        return state


def events(context: UserEventContext) -> UserEventSpec:
    return UserEventSpec(kind="numbers." + context.phase, payload={
        "value": context.value, "node_id": context.node_id,
        "session_id": context.session_id, "invocation_id": context.invocation_id,
        "occurrence_id": context.occurrence_id, "unit_index": context.unit_index,
        "operator_call_id": context.operator_call_id, "operator_id": context.operator_id,
        "bound": context.invocation_context.get("bound", False),
    })


def only_output(context: UserEventContext) -> UserEventSpec | None:
    return events(context) if context.phase == "output" else None


async def only_chunks(context: UserEventContext) -> UserEventSpec | None:
    await asyncio.sleep(0)
    return events(context) if context.phase == "chunk" else None


def bad_mapper(context: UserEventContext) -> UserEventSpec | None:
    raise ValueError("intentional mapping failure")


def bad_return(context: UserEventContext) -> UserEventSpec:
    return {"kind": "wrong", "payload": None}  # type: ignore[return-value]


def corrupt_spec(context: UserEventContext) -> UserEventSpec:
    spec = UserEventSpec(kind="corrupt", payload={})
    spec.payload["number"] = float("nan")
    return spec


def wrong_context(context: OutputBindingContext) -> UserEventSpec:
    return UserEventSpec(kind="wrong", payload=None)


def wrong_annotation(context: UserEventContext) -> Total:
    return {"total": 0}


def bind(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(invocation=(ContextOperation.set("bound", True),))


def fail_binding(context: OutputBindingContext) -> ContextPatch:
    raise ValueError("binding failed")


def immutable(context: UserEventContext) -> UserEventSpec:
    try:
        context.value["number" if context.phase == "chunk" else "total"] = 999
    except TypeError:
        pass
    else:
        raise AssertionError("Mutable User Event value")
    try:
        context.invocation_context["injected"] = True
    except TypeError:
        pass
    else:
        raise AssertionError("Mutable Invocation Context")
    return UserEventSpec(kind="immutable", payload=context.value)


class Sink:
    def __init__(self):
        self.events = []

    async def append_user_event(self, event):
        self.events.append(event)


class UserEventTests(unittest.IsolatedAsyncioTestCase):
    async def run_node(self, node, value=None):
        sink = Sink()
        async with aclosing(AutoAgentApp(user_event_sink=sink)) as app:
            result = await app.ainvoke(Workflow("user-event-demo", nodes=[node]),
                                       {"count": 3} if value is None else value)
        return result, sink.events

    async def test_no_mapper_emits_no_events_and_does_not_serialize_chunks(self):
        """Reduction works without User Events or per-chunk record serialization."""
        serialized_chunks = []
        original = ValueContract._to_record_validated

        def spy(contract, value):
            if contract.annotation is Chunk:
                serialized_chunks.append(value)
            return original(contract, value)

        with patch.object(ValueContract, "_to_record_validated", spy):
            result, observed = await self.run_node(Node("sum", numbers, stream=Stream(Sum())))
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(result.output, {"total": 6})
        self.assertEqual(observed, [])
        self.assertEqual(serialized_chunks, [])

    async def test_single_mapper_receives_chunks_then_committed_output(self):
        """One Mapper chooses dynamic kinds and sees Output Binding only after Node completion."""
        result, observed = await self.run_node(Node("sum", numbers, stream=Stream(Sum()),
            output_binding=bind, user_events=events))
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual([event.kind for event in observed], ["numbers.chunk"] * 3 + ["numbers.output"])
        self.assertEqual([event.sequence for event in observed], [1, 2, 3, 4])
        self.assertTrue(all(not event.payload["bound"] for event in observed[:-1]))
        self.assertTrue(observed[-1].payload["bound"])
        self.assertEqual(observed[-1].payload["value"], {"total": 6})
        for event in observed:
            self.assertEqual(event.payload["invocation_id"], result.invocation_id)
            self.assertEqual(event.payload["session_id"], result.ref.session_id)
            self.assertEqual(event.payload["occurrence_id"], event.occurrence_id)
        self.assertTrue(all(event.payload["operator_call_id"] for event in observed[:-1]))
        self.assertIsNone(observed[-1].payload["operator_call_id"])

    async def test_none_filters_chunks_and_async_mapper_can_filter_final_output(self):
        """Returning None suppresses an event, including either phase independently."""
        for mapper, kinds in ((only_output, ["numbers.output"]), (only_chunks, ["numbers.chunk"] * 3)):
            with self.subTest(mapper=mapper.__name__):
                result, observed = await self.run_node(Node("sum", numbers, stream=Stream(Sum()), user_events=mapper))
                self.assertEqual(result.status, "completed", result.error)
                self.assertEqual([event.kind for event in observed], kinds)
                self.assertEqual(observed[0].sequence, 1)

    async def test_ordinary_node_receives_only_the_output_phase(self):
        """A non-streaming Operator uses the same Mapper after producing its final output."""
        result, observed = await self.run_node(Node("plain", ordinary, user_events=events))
        self.assertEqual(result.output, {"total": 3})
        self.assertEqual([event.kind for event in observed], ["numbers.output"])

    async def test_map_chunks_identify_each_call_and_output_is_aggregated_once(self):
        """Parallel Map Chunk callbacks retain Call and Unit identities while output is per Node."""
        result, observed = await self.run_node(Node("sum", numbers, map=Map(max_parallelism=2),
            input_mapping=map_input, stream=Stream(Sum()), user_events=events), [{"count": 2}, {"count": 3}])
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(result.output, [{"total": 3}, {"total": 6}])
        grouped = defaultdict(list)
        calls = defaultdict(set)
        for event in observed[:-1]:
            grouped[event.payload["unit_index"]].append(event.payload["value"]["number"])
            calls[event.payload["unit_index"]].add(event.payload["operator_call_id"])
        self.assertEqual(dict(grouped), {0: [1, 2], 1: [1, 2, 3]})
        self.assertEqual(len(calls[0] | calls[1]), 2)
        self.assertEqual(observed[-1].kind, "numbers.output")
        self.assertEqual(list(observed[-1].payload["value"]), result.output)

    async def test_mapper_cannot_mutate_values_or_context_used_by_reducer(self):
        """Read-only callback views protect stream aggregation and canonical Context."""
        result, observed = await self.run_node(Node("sum", numbers, stream=Stream(Sum()), user_events=immutable))
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(result.output, {"total": 6})
        self.assertEqual(len(observed), 4)

    async def test_recovered_map_units_validate_their_individual_stream_outputs(self):
        """Completed Map units restore their reduced result without replaying chunks or using the batch contract."""
        from autoagent.core.executor import NodeExecutor
        from autoagent.core.runtime import OperatorCallStarted

        node = WorkflowCompiler().compile_or_raise(Workflow("recovered", nodes=[
            Node("sum", numbers, map=Map(), input_mapping=map_input, stream=Stream(Sum())),
        ])).node("sum")
        calls = []

        async def observe(event):
            if isinstance(event, OperatorCallStarted):
                calls.append(event.unit_index)

        executor = NodeExecutor()
        try:
            result = await executor.execute(node, "occurrence", [{"count": 2}, {"count": 3}],
                completed_calls={0: SimpleNamespace(output={"total": 3})}, on_call_event=observe)
        finally:
            executor.close()
        self.assertEqual(result.output, [{"total": 3}, {"total": 6}])
        self.assertEqual(calls, [1])

    async def test_mapping_failures_log_diagnostics_without_synthetic_events(self):
        """Exceptions, invalid return objects and invalid JSON do not create events or fail the Node."""
        for mapper in (bad_mapper, bad_return, corrupt_spec):
            with self.subTest(mapper=mapper.__name__), self.assertLogs(
                    "autoagent.core.executor.workflow_executor", level="WARNING") as logs:
                result, observed = await self.run_node(Node("sum", numbers, stream=Stream(Sum()), user_events=mapper))
                self.assertEqual(result.status, "completed", result.error)
                self.assertEqual(result.output, {"total": 6})
                self.assertEqual(observed, [])
                self.assertEqual(len(logs.records), 4)

    async def test_failed_source_or_output_binding_has_no_output_event(self):
        """Final-output events require successful Node completion, even after partial stream observations."""
        for source, binding in ((broken, None), (numbers, fail_binding)):
            with self.subTest(source=source.__name__):
                result, observed = await self.run_node(Node("sum", source, stream=Stream(Sum()),
                    output_binding=binding, user_events=events))
                self.assertEqual(result.status, "failed")
                self.assertTrue(observed)
                self.assertTrue(all(event.kind == "numbers.chunk" for event in observed[:-1]))
                self.assertEqual(observed[-1].kind, "numbers.error")


class UserEventContractTests(unittest.TestCase):
    def test_mapper_signature_and_return_type_are_checked_at_compile_time(self):
        """Only the single typed Mapper API is accepted; obsolete tuple mappings are rejected."""
        for mapper, code in ((wrong_context, "USER_EVENT_MAPPING_SIGNATURE"),
                             (wrong_annotation, "USER_EVENT_MAPPING_RETURN"),
                             ((events,), "NODE_CONFIGURATION_INVALID")):
            with self.subTest(mapper=mapper):
                result = WorkflowCompiler().compile(Workflow("invalid", nodes=[Node("plain", ordinary, user_events=mapper)]))
                self.assertFalse(result.ok)
                self.assertIn(code, [diagnostic.code for diagnostic in result.diagnostics])

    def test_spec_validates_event_names_and_owns_finite_json_payloads(self):
        """Payload construction rejects unsupported data and detaches valid mutable caller data."""
        for kind, payload in ((" ", {}), ("valid", {"bad": float("inf")}), ("valid", object())):
            with self.subTest(kind=kind, payload=type(payload).__name__), self.assertRaises(ValidationError):
                UserEventSpec(kind=kind, payload=payload)
        payload = {"values": [1]}
        spec = UserEventSpec(kind="valid", payload=payload)
        payload["values"].append(2)
        self.assertEqual(spec.payload, {"values": [1]})

    def test_workflow_snapshot_records_the_mapper_and_changes_revision(self):
        """Mapper identity participates in the Workflow definition without an obsolete mapping list."""
        plain = WorkflowCompiler().compile_or_raise(Workflow("snapshot", nodes=[Node("n", ordinary)]))
        mapped = WorkflowCompiler().compile_or_raise(Workflow("snapshot", nodes=[Node("n", ordinary, user_events=events)]))
        self.assertNotEqual(plain.workflow_revision_id, mapped.workflow_revision_id)
        from autoagent import WorkflowDefinitionSnapshot

        snapshot = WorkflowDefinitionSnapshot.from_workflow_ir(mapped)
        record = snapshot.to_record()
        self.assertEqual(record["schema_version"], 2)
        self.assertIn("events", record["definition"]["nodes"][0]["user_events"]["name"])
        self.assertEqual(WorkflowDefinitionSnapshot.from_record(record), snapshot)


if __name__ == "__main__":
    unittest.main()
