from __future__ import annotations

import unittest
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any, TypedDict

from autoagent.core import (
    AutoAgentApp,
    ContextPatch,
    Edge,
    ExecutionContext,
    InvocationState,
    Node,
    NodePolicy,
    StreamPolicy,
    UserEventMapping,
    WaitOperator,
    Workflow,
    WorkflowCompileError,
    WorkflowCompiler,
)
from autoagent.core.runtime.serialization import RuntimeSerializationError
from tests.helpers import identity_int, identity_str


@dataclass(frozen=True)
class SafeRecord:
    name: str
    values: list[int]


class SafePayload(TypedDict):
    name: str
    count: int


class IntReducer:
    def __init__(self) -> None:
        self._values: list[int] = []

    def add(self, chunk: int) -> None:
        self._values.append(chunk)

    def finish(self) -> int:
        return sum(self._values)


class WrongFinishReducer:
    def add(self, _chunk: int) -> None:
        pass

    def finish(self) -> int:
        return "wrong"  # type: ignore[return-value]


class WrongChunkReducer:
    def add(self, _chunk: str) -> None:
        pass

    def finish(self) -> str:
        return ""


class ConstructorArgumentReducer:
    def __init__(self, _required: int) -> None:
        pass

    def add(self, _chunk: int) -> None:
        pass

    def finish(self) -> int:
        return 0


def record_identity(value: SafeRecord) -> SafeRecord:
    return value


def payload_identity(value: SafePayload) -> SafePayload:
    return value


def unannotated(value):  # type: ignore[no-untyped-def]
    return value


def any_input(value: Any) -> int:
    return 1


def object_output(value: int) -> object:
    return value


def bare_list(value: list) -> list:  # type: ignore[type-arg]
    return value


def unsafe_mapping(value: dict[int, str]) -> dict[int, str]:
    return value


def lying_output(value: int) -> int:
    return "wrong"  # type: ignore[return-value]


def unsafe_patch(_context: ExecutionContext, value: int) -> ContextPatch:
    return ContextPatch(invocation={"unsafe": object(), "value": value})


def false_int_condition(_context: ExecutionContext) -> bool:
    return 1  # type: ignore[return-value]


def wrong_user_event(_value: int) -> dict[str, int]:
    return {"count": "wrong"}  # type: ignore[dict-item]


def wait_request_mapping(context: ExecutionContext) -> int:
    return int(context.invocation_input)


def wait_response_binding(
    _context: ExecutionContext, response: str
) -> ContextPatch:
    return ContextPatch(session={"last_response": response})


def wrong_wait_request_mapping(_context: ExecutionContext) -> str:
    return "wrong"


def int_stream(_value: None) -> Iterator[int]:
    return iter((1, 2, 3))


def wrong_int_stream(_value: None) -> Iterator[int]:
    return iter((1, "wrong"))  # type: ignore[arg-type]


def declared_stream_returns_list(_value: None) -> Iterator[int]:
    return [1, 2]  # type: ignore[return-value]


def ordinary_int(_value: None) -> int:
    return 1


class StrictCompilationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.compiler = WorkflowCompiler()

    def assert_operator_rejected(self, operator: object, message: str) -> None:
        with self.assertRaisesRegex(WorkflowCompileError, message):
            self.compiler.compile(
                Workflow("invalid-contract", nodes=[Node("node", operator)])  # type: ignore[arg-type]
            )

    def test_accepts_nested_dataclass_and_typed_dict_contracts(self) -> None:
        for workflow_id, operator in (
            ("dataclass", record_identity),
            ("typed-dict", payload_identity),
        ):
            ir = self.compiler.compile(Workflow(workflow_id, nodes=[Node("node", operator)]))
            self.assertTrue(ir.node("node").input_schema)

    def test_rejects_missing_any_object_and_bare_annotations(self) -> None:
        for operator, message in (
            (unannotated, "explicit type annotation"),
            (any_input, "Any or object"),
            (object_output, "Any or object"),
            (bare_list, "bare container"),
            (unsafe_mapping, "mapping keys must be str"),
        ):
            self.assert_operator_rejected(operator, message)

    def test_rejects_local_durable_model_type(self) -> None:
        @dataclass
        class LocalValue:
            value: int

        def local_identity(value: LocalValue) -> LocalValue:
            return value

        self.assert_operator_rejected(local_identity, "cannot be resolved")

    def test_wait_requires_safe_request_and_response_types(self) -> None:
        for wait in (WaitOperator(Any, str), WaitOperator(str, object)):
            with self.assertRaisesRegex(WorkflowCompileError, "Any or object"):
                self.compiler.compile(Workflow("wait", nodes=[Node("wait", wait)]))

    def test_wait_mapping_and_binding_must_match_request_and_response(self) -> None:
        with self.assertRaisesRegex(WorkflowCompileError, "request_type"):
            self.compiler.compile(
                Workflow(
                    "bad-wait-mapping",
                    nodes=[
                        Node(
                            "wait",
                            WaitOperator(int, str),
                            input_mapping=wrong_wait_request_mapping,
                        )
                    ],
                )
            )

    def test_stream_policy_must_match_a_stream_and_its_chunk(self) -> None:
        with self.assertRaisesRegex(WorkflowCompileError, "does not return"):
            self.compiler.compile(
                Workflow(
                    "ordinary-stream-policy",
                    nodes=[
                        Node(
                            "node",
                            ordinary_int,
                            policy=NodePolicy(stream=StreamPolicy(IntReducer)),
                        )
                    ],
                )
            )
        with self.assertRaisesRegex(WorkflowCompileError, "chunk does not match"):
            self.compiler.compile(
                Workflow(
                    "wrong-chunk-reducer",
                    nodes=[
                        Node(
                            "node",
                            int_stream,
                            policy=NodePolicy(stream=StreamPolicy(WrongChunkReducer)),
                        )
                    ],
                )
            )
        with self.assertRaisesRegex(WorkflowCompileError, "constructible without arguments"):
            self.compiler.compile(
                Workflow(
                    "stateful-constructor",
                    nodes=[
                        Node(
                            "node",
                            int_stream,
                            policy=NodePolicy(stream=StreamPolicy(ConstructorArgumentReducer)),
                        )
                    ],
                )
            )


class StrictRuntimeTests(unittest.TestCase):
    def test_rejects_nonserializable_invocation_input_before_execution(self) -> None:
        app = AutoAgentApp()
        workflow = Workflow("input-boundary", nodes=[Node("node", identity_int)])
        app.register_workflow(workflow)
        with self.assertRaises(RuntimeSerializationError):
            app.invoke(workflow, object())
        self.assertEqual(app._sessions, {})
        app.close()

    def test_operator_return_must_match_declared_contract(self) -> None:
        app = AutoAgentApp()
        workflow = Workflow("lying-output", nodes=[Node("node", lying_output)])
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, 1)
        self.assertEqual(invocation.state, InvocationState.FAILED)
        self.assertIn("validation error", invocation.error.message)
        app.close()

    def test_output_binding_patch_must_be_durably_serializable(self) -> None:
        app = AutoAgentApp()
        workflow = Workflow(
            "unsafe-patch",
            nodes=[Node("node", identity_int, output_binding=unsafe_patch)],
        )
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, 1, session_id="session")
        self.assertEqual(invocation.state, InvocationState.FAILED)
        self.assertEqual(app._sessions["session"].context, {})
        app.close()

    def test_edge_condition_runtime_type_is_not_truthiness_coerced(self) -> None:
        app = AutoAgentApp()
        workflow = Workflow(
            "condition-contract",
            nodes=[Node("start", identity_int), Node("finish", identity_int)],
            edges=[Edge("start", "finish", false_int_condition)],
        )
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, 1)
        self.assertEqual(invocation.state, InvocationState.FAILED)
        self.assertIn("must return bool", invocation.error.message)
        app.close()

    def test_user_event_transform_contract_failure_is_isolated(self) -> None:
        app = AutoAgentApp()
        workflow = Workflow(
            "user-event-contract",
            nodes=[
                Node(
                    "node",
                    identity_int,
                    user_event_mappings=(
                        UserEventMapping("agent_output", wrong_user_event),
                    ),
                )
            ],
        )
        app.register_workflow(workflow)
        stream = app.stream_invoke(workflow, 1)
        events = list(stream)
        self.assertEqual(stream.invocation.result(), {"node": 1})
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "user_event_mapping_failed")
        self.assertIn("validation error", events[0].data["error"])
        app.close()

    def test_wait_response_is_checked_before_output_binding(self) -> None:
        app = AutoAgentApp()
        workflow = Workflow("wait-response", nodes=[Node("wait", WaitOperator(str, str))])
        app.register_workflow(workflow)
        waiting = app.invoke(workflow, "question")
        with self.assertRaisesRegex(TypeError, "validation error"):
            app.resume(waiting, waiting.waits[0].id, 42)
        self.assertEqual(waiting.state, InvocationState.WAITING)
        completed = app.resume(waiting, waiting.waits[0].id, "answer")
        self.assertEqual(completed.result(), {"wait": "answer"})
        app.close()

    def test_wait_uses_normal_input_mapping_and_output_binding(self) -> None:
        app = AutoAgentApp()
        workflow = Workflow(
            "wait-hooks",
            nodes=[
                Node(
                    "wait",
                    WaitOperator(int, str),
                    input_mapping=wait_request_mapping,
                    output_binding=wait_response_binding,
                )
            ],
        )
        app.register_workflow(workflow)
        waiting = app.invoke(workflow, "7", session_id="session")
        self.assertEqual(waiting.latest_checkpoint.waits[0].payload, 7)
        completed = app.resume(waiting, waiting.waits[0].id, "approved")
        self.assertEqual(completed.result(), {"wait": "approved"})
        self.assertEqual(
            app._sessions["session"].context, {"last_response": "approved"}
        )
        app.close()

    def test_stream_runtime_shape_chunk_and_final_result_are_checked(self) -> None:
        for workflow_id, operator, reducer in (
            ("not-stream-at-runtime", declared_stream_returns_list, IntReducer),
            ("wrong-stream-chunk", wrong_int_stream, IntReducer),
            ("wrong-stream-final", int_stream, WrongFinishReducer),
        ):
            app = AutoAgentApp()
            workflow = Workflow(
                workflow_id,
                nodes=[
                    Node(
                        "node",
                        operator,
                        policy=NodePolicy(stream=StreamPolicy(reducer)),
                    )
                ],
            )
            app.register_workflow(workflow)
            invocation = app.invoke(workflow, None)
            self.assertEqual(invocation.state, InvocationState.FAILED)
            app.close()


if __name__ == "__main__":
    unittest.main()
