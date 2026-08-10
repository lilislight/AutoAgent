from __future__ import annotations

import json
import unittest
from unittest.mock import patch
from dataclasses import dataclass
from enum import Enum
from uuid import UUID, uuid4

from autoagent.core import (
    BackoffPolicy,
    ContextPatch,
    MapPolicy,
    NodePolicy,
    Operator,
    RecoveryPolicy,
    ReplicationPolicy,
    ResourcePolicy,
    RetryPolicy,
    RuntimeEvent,
    StateOperation,
    StreamPolicy,
    TimeoutPolicy,
    UserEvent,
    WaitOperator,
    InputMappingContext,
)
from autoagent.core.operators import OperatorContract, ValueContract
from autoagent.core.runtime import (
    RuntimeSerializationError,
    RuntimeValueCodec,
    StateOperationBatch,
    apply_patch,
    decode_runtime_value,
    encode_runtime_value,
    patches_conflict,
    hook_context,
)
from autoagent.core.scheduler import (
    EdgeActivation,
    EdgeResolution,
    LoopIteration,
    NodeExecutionRequest,
    occurrence_key,
    scope_key,
)


class Colour(Enum):
    RED = "red"


@dataclass(frozen=True)
class Payload:
    name: str
    values: tuple[int, ...]


class RuntimeSerializationTests(unittest.TestCase):
    def test_checkpoint_values_round_trip_exact_container_types(self) -> None:
        identifier = uuid4()
        value = {
            "uuid": identifier,
            "enum": Colour.RED,
            "payload": Payload("sample", (1, 2)),
            "tuple": ("a", 1),
            "set": {1, 2},
            "frozen": frozenset({"x", "y"}),
        }
        restored = decode_runtime_value(encode_runtime_value(value))
        self.assertEqual(restored, value)
        self.assertIsInstance(restored["tuple"], tuple)
        self.assertIsInstance(restored["set"], set)
        self.assertIsInstance(restored["frozen"], frozenset)

    def test_checkpoint_values_reject_non_string_mapping_keys(self) -> None:
        with self.assertRaisesRegex(RuntimeSerializationError, "keys must be str"):
            encode_runtime_value({1: "one"})

    def test_checkpoint_encoding_rejects_local_types(self) -> None:
        @dataclass
        class LocalValue:
            value: int

        with self.assertRaisesRegex(RuntimeSerializationError, "Local type"):
            encode_runtime_value(LocalValue(1))

    def test_checkpoint_decoding_rejects_unknown_marker_and_type(self) -> None:
        marker = "__autoagent_runtime_type__"
        with self.assertRaisesRegex(RuntimeSerializationError, "cannot be imported"):
            decode_runtime_value(
                {marker: "dataclass", "class": "missing.module:Value", "value": {}}
            )
        with self.assertRaisesRegex(RuntimeSerializationError, "Unknown"):
            decode_runtime_value({marker: "future", "class": "builtins:int", "value": 1})


class ContextTests(unittest.TestCase):
    def test_hook_context_is_detached_from_live_values(self) -> None:
        live = {"nested": {"value": 1}}
        context = hook_context(
            InputMappingContext,
            workflow_id="workflow",
            workflow_revision_id="revision",
            workflow_path=(),
            session_id="session",
            invocation_id=str(uuid4()),
            session_context=live,
            invocation_context={},
            invocation_input={"value": 2},
            node_id="node",
            node_execution_id=str(uuid4()),
        )
        isolated = context.session_context["nested"]
        isolated["value"] = 9
        self.assertEqual(live["nested"]["value"], 1)
        with self.assertRaises(TypeError):
            context.session_context["new"] = 1  # type: ignore[index]

    def test_runtime_value_codec_keeps_isolation_separate_from_json(self) -> None:
        original = Payload("sample", (1, 2))
        isolated = RuntimeValueCodec.isolate(original)
        captured = RuntimeValueCodec.capture(original)
        self.assertEqual(isolated, original)
        self.assertIsNot(isolated, original)
        self.assertEqual(
            RuntimeValueCodec.decode(captured.persistent_value()), original
        )
        self.assertIsNot(captured.clone_for_user(), captured.clone_for_user())

    def test_apply_patch_recursively_merges_without_aliasing(self) -> None:
        session = {"profile": {"name": "Ada"}}
        invocation: dict[str, object] = {}
        source = {"profile": {"age": 37}}
        apply_patch(session, invocation, ContextPatch(session=source))
        source["profile"]["age"] = 99
        self.assertEqual(session, {"profile": {"name": "Ada", "age": 37}})

    def test_patch_conflicts_include_equal_and_prefix_paths(self) -> None:
        self.assertTrue(
            patches_conflict(
                ContextPatch(invocation={"profile": {"name": "Ada"}}),
                ContextPatch(invocation={"profile": {"name": "Grace"}}),
            )
        )
        self.assertTrue(
            patches_conflict(
                ContextPatch(invocation={"profile": "replace"}),
                ContextPatch(invocation={"profile": {"age": 37}}),
            )
        )
        self.assertFalse(
            patches_conflict(
                ContextPatch(invocation={"profile": {"name": "Ada"}}),
                ContextPatch(invocation={"profile": {"age": 37}}),
            )
        )


class OperatorContractTests(unittest.TestCase):
    def test_no_argument_operator_ignores_no_input_only(self) -> None:
        def handler() -> int:
            return 1

        contract = OperatorContract.from_callable(handler)
        self.assertEqual(contract.prepare_call(None), ((), {}))

    def test_mapping_input_binds_multiple_named_parameters(self) -> None:
        def add(left: int, right: int) -> int:
            return left + right

        operator = Operator.from_callable(add)
        args, kwargs = operator.contract.prepare_call({"left": "2", "right": 3})
        self.assertEqual(args, ())
        self.assertEqual(kwargs, {"left": 2, "right": 3})

    def test_single_mapping_parameter_can_receive_mapping_as_one_value(self) -> None:
        def size(value: dict[str, int]) -> int:
            return len(value)

        contract = OperatorContract.from_callable(size)
        args, kwargs = contract.prepare_call({"a": 1})
        self.assertEqual(args, ({"a": 1},))
        self.assertEqual(kwargs, {})

    def test_invalid_operator_input_and_output_are_rejected(self) -> None:
        def integer(value: int) -> int:
            return value

        contract = OperatorContract.from_callable(integer)
        with self.assertRaises(TypeError):
            contract.prepare_call({"unexpected": 1})
        with self.assertRaises(TypeError):
            contract.output.validate("not-an-int")

    def test_operator_rejects_empty_id_and_non_callable_handler(self) -> None:
        def identity(value: int) -> int:
            return value

        with self.assertRaises(ValueError):
            Operator(identity, id=" ")
        with self.assertRaises(TypeError):
            Operator(1, id="bad")  # type: ignore[arg-type]

    def test_operator_id_defaults_to_callable_name(self) -> None:
        def identity(value: int) -> int:
            return value

        self.assertEqual(Operator(identity).id, "identity")

    def test_value_contract_rejects_untyped_any_and_bare_containers(self) -> None:
        for annotation in (object, list, dict):
            with self.assertRaises(TypeError):
                ValueContract.create(annotation)

    def test_wait_operator_requires_explicit_safe_types(self) -> None:
        wait = WaitOperator(str, bool)
        self.assertEqual(wait.request_contract.validate("question"), "question")
        self.assertIs(wait.response_contract.validate(True), True)
        with self.assertRaises(TypeError):
            WaitOperator(str, object).response_contract

    def test_stream_policy_requires_a_reducer_class(self) -> None:
        def factory() -> object:
            return object()

        with self.assertRaises(TypeError):
            StreamPolicy(factory)  # type: ignore[arg-type]


class PolicyValidationTests(unittest.TestCase):
    def test_retry_recovery_timeout_and_parallelism_require_positive_values(self) -> None:
        for constructor in (
            lambda: RetryPolicy(max_attempts=0),
            lambda: RecoveryPolicy(max_attempts=0),
            lambda: TimeoutPolicy(timeout_ms=0),
            lambda: MapPolicy(max_parallelism=0),
            lambda: ReplicationPolicy(count=0),
        ):
            with self.assertRaises(ValueError):
                constructor()

    def test_node_policy_rejects_map_and_replication_together(self) -> None:
        with self.assertRaises(ValueError):
            NodePolicy(map=MapPolicy(), replication=ReplicationPolicy(1))

    def test_backoff_rejects_invalid_delays_and_multiplier(self) -> None:
        for policy in (
            lambda: BackoffPolicy(initial_delay_ms=-1),
            lambda: BackoffPolicy(max_delay_ms=-1),
            lambda: BackoffPolicy(multiplier=0),
        ):
            with self.assertRaises(ValueError):
                policy()

    def test_resource_policy_retains_explicit_limits(self) -> None:
        policy = ResourcePolicy(1, 2, 3)
        self.assertEqual(policy.max_node_executions_per_invocation, 1)
        self.assertEqual(policy.max_operator_attempts_per_invocation, 2)
        self.assertEqual(policy.max_runtime_ms_per_invocation, 3)


class SchedulerModelAndEventTests(unittest.TestCase):
    def test_scope_and_occurrence_keys_are_stable(self) -> None:
        scope = (LoopIteration("outer", 2), LoopIteration("inner", 3))
        self.assertEqual(scope_key(()), "root")
        self.assertEqual(scope_key(scope), "outer:2/inner:3")
        self.assertEqual(occurrence_key("node", scope), "node@outer:2/inner:3")

    def test_scheduler_records_round_trip(self) -> None:
        activation = EdgeActivation("edge", "source", uuid4())
        request = NodeExecutionRequest(
            "target", (LoopIteration("loop", 2),), (activation,), recovery_attempt=1
        )
        resolution = EdgeResolution("edge", request.scope, True, activation)
        self.assertEqual(NodeExecutionRequest.from_record(request.to_record()), request)
        self.assertEqual(EdgeResolution.from_record(resolution.to_record()), resolution)

    def test_runtime_and_user_event_records_preserve_identity(self) -> None:
        invocation_id = uuid4()
        runtime = RuntimeEvent(
            workflow_id="workflow",
            workflow_revision_id="workflow:revision",
            session_id="session",
            invocation_id=invocation_id,
            sequence=1,
            event_name="changed",
            subject_type="node",
            subject_id="node",
            operation_batches=(
                StateOperationBatch(
                    1, (StateOperation("add", ("value",), 1),)
                ),
            ),
        )
        user = UserEvent(
            workflow_id="workflow",
            workflow_revision_id="workflow:revision",
            session_id="session",
            invocation_id=invocation_id,
            sequence=1,
            type="message",
            data={"value": 1},
            node_id="node",
        )
        self.assertEqual(RuntimeEvent.from_record(runtime.to_record()), runtime)
        self.assertEqual(UserEvent.from_record(user.to_record()), user)
        json.dumps(runtime.to_record())
        json.dumps(user.to_record())

    def test_detached_event_reuses_captured_persistence_values(self) -> None:
        runtime = RuntimeEvent.detached(
            workflow_id="workflow",
            workflow_revision_id="workflow:revision",
            session_id="session",
            invocation_id=uuid4(),
            sequence=1,
            event_name="changed",
            subject_type="node",
            subject_id="node",
            payload={"large": list(range(100))},
            operation_batches=(
                StateOperationBatch(
                    1,
                    (
                        StateOperation(
                            "replace", ("context", "large"), list(range(100))
                        ),
                    ),
                ),
            ),
        )
        with patch(
            "autoagent.core.runtime.serialization.encode_runtime_value",
            side_effect=AssertionError("captured values were encoded twice"),
        ):
            first = runtime.to_record()
            second = runtime.to_record()
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
