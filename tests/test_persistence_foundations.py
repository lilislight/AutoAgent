from __future__ import annotations

import json
import unittest
from datetime import date, datetime, time, timezone
from decimal import Decimal
from uuid import uuid4

from pydantic import BaseModel

from autoagent import AutoAgentApp
from autoagent.compiler import WorkflowCompiler
from autoagent.operators import Operator
from autoagent.runtime import (
    ArtifactRef,
    JsonRuntimeSerializer,
    RuntimeCodec,
    RuntimeDeserializationError,
    RuntimeSerializationError,
)
from autoagent.workflow import (
    CapabilityRef,
    Node,
    NodePolicy,
    OperatorRef,
    TimeoutPolicy,
    Workflow,
)


def echo(value: str) -> str:
    return value


class Message(BaseModel):
    role: str
    content: str


class PersistenceIdentityTests(unittest.TestCase):
    def compile(self, workflow: Workflow):
        result = WorkflowCompiler().compile(workflow)
        self.assertTrue(result.ok, [item.model_dump() for item in result.diagnostics])
        self.assertIsNotNone(result.workflow_ir)
        self.assertIsNotNone(result.workflow_snapshot)
        return result.workflow_ir, result.workflow_snapshot

    def test_definition_hash_ignores_identity_and_display_metadata(self) -> None:
        first = Workflow(
            id="first",
            version=1,
            name="First display name",
            metadata={"owner": "one"},
            nodes=[
                Node(
                    id="echo",
                    capability=echo,
                    name="Echo A",
                    metadata={"color": "red"},
                )
            ],
        )
        second = Workflow(
            id="second",
            version=1,
            name="Second display name",
            metadata={"owner": "two"},
            nodes=[
                Node(
                    id="echo",
                    capability=echo,
                    name="Echo B",
                    metadata={"color": "blue"},
                )
            ],
        )

        first_ir, first_snapshot = self.compile(first)
        second_ir, second_snapshot = self.compile(second)

        self.assertEqual(first_ir.definition_hash, second_ir.definition_hash)
        self.assertEqual(first_snapshot.definition_hash, second_snapshot.definition_hash)
        self.assertNotEqual(
            first_snapshot.definition["name"],
            second_snapshot.definition["name"],
        )

    def test_definition_hash_changes_for_version_policy_and_contract(self) -> None:
        base_ir, _ = self.compile(
            Workflow(id="stable", version=1, nodes=[Node(id="echo", capability=echo)])
        )
        version_ir, _ = self.compile(
            Workflow(id="stable", version=2, nodes=[Node(id="echo", capability=echo)])
        )
        policy_ir, _ = self.compile(
            Workflow(
                id="stable",
                version=1,
                nodes=[
                    Node(
                        id="echo",
                        capability=echo,
                        policy=NodePolicy(timeout=TimeoutPolicy(timeout_ms=100)),
                    )
                ],
            )
        )

        def echo_integer(value: int) -> int:
            return value

        contract_ir, _ = self.compile(
            Workflow(
                id="stable",
                version=1,
                nodes=[Node(id="echo", capability=echo_integer)],
            )
        )

        self.assertNotEqual(base_ir.definition_hash, version_ir.definition_hash)
        self.assertNotEqual(base_ir.definition_hash, policy_ir.definition_hash)
        self.assertNotEqual(base_ir.definition_hash, contract_ir.definition_hash)

    def test_operator_manifest_uses_declared_contract_not_source_body(self) -> None:
        def first(value: str) -> str:
            return value.upper()

        def second(value: str) -> str:
            return value.lower()

        first_operator = Operator(
            id="normalize",
            version="1",
            handler=first,
            recovery_mode="replay_safe",
        )
        second_operator = Operator(
            id="normalize",
            version="1",
            handler=second,
            recovery_mode="replay_safe",
        )
        upgraded_operator = Operator(
            id="normalize",
            version="2",
            handler=second,
            recovery_mode="replay_safe",
        )

        self.assertEqual(first_operator.manifest, second_operator.manifest)
        self.assertNotEqual(
            first_operator.manifest.manifest_hash,
            upgraded_operator.manifest.manifest_hash,
        )
        self.assertEqual("replay_safe", first_operator.manifest.recovery_mode)

    def test_snapshot_collects_registered_operator_manifest(self) -> None:
        app = AutoAgentApp()

        @app.operator("echo_v2", version=2, recovery_mode="idempotent")
        def registered(value: str) -> str:
            return value

        workflow = Workflow(
            id="operator_snapshot",
            nodes=[Node(id="echo", capability=OperatorRef(id="echo_v2"))],
        )
        result = app.compiler.compile(workflow)

        self.assertTrue(result.ok)
        assert result.workflow_snapshot is not None
        manifests = {
            manifest.operator_id: manifest
            for manifest in result.workflow_snapshot.operator_manifests
        }
        self.assertEqual(2, manifests["echo_v2"].version)
        self.assertEqual("idempotent", manifests["echo_v2"].recovery_mode)
        json.dumps(result.workflow_snapshot.model_dump(mode="json"))

    def test_app_persists_definition_hash_on_invocation(self) -> None:
        workflow = Workflow(id="persisted_identity")
        workflow.add_node(echo, node_id="echo")
        app = AutoAgentApp()

        invocation = app.invoke(workflow, input={"value": "hello"}, session_id="s1")
        loaded = app.runtime_store.load_invocation(invocation.id)

        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(
            app.workflow_registry[workflow.id].workflow_ir.definition_hash,
            loaded.workflow_definition_hash,
        )
        self.assertEqual(
            app.workflow_registry[
                workflow.id
            ].workflow_snapshot.operator_manifest_hash,
            loaded.workflow_operator_manifest_hash,
        )

    def test_cached_ir_refreshes_late_bound_operator_manifest_environment(self) -> None:
        app = AutoAgentApp()

        @app.capability("search", operator_id="search_primary")
        def primary(query: str) -> str:
            return query

        workflow = Workflow(
            id="late_bound_manifest",
            nodes=[Node(id="search", capability=CapabilityRef(id="search"))],
        )
        first = app.invoke(workflow, input={"query": "first"}, session_id="one")

        @app.operator("search_secondary", capability="search")
        def secondary(query: str) -> str:
            return query

        second = app.invoke(workflow, input={"query": "second"}, session_id="two")

        self.assertEqual(
            first.workflow_definition_hash,
            second.workflow_definition_hash,
        )
        self.assertNotEqual(
            first.workflow_operator_manifest_hash,
            second.workflow_operator_manifest_hash,
        )
        self.assertEqual(2, len(app.runtime_store.workflow_versions))


class RuntimeSerializerTests(unittest.TestCase):
    def test_round_trip_supported_runtime_values(self) -> None:
        serializer = JsonRuntimeSerializer()
        now = datetime(2026, 7, 16, 12, 30, tzinfo=timezone.utc)
        artifact = ArtifactRef(
            uri="artifact://images/result.png",
            media_type="image/png",
            size_bytes=42,
            sha256="abc",
        )
        value = {
            "id": uuid4(),
            "created": now,
            "day": date(2026, 7, 16),
            "clock": time(12, 30),
            "cost": Decimal("1.25"),
            "tuple": (1, "two"),
            "set": {"a", "b"},
            "artifact": artifact,
            "message": Message(role="assistant", content="done"),
            "reserved": {"__autoagent_type__": "user-data"},
        }

        payload = serializer.dumps(value)
        restored = serializer.loads(payload)

        self.assertEqual(value["id"], restored["id"])
        self.assertEqual(now, restored["created"])
        self.assertEqual(Decimal("1.25"), restored["cost"])
        self.assertEqual((1, "two"), restored["tuple"])
        self.assertEqual({"a", "b"}, restored["set"])
        self.assertEqual(artifact, restored["artifact"])
        self.assertIsInstance(restored["message"], Message)
        self.assertEqual(value["reserved"], restored["reserved"])

    def test_fresh_process_requires_pydantic_registration_but_ui_view_does_not(self) -> None:
        writer = JsonRuntimeSerializer()
        payload = writer.dumps(Message(role="user", content="hello"))
        reader = JsonRuntimeSerializer()

        with self.assertRaisesRegex(
            RuntimeDeserializationError,
            "Pydantic runtime type is not registered",
        ):
            reader.loads(payload)
        self.assertEqual(
            {"role": "user", "content": "hello"},
            reader.json_view(payload),
        )

        reader.register_pydantic_model(Message)
        self.assertEqual(
            Message(role="user", content="hello"),
            reader.loads(payload),
        )

    def test_custom_codec_must_be_registered_after_restart(self) -> None:
        class Token:
            def __init__(self, value: str) -> None:
                self.value = value

            def __eq__(self, other: object) -> bool:
                return isinstance(other, Token) and self.value == other.value

        codec = RuntimeCodec(
            type_id="test.token.v1",
            python_type=Token,
            encode=lambda token: {"value": token.value},
            decode=lambda value: Token(value["value"]),
        )
        writer = JsonRuntimeSerializer()
        writer.register_codec(codec)
        payload = writer.dumps(Token("secret"))

        with self.assertRaisesRegex(RuntimeDeserializationError, "not registered"):
            JsonRuntimeSerializer().loads(payload)

        reader = JsonRuntimeSerializer()
        reader.register_codec(codec)
        self.assertEqual(Token("secret"), reader.loads(payload))

    def test_unsafe_or_oversized_values_are_rejected(self) -> None:
        serializer = JsonRuntimeSerializer(max_inline_bytes=32)

        with self.assertRaisesRegex(RuntimeSerializationError, "Raw bytes"):
            serializer.dumps(b"unsafe")
        with self.assertRaisesRegex(RuntimeSerializationError, "keys must be strings"):
            serializer.dumps({1: "value"})
        with self.assertRaisesRegex(RuntimeSerializationError, "ArtifactRef"):
            serializer.dumps("x" * 100)


if __name__ == "__main__":
    unittest.main()
