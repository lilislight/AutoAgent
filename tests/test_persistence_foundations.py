from __future__ import annotations

import json
import unittest
from datetime import date, datetime, time, timezone
from decimal import Decimal
from uuid import uuid4

from pydantic import BaseModel

from autoagent import AutoAgentApp, AutoAgentSettings
from autoagent.core.compiler import WorkflowCompiler
from autoagent.core.operators import Operator
from autoagent.core.runtime import (
    ArtifactRef,
    JsonRuntimeSerializer,
    RuntimeCodec,
    RuntimeDeserializationError,
    RuntimeSerializationError,
)
from autoagent.core.workflow import (
    CapabilityRef,
    Node,
    NodePolicy,
    OperatorRef,
    TimeoutPolicy,
    Workflow,
    workflow_hook,
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

    def test_explicit_hook_version_changes_definition_hash(self) -> None:
        def condition_for(version: int):
            @workflow_hook(version=version)
            def condition(_ctx) -> bool:
                return True

            return condition

        first = Workflow(id="hook_version", version=1)
        first.add_node(echo, node_id="start")
        first.add_node(echo, node_id="finish")
        first.add_edge("start", "finish", condition=condition_for(1))
        second = Workflow(id="hook_version", version=1)
        second.add_node(echo, node_id="start")
        second.add_node(echo, node_id="finish")
        second.add_edge("start", "finish", condition=condition_for(2))

        first_ir, first_snapshot = self.compile(first)
        second_ir, second_snapshot = self.compile(second)

        self.assertNotEqual(first_ir.definition_hash, second_ir.definition_hash)
        self.assertEqual(
            1,
            first_snapshot.definition["edges"][0]["condition"]["version"],
        )
        self.assertEqual(
            2,
            second_snapshot.definition["edges"][0]["condition"]["version"],
        )

    def test_workflow_hook_rejects_invalid_versions(self) -> None:
        with self.assertRaisesRegex(ValueError, "cannot be empty"):
            workflow_hook(version=" ")
        with self.assertRaisesRegex(TypeError, "string or integer"):
            workflow_hook(version=True)

    def test_operator_version_does_not_create_a_second_workflow_revision(
        self,
    ) -> None:
        def first(value: str) -> str:
            return value.upper()

        def second(value: str) -> str:
            return value.lower()

        first_workflow = Workflow(
            id="operator_version",
            nodes=[
                Node(
                    id="normalize",
                    capability=Operator(
                        id="normalize",
                        version="1",
                        handler=first,
                    ),
                )
            ],
        )
        second_workflow = Workflow(
            id="operator_version",
            nodes=[
                Node(
                    id="normalize",
                    capability=Operator(
                        id="normalize",
                        version="2",
                        handler=second,
                    ),
                )
            ],
        )

        _, first_snapshot = self.compile(first_workflow)
        _, second_snapshot = self.compile(second_workflow)

        self.assertEqual(
            first_snapshot.definition_hash,
            second_snapshot.definition_hash,
        )

    def test_snapshot_records_fixed_operator_identity_and_contracts(self) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())

        @app.operator("echo_v2", version=2)
        def registered(value: str) -> str:
            return value

        workflow = Workflow(
            id="operator_snapshot",
            nodes=[Node(id="echo", capability=OperatorRef(id="echo_v2"))],
        )
        result = app.compiler.compile(workflow)

        self.assertTrue(result.ok)
        assert result.workflow_snapshot is not None
        node = result.workflow_snapshot.definition["nodes"][0]
        self.assertEqual(
            {"kind": "operator", "id": "echo_v2"},
            node["capability"],
        )
        self.assertEqual(
            "string",
            node["input_contract"]["json_schema"]["properties"]["value"]["type"],
        )
        self.assertEqual(
            "string",
            node["operator_output_contract"]["json_schema"]["type"],
        )
        json.dumps(result.workflow_snapshot.model_dump(mode="json"))

    def test_app_persists_definition_hash_on_invocation(self) -> None:
        workflow = Workflow(id="persisted_identity")
        workflow.add_node(echo, node_id="echo")
        app = AutoAgentApp(settings=AutoAgentSettings())
        self.addCleanup(app.close)
        app.start()

        invocation = app.invoke(workflow, input={"value": "hello"}, session_id="s1")
        session = app.runtime_store.find_session(
            workflow_revision_id=invocation.workflow_revision_id,
            session_key="s1",
        )
        assert session is not None
        loaded = session.get_current_invocation()

        self.assertIsNotNone(loaded)
        assert loaded is not None
        entry = app.workflow_registry[invocation.workflow_revision_id]
        self.assertEqual(
            entry.workflow_ir.definition_hash,
            loaded.workflow_definition_hash,
        )

    def test_late_bound_capability_operator_does_not_change_revision(self) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        self.addCleanup(app.close)

        @app.capability("search", operator_id="search_primary")
        def primary(query: str) -> str:
            return query

        workflow = Workflow(
            id="late_bound_manifest",
            nodes=[Node(id="search", capability=CapabilityRef(id="search"))],
        )
        app.register_workflow(workflow)
        app.start()
        first = app.invoke(workflow, input={"query": "first"}, session_id="one")

        @app.operator("search_secondary", capability="search")
        def secondary(query: str) -> str:
            return query

        second = app.invoke(workflow, input={"query": "second"}, session_id="two")

        self.assertEqual(
            first.workflow_definition_hash,
            second.workflow_definition_hash,
        )
        self.assertEqual(
            first.workflow_revision_id,
            second.workflow_revision_id,
        )
        self.assertEqual(1, len(app.runtime_store.workflow_versions))

    def test_registered_workflow_uses_fixed_ir_without_recompiling(self) -> None:
        def finish(value: str) -> str:
            return f"finished:{value}"

        workflow = Workflow(id="mutable_workflow_source")
        workflow.add_node(echo, node_id="echo")
        app = AutoAgentApp(settings=AutoAgentSettings())
        self.addCleanup(app.close)
        app.start()

        first = app.invoke(workflow, input={"value": "one"}, session_id="first")
        first_hash = first.workflow_definition_hash

        workflow.add_node(
            finish,
            node_id="finish",
            input_mapping=lambda ctx: {"value": ctx.outputs.latest("echo")},
        )
        workflow.add_edge("echo", "finish")

        second = app.invoke(
            workflow,
            input={"value": "two"},
            session_id="second",
        )

        self.assertEqual(first.result, {"output": "one"})
        self.assertEqual(second.result, {"output": "two"})
        self.assertEqual(1, len(second.node_executions))
        self.assertEqual(
            app.workflow_registry[
                second.workflow_revision_id
            ].workflow_ir.definition_hash,
            first_hash,
        )


class RuntimeSerializerTests(unittest.TestCase):
    def test_explicit_pydantic_type_id_is_used_for_writes_and_aliases_decode(
        self,
    ) -> None:
        serializer = JsonRuntimeSerializer()
        serializer.register_pydantic_model(Message, type_id="test.message.v1")
        serializer.register_pydantic_model(Message, type_id="__main__:Message")

        payload = serializer.dumps(Message(role="user", content="stable"))
        self.assertIn(b'"type_id":"test.message.v1"', payload)
        legacy_payload = (
            b'{"__autoagent_type__":"pydantic","type_id":"__main__:Message",'
            b'"value":{"content":"legacy","role":"user"}}'
        )
        self.assertEqual(
            Message(role="user", content="legacy"),
            serializer.loads(legacy_payload),
        )

    def test_app_owns_runtime_type_registration(self) -> None:
        class Token:
            def __init__(self, value: str) -> None:
                self.value = value

        codec = RuntimeCodec(
            type_id="tests.token",
            python_type=Token,
            encode=lambda value: {"value": value.value},
            decode=lambda value: Token(value["value"]),
        )
        app = AutoAgentApp(
            runtime_codecs=(codec,),
            runtime_models=(Message,),
        )

        token = app.runtime_serializer.loads(
            app.runtime_serializer.dumps(Token("registered"))
        )
        message = app.runtime_serializer.loads(
            app.runtime_serializer.dumps(Message(role="user", content="hello"))
        )

        self.assertIsInstance(token, Token)
        self.assertEqual("registered", token.value)
        self.assertEqual(Message(role="user", content="hello"), message)

    def test_app_rejects_serializer_different_from_store_owner(self) -> None:
        from autoagent.core.runtime import RuntimeStore

        store = RuntimeStore(serializer=JsonRuntimeSerializer())
        with self.assertRaisesRegex(ValueError, "owned by runtime_store"):
            AutoAgentApp(
                runtime_store=store,
                runtime_serializer=JsonRuntimeSerializer(),
            )

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
        self.assertEqual(
            {
                "__autoagent_artifact__": {
                    "id": str(artifact.id),
                    "kind": "artifact",
                    "storage": "external",
                    "uri": "artifact://images/result.png",
                    "media_type": "image/png",
                    "encoding": None,
                    "size_bytes": 42,
                    "sha256": "abc",
                    "metadata": {},
                }
            },
            serializer.json_view(serializer.dumps(artifact)),
        )
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
