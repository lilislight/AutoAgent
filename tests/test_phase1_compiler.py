from __future__ import annotations

import json
import unittest
from typing import Iterator
from typing_extensions import TypedDict

from autoagent.core import (
    AggregationContext,
    Capability,
    ChildHandle,
    CompileResult,
    ConditionContext,
    ContextPatch,
    Edge,
    InputMappingContext,
    Map,
    Node,
    Operator,
    OutputBindingContext,
    Stream,
    StreamContext,
    SubWorkflow,
    Workflow,
    WorkflowCompileError,
    WorkflowCompiler,
    WorkflowDefinitionSnapshot,
    Wait,
    workflow_hook,
)


class Value(TypedDict):
    value: int


class OtherValue(TypedDict):
    value: int


class Accumulator(TypedDict):
    values: list[int]


def identity(value: Value) -> Value:
    return value


def other(value: OtherValue) -> OtherValue:
    return value


def mapping(context: InputMappingContext) -> Value:
    return next(iter(context.incoming.values()))  # type: ignore[return-value]


def map_inputs(context: InputMappingContext) -> list[Value]:
    return list(context.incoming.values())  # type: ignore[return-value]


def aggregate(context: AggregationContext) -> Value:
    return {"value": sum(item["value"] for item in context.outputs)}  # type: ignore[index]


def bind(context: OutputBindingContext) -> ContextPatch | None:
    return ContextPatch() if context.output is not None else None


def condition(context: ConditionContext) -> bool:
    return context.output is not None


def stream_values(value: Value) -> Iterator[Value]:
    yield value


class Reducer:
    def initial(self, context: StreamContext) -> Accumulator:
        return {"values": []}

    def add(self, context: StreamContext, state: Accumulator, chunk: Value) -> Accumulator:
        return {"values": [*state["values"], chunk["value"]]}

    def finish(self, context: StreamContext, state: Accumulator) -> Value:
        return {"value": sum(state["values"])}


class CallableMapping:
    @workflow_hook(version="callable-1")
    def __call__(self, context: InputMappingContext) -> Value:
        return next(iter(context.incoming.values()))  # type: ignore[return-value]


class CompilerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.compiler = WorkflowCompiler()

    def test_compile_serial_workflow_and_revision_is_deterministic(self) -> None:
        """Verify compile serial workflow and revision is deterministic."""
        workflow = Workflow(
            "serial",
            nodes=[Node("a", identity), Node("b", identity)],
            edges=[Edge("a", "b")],
        )
        first = self.compiler.compile_or_raise(workflow)
        second = self.compiler.compile_or_raise(workflow)
        self.assertEqual(first, second)
        self.assertEqual(first.entry_node_ids, ("a",))
        self.assertEqual(first.exit_node_ids, ("b",))
        self.assertEqual(first.workflow_revision_id, second.workflow_revision_id)
        self.assertEqual(
            first.workflow_revision_id,
            f"{first.workflow_id}:{first.definition_hash}",
        )

        @workflow_hook(version="2")
        def changed_identity(value: Value) -> Value:
            return value

        changed_identity.__name__ = "identity"

        changed = self.compiler.compile_or_raise(
            Workflow(
                "serial",
                nodes=[Node("a", changed_identity), Node("b", changed_identity)],
                edges=[Edge("a", "b")],
            )
        )
        self.assertNotEqual(first.definition_hash, changed.definition_hash)

    def test_operator_runtime_version_does_not_change_workflow_revision(self) -> None:
        """Verify Operator deployment versions are separate from Workflow hooks."""

        first = self.compiler.compile_or_raise(
            Workflow(
                "operator-version",
                nodes=[Node("operator", Operator(identity, version="1"))],
            )
        )
        second = self.compiler.compile_or_raise(
            Workflow(
                "operator-version",
                nodes=[Node("operator", Operator(identity, version="2"))],
            )
        )
        self.assertEqual(first.definition_hash, second.definition_hash)

    def test_revision_uses_callable_name_contract_and_hook_version_not_path(self) -> None:
        """Verify callable import paths do not affect semantic revision identity."""

        def make_operator(module: str):
            @workflow_hook(version="2")
            def evaluate(value: Value) -> Value:
                return value

            evaluate.__module__ = module
            return evaluate

        first = self.compiler.compile_or_raise(
            Workflow("stable-path", nodes=[Node("evaluate", make_operator("first.path"))])
        )
        second = self.compiler.compile_or_raise(
            Workflow("stable-path", nodes=[Node("evaluate", make_operator("other.path"))])
        )
        self.assertEqual(first.definition_hash, second.definition_hash)
        self.assertEqual(first.workflow_revision_id, second.workflow_revision_id)

        @workflow_hook(version="3")
        def evaluate(value: Value) -> Value:
            return value

        changed = self.compiler.compile_or_raise(
            Workflow("stable-path", nodes=[Node("evaluate", evaluate)])
        )
        self.assertNotEqual(first.definition_hash, changed.definition_hash)

    def test_hook_name_and_version_participate_in_revision(self) -> None:
        """Verify custom hook name and explicit version change graph semantics."""

        def make_condition(name: str, version: str):
            @workflow_hook(version=version)
            def route(context: ConditionContext) -> bool:
                return context.output is not None

            route.__name__ = name
            route.__module__ = f"temporary.{name}"
            return route

        def compile_with(condition_handler):
            return self.compiler.compile_or_raise(
                Workflow(
                    "hook-revision",
                    nodes=[Node("a", identity), Node("b", identity)],
                    edges=[Edge("a", "b", condition_handler)],
                )
            )

        first = compile_with(make_condition("route", "1"))
        relocated = compile_with(make_condition("route", "1"))
        renamed = compile_with(make_condition("choose", "1"))
        versioned = compile_with(make_condition("route", "2"))
        self.assertEqual(first.definition_hash, relocated.definition_hash)
        self.assertNotEqual(first.definition_hash, renamed.definition_hash)
        self.assertNotEqual(first.definition_hash, versioned.definition_hash)

    def test_compile_returns_portable_workflow_definition_snapshot(self) -> None:
        """Verify successful compilation exposes a JSON-compatible definition snapshot."""

        @workflow_hook(version="2")
        def route(context: ConditionContext) -> bool:
            return context.output is not None

        result = self.compiler.compile(
            Workflow(
                "portable",
                nodes=[Node("a", identity), Node("b", identity)],
                edges=[Edge("a", "b", route)],
            )
        )
        self.assertTrue(result.ok)
        snapshot = result.workflow_definition_snapshot
        self.assertIsInstance(snapshot, WorkflowDefinitionSnapshot)
        assert snapshot is not None and result.workflow_ir is not None
        self.assertEqual(snapshot.definition_hash, result.workflow_ir.definition_hash)
        record = snapshot.to_record()
        encoded = json.dumps(record, sort_keys=True)
        self.assertNotIn(__name__, encoded)
        condition_record = record["definition"]["edges"][0]["condition"]
        self.assertEqual(condition_record["name"], "route")
        self.assertEqual(condition_record["version"], "2")
        self.assertIn("contract", condition_record)

        decoded = WorkflowDefinitionSnapshot.from_record(
            json.loads(json.dumps(record))
        )
        self.assertEqual(decoded, snapshot)

    def test_workflow_snapshot_rejects_tampering_and_external_mutation(self) -> None:
        """Verify portable Snapshot identity is detached and content-addressed."""

        result = self.compiler.compile_or_raise(
            Workflow("snapshot-integrity", nodes=[Node("node", identity)])
        )
        snapshot = WorkflowDefinitionSnapshot.from_workflow_ir(result)
        source_definition = snapshot.to_record()["definition"]
        rebuilt = WorkflowDefinitionSnapshot(
            schema_version=snapshot.schema_version,
            workflow_id=snapshot.workflow_id,
            workflow_version=snapshot.workflow_version,
            workflow_revision_id=snapshot.workflow_revision_id,
            definition_hash=snapshot.definition_hash,
            definition=source_definition,
        )
        source_definition["workflow_id"] = "changed"
        self.assertEqual(rebuilt.workflow_id, rebuilt.definition["workflow_id"])

        corrupted = snapshot.to_record()
        corrupted["definition"]["workflow_id"] = "changed"
        with self.assertRaises(ValueError):
            WorkflowDefinitionSnapshot.from_record(corrupted)

    def test_workflow_hook_rejects_invalid_versions(self) -> None:
        """Verify hook versions are explicit non-empty strings or integers."""

        with self.assertRaisesRegex(ValueError, "cannot be empty"):
            workflow_hook(version=" ")
        with self.assertRaisesRegex(TypeError, "string or integer"):
            workflow_hook(version=True)

    def test_workflow_version_requires_non_empty_string_or_integer(self) -> None:
        """Verify Workflow versions cannot be coerced from invalid values."""

        for version in (True, None, 1.5, "", " "):
            with self.subTest(version=version):
                result = self.compiler.compile(
                    Workflow(
                        "version",
                        nodes=[Node("node", identity)],
                        version=version,  # type: ignore[arg-type]
                    )
                )
                self.assertFalse(result.ok)
                self.assertEqual(
                    [item.code for item in result.diagnostics],
                    ["WORKFLOW_VERSION_INVALID"],
                )

        for version, expected in ((2, "2"), ("release-2", "release-2")):
            with self.subTest(version=version):
                ir = self.compiler.compile_or_raise(
                    Workflow("version", nodes=[Node("node", identity)], version=version)
                )
                self.assertEqual(ir.workflow_version, expected)

    def test_compile_collects_independent_definition_errors(self) -> None:
        """Verify compile collects independent definition errors."""
        invalid = Workflow(
            "multi-error",
            nodes=[Node("same", identity), Node("same", identity)],
            edges=[
                Edge("missing-a", "same", id="first"),
                Edge("same", "missing-b", id="second"),
            ],
        )
        result = self.compiler.compile(invalid)
        self.assertFalse(result.ok)
        codes = [item.code for item in result.diagnostics]
        self.assertIn("NODE_ID_DUPLICATE", codes)
        self.assertEqual(codes.count("EDGE_ENDPOINT_UNKNOWN"), 2)

    def test_multiple_entries_exits_and_fan_out_remain_explicit_in_ir(self) -> None:
        """Verify multiple entries exits and fan out remain explicit in ir."""
        ir = self.compiler.compile_or_raise(
            Workflow(
                "boundaries",
                nodes=[Node(name, identity) for name in ("entry-a", "entry-b", "left", "right")],
                edges=[Edge("entry-a", "left", condition), Edge("entry-a", "right", condition)],
            )
        )
        self.assertEqual(ir.entry_node_ids, ("entry-a", "entry-b"))
        self.assertEqual(ir.exit_node_ids, ("entry-b", "left", "right"))
        self.assertEqual(tuple(edge.target for edge in ir.outgoing("entry-a")), ("left", "right"))

    def test_direct_edge_requires_same_nominal_contract(self) -> None:
        """Verify direct edge requires same nominal contract."""
        with self.assertRaisesRegex(WorkflowCompileError, "CONTRACT_MISMATCH"):
            self.compiler.compile_or_raise(
                Workflow(
                    "mismatch",
                    nodes=[Node("a", identity), Node("b", other)],
                    edges=[Edge("a", "b")],
                )
            )

    def test_multi_input_and_error_target_require_mapping(self) -> None:
        """Verify multi input and error target require mapping."""
        for workflow in (
            Workflow(
                "join",
                nodes=[Node("a", identity), Node("b", identity), Node("c", identity)],
                edges=[Edge("a", "c"), Edge("b", "c")],
            ),
            Workflow(
                "error",
                nodes=[Node("a", identity), Node("c", identity)],
                edges=[Edge("a", "c", on="error")],
            ),
        ):
            with self.subTest(workflow=workflow.id):
                with self.assertRaisesRegex(WorkflowCompileError, "INPUT_MAPPING_REQUIRED"):
                    self.compiler.compile_or_raise(workflow)

    def test_multi_input_with_mapping_compiles(self) -> None:
        """Verify multi input with mapping compiles."""
        ir = self.compiler.compile_or_raise(
            Workflow(
                "join",
                nodes=[
                    Node("a", identity),
                    Node("b", identity),
                    Node("c", identity, input_mapping=mapping),
                ],
                edges=[Edge("a", "c"), Edge("b", "c")],
            )
        )
        self.assertEqual(ir.entry_node_ids, ("a", "b"))

    def test_one_source_target_allows_only_one_edge(self) -> None:
        """Verify one source target allows only one edge."""
        with self.assertRaisesRegex(WorkflowCompileError, "EDGE_DUPLICATE_ENDPOINTS"):
            self.compiler.compile_or_raise(
                Workflow(
                    "duplicate",
                    nodes=[Node("a", identity), Node("b", identity)],
                    edges=[Edge("a", "b"), Edge("a", "b", on="error")],
                )
            )

    def test_unknown_endpoint_and_invalid_condition_are_rejected(self) -> None:
        """Verify unknown endpoint and invalid condition are rejected."""
        with self.assertRaisesRegex(WorkflowCompileError, "EDGE_ENDPOINT_UNKNOWN"):
            self.compiler.compile_or_raise(
                Workflow("unknown", nodes=[Node("a", identity)], edges=[Edge("a", "b")])
            )

        def bad_condition(context: ConditionContext) -> Value:
            return {"value": 1}

        with self.assertRaisesRegex(WorkflowCompileError, "CONDITION_RETURN"):
            self.compiler.compile_or_raise(
                Workflow(
                    "condition",
                    nodes=[Node("a", identity), Node("b", identity)],
                    edges=[Edge("a", "b", bad_condition)],
                )
            )

    def test_map_and_stream_contracts_compile(self) -> None:
        """Verify map and stream contracts compile."""
        mapped = self.compiler.compile_or_raise(
            Workflow(
                "map",
                nodes=[Node("map", identity, input_mapping=map_inputs, map=Map(max_parallelism=2))],
            )
        )
        self.assertTrue(mapped.node("map").output_contract.name.startswith("list["))
        streamed = self.compiler.compile_or_raise(
            Workflow(
                "stream",
                nodes=[
                    Node(
                        "stream",
                        stream_values,
                        stream=Stream(Reducer()),
                    )
                ],
            )
        )
        self.assertIs(streamed.node("stream").output_contract.annotation, Value)

        aggregated = self.compiler.compile_or_raise(
            Workflow(
                "aggregate",
                nodes=[
                    Node(
                        "map",
                        identity,
                        input_mapping=map_inputs,
                        output_binding=bind,
                        map=Map(aggregate=aggregate, max_parallelism=2),
                    )
                ],
            )
        )
        self.assertIs(aggregated.node("map").output_contract.annotation, Value)

    def test_map_wait_combination_is_rejected(self) -> None:
        """Verify one Wait occurrence cannot suspend multiple mapped items."""

        with self.assertRaisesRegex(WorkflowCompileError, "MAP_WAIT_UNSUPPORTED"):
            self.compiler.compile_or_raise(
                Workflow(
                    "map-wait",
                    nodes=[
                        Node(
                            "approval",
                            Wait(Value, OtherValue),
                            input_mapping=map_inputs,
                            map=Map(max_parallelism=2),
                        )
                    ],
                )
            )

    def test_hook_contracts_are_checked(self) -> None:
        """Verify hook contracts are checked."""
        def bad_mapping(context: ConditionContext) -> Value:
            return {"value": 1}

        def bad_binding(context: OutputBindingContext) -> Value:
            return {"value": 1}

        for node, code in (
            (Node("bad-map", identity, input_mapping=bad_mapping), "INPUT_MAPPING_SIGNATURE"),
            (Node("bad-bind", identity, output_binding=bad_binding), "OUTPUT_BINDING_RETURN"),
        ):
            with self.subTest(code=code):
                with self.assertRaisesRegex(WorkflowCompileError, code):
                    self.compiler.compile_or_raise(Workflow(code, nodes=[node]))

    def test_callable_object_hook_uses_bound_signature_everywhere(self) -> None:
        """Verify callable-object hook validation and snapshot contracts agree."""

        result = self.compiler.compile(
            Workflow(
                "callable-hook",
                nodes=[Node("node", identity, input_mapping=CallableMapping())],
            )
        )
        self.assertTrue(result.ok)
        assert result.workflow_definition_snapshot is not None
        mapping_record = result.workflow_definition_snapshot.definition["nodes"][0][
            "input_mapping"
        ]
        self.assertEqual(mapping_record["name"], "CallableMapping")
        self.assertEqual(len(mapping_record["contract"]["parameters"]), 1)
        self.assertEqual(mapping_record["version"], "callable-1")

    def test_reserved_runtime_id_delimiters_are_rejected(self) -> None:
        """Verify author ids cannot collide with composite Runtime identities."""

        invalid_definitions = (
            (
                Workflow("bad:workflow", nodes=[Node("node", identity)]),
                "WORKFLOW_ID_RESERVED",
            ),
            (
                Workflow("node-id", nodes=[Node("bad@node", identity)]),
                "NODE_ID_RESERVED",
            ),
            (
                Workflow(
                    "edge-id",
                    nodes=[Node("a", identity), Node("b", identity)],
                    edges=[Edge("a", "b", id="bad/edge")],
                ),
                "EDGE_ID_RESERVED",
            ),
            (
                Workflow(
                    "namespace-id",
                    sub_workflows=[
                        SubWorkflow(
                            "bad:part",
                            Workflow("child", nodes=[Node("node", identity)]),
                        )
                    ],
                ),
                "SUBWORKFLOW_ID_RESERVED",
            ),
            (
                Workflow(
                    "default-edge",
                    nodes=[
                        Node("a", identity),
                        Node("b->c", identity),
                        Node("a->b", identity),
                        Node("c", identity),
                    ],
                    edges=[Edge("a", "b->c"), Edge("a->b", "c")],
                ),
                "NODE_ID_RESERVED",
            ),
        )
        for workflow, code in invalid_definitions:
            with self.subTest(code=code):
                with self.assertRaisesRegex(WorkflowCompileError, code):
                    self.compiler.compile_or_raise(workflow)

    def test_node_occurrence_limit_is_not_an_authoring_field(self) -> None:
        """Verify Loop safety is not encoded as a per-Node authoring limit."""

        with self.assertRaises(TypeError):
            Node(  # type: ignore[call-arg]
                "node",
                identity,
                max_occurrences_per_invocation=2,
            )

    def test_compile_result_has_stable_agent_facing_diagnostic(self) -> None:
        """Verify compile result has stable agent facing diagnostic."""
        workflow = Workflow(
            "invalid",
            nodes=[Node("source", identity)],
            edges=[Edge("source", "missing", id="broken")],
        )
        result = self.compiler.compile(workflow)
        self.assertIsInstance(result, CompileResult)
        self.assertFalse(result.ok)
        diagnostic = result.diagnostics[0]
        self.assertEqual(diagnostic.code, "EDGE_ENDPOINT_UNKNOWN")
        self.assertEqual(diagnostic.object_id, "broken")
        self.assertEqual(diagnostic.field, "target")
        self.assertEqual(result.to_diagnostic_json(), self.compiler.compile(workflow).to_diagnostic_json())
        with self.assertRaisesRegex(WorkflowCompileError, "EDGE_ENDPOINT_UNKNOWN"):
            result.require_workflow_ir()

    def test_unreachable_cycle_is_rejected(self) -> None:
        """Verify unreachable cycle is rejected."""
        workflow = Workflow(
            "unreachable",
            nodes=[Node(name, identity) for name in ("start", "finish", "a", "b")],
            edges=[Edge("start", "finish"), Edge("a", "b"), Edge("b", "a")],
        )
        with self.assertRaisesRegex(WorkflowCompileError, "WORKFLOW_UNREACHABLE_NODE"):
            self.compiler.compile_or_raise(workflow)

    def test_inline_subworkflow_is_namespaced_and_expanded(self) -> None:
        """Verify inline subworkflow is namespaced and expanded."""
        child = Workflow(
            "child",
            nodes=[Node("a", identity), Node("b", identity)],
            edges=[Edge("a", "b")],
        )
        parent = Workflow("parent", sub_workflows=[SubWorkflow("part", child)])
        ir = self.compiler.compile_or_raise(parent)
        self.assertEqual(tuple(node.id for node in ir.nodes), ("part.a", "part.b"))
        self.assertEqual(ir.edges[0].source, "part.a")

    def test_recursive_subworkflow_and_child_invocation_are_rejected(self) -> None:
        """Verify recursive subworkflow and child invocation are rejected."""
        inline = Workflow("inline")
        inline.sub_workflows.append(SubWorkflow("self", inline))
        with self.assertRaisesRegex(WorkflowCompileError, "SUBWORKFLOW_RECURSION"):
            self.compiler.compile_or_raise(inline)

        child = Workflow("child")
        child.nodes.append(Node("self", child))
        with self.assertRaisesRegex(WorkflowCompileError, "CHILD_WORKFLOW_RECURSION"):
            self.compiler.compile_or_raise(child)

    def test_child_workflow_executable_requires_unambiguous_boundary(self) -> None:
        """Verify child workflow executable requires unambiguous boundary."""
        child = Workflow("child", nodes=[Node("only", identity)])
        ir = self.compiler.compile_or_raise(Workflow("parent", nodes=[Node("child", child)]))
        self.assertEqual(ir.node("child").executable.workflow_id, "child")
        ambiguous = Workflow("ambiguous", nodes=[Node("a", identity), Node("b", identity)])
        with self.assertRaisesRegex(WorkflowCompileError, "CHILD_WORKFLOW_BOUNDARY_AMBIGUOUS"):
            self.compiler.compile_or_raise(
                Workflow("bad-parent", nodes=[Node("child", ambiguous)])
            )

    def test_spawn_child_has_durable_handle_contract(self) -> None:
        """Verify spawn child has durable handle contract."""
        child = Workflow("child", nodes=[Node("only", identity)])
        ir = self.compiler.compile_or_raise(
            Workflow(
                "parent",
                nodes=[Node("child", child, execution_mode="spawn")],
            )
        )
        self.assertIs(ir.node("child").output_contract.annotation, ChildHandle)
        with self.assertRaisesRegex(WorkflowCompileError, "EXECUTION_MODE_NOT_WORKFLOW"):
            self.compiler.compile_or_raise(
                Workflow("bad-spawn", nodes=[Node("node", identity, execution_mode="spawn")])
            )

    def test_wait_and_capability_compile_through_the_same_node_boundary(self) -> None:
        """Verify wait and capability compile through the same node boundary."""
        wait_ir = self.compiler.compile_or_raise(
            Workflow("wait", nodes=[Node("approval", Wait(Value, OtherValue))])
        )
        self.assertIs(wait_ir.node("approval").input_contract.annotation, Value)
        self.assertIs(wait_ir.node("approval").output_contract.annotation, OtherValue)

        capability = Capability("identity", Operator(identity, id="first").contract)
        capability_ir = self.compiler.compile_or_raise(
            Workflow("capability", nodes=[Node("choose", capability)])
        )
        self.assertIs(capability_ir.node("choose").output_contract.annotation, Value)


if __name__ == "__main__":
    unittest.main()
