from __future__ import annotations

import asyncio
import unittest
from typing import Any

import autoagent
from pydantic import BaseModel, ConfigDict, ValidationError

from autoagent import (
    CapabilityRef,
    CapabilitySelectionPolicy,
    ArtifactRef,
    Node,
    NodePolicy,
    Operator,
    OperatorRef,
    StreamingResult,
    Workflow,
)
from autoagent.core.operators import OperatorContractWarning
from tests.helpers import isolated_app, started_app


class SearchRequest(BaseModel):
    query: str
    limit: int = 10


class DatabaseConnection:
    pass


class InvalidRuntimeModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    connection: DatabaseConnection


class DynamicRuntimeModel(BaseModel):
    value: Any


class OperatorRegistrationTests(unittest.TestCase):
    def test_streaming_result_contract_uses_final_output_annotation(self) -> None:
        def stream() -> str | StreamingResult[int, str]:
            return "normal"

        operator = Operator.from_callable(stream)

        self.assertIs(operator.contract.output.annotation, str)
        self.assertEqual(
            operator.contract.output.json_schema,
            {"type": "string"},
        )

    def test_public_registration_requires_an_explicit_app(self) -> None:
        self.assertFalse(hasattr(autoagent, "get_default_app"))
        self.assertFalse(hasattr(autoagent, "operator"))
        self.assertFalse(hasattr(autoagent, "capability"))

    def test_operator_requires_parameter_and_return_annotations(self) -> None:
        app = isolated_app()

        def missing_parameter(value) -> str:
            return str(value)

        def missing_return(value: str):
            return value

        with self.assertRaisesRegex(ValueError, "Parameter 'value'.*annotation"):
            app.register_operator(missing_parameter, operator_id="missing_parameter")
        with self.assertRaisesRegex(ValueError, "return type annotation"):
            app.register_operator(missing_return, operator_id="missing_return")

    def test_app_capability_decorator_registers_contract_and_default_operator(self) -> None:
        app = isolated_app()

        @app.capability("web_search", operator_id="builtin_search")
        def search(query: str) -> str:
            return f"builtin:{query}"

        registered_capability = app.capability_registry.get("web_search")
        self.assertIsNotNone(registered_capability)
        assert registered_capability is not None
        self.assertIsNotNone(registered_capability.contract)
        assert registered_capability.contract is not None
        self.assertEqual(
            registered_capability.contract.input.json_schema["required"],
            ["query"],
        )
        self.assertEqual(
            registered_capability.contract.input.json_schema["properties"]["query"][
                "type"
            ],
            "string",
        )
        registered = app.operator_registry.get("builtin_search")
        self.assertIsNotNone(registered)
        assert registered is not None
        self.assertIs(registered.handler, search)
        self.assertEqual(registered.capability_id, "web_search")
        self.assertEqual(
            app.operator_registry.default_for_capability("web_search").id,
            "builtin_search",
        )

    def test_register_capability_without_operator_cannot_compile_reference(self) -> None:
        app = isolated_app()
        app.register_capability("web_search")
        workflow = Workflow(
            id="missing_implementation",
            nodes=[Node(id="search", capability=CapabilityRef(id="web_search"))],
        )

        result = app.compiler.compile(workflow)

        self.assertFalse(result.ok)
        self.assertEqual(result.diagnostics[0].code, "CAPABILITY_HAS_NO_OPERATOR")

    def test_operator_requires_registered_capability(self) -> None:
        app = isolated_app()

        with self.assertRaisesRegex(ValueError, "unknown capability"):
            app.register_operator(
                lambda query: query,
                operator_id="search",
                capability_id="missing",
            )

    def test_duplicate_ids_are_rejected_without_overwriting_registry(self) -> None:
        app = isolated_app()
        app.register_capability("search")

        with self.assertRaisesRegex(ValueError, "already registered"):
            app.register_capability("search")

        def first() -> str:
            return "first"

        def second() -> str:
            return "second"

        app.register_operator(first, operator_id="search_impl")
        original = app.operator_registry.get("search_impl")
        with self.assertRaisesRegex(ValueError, "already registered"):
            app.register_operator(second, operator_id="search_impl")
        self.assertIs(app.operator_registry.get("search_impl"), original)

    def test_explicit_apps_have_isolated_registries(self) -> None:
        first = isolated_app()
        second = isolated_app()

        @first.capability("isolated")
        def implementation(value: str) -> str:
            return value

        self.assertTrue(first.capability_registry.contains("isolated"))
        self.assertFalse(second.capability_registry.contains("isolated"))
        self.assertFalse(second.operator_registry.contains("implementation"))

    def test_operator_type_mismatch_warns_but_registers(self) -> None:
        app = isolated_app()

        @app.capability("normalize")
        def normalize(value: str) -> str:
            return value

        def incompatible(value: int) -> str:
            return str(value)

        with self.assertWarns(OperatorContractWarning):
            registered = app.register_operator(
                incompatible,
                operator_id="integer_normalizer",
                capability_id="normalize",
            )
        self.assertEqual(registered.id, "integer_normalizer")

    def test_operator_missing_required_capability_parameter_is_rejected(self) -> None:
        app = isolated_app()

        @app.capability("search")
        def search(query: str) -> str:
            return query

        def incompatible(text: str) -> str:
            return text

        with self.assertRaisesRegex(ValueError, "cannot accept.*query"):
            app.register_operator(
                incompatible,
                operator_id="incompatible_search",
                capability_id="search",
            )

    def test_operator_kwargs_can_accept_capability_parameters(self) -> None:
        app = isolated_app()

        @app.capability("search_kwargs")
        def search(query: str, limit: int = 10) -> str:
            return query

        def flexible(**options: Any) -> str:
            return str(options["query"])

        with self.assertWarns(OperatorContractWarning):
            registered = app.register_operator(
                flexible,
                operator_id="flexible_search",
                capability_id="search_kwargs",
            )
        self.assertEqual(registered.id, "flexible_search")

    def test_operator_varargs_are_rejected(self) -> None:
        app = isolated_app()

        def unsupported(*values: str) -> str:
            return "".join(values)

        with self.assertRaisesRegex(ValueError, "Variadic positional"):
            app.register_operator(unsupported, operator_id="unsupported")

    def test_pydantic_annotation_generates_contract_and_json_schema(self) -> None:
        app = isolated_app()
        app.register_capability("typed_search")

        def typed_search(request: SearchRequest) -> list[str]:
            return [request.query]

        registered = app.register_operator(
            typed_search,
            operator_id="typed_search_impl",
            capability_id="typed_search",
        )

        self.assertEqual(
            registered.contract.input.parameters[0].annotation,
            SearchRequest,
        )
        input_schema = registered.contract.input.json_schema
        self.assertEqual(
            input_schema["properties"]["request"]["$ref"],
            "#/$defs/SearchRequest",
        )
        self.assertEqual(
            registered.contract.output.json_schema,
            {"items": {"type": "string"}, "type": "array"},
        )

    def test_workflow_contract_rejects_arbitrary_resource_types(self) -> None:
        app = isolated_app()

        def use_connection(connection: DatabaseConnection) -> str:
            return str(connection)

        def return_invalid_model() -> InvalidRuntimeModel:
            return InvalidRuntimeModel(connection=DatabaseConnection())

        with self.assertRaisesRegex(ValueError, "non-serializable.*DatabaseConnection"):
            app.register_operator(use_connection, operator_id="use_connection")
        with self.assertRaisesRegex(ValueError, "non-serializable.*InvalidRuntimeModel"):
            app.register_operator(return_invalid_model, operator_id="invalid_model")

    def test_explicit_any_is_a_normalized_dynamic_json_contract(self) -> None:
        app = isolated_app()

        def dynamic(value: Any) -> Any:
            return value

        registered = app.register_operator(dynamic, operator_id="dynamic_json")
        artifact = ArtifactRef(uri="artifact://result")
        normalized = registered.contract.output.validate(
            {"request": SearchRequest(query="docs"), "artifact": artifact}
        )

        self.assertTrue(registered.contract.output.known)
        self.assertEqual("json", registered.contract.output.restoration_mode)
        self.assertEqual(
            {"query": "docs", "limit": 10},
            normalized["request"],
        )
        self.assertEqual(artifact, normalized["artifact"])
        with self.assertRaisesRegex(ValueError, "Unsupported runtime value"):
            registered.contract.input.restore({"value": DatabaseConnection()})

    def test_public_apis_reject_schema_overrides(self) -> None:
        app = isolated_app()

        def handler(value: str) -> str:
            return value

        with self.assertRaises(TypeError):
            app.register_capability("custom", input_schema={"type": "object"})
        with self.assertRaises(TypeError):
            app.register_operator(
                handler,
                operator_id="custom",
                output_schema={"type": "string"},
            )
        with self.assertRaises(ValidationError):
            Node(
                id="handler",
                capability=handler,
                input_schema={"type": "object"},
            )

    def test_json_schema_view_cannot_mutate_operator_contract(self) -> None:
        app = isolated_app()

        @app.operator("detached_schema")
        def handler(value: str) -> str:
            return value

        registered = app.operator_registry.get("detached_schema")
        assert registered is not None
        schema = registered.contract.input.json_schema
        schema["properties"].clear()

        self.assertIn("value", registered.contract.input.json_schema["properties"])

    def test_no_argument_operator_has_closed_object_input_schema(self) -> None:
        app = isolated_app()

        @app.operator("no_arguments")
        def no_arguments() -> str:
            return "ok"

        registered = app.operator_registry.get("no_arguments")
        assert registered is not None
        self.assertEqual(
            registered.contract.input.json_schema,
            {
                "additionalProperties": False,
                "properties": {},
                "type": "object",
            },
        )

    def test_typed_kwargs_are_open_and_validated(self) -> None:
        app = isolated_app()

        @app.operator("typed_kwargs")
        def typed_kwargs(query: str, **options: int) -> str:
            return f"{query}:{sum(options.values())}"

        registered = app.operator_registry.get("typed_kwargs")
        assert registered is not None
        contract = registered.contract.input
        self.assertTrue(contract.allow_extra)
        self.assertEqual(
            contract.json_schema["additionalProperties"],
            {"type": "integer"},
        )
        self.assertEqual(
            contract.validate({"query": "docs", "limit": 2}),
            {"query": "docs", "limit": 2},
        )
        with self.assertRaises(ValidationError):
            contract.validate({"query": "docs", "limit": "two"})

    def test_standalone_operator_can_be_registered_without_capability(self) -> None:
        app = isolated_app()

        @app.operator("specific_task")
        def specific_task(value: str) -> str:
            return value.upper()

        registered = app.operator_registry.get("specific_task")
        self.assertIsNotNone(registered)
        assert registered is not None
        self.assertIsNone(registered.capability_id)
        self.assertIs(registered.handler, specific_task)

class OperatorExecutionTests(unittest.TestCase):
    def test_nested_any_cannot_hide_a_process_local_resource(self) -> None:
        def invalid_output() -> DynamicRuntimeModel:
            return DynamicRuntimeModel(value=DatabaseConnection())

        workflow = Workflow(id="invalid_nested_any")
        workflow.add_node(invalid_output, node_id="invalid")

        invocation = started_app().invoke(workflow)

        self.assertEqual("failed", invocation.state)
        self.assertEqual("OPERATOR_OUTPUT_INVALID", invocation.error.code)

    def test_direct_operator_executes_without_app_registration(self) -> None:
        def uppercase(value: str) -> str:
            return value.upper()

        direct = Operator(
            id="direct_uppercase",
            handler=uppercase,
            version=2,
        )
        workflow = Workflow(
            id="direct_operator",
            nodes=[Node(id="uppercase", capability=direct)],
        )

        invocation = started_app().invoke(
            workflow,
            input={"value": "hello"},
        )

        self.assertEqual("completed", invocation.state)
        self.assertEqual({"output": "HELLO"}, invocation.result)
        call = invocation.node_executions[0].operator_executions[0]
        self.assertEqual("direct_uppercase", call.operator_id)
        self.assertFalse(hasattr(call, "operator_manifest"))

    def test_pydantic_input_is_validated_and_passed_to_operator(self) -> None:
        app = started_app()

        @app.operator("typed_request")
        def typed_request(request: SearchRequest) -> str:
            return f"{request.query}:{request.limit}"

        workflow = Workflow(
            id="typed_request_workflow",
            nodes=[Node(id="typed", capability=OperatorRef(id="typed_request"))],
        )

        invocation = app.invoke(
            workflow,
            input={"request": {"query": "schema", "limit": 3}},
        )

        self.assertEqual(invocation.result, {"output": "schema:3"})

    def test_single_mapping_parameter_requires_its_parameter_name(self) -> None:
        app = started_app()
        calls = 0

        @app.operator("save_document")
        def save_document(document: dict[str, str]) -> str:
            nonlocal calls
            calls += 1
            return document["title"]

        workflow = Workflow(
            id="named_document_argument",
            nodes=[
                Node(
                    id="save",
                    capability=OperatorRef(id="save_document"),
                )
            ],
        )

        invalid = app.invoke(
            workflow,
            input={"title": "Design", "content": "..."},
            session_id="invalid",
        )
        valid = app.invoke(
            workflow,
            input={
                "document": {
                    "title": "Design",
                    "content": "...",
                }
            },
            session_id="valid",
        )

        self.assertEqual(invalid.state, "failed")
        self.assertEqual(invalid.error.code, "INPUT_MAPPING_INVALID")
        self.assertEqual(invalid.node_executions[0].operator_executions, [])
        self.assertEqual(valid.result, {"output": "Design"})
        self.assertEqual(calls, 1)

    def test_capability_ref_default_mode_uses_default_operator(self) -> None:
        app = started_app()

        @app.capability("web_search", operator_id="default_search")
        def default_search(query: str) -> str:
            return f"default:{query}"

        @app.operator("high_priority_search", capability="web_search", priority=100)
        def high_priority_search(query: str) -> str:
            return f"priority:{query}"

        workflow = Workflow(
            id="default_selection",
            nodes=[Node(id="search", capability=CapabilityRef(id="web_search"))],
        )

        invocation = app.invoke(workflow, input={"query": "docs"})

        self.assertEqual(invocation.result, {"output": "default:docs"})
        self.assertEqual(
            invocation.node_executions[0].operator_executions[0].operator_id,
            "default_search",
        )

    def test_priority_policy_selects_highest_priority_operator(self) -> None:
        app = started_app()

        @app.capability("web_search", operator_id="default_search")
        def default_search(query: str) -> str:
            return f"default:{query}"

        @app.operator("priority_search", capability="web_search", priority=50)
        def priority_search(query: str) -> str:
            return f"priority:{query}"

        workflow = Workflow(
            id="priority_selection",
            nodes=[
                Node(
                    id="search",
                    capability=CapabilityRef(id="web_search"),
                    policy=NodePolicy(
                        selection=CapabilitySelectionPolicy(mode="priority")
                    ),
                )
            ],
        )

        invocation = app.invoke(workflow, input={"query": "docs"})

        self.assertEqual(invocation.result, {"output": "priority:docs"})
        self.assertEqual(
            invocation.node_executions[0].operator_executions[0].operator_id,
            "priority_search",
        )

    def test_priority_policy_breaks_ties_by_operator_id(self) -> None:
        app = started_app()
        app.register_capability("stable_search")

        @app.operator("z_search", capability="stable_search", priority=10)
        def z_search(query: str) -> str:
            return f"z:{query}"

        @app.operator("a_search", capability="stable_search", priority=10)
        def a_search(query: str) -> str:
            return f"a:{query}"

        workflow = Workflow(
            id="priority_tie_breaker",
            nodes=[
                Node(
                    id="search",
                    capability=CapabilityRef(id="stable_search"),
                    policy=NodePolicy(
                        selection=CapabilitySelectionPolicy(mode="priority")
                    ),
                )
            ],
        )

        invocation = app.invoke(workflow, input={"query": "docs"})

        self.assertEqual(invocation.result, {"output": "a:docs"})
        self.assertEqual(
            invocation.node_executions[0].operator_executions[0].operator_id,
            "a_search",
        )

    def test_first_available_policy_breaks_ties_by_operator_id(self) -> None:
        app = started_app()
        app.register_capability("stable_available")

        @app.operator("z_available", capability="stable_available")
        def z_available(query: str) -> str:
            return f"z:{query}"

        @app.operator("a_available", capability="stable_available")
        def a_available(query: str) -> str:
            return f"a:{query}"

        workflow = Workflow(
            id="first_available_tie_breaker",
            nodes=[
                Node(
                    id="search",
                    capability=CapabilityRef(id="stable_available"),
                    policy=NodePolicy(
                        selection=CapabilitySelectionPolicy(mode="first_available")
                    ),
                )
            ],
        )

        invocation = app.invoke(workflow, input={"query": "docs"})

        self.assertEqual(invocation.result, {"output": "a:docs"})
        self.assertEqual(
            invocation.node_executions[0].operator_executions[0].operator_id,
            "a_available",
        )

    def test_first_available_skips_disabled_operator(self) -> None:
        app = started_app()

        @app.capability("search", operator_id="disabled_default")
        def disabled_default(query: str) -> str:
            return "disabled"

        app.operator_registry.get("disabled_default").disable()

        @app.operator("available_search", capability="search")
        def available_search(query: str) -> str:
            return f"available:{query}"

        workflow = Workflow(
            id="available_selection",
            nodes=[
                Node(
                    id="search",
                    capability=CapabilityRef(id="search"),
                    policy=NodePolicy(
                        selection=CapabilitySelectionPolicy(mode="first_available")
                    ),
                )
            ],
        )

        invocation = app.invoke(workflow, input={"query": "docs"})

        self.assertEqual(invocation.result, {"output": "available:docs"})

    def test_operator_ref_executes_exact_standalone_operator(self) -> None:
        app = started_app()

        @app.operator("exact_operator")
        def exact(value: str) -> str:
            return f"exact:{value}"

        workflow = Workflow(
            id="exact_operator_workflow",
            nodes=[Node(id="exact", capability=OperatorRef(id="exact_operator"))],
        )

        invocation = app.invoke(workflow, input={"value": "input"})

        self.assertEqual(invocation.result, {"output": "exact:input"})
        self.assertEqual(
            invocation.node_executions[0].operator_executions[0].operator_id,
            "exact_operator",
        )

    def test_async_registered_operator_uses_native_async_path(self) -> None:
        app = started_app()

        @app.capability("async_capability", operator_id="async_operator")
        async def async_operator(value: str) -> str:
            await asyncio.sleep(0.001)
            return f"async:{value}"

        workflow = Workflow(
            id="async_registered_operator",
            nodes=[Node(id="async", capability=CapabilityRef(id="async_capability"))],
        )

        invocation = app.invoke(workflow, input={"value": "input"})

        self.assertEqual(invocation.result, {"output": "async:input"})

    def test_new_operator_is_selected_after_workflow_ir_was_cached(self) -> None:
        app = started_app()

        @app.capability("late_bound", operator_id="initial_operator")
        def initial(value: str) -> str:
            return f"initial:{value}"

        workflow = Workflow(
            id="late_binding",
            nodes=[
                Node(
                    id="work",
                    capability=CapabilityRef(id="late_bound"),
                    policy=NodePolicy(
                        selection=CapabilitySelectionPolicy(mode="priority")
                    ),
                )
            ],
        )
        first = app.invoke(workflow, input={"value": "one"})

        @app.operator("new_operator", capability="late_bound", priority=100)
        def new(value: str) -> str:
            return f"new:{value}"

        second = app.invoke(workflow, input={"value": "two"})

        self.assertEqual(first.result, {"output": "initial:one"})
        self.assertEqual(second.result, {"output": "new:two"})
        self.assertEqual(len(app.workflow_registry), 1)

    def test_failed_default_operator_falls_back_and_records_both_calls(self) -> None:
        app = started_app()

        @app.capability("resilient", operator_id="failing_operator")
        def failing(value: str) -> str:
            raise RuntimeError("primary failed")

        @app.operator("fallback_operator", capability="resilient")
        def fallback(value: str) -> str:
            return f"fallback:{value}"

        workflow = Workflow(
            id="capability_fallback",
            nodes=[Node(id="work", capability=CapabilityRef(id="resilient"))],
        )

        invocation = app.invoke(workflow, input={"value": "input"})

        self.assertEqual(invocation.result, {"output": "fallback:input"})
        calls = invocation.node_executions[0].operator_executions
        self.assertEqual([call.operator_id for call in calls], [
            "failing_operator",
            "fallback_operator",
        ])
        self.assertEqual([call.reason for call in calls], ["normal", "fallback"])
        self.assertEqual([call.state for call in calls], ["failed", "completed"])

    def test_invalid_operator_output_enters_fallback(self) -> None:
        app = started_app()

        @app.capability("validated_output", operator_id="invalid_output")
        def invalid_output(value: int) -> int:
            return "not-an-int"  # type: ignore[return-value]

        @app.operator("valid_output", capability="validated_output")
        def valid_output(value: int) -> int:
            return value * 2

        workflow = Workflow(
            id="output_validation_fallback",
            nodes=[
                Node(
                    id="work",
                    capability=CapabilityRef(id="validated_output"),
                )
            ],
        )

        invocation = app.invoke(workflow, input={"value": 3})

        self.assertEqual(invocation.result, {"output": 6})
        calls = invocation.node_executions[0].operator_executions
        self.assertEqual([call.state for call in calls], ["failed", "completed"])
        self.assertEqual(calls[0].error.code, "OPERATOR_OUTPUT_INVALID")

    def test_allow_fallback_false_stops_after_selected_operator_failure(self) -> None:
        app = started_app()

        @app.capability("no_fallback", operator_id="failing_operator")
        def failing(value: str) -> str:
            raise RuntimeError("primary failed")

        @app.operator("unused_operator", capability="no_fallback")
        def unused(value: str) -> str:
            return value

        workflow = Workflow(
            id="fallback_disabled",
            nodes=[
                Node(
                    id="work",
                    capability=CapabilityRef(id="no_fallback"),
                    policy=NodePolicy(
                        selection=CapabilitySelectionPolicy(allow_fallback=False)
                    ),
                )
            ],
        )

        invocation = app.invoke(workflow, input={"value": "input"})

        self.assertEqual(invocation.state, "failed")
        calls = invocation.node_executions[0].operator_executions
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].operator_id, "failing_operator")

    def test_string_node_capability_is_capability_ref_shorthand(self) -> None:
        app = started_app()

        @app.capability("uppercase")
        def uppercase(value: str) -> str:
            return value.upper()

        workflow = Workflow(id="string_shorthand")
        workflow.add_node("uppercase", node_id="uppercase")

        invocation = app.invoke(workflow, input={"value": "hello"})

        self.assertEqual(invocation.result, {"output": "HELLO"})

    def test_metric_based_selection_modes_are_not_public_v1_values(self) -> None:
        for mode in ("lowest_cost", "lowest_latency", "highest_reliability"):
            with self.subTest(mode=mode), self.assertRaises(ValidationError):
                CapabilitySelectionPolicy(mode=mode)

    def test_selection_policy_rejects_unknown_operator_id(self) -> None:
        app = started_app()

        @app.capability("known_capability")
        def known(value: str) -> str:
            return value

        workflow = Workflow(
            id="unknown_preferred_operator",
            nodes=[
                Node(
                    id="known",
                    capability=CapabilityRef(id="known_capability"),
                    policy=NodePolicy(
                        selection=CapabilitySelectionPolicy(
                            preferred_operator_ids=("missing_operator",)
                        )
                    ),
                )
            ],
        )

        result = app.compiler.compile(workflow)

        self.assertFalse(result.ok)
        self.assertEqual(
            [diagnostic.code for diagnostic in result.diagnostics],
            ["POLICY_SELECTION_OPERATOR_UNKNOWN"],
        )


if __name__ == "__main__":
    unittest.main()
