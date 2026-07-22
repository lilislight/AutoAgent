from __future__ import annotations

import unittest

from pydantic import ValidationError

from autoagent.core.compiler import CompileResult, Diagnostic, WorkflowCompiler, WorkflowIR
from autoagent.core.operators import Operator
from autoagent.core.workflow import (
    BackoffPolicy,
    CapabilityRef,
    CapabilitySelectionPolicy,
    Edge,
    EdgePolicy,
    MapPolicy,
    Node,
    NodePolicy,
    OperatorRef,
    ReplicationPolicy,
    ResourcePolicy,
    RetryPolicy,
    SystemCommand,
    TimeoutPolicy,
    Workflow,
)


def task(value=None):
    return value


def other_task(value=None):
    return value


def third_task(value=None):
    return value


def edge_condition(*args, **kwargs):
    return True


def input_mapping(*args, **kwargs):
    return {"value": 1}


def output_binding(result):
    return {"result": result}


def make_workflow(**fields) -> Workflow:
    """Keep compiler tests focused while every source Workflow has stable identity."""

    fields.setdefault("id", "compiler_test_workflow")
    return Workflow(**fields)


class WorkflowCompilerTests(unittest.TestCase):
    def compile_ok(self, workflow: Workflow) -> WorkflowIR:
        result = WorkflowCompiler().compile(workflow)
        diagnostics = [diagnostic.model_dump() for diagnostic in result.diagnostics]
        self.assertTrue(result.ok, diagnostics)
        self.assertIsNotNone(result.workflow_ir)
        return result.workflow_ir

    def test_direct_callable_varargs_are_rejected(self) -> None:
        def unsupported(*values: str) -> str:
            return "".join(values)

        result = WorkflowCompiler().compile(
            make_workflow(nodes=[Node(id="unsupported", capability=unsupported)])
        )

        self.assertFalse(result.ok)
        self.assertEqual(
            ["OPERATOR_CONTRACT_INVALID"],
            self.diagnostic_codes(result),
        )

    def diagnostic_codes(self, result: CompileResult) -> list[str]:
        return [diagnostic.code for diagnostic in result.diagnostics]

    def test_single_callable_node_preserves_required_id_and_defaults_version(self):
        workflow_ir = self.compile_ok(
            make_workflow(nodes=[Node(id="task", capability=task)])
        )

        self.assertEqual("compiler_test_workflow", workflow_ir.workflow_id)
        self.assertEqual(1, workflow_ir.workflow_version)
        self.assertEqual(["task"], list(workflow_ir.nodes))
        capability = workflow_ir.nodes["task"].capability
        self.assertIsInstance(capability, Operator)
        self.assertIs(task, capability.handler)
        self.assertEqual(("task",), workflow_ir.entry_node_ids)
        self.assertEqual(("task",), workflow_ir.exit_node_ids)
        self.assertEqual({"task": ()}, workflow_ir.graph.outgoing_edges)
        self.assertEqual({"task": ()}, workflow_ir.graph.incoming_edges)

    def test_workflow_requires_non_empty_stable_id(self) -> None:
        with self.assertRaises(ValidationError):
            Workflow(nodes=[Node(id="task", capability=task)])
        with self.assertRaises(ValidationError):
            Workflow(id="   ", nodes=[Node(id="task", capability=task)])

    def test_node_requires_explicit_id(self) -> None:
        with self.assertRaises(ValidationError):
            Node(capability=task)
        with self.assertRaises(ValidationError):
            Node(id="  ", capability=task)

        workflow = make_workflow()
        with self.assertRaisesRegex(ValueError, "node_id is required"):
            workflow.add_node(task)

    def test_workflow_ir_serializes_contract_descriptor_without_live_validators(self):
        workflow_ir = self.compile_ok(
            make_workflow(nodes=[Node(id="task", capability=task)])
        )

        node = workflow_ir.model_dump()["nodes"]["task"]
        input_contract = node["input_contract"]
        self.assertEqual(input_contract["kind"], "arguments")
        self.assertEqual(input_contract["json_schema"]["type"], "object")
        self.assertNotIn("_signature", input_contract)
        self.assertNotIn("_parameter_adapters", input_contract)

    def test_manual_node_ids_are_preserved(self):
        workflow = make_workflow(
            id="workflow_manual",
            version="2",
            nodes=[
                Node(id="source", capability=task),
                Node(id="target", capability=other_task),
            ],
            edges=[Edge(from_node="source", to_node="target")],
        )

        workflow_ir = self.compile_ok(workflow)

        self.assertEqual("workflow_manual", workflow_ir.workflow_id)
        self.assertEqual("2", workflow_ir.workflow_version)
        self.assertEqual(["source", "target"], list(workflow_ir.nodes))
        self.assertEqual(["edge_source_target"], list(workflow_ir.edges))

    def test_direct_callable_bindings_reuse_only_the_same_object(self):
        def factory():
            def generated(value=None):
                return value

            return generated

        first = factory()
        second = factory()
        workflow_ir = self.compile_ok(
            make_workflow(
                nodes=[
                    Node(id="first", capability=first),
                    Node(id="first_again", capability=first),
                    Node(id="second", capability=second),
                ]
            )
        )

        first_operator = workflow_ir.nodes["first"].capability
        self.assertIs(first_operator, workflow_ir.nodes["first_again"].capability)
        self.assertIsNot(first_operator, workflow_ir.nodes["second"].capability)
        self.assertNotEqual(
            first_operator.id,
            workflow_ir.nodes["second"].capability.id,
        )

    def test_child_workflow_is_expanded_and_parent_edges_are_rewired(self):
        child = Workflow(id="child")
        child.add_node(task, node_id="prepare")
        child.add_node(other_task, node_id="finish")
        child.add_edge("prepare", "finish", edge_id="inside")

        parent = make_workflow(id="parent")
        parent.add_node(task, node_id="start")
        parent.add_node(child, node_id="child")
        parent.add_node(third_task, node_id="end")
        parent.add_edge("start", "child", edge_id="enter_child")
        parent.add_edge("child", "end", edge_id="leave_child")

        workflow_ir = self.compile_ok(parent)

        self.assertEqual(
            ["start", "child/prepare", "child/finish", "end"],
            list(workflow_ir.nodes),
        )
        self.assertEqual(
            ("child/prepare", "child/finish"),
            (
                workflow_ir.edges["child/inside"].from_node,
                workflow_ir.edges["child/inside"].to_node,
            ),
        )
        self.assertEqual("child/prepare", workflow_ir.edges["enter_child"].to_node)
        self.assertEqual("child/finish", workflow_ir.edges["leave_child"].from_node)
        self.assertEqual(("start",), workflow_ir.entry_node_ids)
        self.assertEqual(("end",), workflow_ir.exit_node_ids)
        child_finish = workflow_ir.nodes["child/finish"]
        self.assertEqual("finish", child_finish.local_id)
        self.assertEqual(("child",), child_finish.workflow_path)
        self.assertEqual("child/prepare", child_finish.scope_node_ids["prepare"])

    def test_same_child_workflow_can_be_expanded_more_than_once(self):
        child = Workflow(id="reusable_child")
        child.add_node(task, node_id="work")

        parent = make_workflow(id="parent")
        parent.add_node(child, node_id="first")
        parent.add_node(child, node_id="second")
        parent.add_edge("first", "second")

        workflow_ir = self.compile_ok(parent)

        self.assertEqual(["first/work", "second/work"], list(workflow_ir.nodes))
        self.assertEqual(
            ("first/work", "second/work"),
            (
                workflow_ir.edges["edge_first_second"].from_node,
                workflow_ir.edges["edge_first_second"].to_node,
            ),
        )
        self.assertIs(
            workflow_ir.nodes["first/work"].capability,
            workflow_ir.nodes["second/work"].capability,
        )

    def test_nested_child_workflows_expand_recursively(self):
        inner = Workflow(id="inner")
        inner.add_node(task, node_id="work")
        middle = Workflow(id="middle")
        middle.add_node(inner, node_id="inner")
        outer = make_workflow(id="outer")
        outer.add_node(middle, node_id="middle")

        workflow_ir = self.compile_ok(outer)

        self.assertEqual(["middle/inner/work"], list(workflow_ir.nodes))
        self.assertEqual(("middle/inner/work",), workflow_ir.entry_node_ids)
        self.assertEqual(("middle/inner/work",), workflow_ir.exit_node_ids)
        self.assertEqual(
            ("middle", "inner"),
            workflow_ir.nodes["middle/inner/work"].workflow_path,
        )

    def test_child_workflow_multiple_entries_require_explicit_selection(self):
        child = Workflow(id="multi_entry")
        child.add_node(task, node_id="left")
        child.add_node(other_task, node_id="left_end")
        child.add_node(task, node_id="right")
        child.add_node(other_task, node_id="right_end")
        child.add_edge("left", "left_end")
        child.add_edge("right", "right_end")

        parent = make_workflow(id="ambiguous_parent")
        parent.add_node(child, node_id="child")
        result = WorkflowCompiler().compile(parent)

        self.assertFalse(result.ok)
        self.assertIn("SUBWORKFLOW_ENTRY_REQUIRED", self.diagnostic_codes(result))

        selected_parent = make_workflow(id="selected_parent")
        selected_parent.add_node(
            child,
            node_id="child",
            child_entry_node_id="right",
            child_exit_node_id="right_end",
        )
        workflow_ir = self.compile_ok(selected_parent)
        self.assertEqual(["child/right", "child/right_end"], list(workflow_ir.nodes))

    def test_child_workflow_multiple_reachable_exits_require_selection(self):
        child = Workflow(id="multi_exit")
        child.add_node(task, node_id="start")
        child.add_node(other_task, node_id="success")
        child.add_node(third_task, node_id="failure")
        child.add_edge("start", "success", condition=edge_condition)
        child.add_edge("start", "failure", condition=lambda _ctx: False)

        parent = make_workflow(id="ambiguous_exit_parent")
        parent.add_node(child, node_id="child")
        result = WorkflowCompiler().compile(parent)
        self.assertFalse(result.ok)
        self.assertIn("SUBWORKFLOW_EXIT_REQUIRED", self.diagnostic_codes(result))

        selected_parent = make_workflow(id="selected_exit_parent")
        selected_parent.add_node(
            child,
            node_id="child",
            child_exit_node_id="success",
        )
        selected_parent.add_node(task, node_id="after")
        selected_parent.add_edge("child", "after")
        workflow_ir = self.compile_ok(selected_parent)
        self.assertEqual("child/success", workflow_ir.edges["edge_child_after"].from_node)
        self.assertIn("child/failure", workflow_ir.exit_node_ids)
        self.assertIn("after", workflow_ir.exit_node_ids)

    def test_child_boundary_selector_must_name_a_valid_boundary(self):
        child = Workflow(id="child")
        child.add_node(task, node_id="start")
        child.add_node(other_task, node_id="finish")
        child.add_edge("start", "finish")
        parent = make_workflow(id="parent")
        parent.add_node(
            child,
            node_id="child",
            child_entry_node_id="finish",
        )

        result = WorkflowCompiler().compile(parent)

        self.assertFalse(result.ok)
        self.assertIn("SUBWORKFLOW_ENTRY_INVALID", self.diagnostic_codes(result))

    def test_reachable_invalid_child_edge_is_not_hidden_by_expansion(self):
        child = Workflow(id="child")
        child.add_node(task, node_id="start")
        child.add_node(other_task, node_id="finish")
        child.add_edge("start", "finish", condition=edge_condition)
        child.add_edge("start", "missing", condition=lambda _ctx: False)
        parent = make_workflow(id="parent")
        parent.add_node(child, node_id="child")

        result = WorkflowCompiler().compile(parent)

        self.assertFalse(result.ok)
        self.assertIn("EDGE_UNKNOWN_NODE", self.diagnostic_codes(result))

    def test_expanded_node_id_collision_is_rejected(self):
        child = Workflow(id="child", nodes=[Node(id="work", capability=task)])
        parent = make_workflow(id="parent")
        parent.add_node(child, node_id="child")
        parent.add_node(other_task, node_id="child/work")

        result = WorkflowCompiler().compile(parent)

        self.assertFalse(result.ok)
        self.assertIn("NODE_DUPLICATE_ID", self.diagnostic_codes(result))

    def test_child_workflow_placeholder_rejects_node_execution_behavior(self):
        child = Workflow(id="child", nodes=[Node(id="work", capability=task)])
        parent = make_workflow(id="parent")
        parent.add_node(
            child,
            node_id="child",
            input_mapping=input_mapping,
            policy=NodePolicy(timeout=TimeoutPolicy(timeout_ms=10)),
        )

        result = WorkflowCompiler().compile(parent)

        self.assertFalse(result.ok)
        self.assertIn(
            "SUBWORKFLOW_NODE_BEHAVIOR_UNSUPPORTED",
            self.diagnostic_codes(result),
        )

    def test_map_policy_cannot_target_child_workflow_placeholder(self):
        child = Workflow(id="child", nodes=[Node(id="work", capability=task)])
        parent = make_workflow(id="parent")
        parent.add_node(task, node_id="start")
        parent.add_node(child, node_id="child")
        parent.add_edge(
            "start",
            "child",
            policy=EdgePolicy(map=MapPolicy()),
        )

        result = WorkflowCompiler().compile(parent)

        self.assertFalse(result.ok)
        self.assertIn("SUBWORKFLOW_MAP_UNSUPPORTED", self.diagnostic_codes(result))

    def test_child_boundary_selectors_are_rejected_for_regular_nodes(self):
        workflow = make_workflow(
            nodes=[
                Node(
                    id="task",
                    capability=task,
                    child_entry_node_id="other",
                )
            ]
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertFalse(result.ok)
        self.assertIn("SUBWORKFLOW_SELECTOR_INVALID", self.diagnostic_codes(result))

    def test_recursive_child_workflow_is_rejected(self):
        workflow = make_workflow(id="recursive")
        workflow.add_node(workflow, node_id="self")

        result = WorkflowCompiler().compile(workflow)

        self.assertFalse(result.ok)
        self.assertIn("SUBWORKFLOW_RECURSION", self.diagnostic_codes(result))

    def test_auto_edge_ids_use_endpoint_ids_and_duplicate_suffix(self):
        workflow = make_workflow(
            nodes=[
                Node(id="from", capability=task),
                Node(id="to", capability=other_task),
            ],
            edges=[
                Edge(from_node="from", to_node="to"),
                Edge(from_node="from", to_node="to"),
            ],
        )

        workflow_ir = self.compile_ok(workflow)

        self.assertEqual(["edge_from_to", "edge_from_to_2"], list(workflow_ir.edges))
        self.assertEqual(0, workflow_ir.edges["edge_from_to"].order)
        self.assertEqual(1, workflow_ir.edges["edge_from_to_2"].order)

    def test_edge_endpoints_resolve_node_objects_and_strings(self):
        source = Node(id="source", capability=task)
        middle = Node(id="middle", capability=other_task)
        target = Node(id="target", capability=third_task)
        workflow = make_workflow()
        workflow.add_node(source)
        workflow.add_node(middle)
        workflow.add_node(target)
        workflow.add_edge(source, "middle", edge_id="object_to_string")
        workflow.add_edge("middle", target, edge_id="string_to_object")

        workflow_ir = self.compile_ok(workflow)

        self.assertEqual("source", workflow_ir.edges["object_to_string"].from_node)
        self.assertEqual("middle", workflow_ir.edges["object_to_string"].to_node)
        self.assertEqual("middle", workflow_ir.edges["string_to_object"].from_node)
        self.assertEqual("target", workflow_ir.edges["string_to_object"].to_node)

    def test_unknown_edge_endpoints_emit_diagnostics_and_fail(self):
        workflow = make_workflow(
            nodes=[Node(id="known", capability=task)],
            edges=[
                Edge(id="missing_source", from_node="missing", to_node="known"),
                Edge(id="missing_target", from_node="known", to_node="missing"),
            ],
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertFalse(result.ok)
        self.assertIsNone(result.workflow_ir)
        self.assertEqual(["EDGE_UNKNOWN_NODE", "EDGE_UNKNOWN_NODE"], self.diagnostic_codes(result))
        self.assertEqual(
            {"missing_source", "missing_target"},
            {diagnostic.subject for diagnostic in result.diagnostics},
        )

    def test_independent_node_and_edge_errors_are_collected_together(self):
        workflow = make_workflow(
            nodes=[
                Node(id="known", capability=task),
                Node(id="missing_capability", capability=CapabilityRef(id="missing")),
            ],
            edges=[
                Edge(from_node="known", to_node="unknown"),
                Edge(from_node="known", to_node="missing_capability", condition="bad"),
            ],
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertFalse(result.ok)
        self.assertEqual(
            {
                "EDGE_UNKNOWN_NODE",
                "CAPABILITY_NOT_REGISTERED",
                "STRING_CONDITION_UNSUPPORTED",
            },
            set(self.diagnostic_codes(result)),
        )

    def test_explicit_entry_with_incoming_edge_is_rejected(self):
        workflow = make_workflow(
            nodes=[
                Node(id="first", capability=task),
                Node(id="second", capability=other_task, entry=True),
            ],
            edges=[Edge(from_node="first", to_node="second")],
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertFalse(result.ok)
        self.assertIn("WF_ENTRY_HAS_INCOMING_EDGE", self.diagnostic_codes(result))

    def test_explicit_entry_with_self_loop_is_rejected(self):
        workflow = make_workflow(
            nodes=[Node(id="loop", capability=task, entry=True)],
            edges=[Edge(from_node="loop", to_node="loop", condition=edge_condition)],
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertFalse(result.ok)
        self.assertIn("WF_ENTRY_HAS_INCOMING_EDGE", self.diagnostic_codes(result))

    def test_explicit_entry_does_not_hide_other_inferred_entries(self):
        workflow_ir = self.compile_ok(
            make_workflow(
                nodes=[
                    Node(id="first", capability=task),
                    Node(id="second", capability=other_task, entry=True),
                ]
            )
        )

        self.assertEqual(("first", "second"), workflow_ir.entry_node_ids)
        self.assertTrue(workflow_ir.nodes["first"].entry)
        self.assertTrue(workflow_ir.nodes["second"].entry)

    def test_infers_entries_and_exits(self):
        workflow = make_workflow(
            nodes=[
                Node(id="first", capability=task),
                Node(id="second", capability=other_task),
                Node(id="third", capability=third_task),
            ],
            edges=[
                Edge(from_node="first", to_node="second"),
                Edge(from_node="second", to_node="third"),
            ],
        )

        workflow_ir = self.compile_ok(workflow)

        self.assertEqual(("first",), workflow_ir.entry_node_ids)
        self.assertEqual(("third",), workflow_ir.exit_node_ids)
        self.assertTrue(workflow_ir.nodes["first"].entry)
        self.assertFalse(workflow_ir.nodes["second"].entry)
        self.assertFalse(workflow_ir.nodes["third"].entry)
        self.assertFalse(workflow_ir.nodes["first"].exit)
        self.assertFalse(workflow_ir.nodes["second"].exit)
        self.assertTrue(workflow_ir.nodes["third"].exit)

    def test_graph_indexes_include_edges_predecessors_and_successors(self):
        workflow = make_workflow(
            nodes=[
                Node(id="a", capability=task),
                Node(id="b", capability=other_task),
                Node(id="c", capability=third_task),
            ],
            edges=[
                Edge(id="a_to_b", from_node="a", to_node="b"),
                Edge(id="a_to_c", from_node="a", to_node="c"),
                Edge(id="b_to_c", from_node="b", to_node="c"),
            ],
        )

        workflow_ir = self.compile_ok(workflow)

        self.assertEqual(
            {"a": ("a_to_b", "a_to_c"), "b": ("b_to_c",), "c": ()},
            workflow_ir.graph.outgoing_edges,
        )
        self.assertEqual(
            {"a": (), "b": ("a_to_b",), "c": ("a_to_c", "b_to_c")},
            workflow_ir.graph.incoming_edges,
        )
        self.assertEqual({"a": (), "b": ("a",), "c": ("a", "b")}, workflow_ir.graph.predecessors)
        self.assertEqual({"a": ("b", "c"), "b": ("c",), "c": ()}, workflow_ir.graph.successors)

    def test_callable_edge_condition_is_carried_into_edge_ir(self):
        workflow = make_workflow(
            nodes=[
                Node(id="from", capability=task),
                Node(id="to", capability=other_task),
            ],
            edges=[Edge(id="conditional", from_node="from", to_node="to", condition=edge_condition)],
        )

        workflow_ir = self.compile_ok(workflow)

        self.assertIs(edge_condition, workflow_ir.edges["conditional"].condition)

    def test_string_edge_condition_emits_unsupported_diagnostic(self):
        workflow = make_workflow(
            nodes=[
                Node(id="from", capability=task),
                Node(id="to", capability=other_task),
            ],
            edges=[Edge(id="conditional", from_node="from", to_node="to", condition="state.ready")],
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertFalse(result.ok)
        self.assertIsNone(result.workflow_ir)
        self.assertEqual(["STRING_CONDITION_UNSUPPORTED"], self.diagnostic_codes(result))
        self.assertEqual("conditional", result.diagnostics[0].subject)

    def test_callable_mappings_are_carried_into_node_ir(self):
        workflow = make_workflow(
            nodes=[
                Node(
                    id="mapped",
                    capability=task,
                    input_mapping=input_mapping,
                    output_binding=output_binding,
                )
            ]
        )

        workflow_ir = self.compile_ok(workflow)

        self.assertIs(input_mapping, workflow_ir.nodes["mapped"].input_plan)
        self.assertIs(output_binding, workflow_ir.nodes["mapped"].output_binding)

    def test_node_policy_is_carried_into_node_ir(self):
        policy = NodePolicy(
            retry=RetryPolicy(max_attempts=2),
            timeout=TimeoutPolicy(timeout_ms=1000),
            resource=ResourcePolicy(max_node_executions_per_invocation=3),
        )
        workflow = make_workflow(nodes=[Node(id="limited", capability=task, policy=policy)])

        workflow_ir = self.compile_ok(workflow)

        self.assertIs(policy, workflow_ir.nodes["limited"].policy)

    def test_edge_map_policy_is_carried_into_edge_ir(self):
        def select_items(output):
            return output["items"]

        def aggregate(outputs):
            return {"items": outputs}

        policy = EdgePolicy(
            map=MapPolicy(
                item_selector=select_items,
                output_aggregator=aggregate,
                max_parallelism=2,
            )
        )
        workflow = make_workflow(
            nodes=[
                Node(id="source", capability=task),
                Node(id="target", capability=other_task),
            ],
            edges=[Edge(id="map_edge", from_node="source", to_node="target", policy=policy)],
        )

        workflow_ir = self.compile_ok(workflow)

        self.assertIs(policy, workflow_ir.edges["map_edge"].policy)

    def test_replication_policy_requires_output_aggregator(self):
        result = WorkflowCompiler().compile(
            make_workflow(
                nodes=[
                    Node(
                        id="sample",
                        capability=task,
                        policy=NodePolicy(replication=ReplicationPolicy(count=2)),
                    )
                ]
            )
        )

        self.assertFalse(result.ok)
        self.assertEqual(["POLICY_REPLICATION_INVALID"], self.diagnostic_codes(result))
        self.assertIn("requires output_aggregator", result.diagnostics[0].message)

    def test_invalid_map_policy_emits_diagnostic(self):
        workflow = make_workflow(
            nodes=[
                Node(id="source", capability=task),
                Node(id="target", capability=other_task),
            ],
            edges=[
                Edge(
                    id="map_edge",
                    from_node="source",
                    to_node="target",
                    policy=EdgePolicy(map=MapPolicy(max_parallelism=0)),
                )
            ],
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertFalse(result.ok)
        self.assertEqual(["POLICY_MAP_INVALID"], self.diagnostic_codes(result))
        self.assertEqual("map_edge", result.diagnostics[0].subject)

    def test_compiler_derives_natural_loop_region(self):
        workflow = make_workflow(
            nodes=[
                Node(id="start", capability=task),
                Node(id="agent", capability=other_task),
                Node(id="tools", capability=third_task),
                Node(id="final", capability=task),
            ],
            edges=[
                Edge(id="start_agent", from_node="start", to_node="agent"),
                Edge(id="agent_tools", from_node="agent", to_node="tools", condition=edge_condition),
                Edge(id="tools_agent", from_node="tools", to_node="agent"),
                Edge(id="agent_final", from_node="agent", to_node="final", condition=edge_condition),
            ],
        )

        workflow_ir = self.compile_ok(workflow)

        region = workflow_ir.graph.loop_regions["loop_1"]
        self.assertEqual(("agent", "tools"), region.node_ids)
        self.assertEqual("agent", region.header_node_id)
        self.assertEqual(("start_agent",), region.external_entry_edge_ids)
        self.assertEqual(("tools_agent",), region.back_edge_ids)
        self.assertEqual({"agent_tools", "tools_agent"}, set(region.internal_edge_ids))
        self.assertEqual(("agent_final",), region.exit_edge_ids)
        self.assertEqual(
            {"agent": ("loop_1",), "tools": ("loop_1",)},
            workflow_ir.graph.node_loop_stacks,
        )

    def test_loop_with_multiple_entry_nodes_is_rejected(self):
        workflow = make_workflow(
            nodes=[
                Node(id="start", capability=task),
                Node(id="left", capability=other_task),
                Node(id="right", capability=third_task),
                Node(id="final", capability=task),
            ],
            edges=[
                Edge(from_node="start", to_node="left"),
                Edge(from_node="start", to_node="right"),
                Edge(from_node="left", to_node="right"),
                Edge(from_node="right", to_node="left"),
                Edge(from_node="right", to_node="final", condition=edge_condition),
            ],
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertFalse(result.ok)
        self.assertIn("LOOP_ENTRY_INVALID", self.diagnostic_codes(result))
        loop_diagnostic = next(
            item for item in result.diagnostics if item.code == "LOOP_ENTRY_INVALID"
        )
        self.assertEqual("edge_start_right", loop_diagnostic.subject)
        self.assertEqual(
            "left",
            loop_diagnostic.metadata["expected_entry_node_id"],
        )

    def test_loop_node_with_unconditional_fan_out_uses_ordinary_edge_semantics(self):
        workflow = make_workflow(
            nodes=[
                Node(id="start", capability=task, entry=True),
                Node(id="loop", capability=task),
                Node(id="final", capability=other_task),
            ],
            edges=[
                Edge(from_node="start", to_node="loop"),
                Edge(from_node="loop", to_node="loop"),
                Edge(from_node="loop", to_node="final", condition=edge_condition),
            ],
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertTrue(result.ok, result.diagnostics)
        assert result.workflow_ir is not None
        region = result.workflow_ir.graph.loop_regions["loop_1"]
        self.assertEqual(("edge_loop_loop",), region.back_edge_ids)
        self.assertEqual(("edge_loop_final",), region.exit_edge_ids)

    def test_selection_policy_cannot_prefer_and_exclude_same_operator(self):
        workflow = make_workflow(
            nodes=[
                Node(
                    id="select",
                    capability=task,
                    policy=NodePolicy(
                        selection=CapabilitySelectionPolicy(
                            preferred_operator_ids=("fast_search",),
                            excluded_operator_ids=("fast_search",),
                        )
                    ),
                )
            ]
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertFalse(result.ok)
        self.assertEqual(
            ["POLICY_SELECTION_INVALID", "POLICY_SELECTION_INVALID"],
            self.diagnostic_codes(result),
        )
        self.assertEqual("select", result.diagnostics[0].subject)

    def test_retry_backoff_timeout_and_resource_policy_validation(self):
        cases = [
            (
                "retry",
                NodePolicy(retry=RetryPolicy(max_attempts=0)),
                "POLICY_RETRY_INVALID",
            ),
            (
                "backoff_initial_delay",
                NodePolicy(retry=RetryPolicy(backoff=BackoffPolicy(initial_delay_ms=-1))),
                "POLICY_BACKOFF_INVALID",
            ),
            (
                "backoff_max_delay",
                NodePolicy(retry=RetryPolicy(backoff=BackoffPolicy(max_delay_ms=-1))),
                "POLICY_BACKOFF_INVALID",
            ),
            (
                "backoff_multiplier",
                NodePolicy(retry=RetryPolicy(backoff=BackoffPolicy(multiplier=0))),
                "POLICY_BACKOFF_INVALID",
            ),
            (
                "timeout",
                NodePolicy(timeout=TimeoutPolicy(timeout_ms=0)),
                "POLICY_TIMEOUT_INVALID",
            ),
            (
                "resource",
                NodePolicy(
                    resource=ResourcePolicy(
                        max_operator_calls_per_invocation=-1,
                    )
                ),
                "POLICY_RESOURCE_INVALID",
            ),
        ]

        for name, policy, expected_code in cases:
            with self.subTest(name=name):
                result = WorkflowCompiler().compile(
                    make_workflow(nodes=[Node(id=name, capability=task, policy=policy)])
                )

                self.assertFalse(result.ok)
                self.assertIsNone(result.workflow_ir)
                self.assertEqual([expected_code], self.diagnostic_codes(result))
                self.assertEqual(name, result.diagnostics[0].subject)

    def test_resource_policy_validation_covers_invocation_scope_limits(self):
        cases = [
            ResourcePolicy(max_node_executions_per_invocation=0),
            ResourcePolicy(max_operator_calls_per_invocation=0),
            ResourcePolicy(max_runtime_ms_per_invocation=0),
        ]

        for policy in cases:
            with self.subTest(policy=policy):
                result = WorkflowCompiler().compile(
                    make_workflow(
                        nodes=[
                            Node(
                                id="limited",
                                capability=task,
                                policy=NodePolicy(resource=policy),
                            )
                        ]
                    )
                )

                self.assertFalse(result.ok)
                self.assertEqual(["POLICY_RESOURCE_INVALID"], self.diagnostic_codes(result))
                self.assertEqual("limited", result.diagnostics[0].subject)

    def test_unregistered_capabilities_emit_diagnostics(self):
        cases = [
            ("string", "raw.capability", "CAPABILITY_NOT_REGISTERED"),
            ("capability", CapabilityRef(id="capability"), "CAPABILITY_NOT_REGISTERED"),
            ("operator", OperatorRef(id="operator"), "OPERATOR_NOT_REGISTERED"),
            (
                "system_command",
                SystemCommand(id="unknown"),
                "SYSTEM_COMMAND_UNSUPPORTED",
            ),
        ]

        for name, capability, expected_code in cases:
            with self.subTest(name=name):
                result = WorkflowCompiler().compile(
                    make_workflow(nodes=[Node(id="unsupported", capability=capability)])
                )

                self.assertFalse(result.ok)
                self.assertIsNone(result.workflow_ir)
                self.assertEqual([expected_code], self.diagnostic_codes(result))
                self.assertEqual("unsupported", result.diagnostics[0].subject)

    def test_wait_system_command_compiles_framework_contract(self):
        result = WorkflowCompiler().compile(
            make_workflow(
                id="wait_contract",
                nodes=[Node(id="approval", capability=SystemCommand(id="wait"))],
            )
        )

        self.assertTrue(result.ok)
        assert result.workflow_ir is not None
        node = result.workflow_ir.nodes["approval"]
        self.assertIsInstance(node.capability, SystemCommand)
        self.assertEqual(
            set(node.input_contract.json_schema["properties"]),
            {"wait_key", "wait_type", "payload"},
        )
        self.assertFalse(node.output_contract.known)

    def test_wait_system_command_rejects_node_policy(self):
        result = WorkflowCompiler().compile(
            make_workflow(
                nodes=[
                    Node(
                        id="approval",
                        capability=SystemCommand(id="wait"),
                        policy=NodePolicy(max_concurrency=1),
                    )
                ]
            )
        )

        self.assertFalse(result.ok)
        self.assertEqual(
            ["SYSTEM_COMMAND_POLICY_UNSUPPORTED"],
            self.diagnostic_codes(result),
        )

    def test_wait_system_command_rejects_reserved_command_object(self):
        result = WorkflowCompiler().compile(
            make_workflow(
                nodes=[
                    Node(
                        id="approval",
                        capability=SystemCommand(id="wait", command=object()),
                    )
                ]
            )
        )

        self.assertFalse(result.ok)
        self.assertEqual(
            ["SYSTEM_COMMAND_CONFIG_UNSUPPORTED"],
            self.diagnostic_codes(result),
        )

    def test_map_policy_cannot_target_wait_system_command(self):
        workflow = make_workflow(
            nodes=[
                Node(id="source", capability=lambda: [{"wait_key": "one"}]),
                Node(id="wait", capability=SystemCommand(id="wait")),
            ],
            edges=[
                Edge(
                    from_node="source",
                    to_node="wait",
                    policy=EdgePolicy(map=MapPolicy()),
                )
            ],
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertFalse(result.ok)
        self.assertIn(
            "SYSTEM_COMMAND_MAP_UNSUPPORTED",
            self.diagnostic_codes(result),
        )

    def test_duplicate_node_id_emits_diagnostic(self):
        workflow = make_workflow(
            nodes=[
                Node(id="duplicate", capability=task),
                Node(id="duplicate", capability=other_task),
            ]
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertFalse(result.ok)
        self.assertIsNone(result.workflow_ir)
        self.assertEqual(["NODE_DUPLICATE_ID"], self.diagnostic_codes(result))
        self.assertEqual("duplicate", result.diagnostics[0].subject)

    def test_duplicate_edge_id_emits_diagnostic(self):
        workflow = make_workflow(
            nodes=[
                Node(id="from", capability=task),
                Node(id="to", capability=other_task),
            ],
            edges=[
                Edge(id="duplicate", from_node="from", to_node="to"),
                Edge(id="duplicate", from_node="from", to_node="to"),
            ],
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertFalse(result.ok)
        self.assertIsNone(result.workflow_ir)
        self.assertEqual(["EDGE_DUPLICATE_ID"], self.diagnostic_codes(result))
        self.assertEqual("duplicate", result.diagnostics[0].subject)

    def test_compile_result_ok_requires_ir_and_no_error_diagnostics(self):
        workflow_ir = WorkflowIR(
            ir_version="0.1",
            compiler_version="0.1",
            workflow_id="workflow_test",
            workflow_version=1,
            definition_hash="test_definition_hash",
        )

        self.assertTrue(CompileResult(workflow_ir=workflow_ir).ok)
        self.assertFalse(CompileResult().ok)
        self.assertTrue(
            CompileResult(
                workflow_ir=workflow_ir,
                diagnostics=[
                    Diagnostic(
                        code="WF_WARNING",
                        severity="warning",
                        message="Warning diagnostics do not fail compilation.",
                    )
                ],
            ).ok
        )
        self.assertFalse(
            CompileResult(
                workflow_ir=workflow_ir,
                diagnostics=[
                    Diagnostic(
                        code="WF_ERROR",
                        severity="error",
                        message="Error diagnostics fail compilation.",
                    )
                ],
            ).ok
        )


if __name__ == "__main__":
    unittest.main()
