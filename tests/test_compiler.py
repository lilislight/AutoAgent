from __future__ import annotations

import unittest

from autoagent.compiler import CompileResult, Diagnostic, WorkflowCompiler, WorkflowIR
from autoagent.workflow import (
    BackoffPolicy,
    CapabilityRef,
    CapabilitySelectionPolicy,
    Edge,
    EdgePolicy,
    JoinPolicy,
    MapPolicy,
    Node,
    NodePolicy,
    OperatorRef,
    ReplicationPolicy,
    ResourcePolicy,
    RetryPolicy,
    SystemCommand,
    TimerPolicy,
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


class WorkflowCompilerTests(unittest.TestCase):
    def compile_ok(self, workflow: Workflow) -> WorkflowIR:
        result = WorkflowCompiler().compile(workflow)
        diagnostics = [diagnostic.model_dump() for diagnostic in result.diagnostics]
        self.assertTrue(result.ok, diagnostics)
        self.assertIsNotNone(result.workflow_ir)
        return result.workflow_ir

    def diagnostic_codes(self, result: CompileResult) -> list[str]:
        return [diagnostic.code for diagnostic in result.diagnostics]

    def test_single_callable_node_success_uses_auto_workflow_id_and_version(self):
        workflow_ir = self.compile_ok(Workflow(nodes=[Node(capability=task)]))

        self.assertTrue(workflow_ir.workflow_id.startswith("workflow_"))
        self.assertEqual(1, workflow_ir.workflow_version)
        self.assertEqual(["node_1"], list(workflow_ir.nodes))
        self.assertIs(task, workflow_ir.nodes["node_1"].capability)
        self.assertEqual(("node_1",), workflow_ir.entry_node_ids)
        self.assertEqual(("node_1",), workflow_ir.exit_node_ids)
        self.assertEqual({"node_1": ()}, workflow_ir.graph.outgoing_edges)
        self.assertEqual({"node_1": ()}, workflow_ir.graph.incoming_edges)

    def test_manual_node_ids_are_preserved(self):
        workflow = Workflow(
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

    def test_auto_node_ids_skip_manual_conflicts(self):
        workflow = Workflow(
            nodes=[
                Node(id="node_1", capability=task),
                Node(capability=other_task),
                Node(id="node_3", capability=third_task),
                Node(capability=task),
            ]
        )

        workflow_ir = self.compile_ok(workflow)

        self.assertEqual(["node_1", "node_2", "node_3", "node_4"], list(workflow_ir.nodes))

    def test_auto_edge_ids_use_endpoint_ids_and_duplicate_suffix(self):
        workflow = Workflow(
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
        workflow = Workflow()
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
        workflow = Workflow(
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

    def test_explicit_entry_takes_precedence_over_inferred_entries(self):
        workflow = Workflow(
            nodes=[
                Node(id="first", capability=task),
                Node(id="second", capability=other_task, entry=True),
            ],
            edges=[Edge(from_node="first", to_node="second")],
        )

        workflow_ir = self.compile_ok(workflow)

        self.assertEqual(("second",), workflow_ir.entry_node_ids)
        self.assertFalse(workflow_ir.nodes["first"].entry)
        self.assertTrue(workflow_ir.nodes["second"].entry)

    def test_infers_entries_and_exits(self):
        workflow = Workflow(
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
        workflow = Workflow(
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
        workflow = Workflow(
            nodes=[
                Node(id="from", capability=task),
                Node(id="to", capability=other_task),
            ],
            edges=[Edge(id="conditional", from_node="from", to_node="to", condition=edge_condition)],
        )

        workflow_ir = self.compile_ok(workflow)

        self.assertIs(edge_condition, workflow_ir.edges["conditional"].condition)

    def test_string_edge_condition_emits_unsupported_diagnostic(self):
        workflow = Workflow(
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
        workflow = Workflow(
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
        workflow = Workflow(nodes=[Node(id="limited", capability=task, policy=policy)])

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
        workflow = Workflow(
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
            Workflow(
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

    def test_timer_policy_long_blocking_delay_emits_warning_only(self):
        workflow = Workflow(
            nodes=[
                Node(
                    id="sleep",
                    capability=task,
                    policy=NodePolicy(timer=TimerPolicy(delay_ms=6000, mode="blocking")),
                )
            ]
        )

        result = WorkflowCompiler().compile(workflow)

        self.assertTrue(result.ok)
        self.assertEqual(["POLICY_TIMER_BLOCKING_LONG"], self.diagnostic_codes(result))
        self.assertEqual("warning", result.diagnostics[0].severity)

    def test_invalid_map_policy_emits_diagnostic(self):
        workflow = Workflow(
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

    def test_join_policy_validation_emits_diagnostics(self):
        cases = [
            (
                "missing_count",
                JoinPolicy(mode="n"),
                "JoinPolicy mode n requires count.",
            ),
            (
                "zero_count",
                JoinPolicy(mode="n", count=0),
                "JoinPolicy count must be positive.",
            ),
            (
                "count_too_large",
                JoinPolicy(mode="n", count=3),
                "JoinPolicy count exceeds incoming edge count.",
            ),
            (
                "count_without_n",
                JoinPolicy(mode="all", count=1),
                "JoinPolicy count is only valid when mode is n.",
            ),
        ]

        for name, join_policy, expected_message in cases:
            with self.subTest(name=name):
                workflow = Workflow(
                    nodes=[
                        Node(id="left", capability=task),
                        Node(id="right", capability=other_task),
                        Node(
                            id="join",
                            capability=third_task,
                            policy=NodePolicy(join=join_policy),
                        ),
                    ],
                    edges=[
                        Edge(from_node="left", to_node="join"),
                        Edge(from_node="right", to_node="join"),
                    ],
                )

                result = WorkflowCompiler().compile(workflow)

                self.assertFalse(result.ok)
                self.assertIsNone(result.workflow_ir)
                self.assertEqual(["POLICY_JOIN_INVALID"], self.diagnostic_codes(result))
                self.assertEqual("join", result.diagnostics[0].subject)
                self.assertEqual(expected_message, result.diagnostics[0].message)

    def test_selection_policy_cannot_prefer_and_exclude_same_operator(self):
        workflow = Workflow(
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
        self.assertEqual(["POLICY_SELECTION_INVALID"], self.diagnostic_codes(result))
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
                    Workflow(nodes=[Node(id=name, capability=task, policy=policy)])
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
                    Workflow(
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

    def test_unsupported_capabilities_emit_diagnostics(self):
        cases = [
            ("string", "raw.capability", "CAPABILITY_REF_UNSUPPORTED"),
            ("capability", CapabilityRef(id="capability"), "CAPABILITY_REF_UNSUPPORTED"),
            ("operator", OperatorRef(id="operator"), "OPERATOR_REF_UNSUPPORTED"),
            ("system_command", SystemCommand(id="wait"), "SYSTEM_COMMAND_UNSUPPORTED"),
        ]

        for name, capability, expected_code in cases:
            with self.subTest(name=name):
                result = WorkflowCompiler().compile(
                    Workflow(nodes=[Node(id="unsupported", capability=capability)])
                )

                self.assertFalse(result.ok)
                self.assertIsNone(result.workflow_ir)
                self.assertEqual([expected_code], self.diagnostic_codes(result))
                self.assertEqual("unsupported", result.diagnostics[0].subject)

    def test_duplicate_node_id_emits_diagnostic(self):
        workflow = Workflow(
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
        workflow = Workflow(
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
