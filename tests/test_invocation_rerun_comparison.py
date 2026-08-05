from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from autoagent import AutoAgentApp, AutoAgentSettings, Workflow
from autoagent.debug import DebugQueryService, build_rerun_result


def _history_workflow(workflow_id: str = "rerun_history") -> Workflow:
    def mapping(ctx):
        return {
            "value": ctx.invocation_input["value"],
            "history": list(ctx.session_context.data.get("history", ())),
        }

    def execute(value: int, history: list[int]):
        return {"value": value, "history_before": history}

    def remember(ctx) -> None:
        history = list(ctx.session_context.data.get("history", ()))
        history.append(ctx.output["value"])
        ctx.session_context.data["history"] = history

    workflow = Workflow(id=workflow_id)
    workflow.add_node(
        execute,
        node_id="work",
        input_mapping=mapping,
        output_binding=remember,
    )
    return workflow


class InvocationComparisonTests(unittest.IsolatedAsyncioTestCase):
    async def test_loop_iterations_align_by_execution_scope(self) -> None:
        workflow = Workflow(id="comparison_loop")
        workflow.add_node(lambda: 0, node_id="start")
        workflow.add_node(
            lambda value: value + 1,
            node_id="agent",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        workflow.add_node(
            lambda value: value,
            node_id="finish",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        workflow.add_edge("start", "agent", edge_id="enter")
        workflow.add_edge(
            "agent",
            "agent",
            edge_id="continue",
            condition=lambda ctx: ctx.source_output < 3,
        )
        workflow.add_edge(
            "agent",
            "finish",
            edge_id="exit",
            condition=lambda ctx: ctx.source_output >= 3,
        )
        app = AutoAgentApp(settings=AutoAgentSettings())
        app.register_workflow(workflow)
        await app.astart()
        try:
            baseline = await app.ainvoke(workflow, event_mode="full")
            candidate = await app.ainvoke(workflow, event_mode="full")
            comparison = await DebugQueryService(
                app.runtime_store,
                source="server",
            ).compare(baseline.id, candidate.id)
        finally:
            await app.aclose()

        self.assertEqual(0, comparison.difference_count)

    async def test_parallel_completion_order_does_not_create_graph_differences(
        self,
    ) -> None:
        calls = {"left": 0, "right": 0}

        async def left() -> str:
            calls["left"] += 1
            await asyncio.sleep(0.01 if calls["left"] == 1 else 0)
            return "left"

        async def right() -> str:
            calls["right"] += 1
            await asyncio.sleep(0 if calls["right"] == 1 else 0.01)
            return "right"

        def collect_input(ctx):
            return {
                "values": sorted(item.value for item in ctx.incoming),
            }

        workflow = Workflow(id="comparison_parallel")
        workflow.add_node(lambda: None, node_id="start")
        workflow.add_node(left, node_id="left")
        workflow.add_node(right, node_id="right")
        workflow.add_node(
            lambda values: values,
            node_id="collect",
            input_mapping=collect_input,
        )
        workflow.add_edge("start", "left")
        workflow.add_edge("start", "right")
        workflow.add_edge("left", "collect")
        workflow.add_edge("right", "collect")
        app = AutoAgentApp(settings=AutoAgentSettings())
        app.register_workflow(workflow)
        await app.astart()
        try:
            baseline = await app.ainvoke(workflow, event_mode="full")
            candidate = await app.ainvoke(workflow, event_mode="full")
            comparison = await DebugQueryService(
                app.runtime_store,
                source="server",
            ).compare(baseline.id, candidate.id)
        finally:
            await app.aclose()

        self.assertTrue(comparison.result_equal)
        self.assertFalse(
            any(
                difference.category in {"node", "edge", "operator_call"}
                for difference in comparison.differences
            )
        )

    async def test_full_comparison_aligns_semantic_node_and_reports_output_change(
        self,
    ) -> None:
        def baseline_operator(value: int) -> int:
            return value

        def candidate_operator(value: int) -> int:
            return value + 1

        baseline_workflow = Workflow(id="comparison_revision")
        baseline_workflow.add_node(baseline_operator, node_id="work")
        candidate_workflow = Workflow(id="comparison_revision")
        candidate_workflow.add_node(candidate_operator, node_id="work")
        app = AutoAgentApp(settings=AutoAgentSettings())
        app.register_workflow(baseline_workflow)
        app.register_workflow(candidate_workflow)
        await app.astart()
        try:
            baseline = await app.ainvoke(
                baseline_workflow,
                input={"value": 2},
                event_mode="full",
            )
            candidate = await app.ainvoke(
                candidate_workflow,
                input={"value": 2},
                event_mode="full",
            )
            comparison = await DebugQueryService(
                app.runtime_store,
                source="server",
            ).compare(baseline.id, candidate.id)
        finally:
            await app.aclose()

        self.assertEqual("comparable", comparison.status)
        self.assertTrue(comparison.input_equal)
        self.assertFalse(comparison.result_equal)
        self.assertNotEqual(
            comparison.baseline_workflow_revision_id,
            comparison.candidate_workflow_revision_id,
        )
        self.assertTrue(
            any(
                difference.category == "node" and difference.key == "work"
                for difference in comparison.differences
            )
        )

    async def test_comparison_rejects_different_modes(self) -> None:
        workflow = Workflow(id="comparison_modes")
        workflow.add_node(lambda value: value, node_id="work")
        app = AutoAgentApp(settings=AutoAgentSettings())
        app.register_workflow(workflow)
        await app.astart()
        try:
            baseline = await app.ainvoke(
                workflow,
                input={"value": 1},
                event_mode="minimal",
            )
            candidate = await app.ainvoke(
                workflow,
                input={"value": 2},
                event_mode="full",
            )
            with self.assertRaisesRegex(ValueError, "matching Event modes"):
                await DebugQueryService(
                    app.runtime_store,
                    source="server",
                ).compare(baseline.id, candidate.id)
        finally:
            await app.aclose()

    async def test_comparison_rejects_minimal_mode(self) -> None:
        workflow = Workflow(id="comparison_minimal")
        workflow.add_node(lambda value: value, node_id="work")
        app = AutoAgentApp(settings=AutoAgentSettings())
        app.register_workflow(workflow)
        await app.astart()
        try:
            baseline = await app.ainvoke(workflow, event_mode="minimal")
            candidate = await app.ainvoke(workflow, event_mode="minimal")
            with self.assertRaisesRegex(ValueError, "Standard or Full"):
                await DebugQueryService(
                    app.runtime_store,
                    source="server",
                ).compare(baseline.id, candidate.id)
        finally:
            await app.aclose()

    async def test_different_workflows_are_incompatible(self) -> None:
        first = Workflow(id="comparison_first")
        first.add_node(lambda: "done", node_id="work")
        second = Workflow(id="comparison_second")
        second.add_node(lambda: "done", node_id="work")
        app = AutoAgentApp(settings=AutoAgentSettings())
        app.register_workflow(first)
        app.register_workflow(second)
        await app.astart()
        try:
            left = await app.ainvoke(first, event_mode="full")
            right = await app.ainvoke(second, event_mode="full")
            comparison = await DebugQueryService(
                app.runtime_store,
                source="server",
            ).compare(left.id, right.id)
        finally:
            await app.aclose()

        self.assertEqual("incompatible", comparison.status)
        self.assertIsNone(comparison.workflow_id)
        self.assertIn(
            "WORKFLOW_IDS_DIFFER",
            {warning.code for warning in comparison.warnings},
        )


class InvocationRerunTests(unittest.IsolatedAsyncioTestCase):
    async def test_rerun_executes_current_registered_revision(self) -> None:
        baseline = Workflow(id="rerun_revision", version=1)
        baseline.add_node(lambda value: value, node_id="work")
        candidate_workflow = Workflow(id="rerun_revision", version=2)
        candidate_workflow.add_node(lambda value: value + 1, node_id="work")
        app = AutoAgentApp(settings=AutoAgentSettings())
        app.register_workflow(baseline)
        app.register_workflow(candidate_workflow)
        await app.astart()
        try:
            source = await app.ainvoke(
                baseline,
                input={"value": 4},
                event_mode="full",
            )
            admitted, candidate = await app._arerun(
                candidate_workflow,
                source_invocation_id=source.id,
            )
        finally:
            await app.aclose()

        self.assertNotEqual(
            admitted.source_workflow_revision_id,
            candidate.workflow_revision_id,
        )
        self.assertEqual({"value": 4}, candidate.input)
        self.assertEqual({"output": 5}, candidate.result)
        self.assertEqual("full", candidate.event_mode)

    async def test_standard_rerun_copies_pre_invocation_session_context(self) -> None:
        workflow = _history_workflow()
        app = AutoAgentApp(settings=AutoAgentSettings())
        app.register_workflow(workflow)
        await app.astart()
        try:
            await app.ainvoke(
                workflow,
                input={"value": 1},
                session_id="conversation",
                event_mode="standard",
            )
            source = await app.ainvoke(
                workflow,
                input={"value": 2},
                session_id="conversation",
                event_mode="standard",
            )
            original_session = app.runtime_store.find_session(
                workflow_revision_id=source.workflow_revision_id,
                session_key="conversation",
            )
            assert original_session is not None
            admitted, candidate = await app._arerun(
                workflow,
                source_invocation_id=source.id,
            )
            result = build_rerun_result(admitted, candidate)
        finally:
            await app.aclose()

        self.assertEqual(source.input, candidate.input)
        self.assertEqual(source.entry_node_id, candidate.entry_node_id)
        self.assertEqual(source.result, candidate.result)
        self.assertEqual([1, 2], original_session.context.data["history"])
        self.assertNotEqual(result.session_id, str(original_session.id))
        self.assertEqual("standard", candidate.event_mode)

    async def test_minimal_cannot_be_rerun(self) -> None:
        workflow = _history_workflow("rerun_minimal")
        app = AutoAgentApp(settings=AutoAgentSettings())
        app.register_workflow(workflow)
        await app.astart()
        try:
            source = await app.ainvoke(
                workflow,
                input={"value": 3},
                event_mode="minimal",
            )
            with self.assertRaisesRegex(ValueError, "Standard or Full"):
                await app._arerun(workflow, source_invocation_id=source.id)
        finally:
            await app.aclose()

    async def test_standard_genesis_supports_rerun_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_url = f"sqlite:///{Path(directory) / 'runtime.db'}"
            workflow = _history_workflow("rerun_database")
            first = AutoAgentApp(
                settings=AutoAgentSettings(database_url=database_url)
            )
            first.register_workflow(workflow)
            await first.astart()
            await first.ainvoke(
                workflow,
                input={"value": 1},
                session_id="conversation",
                event_mode="standard",
            )
            source = await first.ainvoke(
                workflow,
                input={"value": 2},
                session_id="conversation",
                event_mode="standard",
            )
            await first.runtime_store.aflush()
            source_id = source.id
            source_result = source.result
            await first.aclose()

            reopened = AutoAgentApp(
                settings=AutoAgentSettings(database_url=database_url)
            )
            reopened.register_workflow(workflow)
            await reopened.astart()
            try:
                admitted, candidate = await reopened._arerun(
                    workflow,
                    source_invocation_id=source_id,
                )
                result = build_rerun_result(admitted, candidate)
            finally:
                await reopened.aclose()

        self.assertEqual(source_result, candidate.result)
        self.assertEqual("standard", candidate.event_mode)
