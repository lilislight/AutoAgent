"""Checkpoint boundaries for independent Parent and Child Runtime Sessions."""

from __future__ import annotations

from autoagent import RuntimeGraphCheckpoint

from tests.graph_fixtures import (
    join_observed,
    load_graph,
    root_snapshot,
)

import asyncio
import json
import unittest
from dataclasses import replace

from typing_extensions import TypedDict

from autoagent import (
    AppCheckpoint,
    AutoAgentApp,
    Node,
    RuntimeTransitionError,
    SessionCheckpoint,
    Workflow,
)


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


def definitions() -> tuple[Workflow, Workflow]:
    child = Workflow("checkpoint-child", nodes=[Node("work", identity)])
    parent = Workflow(
        "checkpoint-parent",
        nodes=[Node("child", child, execution_mode="spawn")],
    )
    return child, parent


class ChildCheckpointValidationTests(unittest.TestCase):
    def test_parent_unload_checkpoint_contains_complete_graph(self) -> None:
        """Keep every owned Child in the Root graph checkpoint."""

        _child, parent = definitions()
        app = AutoAgentApp()
        try:
            result = app.invoke(parent, {"value": 1}, session_id="parent-session")
            join_observed(app, result.output, timeout=1)
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            self.assertEqual(root_snapshot(checkpoint).session_id, "parent-session")
            self.assertEqual(root_snapshot(checkpoint).state.session.id, "parent-session")
        finally:
            app.close()


    def test_load_does_not_require_registered_workflow(self) -> None:
        """Install Session State before its executable Workflow is registered."""

        _child, parent = definitions()
        source = AutoAgentApp()
        try:
            result = source.invoke(parent, {"value": 1})
            join_observed(source, result.output, timeout=1)
            checkpoint = source.unload_session(result.ref, capture_checkpoint=True)
        finally:
            source.close()

        target = AutoAgentApp()
        try:
            loaded = load_graph(target, checkpoint)
            self.assertEqual(loaded.invocations[0].session_id, root_snapshot(checkpoint).session_id)
            with self.assertRaisesRegex(RuntimeTransitionError, "WORKFLOW_NOT_REGISTERED"):
                target.recover(loaded.invocations[0])
        finally:
            target.close()



    def test_async_unload_returns_the_requested_session_checkpoint(self) -> None:
        """Return one Session checkpoint through the async unload facade."""

        async def run() -> None:
            app = AutoAgentApp()
            try:
                result = await app.ainvoke(
                    Workflow("async-checkpoint", nodes=[Node("work", identity)]),
                    {"value": 1},
                )
                checkpoint = await app.aunload_session(result.ref, capture_checkpoint=True)
                self.assertEqual(root_snapshot(checkpoint).session_id, result.session_id)
            finally:
                await app.aclose()

        asyncio.run(run())

    def test_session_checkpoint_record_round_trips_canonically(self) -> None:
        """Round-trip one immutable Session checkpoint through canonical JSON."""

        app = AutoAgentApp()
        try:
            result = app.invoke(
                Workflow("checkpoint-codec", nodes=[Node("work", identity)]),
                {"value": 1},
            )
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            record = json.loads(json.dumps(checkpoint.to_record()))
            self.assertEqual(RuntimeGraphCheckpoint.from_record(record), checkpoint)
        finally:
            app.close()


if __name__ == "__main__":
    unittest.main()
