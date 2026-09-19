"""Checkpoint boundaries for independent Parent and Child Runtime Sessions."""

from __future__ import annotations

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
    def test_parent_unload_checkpoint_contains_only_parent_session(self) -> None:
        """Keep an unloaded Parent checkpoint independent from its spawned Child."""

        _child, parent = definitions()
        app = AutoAgentApp()
        try:
            result = app.invoke(parent, {"value": 1}, session_id="parent-session")
            app.join(result.output, timeout=1)
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            self.assertEqual(checkpoint.session_id, "parent-session")
            self.assertEqual(checkpoint.state.session.id, "parent-session")
        finally:
            app.close()

    def test_child_unload_checkpoint_uses_generic_ref(self) -> None:
        """Unload the Child Session through its durable InvocationRef."""

        _child, parent = definitions()
        app = AutoAgentApp()
        try:
            parent_result = app.invoke(parent, {"value": 1})
            child_result = app.join(parent_result.output, timeout=1)
            checkpoint = app.unload_session(parent_result.output, capture_checkpoint=True)
            self.assertEqual(checkpoint.session_id, child_result.session_id)
            self.assertEqual(checkpoint.state.invocation.id, child_result.invocation_id)
        finally:
            app.close()

    def test_load_does_not_require_registered_workflow(self) -> None:
        """Install Session State before its executable Workflow is registered."""

        _child, parent = definitions()
        source = AutoAgentApp()
        try:
            result = source.invoke(parent, {"value": 1})
            source.join(result.output, timeout=1)
            checkpoint = source.unload_session(result.ref, capture_checkpoint=True)
        finally:
            source.close()

        target = AutoAgentApp()
        try:
            loaded = target.load_checkpoint(checkpoint)
            self.assertEqual(loaded.invocations[0].session_id, checkpoint.session_id)
            with self.assertRaisesRegex(RuntimeTransitionError, "WORKFLOW_NOT_REGISTERED"):
                target.recover(loaded.invocations[0])
        finally:
            target.close()

    def test_separate_parent_and_child_checkpoints_load_together(self) -> None:
        """Install related Sessions atomically without aggregating their snapshots."""

        _child, parent = definitions()
        source = AutoAgentApp()
        try:
            parent_result = source.invoke(parent, {"value": 1})
            source.join(parent_result.output, timeout=1)
            child_checkpoint = source.unload_session(parent_result.output, capture_checkpoint=True)
            parent_checkpoint = source.unload_session(parent_result.ref, capture_checkpoint=True)
        finally:
            source.close()

        target = AutoAgentApp()
        try:
            loaded = target.load_checkpoint(
                AppCheckpoint((parent_checkpoint, child_checkpoint))
            )
            self.assertEqual(len(loaded.invocations), 2)
        finally:
            target.close()

    def test_load_rejects_child_identity_that_conflicts_with_parent_plan(self) -> None:
        """Reject independently stored Child State with another durable identity."""

        _child, parent = definitions()
        source = AutoAgentApp()
        try:
            parent_result = source.invoke(parent, {"value": 1})
            source.join(parent_result.output, timeout=1)
            child_checkpoint = source.unload_session(parent_result.output, capture_checkpoint=True)
            parent_checkpoint = source.unload_session(parent_result.ref, capture_checkpoint=True)
        finally:
            source.close()

        child_state = child_checkpoint.state
        assert child_state.invocation is not None
        forged = SessionCheckpoint.from_state(
            replace(
                child_state,
                invocation=replace(child_state.invocation, workflow_id="another"),
            )
        )
        target = AutoAgentApp()
        try:
            with self.assertRaisesRegex(
                RuntimeTransitionError, "CHECKPOINT_CHILD_IDENTITY_MISMATCH"
            ):
                target.load_checkpoint(AppCheckpoint((parent_checkpoint, forged)))
            self.assertEqual(target._repository.session_ids(), ())
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
                self.assertEqual(checkpoint.session_id, result.session_id)
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
            self.assertEqual(SessionCheckpoint.from_record(record), checkpoint)
        finally:
            app.close()


if __name__ == "__main__":
    unittest.main()
