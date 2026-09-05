"""Cross-Session Child semantics enforced when loading checkpoints."""

from __future__ import annotations

import unittest
from dataclasses import replace

from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp,
    Edge,
    InvocationUpdate,
    Node,
    RuntimeCheckpointBundle,
    RuntimeTransitionError,
    Workflow,
)
from autoagent.core.runtime import freeze


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


def _definitions() -> tuple[Workflow, Workflow]:
    child = Workflow("checkpoint-child", nodes=[Node("work", identity)])
    parent = Workflow(
        "checkpoint-parent",
        nodes=[Node("start", identity), Node("child", child)],
        edges=[Edge("start", "child")],
    )
    return child, parent


def _completed_checkpoint(parent: Workflow) -> RuntimeCheckpointBundle:
    app = AutoAgentApp()
    try:
        return app.invoke(
            parent,
            {"value": 1},
            session_id="checkpoint-parent-session",
        ).checkpoint
    finally:
        app.close()


def _replace_plan(
    checkpoint: RuntimeCheckpointBundle,
    **changes: object,
) -> RuntimeCheckpointBundle:
    states = dict(checkpoint.states)
    root = states[checkpoint.root_session_id]
    invocation = root.invocation
    assert invocation is not None
    creation_id, plan = next(iter(invocation.child_plans.items()))
    changed = replace(plan, **changes)
    states[checkpoint.root_session_id] = replace(
        root,
        invocation=replace(
            invocation,
            child_plans={creation_id: changed},
        ),
    )
    return RuntimeCheckpointBundle.from_states(checkpoint.root_session_id, states)


class ChildCheckpointValidationTests(unittest.TestCase):
    def _assert_load_rejected(
        self,
        checkpoint: RuntimeCheckpointBundle,
        *workflows: Workflow,
    ) -> None:
        app = AutoAgentApp()
        try:
            for workflow in workflows:
                app.register_workflow(workflow)
            with self.assertRaisesRegex(
                RuntimeTransitionError,
                "CHECKPOINT_CHILD_SEMANTICS_INVALID",
            ):
                app.load_checkpoint(checkpoint)
            self.assertEqual(app._journal.session_ids(), ())
        finally:
            app.close()

    def test_child_plan_requires_a_child_workflow_parent_node(self) -> None:
        """Reject a plan attached to an ordinary parent Node occurrence."""

        _child, parent = _definitions()
        checkpoint = _completed_checkpoint(parent)
        root = checkpoint.state(checkpoint.root_session_id)
        invocation = root.invocation
        assert invocation is not None
        ordinary_occurrence_id = next(
            item.id
            for item in invocation.scheduler.occurrences.values()
            if item.node_id == "start"
        )
        invalid = _replace_plan(
            checkpoint,
            parent_occurrence_id=ordinary_occurrence_id,
        )

        self._assert_load_rejected(invalid, parent)

    def test_child_plan_mode_must_match_parent_node(self) -> None:
        """Reject a Spawn plan recovered for an Await Child Workflow Node."""

        _child, parent = _definitions()
        invalid = _replace_plan(
            _completed_checkpoint(parent),
            mode="spawn",
        )

        self._assert_load_rejected(invalid, parent)

    def test_child_plan_revision_must_match_parent_node(self) -> None:
        """Reject a valid Child State owned by another registered revision."""

        _child, parent = _definitions()
        alternate_child = Workflow(
            "checkpoint-child",
            nodes=[Node("work", identity)],
            version="2",
        )
        compiler_app = AutoAgentApp()
        try:
            alternate_ir = compiler_app.register_workflow(alternate_child)
        finally:
            compiler_app.close()

        checkpoint = _completed_checkpoint(parent)
        states = dict(checkpoint.states)
        root = states[checkpoint.root_session_id]
        root_invocation = root.invocation
        assert root_invocation is not None
        creation_id, plan = next(iter(root_invocation.child_plans.items()))
        unit = plan.units[0]
        child_state = states[unit.session_id]
        child_invocation = child_state.invocation
        assert child_invocation is not None
        states[unit.session_id] = replace(
            child_state,
            invocation=replace(
                child_invocation,
                workflow_revision_id=alternate_ir.workflow_revision_id,
            ),
        )
        changed_plan = replace(
            plan,
            workflow_revision_id=alternate_ir.workflow_revision_id,
        )
        states[checkpoint.root_session_id] = replace(
            root,
            invocation=replace(
                root_invocation,
                child_plans={creation_id: changed_plan},
            ),
        )
        invalid = RuntimeCheckpointBundle.from_states(
            checkpoint.root_session_id,
            states,
        )

        self._assert_load_rejected(invalid, parent, alternate_child)

    def test_child_invocation_input_must_match_parent_plan(self) -> None:
        """Reject a Child State that would replay a different durable input."""

        _child, parent = _definitions()
        checkpoint = _completed_checkpoint(parent)
        states = dict(checkpoint.states)
        root = states[checkpoint.root_session_id]
        root_invocation = root.invocation
        assert root_invocation is not None
        plan = next(iter(root_invocation.child_plans.values()))
        child_state = states[plan.units[0].session_id]
        child_invocation = child_state.invocation
        assert child_invocation is not None
        states[plan.units[0].session_id] = replace(
            child_state,
            invocation=replace(
                child_invocation,
                input=freeze({"value": 999}),
            ),
        )
        invalid = RuntimeCheckpointBundle.from_states(
            checkpoint.root_session_id,
            states,
        )

        self._assert_load_rejected(invalid, parent)

    def test_completed_spawn_parent_cannot_reference_a_missing_planned_child(self) -> None:
        """Reject a completed Handle result without its owned Child State."""

        child = Workflow("checkpoint-spawn-child", nodes=[Node("work", identity)])
        parent = Workflow(
            "checkpoint-spawn-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        checkpoint = _completed_checkpoint(parent)
        root = checkpoint.state(checkpoint.root_session_id)
        invocation = root.invocation
        assert invocation is not None
        creation_id, plan = next(iter(invocation.child_plans.items()))
        missing_unit = replace(plan.units[0], phase="planned")
        forged_plan = replace(plan, units=(missing_unit,))
        forged_root = replace(
            root,
            invocation=replace(
                invocation,
                child_plans={creation_id: forged_plan},
            ),
        )
        invalid = RuntimeCheckpointBundle.from_states(
            checkpoint.root_session_id,
            {checkpoint.root_session_id: forged_root},
        )

        self._assert_load_rejected(invalid, parent)

    def test_completed_child_occurrence_requires_a_closed_plan_phase(self) -> None:
        """Reject a completed Spawn Node whose admitted Child remains planned."""

        child = Workflow("checkpoint-phase-child", nodes=[Node("work", identity)])
        parent = Workflow(
            "checkpoint-phase-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        checkpoint = _completed_checkpoint(parent)
        root = checkpoint.state(checkpoint.root_session_id)
        invocation = root.invocation
        assert invocation is not None
        creation_id, plan = next(iter(invocation.child_plans.items()))
        forged_plan = replace(
            plan,
            units=(replace(plan.units[0], phase="planned"),),
        )
        forged_root = replace(
            root,
            invocation=replace(
                invocation,
                child_plans={creation_id: forged_plan},
            ),
        )
        states = dict(checkpoint.states)
        states[checkpoint.root_session_id] = forged_root
        invalid = RuntimeCheckpointBundle.from_states(
            checkpoint.root_session_id,
            states,
        )

        self._assert_load_rejected(invalid, parent)

    def test_planned_child_without_runtime_state_remains_loadable(self) -> None:
        """Allow the write-ahead plan boundary before Child Session admission."""

        _child, parent = _definitions()
        source = AutoAgentApp()
        stream = source.stream(
            parent,
            {"value": 1},
            session_id="planned-child-session",
        )
        checkpoint = None
        try:
            for item in stream:
                if (
                    isinstance(item, InvocationUpdate)
                    and item.event.kind == "child_invocation.planned"
                ):
                    checkpoint = item.checkpoint
                    break
        finally:
            stream.close()
            source.close()
        assert checkpoint is not None
        root = checkpoint.state(checkpoint.root_session_id)
        invocation = root.invocation
        assert invocation is not None
        unit = next(iter(invocation.child_plans.values())).units[0]
        self.assertEqual(unit.phase, "planned")
        self.assertNotIn(unit.session_id, checkpoint.states)

        target = AutoAgentApp()
        try:
            target.register_workflow(parent)
            loaded = target.load_checkpoint(checkpoint)
            self.assertEqual(loaded.roots[0].session_id, checkpoint.root_session_id)
            reloaded = target.load_checkpoint(checkpoint)
            self.assertEqual(reloaded, loaded)
        finally:
            target.close()


if __name__ == "__main__":
    unittest.main()
