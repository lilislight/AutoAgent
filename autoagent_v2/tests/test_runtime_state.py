from __future__ import annotations

import copy
import json
import threading
import unittest
from uuid import UUID

from autoagent.core import (
    AutoAgentApp,
    ContextPatch,
    EventMode,
    Node,
    OutputBindingContext,
    Workflow,
)
from autoagent.core.runtime import (
    RUNTIME_STATE_SCHEMA_VERSION,
    RuntimeEvent,
    RuntimeState,
    SerializedCheckpoint,
    SerializedEvent,
    StateOperation,
)


class RecordingSink:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []
        self.checkpoints: list[object] = []

    async def wait_until_admissible(self) -> None:
        return None

    async def submit_events(self, events: tuple[SerializedEvent, ...]) -> None:
        self.events.extend(
            event.decode() for event in events if event.channel == "runtime"
        )

    def offer_checkpoint(self, checkpoint: SerializedCheckpoint) -> None:
        self.checkpoints.append(checkpoint.decode())


class RuntimeStateOperationTests(unittest.TestCase):
    def test_captured_operation_is_reused_without_second_value_traversal(self) -> None:
        captured = StateOperation.capture(
            StateOperation("add", ("value",), {"items": list(range(20))})
        )

        self.assertIs(StateOperation.capture(captured), captured)
        self.assertEqual(captured.to_record()["value"]["items"], list(range(20)))

    def make_state(self) -> RuntimeState:
        return RuntimeState.create(
            workflow_id="workflow",
            workflow_revision_id="workflow:revision",
            session_id="session",
            invocation_id=UUID("00000000-0000-0000-0000-000000000001"),
            event_mode="full",
            invocation_input={"value": 1},
            session_context={"profile": {"name": "before"}},
            session_created_at_ms=10,
            invocation_created_at_ms=20,
        )

    def test_ordered_batch_updates_python_and_checkpoint_state(self) -> None:
        state = self.make_state()

        batch = state.apply(
            (
                StateOperation("replace", ("invocation", "state"), "running"),
                StateOperation("add", ("scheduler", "ready", "-"), "a"),
                StateOperation("remove", ("scheduler", "ready", 0)),
                StateOperation("add", ("scheduler", "ready", "-"), "b"),
                StateOperation(
                    "add",
                    (
                        "node_executions",
                        "00000000-0000-0000-0000-000000000002",
                    ),
                    {
                        "id": "00000000-0000-0000-0000-000000000002",
                        "node_id": "node",
                        "scope": [],
                        "state": "running",
                        "input": None,
                        "output": None,
                        "error": None,
                        "logical_occurrence": 1,
                        "idempotency_key": None,
                        "started_state_version": 0,
                        "restart_session_context": {},
                        "restart_invocation_context": {},
                    },
                ),
            )
        )

        self.assertEqual(batch.state_version, 1)
        self.assertEqual(state.state_version, 1)
        self.assertEqual(state.read("invocation", "state"), "running")
        self.assertEqual(state.read("scheduler", "ready"), ["b"])
        self.assertEqual(
            state.checkpoint_record()["state"]["node_executions"]
            ["00000000-0000-0000-0000-000000000002"]["state"],
            "running",
        )

    def test_bad_later_operation_rolls_back_whole_batch(self) -> None:
        state = self.make_state()
        before = state.checkpoint_record()

        with self.assertRaises(KeyError):
            state.apply(
                (
                    StateOperation(
                        "replace", ("invocation", "state"), "running"
                    ),
                    StateOperation("remove", ("scheduler", "missing")),
                )
            )

        self.assertEqual(state.state_version, 0)
        self.assertEqual(state.checkpoint_record(), before)
        self.assertEqual(state.read("invocation", "state"), "created")

    def test_empty_batch_cannot_create_a_fake_state_version(self) -> None:
        state = self.make_state()

        with self.assertRaisesRegex(ValueError, "cannot be empty"):
            state.apply(())

        self.assertEqual(state.state_version, 0)

    def test_add_existing_and_replace_missing_are_rejected_atomically(self) -> None:
        state = self.make_state()
        with self.assertRaises(KeyError):
            state.apply((StateOperation("add", ("invocation", "state"), "x"),))
        with self.assertRaises(KeyError):
            state.apply((StateOperation("replace", ("invocation", "missing"), 1),))
        self.assertEqual(state.state_version, 0)

    def test_operation_capture_detaches_caller_owned_value(self) -> None:
        state = self.make_state()
        caller = {"items": [1]}
        state.apply((StateOperation("add", ("invocation", "context", "value"), caller),))
        caller["items"].append(2)
        self.assertEqual(
            state.read("invocation", "context", "value"), {"items": [1]}
        )
        self.assertEqual(
            state.checkpoint_record()["state"]["invocation"]["context"]["value"],
            {"items": [1]},
        )

    def test_checkpoint_round_trip_restores_runtime_types_and_version(self) -> None:
        state = self.make_state()
        state.apply(
            (
                StateOperation(
                    "add", ("invocation", "context", "pair"), ("a", 1)
                ),
            )
        )

        restored = RuntimeState.from_checkpoint_record(state.checkpoint_record())

        self.assertEqual(restored.state_version, 1)
        self.assertEqual(restored.read("invocation", "context", "pair"), ("a", 1))
        restored.apply(
            (StateOperation("replace", ("invocation", "state"), "completed"),)
        )
        self.assertEqual(restored.state_version, 2)

    def test_isolate_never_exposes_runtime_owned_nested_value(self) -> None:
        state = self.make_state()
        isolated = state.isolate("session", "context")
        isolated["profile"]["name"] = "hook-only"
        self.assertEqual(
            state.read("session", "context", "profile", "name"), "before"
        )

    def test_operation_batch_record_preserves_atomic_boundary_and_order(self) -> None:
        state = self.make_state()
        batch = state.apply(
            (
                StateOperation("replace", ("invocation", "state"), "running"),
                StateOperation("replace", ("invocation", "state"), "waiting"),
            )
        )
        restored = type(batch).from_record(batch.to_record())
        self.assertEqual(restored.state_version, 1)
        self.assertEqual(
            [operation.value for operation in restored.operations],
            ["running", "waiting"],
        )

    def test_create_has_exact_canonical_schema_and_defaults(self) -> None:
        state = self.make_state()
        value = state.read()

        self.assertEqual(value["schema_version"], RUNTIME_STATE_SCHEMA_VERSION)
        self.assertEqual(
            set(value),
            {
                "schema_version",
                "session",
                "invocation",
                "scheduler",
                "node_executions",
                "waits",
                "pending_advances",
                "counters",
            },
        )
        self.assertEqual(value["invocation"]["state"], "created")
        self.assertEqual(value["scheduler"]["ready"], [])
        self.assertEqual(value["scheduler"]["active_requests"], {})
        self.assertEqual(value["pending_advances"], {})
        self.assertEqual(value["counters"]["operator_attempts"], {})

    def test_schema_round_trip_is_json_and_runtime_type_safe(self) -> None:
        state = self.make_state()
        state.apply(
            (
                StateOperation(
                    "replace", ("invocation", "input"), {"pair": ("a", 1)}
                ),
            )
        )
        record = state.checkpoint_record()

        json.dumps(record)
        restored = RuntimeState.from_checkpoint_record(record)

        self.assertEqual(restored.read("invocation", "input", "pair"), ("a", 1))
        self.assertEqual(restored.state_version, 1)


class LiveRuntimeStateIntegrationTests(unittest.TestCase):
    def make_state(self) -> RuntimeState:
        return RuntimeState.create(
            workflow_id="workflow",
            workflow_revision_id="workflow:revision",
            session_id="session",
            invocation_id=UUID("00000000-0000-0000-0000-000000000001"),
            event_mode="full",
            invocation_input={"value": 1},
            session_context={"profile": {"name": "before"}},
            session_created_at_ms=10,
            invocation_created_at_ms=20,
        )

    def test_live_invocation_identity_and_state_have_one_canonical_owner(self) -> None:
        started = threading.Event()
        release = threading.Event()

        def block(value: int) -> int:
            started.set()
            release.wait(2)
            return value

        app = AutoAgentApp()
        workflow = Workflow("live-runtime-owner", nodes=[Node("node", block)])
        app.register_workflow(workflow)
        invocation = app.submit_invoke(workflow, 3, session_id="session")
        self.assertTrue(started.wait(1))

        execution = app._active[invocation.id]
        state = execution.runtime_state.read()
        self.assertEqual(state["session"]["id"], "session")
        self.assertEqual(state["session"]["workflow_id"], workflow.id)
        self.assertEqual(state["invocation"]["id"], str(invocation.id))
        self.assertEqual(state["invocation"]["state"], "running")
        self.assertEqual(state["invocation"]["input"], 3)
        self.assertIs(app._sessions["session"]._runtime_state, execution.runtime_state)

        detached = app._sessions["session"].context
        detached["not_runtime"] = True
        self.assertEqual(execution.runtime_state.read("session", "context"), {})

        release.set()
        invocation.wait(2)
        self.assertEqual(
            app._sessions["session"]._runtime_state.read("invocation", "state"),
            "completed",
        )
        app.close()

    def test_context_commit_and_full_event_reuse_the_applied_batch(self) -> None:
        def identity(value: int) -> int:
            return value

        def bind(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(
                session={"turns": 1},
                invocation={"answer": context.output},
            )

        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "context-operation-owner",
            nodes=[Node("node", identity, output_binding=bind)],
        )
        app.register_workflow(workflow)

        invocation = app.invoke(
            workflow, 7, session_id="session", event_mode="full"
        )
        runtime_state = app._sessions["session"]._runtime_state
        binding = next(
            event
            for event in sink.events
            if event.event_name == "output_binding_finished"
        )
        context_batch = next(
            batch
            for batch in binding.operation_batches
            if any(operation.path[:2] == ("session", "context") for operation in batch.operations)
        )

        self.assertEqual(
            [(operation.op, operation.path, operation.value) for operation in context_batch.operations],
            [
                ("add", ("session", "context", "turns"), 1),
                ("add", ("invocation", "context", "answer"), 7),
                (
                    "add",
                    ("invocation", "context_path_revisions", "/answer"),
                    context_batch.state_version,
                ),
                (
                    "add",
                    ("session", "context_path_revisions", "/turns"),
                    context_batch.state_version,
                ),
            ],
        )
        self.assertEqual(runtime_state.read("session", "context"), {"turns": 1})
        self.assertEqual(runtime_state.read("invocation", "context"), {"answer": 7})
        self.assertEqual(runtime_state.read("invocation", "output"), {"node": 7})
        self.assertEqual(
            runtime_state.read("invocation", "runtime_event_sequence"),
            len(sink.events),
        )
        self.assertEqual(
            invocation.latest_checkpoint.runtime_event_sequence,
            len(sink.events),
        )
        app.close()

    def test_full_event_batches_replay_genesis_to_identical_terminal_state(self) -> None:
        def plus_one(value: int) -> int:
            return value + 1

        def bind(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation={"latest": context.output})

        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "operation-replay",
            nodes=[Node("node", plus_one, output_binding=bind)],
        )
        app.register_workflow(workflow)
        execution = app._runtime.run(
            app._create_execution(
                workflow,
                4,
                session_id="session",
                event_mode=EventMode.FULL,
                stream=None,
            )
        )
        genesis = execution.runtime_state.checkpoint_record()

        async def launch_and_wait() -> None:
            app._launch(execution)
            await execution.boundary.wait()

        app._runtime.run(launch_and_wait())
        final_record = execution.runtime_state.checkpoint_record()
        replayed = RuntimeState.from_checkpoint_record(genesis)

        batches = [
            batch for event in sink.events for batch in event.operation_batches
        ]
        self.assertTrue(batches)
        for batch in batches:
            self.assertEqual(batch.state_version, replayed.state_version + 1)
            applied = replayed.apply(batch.operations)
            self.assertEqual(applied.to_record(), batch.to_record())

        self.assertEqual(replayed.checkpoint_record(), final_record)
        self.assertEqual(
            final_record["state"]["counters"],
            {
                "node_executions": {"node": 1},
                "operator_attempts": {"node": 1},
                "operator_runtime_ns": final_record["state"]["counters"][
                    "operator_runtime_ns"
                ],
            },
        )
        self.assertEqual(
            next(iter(final_record["state"]["node_executions"].values()))[
                "state"
            ],
            "completed",
        )
        terminal_node = next(
            iter(final_record["state"]["node_executions"].values())
        )
        self.assertNotIn("started_at_ms", terminal_node)
        self.assertNotIn("completed_at_ms", terminal_node)
        self.assertNotIn("duration_ns", terminal_node)
        self.assertIsNone(terminal_node["restart_session_context"])
        self.assertIsNone(terminal_node["restart_invocation_context"])
        app.close()

    def test_wait_and_resume_are_owned_by_runtime_state(self) -> None:
        from autoagent.core import WaitOperator

        app = AutoAgentApp()
        workflow = Workflow(
            "runtime-wait-owner",
            nodes=[Node("approval", WaitOperator(str, str))],
        )
        app.register_workflow(workflow)
        invocation = app.submit_invoke(workflow, "request", session_id="session")
        invocation.waits  # force the Handle projection read while work starts
        deadline = threading.Event()
        for _ in range(1_000):
            if invocation.waits:
                break
            deadline.wait(0.001)
        self.assertTrue(invocation.waits)
        execution = app._active[invocation.id]
        state = execution.runtime_state.read()
        wait_id = str(invocation.waits[0].id)
        node_id = str(invocation.waits[0].node_execution_id)
        self.assertEqual(state["waits"][wait_id]["payload"], "request")
        self.assertEqual(state["node_executions"][node_id]["state"], "waiting")

        app.resume(invocation, invocation.waits[0].id, "approved")
        final = app._sessions["session"]._runtime_state.read()
        self.assertEqual(final["waits"], {})
        self.assertEqual(final["pending_advances"], {})
        self.assertEqual(final["node_executions"][node_id]["state"], "completed")
        app.close()

    def test_schema_rejects_unknown_missing_and_invalid_fields(self) -> None:
        record = self.make_state().checkpoint_record()

        unknown = copy.deepcopy(record)
        unknown["state"]["unknown"] = {}
        with self.assertRaisesRegex(ValueError, "extra=.*unknown"):
            RuntimeState.from_checkpoint_record(unknown)

        missing = copy.deepcopy(record)
        missing["state"].pop("waits")
        with self.assertRaisesRegex(ValueError, "missing=.*waits"):
            RuntimeState.from_checkpoint_record(missing)

        invalid = copy.deepcopy(record)
        invalid["state"]["invocation"]["state"] = "paused"
        with self.assertRaisesRegex(ValueError, "invocation.state"):
            RuntimeState.from_checkpoint_record(invalid)

    def test_operation_cannot_remove_required_schema_field(self) -> None:
        state = self.make_state()
        before = state.checkpoint_record()

        with self.assertRaisesRegex(ValueError, "missing=.*invocation"):
            state.apply((StateOperation("remove", ("invocation",)),))

        self.assertEqual(state.checkpoint_record(), before)
        self.assertEqual(state.state_version, 0)

    def test_read_returns_detached_values_and_cannot_bypass_operations(self) -> None:
        state = self.make_state()
        session = state.read("session")
        session["context"]["profile"]["name"] = "mutated"

        self.assertEqual(
            state.read("session", "context", "profile", "name"), "before"
        )
        self.assertEqual(state.state_version, 0)

    def test_create_rejects_invalid_event_mode_and_invocation_id(self) -> None:
        common = {
            "workflow_id": "workflow",
            "workflow_revision_id": "revision",
            "session_id": "session",
            "invocation_input": 1,
            "session_created_at_ms": 1,
            "invocation_created_at_ms": 1,
        }
        with self.assertRaisesRegex(ValueError, "event_mode"):
            RuntimeState.create(
                **common,
                invocation_id=UUID("00000000-0000-0000-0000-000000000001"),
                event_mode="unknown",
            )
        with self.assertRaisesRegex(ValueError, "UUID"):
            RuntimeState.create(
                **common,
                invocation_id="not-a-uuid",
                event_mode="standard",
            )


if __name__ == "__main__":
    unittest.main()
