from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from pydantic import BaseModel
from sqlalchemy import func, select

from autoagent import AutoAgentApp
from autoagent.compiler import WorkflowCompiler
from autoagent.observer import ObservationService
from autoagent.runtime import (
    EdgeActivation,
    Invocation,
    SQLiteRuntimeStore,
    SessionBusyError,
)
from autoagent.runtime.database_models import (
    InvocationRow,
    NodeExecutionRow,
    OperatorCallRow,
    RuntimeEventRow,
    RuntimeProjectionCheckpointRow,
    SessionRow,
    WorkflowVersionRow,
)
from autoagent.workflow import (
    EdgePolicy,
    MapPolicy,
    NodePolicy,
    OperatorRef,
    ReplicationPolicy,
    RetryPolicy,
    SystemCommand,
    Workflow,
)


class DurableMessage(BaseModel):
    text: str


def build_durable_message(text: str) -> DurableMessage:
    return DurableMessage(text=text.upper())


class SQLiteRuntimeStoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temporary_directory.name) / "runtime.db"
        self.store = SQLiteRuntimeStore.from_path(self.database_path)

    async def asyncTearDown(self) -> None:
        await self.store.close()
        self.temporary_directory.cleanup()

    async def test_completed_invocation_round_trips_state_events_and_observation(self) -> None:
        def produce(value: str) -> dict[str, str]:
            return {"value": value.upper()}

        workflow = Workflow(id="sqlite_complete")
        workflow.add_node(produce, node_id="produce")
        app = AutoAgentApp(runtime_store=self.store)

        completed = await app.ainvoke(
            workflow,
            input={"value": "saved"},
            session_id="session-1",
        )
        await self.store.close()

        reopened = SQLiteRuntimeStore.from_path(self.database_path)
        self.store = reopened
        loaded = await reopened.aload_invocation(completed.id)
        session = await reopened.afind_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="session-1",
        )
        snapshot = await reopened.aload_workflow_snapshot(
            namespace="default",
            workflow_id=workflow.id,
            definition_hash=completed.workflow_definition_hash,
        )

        self.assertIsNotNone(loaded)
        self.assertIsNotNone(session)
        self.assertIsNotNone(snapshot)
        assert loaded is not None and session is not None and snapshot is not None
        self.assertEqual("completed", loaded.state)
        self.assertEqual({"output": {"value": "SAVED"}}, loaded.result)
        self.assertEqual(completed.workflow_definition_hash, snapshot.definition_hash)
        self.assertEqual(1, len(loaded.node_executions))
        self.assertEqual(1, len(loaded.node_executions[0].operator_calls))
        events = await reopened.alist_runtime_events(
            session_id=session.id,
            invocation_id=loaded.id,
            limit=10_000,
        )
        self.assertGreater(len(events), 0)
        self.assertEqual(
            list(range(1, len(events) + 1)),
            [event.sequence for event in events],
        )
        latest_page = await reopened.alist_runtime_events(
            session_id=session.id,
            invocation_id=loaded.id,
            before_sequence=events[-1].sequence + 1,
            limit=2,
        )
        previous_page = await reopened.alist_runtime_events(
            session_id=session.id,
            invocation_id=loaded.id,
            before_sequence=latest_page[0].sequence,
            limit=2,
        )
        self.assertEqual(events[-2:], latest_page)
        self.assertEqual(events[-4:-2], previous_page)
        observation = await ObservationService(
            reopened,
            bootstrap_event_limit=3,
        ).bootstrap(
            session_id=session.id,
            invocation_id=loaded.id,
        )
        self.assertEqual("sqlite_complete", observation.graph.workflow_id)
        self.assertEqual("completed", observation.projection.invocation_state)
        manifest = loaded.node_executions[0].operator_calls[0].operator_manifest
        self.assertIsNotNone(manifest)
        assert manifest is not None
        self.assertEqual("never", manifest.recovery_mode)

        async with reopened._sessions() as database:
            counts = [
                await database.scalar(select(func.count()).select_from(model))
                for model in (
                    WorkflowVersionRow,
                    SessionRow,
                    InvocationRow,
                    NodeExecutionRow,
                    OperatorCallRow,
                    RuntimeEventRow,
                    RuntimeProjectionCheckpointRow,
                )
            ]
        self.assertEqual([1, 1, 1, 1, 1, len(events), 1], counts)

    async def test_running_operator_call_is_durable_before_handler_continues(
        self,
    ) -> None:
        handler_started = asyncio.Event()
        release_handler = asyncio.Event()

        async def process(value: str) -> str:
            handler_started.set()
            await release_handler.wait()
            return value.upper()

        workflow = Workflow(id="sqlite_call_checkpoint")
        workflow.add_node(process, node_id="process")
        app = AutoAgentApp(runtime_store=self.store)
        task = asyncio.create_task(
            app.ainvoke(
                workflow,
                input={"value": "saved"},
                session_id="checkpoint",
            )
        )
        await asyncio.wait_for(handler_started.wait(), timeout=1)

        session = await self.store.afind_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="checkpoint",
        )
        assert session is not None and session.current_invocation_id is not None
        running = await self.store.aload_invocation(session.current_invocation_id)
        assert running is not None
        call = running.node_executions[0].operator_calls[0]
        self.assertEqual("running", call.state)

        release_handler.set()
        completed = await asyncio.wait_for(task, timeout=1)
        stored = await self.store.aload_invocation(completed.id)
        assert stored is not None
        final_call = stored.node_executions[0].operator_calls[0]
        self.assertEqual(call.id, final_call.id)
        self.assertEqual("completed", final_call.state)

    async def test_app_registers_runtime_model_before_restart_load(self) -> None:
        workflow = Workflow(id="sqlite_registered_model")
        workflow.add_node(build_durable_message, node_id="message")
        app = AutoAgentApp(runtime_store=self.store)
        completed = await app.ainvoke(
            workflow,
            input={"text": "persisted"},
            session_id="model",
        )
        await self.store.close()

        reopened = SQLiteRuntimeStore.from_path(self.database_path)
        self.store = reopened
        AutoAgentApp(
            runtime_store=reopened,
            runtime_models=(DurableMessage,),
        )
        loaded = await reopened.aload_invocation(completed.id)

        assert loaded is not None
        self.assertEqual(
            DurableMessage(text="PERSISTED"),
            loaded.node_executions[0].output,
        )

    async def test_wait_survives_close_and_resumes_in_new_app(self) -> None:
        workflow = Workflow(id="sqlite_wait")
        workflow.add_node(SystemCommand(id="wait"), node_id="approval")
        app = AutoAgentApp(runtime_store=self.store)

        waiting = await app.ainvoke(
            workflow,
            input={"wait_key": "approval:42", "payload": {"ticket": 42}},
            session_id="reviewer",
        )
        self.assertEqual("waiting", waiting.state)
        await self.store.close()

        reopened = SQLiteRuntimeStore.from_path(self.database_path)
        self.store = reopened
        restarted_app = AutoAgentApp(runtime_store=reopened)
        resumed = await restarted_app.aresume(
            workflow,
            session_id="reviewer",
            wait_key="approval:42",
            output={"approved": True},
        )

        self.assertEqual("completed", resumed.state)
        self.assertEqual({"output": {"approved": True}}, resumed.result)
        loaded = await reopened.aload_invocation(waiting.id)
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual("completed", loaded.state)
        self.assertEqual({"approved": True}, loaded.node_executions[0].output)

        with self.assertRaisesRegex(ValueError, "waiting Invocation"):
            await restarted_app.aresume(
                workflow,
                session_id="reviewer",
                wait_key="approval:42",
            )

    async def test_workflow_definition_mismatch_cannot_claim_old_wait(self) -> None:
        workflow = Workflow(id="versioned_wait", version=1)
        workflow.add_node(SystemCommand(id="wait"), node_id="approval")
        first_app = AutoAgentApp(runtime_store=self.store)
        waiting = await first_app.ainvoke(
            workflow,
            input={"wait_key": "approval"},
            session_id="same-session",
        )
        self.assertEqual("waiting", waiting.state)

        changed = Workflow(id="versioned_wait", version=2)
        changed.add_node(SystemCommand(id="wait"), node_id="approval")
        second_app = AutoAgentApp(runtime_store=self.store)
        with self.assertRaisesRegex(ValueError, "different Workflow definition"):
            await second_app.aresume(
                changed,
                session_id="same-session",
                wait_key="approval",
            )

        still_waiting = await self.store.aload_invocation(waiting.id)
        self.assertIsNotNone(still_waiting)
        assert still_waiting is not None
        self.assertEqual("waiting", still_waiting.state)

    async def test_store_recovery_marks_non_replayed_work_terminal_interrupted(self) -> None:
        def work() -> str:
            return "done"

        workflow = Workflow(id="crashed_never")
        workflow.add_node(work, node_id="work")
        compiled = WorkflowCompiler().compile(workflow)
        assert compiled.workflow_ir is not None
        assert compiled.workflow_snapshot is not None
        await self.store.asave_workflow_snapshot(
            "default",
            compiled.workflow_snapshot,
        )
        session = await self.store.aget_or_create_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="crashed-session",
        )
        invocation = Invocation(
            workflow_id=workflow.id,
            workflow_version=1,
            workflow_definition_hash=compiled.workflow_ir.definition_hash,
            workflow_operator_manifest_hash=(
                compiled.workflow_snapshot.operator_manifest_hash
            ),
            entry_node_id="work",
        )
        execution = invocation.create_node_execution("work")
        invocation.mark_node_running(execution.id, input={})
        await self.store.aadmit_invocation(session.id, invocation)

        recovered = await self.store.arecover_interrupted_invocations()
        self.assertEqual(1, len(recovered))
        self.assertEqual("interrupted", recovered[0].state)
        self.assertEqual("interrupted", recovered[0].node_executions[0].state)

        # Interrupted is terminal, so a fresh request may enter this Session.
        replacement = Invocation(
            workflow_id=workflow.id,
            workflow_version=1,
            workflow_definition_hash=compiled.workflow_ir.definition_hash,
            workflow_operator_manifest_hash=(
                compiled.workflow_snapshot.operator_manifest_hash
            ),
            entry_node_id="work",
        )
        admitted = await self.store.aadmit_invocation(session.id, replacement)
        self.assertEqual(replacement.id, admitted.current_invocation_id)

    async def test_first_new_request_automatically_replays_compatible_crash(self) -> None:
        app = AutoAgentApp(runtime_store=self.store)

        @app.operator(
            "safe_operator",
            version=1,
            recovery_mode="replay_safe",
        )
        def safe_operator(value: str) -> str:
            return f"recovered:{value}"

        workflow = Workflow(id="automatic_recovery")
        workflow.add_node(
            OperatorRef(id="safe_operator"),
            node_id="safe",
            policy=NodePolicy(retry=RetryPolicy(max_attempts=2)),
        )
        compiled = app.compiler.compile(workflow)
        assert compiled.workflow_ir is not None
        assert compiled.workflow_snapshot is not None
        await self.store.asave_workflow_snapshot("default", compiled.workflow_snapshot)
        session = await self.store.aget_or_create_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="recoverable",
        )
        interrupted = Invocation(
            workflow_id=workflow.id,
            workflow_version=1,
            workflow_definition_hash=compiled.workflow_ir.definition_hash,
            workflow_operator_manifest_hash=(
                compiled.workflow_snapshot.operator_manifest_hash
            ),
            entry_node_id="safe",
            input={"value": "old"},
        )
        interrupted.scheduler.drain_ready()
        execution = interrupted.create_node_execution(
            "safe",
            input={"value": "old"},
            idempotency_key="stable-operation",
        )
        interrupted.mark_node_running(execution.id, input={"value": "old"})
        await self.store.aadmit_invocation(session.id, interrupted)
        await self.store.close()

        reopened = SQLiteRuntimeStore.from_path(self.database_path)
        self.store = reopened
        restarted = AutoAgentApp(runtime_store=reopened)

        @restarted.operator(
            "safe_operator",
            version=1,
            recovery_mode="replay_safe",
        )
        def safe_operator_after_restart(value: str) -> str:
            return f"recovered:{value}"

        # The unfinished Invocation takes precedence; "new" is not mixed into
        # it. A second invoke can submit new input after recovery completes.
        recovered = await restarted.ainvoke(
            workflow,
            input={"value": "new"},
            session_id="recoverable",
        )

        self.assertEqual(interrupted.id, recovered.id)
        self.assertEqual("completed", recovered.state)
        self.assertEqual({"output": "recovered:old"}, recovered.result)
        self.assertEqual(2, len(recovered.node_executions))
        self.assertEqual("interrupted", recovered.node_executions[0].state)
        self.assertEqual("completed", recovered.node_executions[1].state)
        self.assertEqual(
            recovered.node_executions[0].id,
            recovered.node_executions[1].recovery_of_execution_id,
        )
        self.assertEqual(
            "recover",
            recovered.node_executions[1].operator_calls[0].kind,
        )

    async def test_created_invocation_recovers_without_replaying_an_operator(self) -> None:
        def unsafe(value: str) -> str:
            return f"completed:{value}"

        workflow = Workflow(id="created_recovery")
        workflow.add_node(unsafe, node_id="unsafe")
        compiled = WorkflowCompiler().compile(workflow)
        assert compiled.workflow_ir is not None
        assert compiled.workflow_snapshot is not None
        await self.store.asave_workflow_snapshot("default", compiled.workflow_snapshot)
        session = await self.store.aget_or_create_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="created",
        )
        created = Invocation(
            workflow_id=workflow.id,
            workflow_version=1,
            workflow_definition_hash=compiled.workflow_ir.definition_hash,
            workflow_operator_manifest_hash=(
                compiled.workflow_snapshot.operator_manifest_hash
            ),
            entry_node_id="unsafe",
            input={"value": "old"},
        )
        await self.store.aadmit_invocation(session.id, created)
        await self.store.close()

        reopened = SQLiteRuntimeStore.from_path(self.database_path)
        self.store = reopened
        restarted = AutoAgentApp(runtime_store=reopened)
        recovered = await restarted.ainvoke(
            workflow,
            input={"value": "new"},
            session_id="created",
        )

        # No Operator had started before the crash, so even the default
        # recovery_mode="never" does not forbid continuing the admitted work.
        self.assertEqual(created.id, recovered.id)
        self.assertEqual({"output": "completed:old"}, recovered.result)

    async def test_replay_safe_operator_still_obeys_retry_replay_limit(self) -> None:
        app = AutoAgentApp(runtime_store=self.store)

        @app.operator("bounded_recovery", recovery_mode="replay_safe")
        def bounded_recovery(value: str) -> str:
            return value

        workflow = Workflow(id="bounded_recovery")
        workflow.add_node(
            OperatorRef(id="bounded_recovery"),
            node_id="bounded",
            policy=NodePolicy(retry=RetryPolicy(max_attempts=1)),
        )
        compiled = app.compiler.compile(workflow)
        assert compiled.workflow_ir is not None
        assert compiled.workflow_snapshot is not None
        await self.store.asave_workflow_snapshot("default", compiled.workflow_snapshot)
        session = await self.store.aget_or_create_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="bounded",
        )
        old = Invocation(
            workflow_id=workflow.id,
            workflow_version=1,
            workflow_definition_hash=compiled.workflow_ir.definition_hash,
            workflow_operator_manifest_hash=(
                compiled.workflow_snapshot.operator_manifest_hash
            ),
            entry_node_id="bounded",
            input={"value": "old"},
        )
        old.scheduler.drain_ready()
        execution = old.create_node_execution("bounded", input={"value": "old"})
        old.mark_node_running(execution.id, input={"value": "old"})
        await self.store.aadmit_invocation(session.id, old)
        await self.store.close()

        reopened = SQLiteRuntimeStore.from_path(self.database_path)
        self.store = reopened
        restarted = AutoAgentApp(runtime_store=reopened)

        @restarted.operator("bounded_recovery", recovery_mode="replay_safe")
        def bounded_recovery_after_restart(value: str) -> str:
            return value

        fresh = await restarted.ainvoke(
            workflow,
            input={"value": "new"},
            session_id="bounded",
        )
        loaded_old = await reopened.aload_invocation(old.id)

        self.assertNotEqual(old.id, fresh.id)
        self.assertEqual({"output": "new"}, fresh.result)
        self.assertIsNotNone(loaded_old)
        assert loaded_old is not None
        self.assertEqual("interrupted", loaded_old.state)
        self.assertIsNotNone(loaded_old.error)
        assert loaded_old.error is not None
        self.assertIn("exhausted RetryPolicy.max_attempts", loaded_old.error.message)

    async def test_non_recoverable_crash_is_interrupted_before_new_invocation(self) -> None:
        def unsafe(value: str) -> str:
            return f"new:{value}"

        workflow = Workflow(id="automatic_abort")
        workflow.add_node(
            unsafe,
            node_id="unsafe",
            policy=NodePolicy(retry=RetryPolicy(max_attempts=2)),
        )
        compiled = WorkflowCompiler().compile(workflow)
        assert compiled.workflow_ir is not None
        assert compiled.workflow_snapshot is not None
        await self.store.asave_workflow_snapshot("default", compiled.workflow_snapshot)
        session = await self.store.aget_or_create_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="non-recoverable",
        )
        old = Invocation(
            workflow_id=workflow.id,
            workflow_version=1,
            workflow_definition_hash=compiled.workflow_ir.definition_hash,
            workflow_operator_manifest_hash=(
                compiled.workflow_snapshot.operator_manifest_hash
            ),
            entry_node_id="unsafe",
            input={"value": "old"},
        )
        old.scheduler.drain_ready()
        execution = old.create_node_execution("unsafe", input={"value": "old"})
        old.mark_node_running(execution.id, input={"value": "old"})
        await self.store.aadmit_invocation(session.id, old)
        await self.store.close()

        reopened = SQLiteRuntimeStore.from_path(self.database_path)
        self.store = reopened
        restarted = AutoAgentApp(runtime_store=reopened)
        fresh = await restarted.ainvoke(
            workflow,
            input={"value": "new"},
            session_id="non-recoverable",
        )

        self.assertNotEqual(old.id, fresh.id)
        self.assertEqual("completed", fresh.state)
        self.assertEqual({"output": "new:new"}, fresh.result)
        loaded_old = await reopened.aload_invocation(old.id)
        self.assertIsNotNone(loaded_old)
        assert loaded_old is not None
        self.assertEqual("interrupted", loaded_old.state)
        self.assertEqual("NODE_RECOVERY_REJECTED", loaded_old.error.code)

    async def test_operator_upgrade_interrupts_old_invocation_and_starts_new_one(self) -> None:
        first_app = AutoAgentApp(runtime_store=self.store)

        @first_app.operator("versioned", version=1, recovery_mode="replay_safe")
        def version_one(value: str) -> str:
            return f"v1:{value}"

        workflow = Workflow(id="operator_upgrade")
        workflow.add_node(
            OperatorRef(id="versioned"),
            node_id="versioned",
            policy=NodePolicy(retry=RetryPolicy(max_attempts=2)),
        )
        compiled = first_app.compiler.compile(workflow)
        assert compiled.workflow_ir is not None
        assert compiled.workflow_snapshot is not None
        await self.store.asave_workflow_snapshot("default", compiled.workflow_snapshot)
        session = await self.store.aget_or_create_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="upgrade",
        )
        old = Invocation(
            workflow_id=workflow.id,
            workflow_version=1,
            workflow_definition_hash=compiled.workflow_ir.definition_hash,
            workflow_operator_manifest_hash=(
                compiled.workflow_snapshot.operator_manifest_hash
            ),
            entry_node_id="versioned",
            input={"value": "old"},
        )
        old.scheduler.drain_ready()
        execution = old.create_node_execution(
            "versioned",
            input={"value": "old"},
        )
        old.mark_node_running(execution.id, input={"value": "old"})
        await self.store.aadmit_invocation(session.id, old)
        await self.store.close()

        reopened = SQLiteRuntimeStore.from_path(self.database_path)
        self.store = reopened
        restarted = AutoAgentApp(runtime_store=reopened)

        @restarted.operator("versioned", version=2, recovery_mode="replay_safe")
        def version_two(value: str) -> str:
            return f"v2:{value}"

        fresh = await restarted.ainvoke(
            workflow,
            input={"value": "new"},
            session_id="upgrade",
        )
        loaded_old = await reopened.aload_invocation(old.id)

        self.assertNotEqual(old.id, fresh.id)
        self.assertEqual({"output": "v2:new"}, fresh.result)
        self.assertIsNotNone(loaded_old)
        assert loaded_old is not None
        self.assertEqual("interrupted", loaded_old.state)
        self.assertEqual("OPERATOR_MANIFEST_CHANGED", loaded_old.error.code)
        async with reopened._sessions() as database:
            version_count = await database.scalar(
                select(func.count()).select_from(WorkflowVersionRow)
            )
        self.assertEqual(2, version_count)

    async def test_live_invocation_is_not_misclassified_as_crash(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        app = AutoAgentApp(runtime_store=self.store)

        @app.operator("slow", recovery_mode="replay_safe")
        async def slow(value: str) -> str:
            started.set()
            await release.wait()
            return value

        workflow = Workflow(id="live_not_crashed")
        workflow.add_node(OperatorRef(id="slow"), node_id="slow")
        first = asyncio.create_task(
            app.ainvoke(workflow, input={"value": "first"}, session_id="same")
        )
        await started.wait()

        with self.assertRaises(SessionBusyError):
            await app.ainvoke(
                workflow,
                input={"value": "second"},
                session_id="same",
            )

        release.set()
        completed = await first
        self.assertEqual({"output": "first"}, completed.result)
        self.assertEqual(1, len(completed.node_executions))

    async def test_concurrent_resume_has_one_winner(self) -> None:
        workflow = Workflow(id="single_resume_winner")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        app = AutoAgentApp(runtime_store=self.store)
        await app.ainvoke(
            workflow,
            input={"wait_key": "signal"},
            session_id="session",
        )

        results = await asyncio.gather(
            app.aresume(
                workflow,
                session_id="session",
                wait_key="signal",
                output={"winner": 1},
            ),
            app.aresume(
                workflow,
                session_id="session",
                wait_key="signal",
                output={"winner": 2},
            ),
            return_exceptions=True,
        )

        successes = [item for item in results if isinstance(item, Invocation)]
        failures = [item for item in results if isinstance(item, Exception)]
        self.assertEqual(1, len(successes))
        self.assertEqual(1, len(failures))
        self.assertEqual("completed", successes[0].state)

    async def test_namespace_is_part_of_durable_session_identity(self) -> None:
        workflow = Workflow(id="namespace_isolation")
        workflow.add_node(lambda value: value, node_id="echo")
        first_app = AutoAgentApp(namespace="first", runtime_store=self.store)
        second_app = AutoAgentApp(namespace="second", runtime_store=self.store)

        first = await first_app.ainvoke(
            workflow,
            input={"value": "one"},
            session_id="same-key",
        )
        # A distinct App needs its own Workflow object because registries own
        # object identity independently even when the definition is identical.
        second_workflow = Workflow(id="namespace_isolation")
        second_workflow.add_node(lambda value: value, node_id="echo")
        second = await second_app.ainvoke(
            second_workflow,
            input={"value": "two"},
            session_id="same-key",
        )

        first_session = await self.store.afind_session(
            namespace="first",
            workflow_id=workflow.id,
            session_key="same-key",
        )
        second_session = await self.store.afind_session(
            namespace="second",
            workflow_id=workflow.id,
            session_key="same-key",
        )
        self.assertIsNotNone(first_session)
        self.assertIsNotNone(second_session)
        assert first_session is not None and second_session is not None
        self.assertNotEqual(first_session.id, second_session.id)
        self.assertEqual({"output": "one"}, first.result)
        self.assertEqual({"output": "two"}, second.result)

    async def test_replication_recovery_replays_whole_node(self) -> None:
        app = AutoAgentApp(runtime_store=self.store)

        @app.operator("sample", version=1, recovery_mode="replay_safe")
        def sample(value: int) -> int:
            return value

        def aggregate(values: list[int]) -> int:
            return sum(values)

        workflow = Workflow(id="replication_recovery")
        workflow.add_node(
            OperatorRef(id="sample"),
            node_id="sample",
            policy=NodePolicy(
                retry=RetryPolicy(max_attempts=2),
                replication=ReplicationPolicy(
                    count=2,
                    output_aggregator=aggregate,
                ),
            ),
        )
        compiled = app.compiler.compile(workflow)
        assert compiled.workflow_ir is not None
        assert compiled.workflow_snapshot is not None
        await self.store.asave_workflow_snapshot("default", compiled.workflow_snapshot)
        session = await self.store.aget_or_create_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="replication",
        )
        old = Invocation(
            workflow_id=workflow.id,
            workflow_version=1,
            workflow_definition_hash=compiled.workflow_ir.definition_hash,
            workflow_operator_manifest_hash=(
                compiled.workflow_snapshot.operator_manifest_hash
            ),
            entry_node_id="sample",
            input={"value": 2},
        )
        old.scheduler.drain_ready()
        execution = old.create_node_execution("sample", input={"value": 2})
        old.mark_node_running(execution.id, input={"value": 2})
        await self.store.aadmit_invocation(session.id, old)
        await self.store.close()

        reopened = SQLiteRuntimeStore.from_path(self.database_path)
        self.store = reopened
        restarted = AutoAgentApp(runtime_store=reopened)

        @restarted.operator("sample", version=1, recovery_mode="replay_safe")
        def sample_after_restart(value: int) -> int:
            return value

        recovered = await restarted.ainvoke(
            workflow,
            input={"value": 99},
            session_id="replication",
        )

        self.assertEqual(old.id, recovered.id)
        self.assertEqual({"output": 4}, recovered.result)
        self.assertEqual(2, len(recovered.node_executions[-1].operator_calls))
        self.assertTrue(
            all(
                call.kind == "recover"
                for call in recovered.node_executions[-1].operator_calls
            )
        )

    async def test_map_recovery_replays_whole_logical_node(self) -> None:
        app = AutoAgentApp(runtime_store=self.store)

        @app.operator("map_item", version=1, recovery_mode="replay_safe")
        def map_item(value: int) -> int:
            return value * value

        workflow = Workflow(id="map_recovery")
        workflow.add_node(lambda: [1, 2, 3], node_id="source")
        workflow.add_node(
            OperatorRef(id="map_item"),
            node_id="square",
            policy=NodePolicy(retry=RetryPolicy(max_attempts=2)),
        )
        workflow.add_edge(
            "source",
            "square",
            edge_id="source_to_square",
            policy=EdgePolicy(
                map=MapPolicy(
                    item_selector=lambda output: [
                        {"value": item} for item in output
                    ],
                    output_aggregator=lambda outputs: tuple(outputs),
                )
            ),
        )
        compiled = app.compiler.compile(workflow)
        assert compiled.workflow_ir is not None
        assert compiled.workflow_snapshot is not None
        await self.store.asave_workflow_snapshot("default", compiled.workflow_snapshot)
        session = await self.store.aget_or_create_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="map",
        )
        old = Invocation(
            workflow_id=workflow.id,
            workflow_version=1,
            workflow_definition_hash=compiled.workflow_ir.definition_hash,
            workflow_operator_manifest_hash=(
                compiled.workflow_snapshot.operator_manifest_hash
            ),
            entry_node_id="source",
        )
        old.scheduler.drain_ready()
        source = old.create_node_execution("source", input={})
        source.mark_completed([1, 2, 3])
        activation = EdgeActivation(
            edge_id="source_to_square",
            source_node_id="source",
            source_execution_id=source.id,
        )
        interrupted = old.create_node_execution(
            "square",
            input=[1, 2, 3],
            incoming_activations=(activation,),
        )
        old.mark_node_running(interrupted.id, input=[1, 2, 3])
        await self.store.aadmit_invocation(session.id, old)
        await self.store.close()

        reopened = SQLiteRuntimeStore.from_path(self.database_path)
        self.store = reopened
        restarted = AutoAgentApp(runtime_store=reopened)

        @restarted.operator("map_item", version=1, recovery_mode="replay_safe")
        def map_item_after_restart(value: int) -> int:
            return value * value

        recovered = await restarted.ainvoke(
            workflow,
            input={"ignored": True},
            session_id="map",
        )

        self.assertEqual(old.id, recovered.id)
        self.assertEqual({"output": (1, 4, 9)}, recovered.result)
        replacement = recovered.node_executions[-1]
        self.assertEqual(interrupted.id, replacement.recovery_of_execution_id)
        self.assertEqual(
            [0, 1, 2],
            [call.item_index for call in replacement.operator_calls],
        )
        self.assertTrue(
            all(call.kind == "recover" for call in replacement.operator_calls)
        )


class SQLiteSyncAdapterTests(unittest.TestCase):
    def test_repeated_sync_invoke_and_close_across_event_loops(self) -> None:
        def echo(value: str) -> str:
            return value

        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore.from_path(Path(directory) / "sync.db")
            app = AutoAgentApp(runtime_store=store)
            workflow = Workflow(id="sync_sqlite")
            workflow.add_node(echo, node_id="echo")
            try:
                first = app.invoke(
                    workflow,
                    input={"value": "one"},
                    session_id="one",
                )
                second = app.invoke(
                    workflow,
                    input={"value": "two"},
                    session_id="two",
                )
            finally:
                app.close()

        self.assertEqual({"output": "one"}, first.result)
        self.assertEqual({"output": "two"}, second.result)
