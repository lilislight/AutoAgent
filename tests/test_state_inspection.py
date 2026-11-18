"""Exact read-only historical Context and state inspection on both databases."""

import asyncio
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from sqlalchemy import update
from typing_extensions import TypedDict

from autoagent import (
    Await,
    ContextOperation,
    ContextPatch,
    Edge,
    Node,
    Recovery,
    OutputBindingContext,
    Spawn,
    Wait,
    Workflow,
)
from autoagent.core.app import AutoAgentApp
from autoagent.core.runtime.events import RuntimeEvent
from autoagent.core.runtime.reducer import StateReducer
from autoagent.core.runtime.state import RuntimeState
from autoagent.core.runtime.values import thaw
from autoagent.host import AutoAgentHost, HostNotFoundError, HostRunConflict, HostSettings
from autoagent.host.persistence.inspection import BoundedReader
from autoagent.host.persistence.models import PersistenceConfig
from autoagent.host.persistence.schema import meta
from autoagent.host.persistence.storage import Storage
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class Value(TypedDict):
    text: str


def echo(value: Value) -> Value:
    return value


def bind(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(
        session=(ContextOperation.set("last", context.output["text"]), ContextOperation.set("nullable", None)),
        invocation=(ContextOperation.set("result", context.output),))


class StateInspectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        # Full-suite dataset tests can leave Python arenas resident. These tests
        # exercise history reads, independently of process residency admission.
        self.settings = HostSettings(database_url="sqlite+aiosqlite:///" + str(Path(self.temp.name) / "state.db"),
            memory_budget_bytes=2 * 1024 * 1024 * 1024)

    async def asyncTearDown(self):
        self.temp.cleanup()

    def workflow(self, identity="inspect"):
        return Workflow(identity, nodes=[Node("write", echo, output_binding=bind),
            Node("ask", Wait(Value, Value), recovery_mode=Recovery("replay_safe")), Node("finish", echo, output_binding=bind)],
            edges=[Edge("write", "ask", id="ask"), Edge("ask", "finish", id="finish")])

    def host(self, workflows=None, *, settings=None, read_only=False):
        return AutoAgentHost(workflows=workflows or [], settings=settings or self.settings,
            environment={}, read_only=read_only)

    async def start(self, host, *, workflow="inspect", session_id="session", context=None):
        ref = await host.execute(workflow, {"text": "before"}, entry_node_id="write",
            session_id=session_id, session_context=context)
        await host.join(ref)
        await host._persistence.flush()
        return ref

    async def events(self, host, ref, name=None):
        await host._persistence.flush()
        return (await host.list_execution_events(ref.invocation_id, names=[name] if name else (), limit=200)).items

    async def test_every_event_matches_core_reducer_and_commits_only_at_node_completion(self):
        """Historical reads match Core after each delta, including staged but uncommitted Output Binding."""
        async with self.host([self.workflow()]) as host:
            ref = await self.start(host, context={"initial": "present"})
            records = (await host._persistence.runtime_events(ref.session_id, limit=200)).items
            state = RuntimeState()
            saw_pending = False
            for record in records:
                event = RuntimeEvent.from_record(record)
                state = StateReducer().apply(state, event)
                if event.invocation_id is None:
                    continue
                summary = await host.inspect_execution_state(ref.invocation_id, sequence=event.sequence)
                self.assertEqual(summary["state"]["status"], state.invocation.status)
                self.assertEqual(summary["state"]["control_state"], state.invocation.control_state)
                self.assertEqual(summary["position"]["event_id"], event.id)
                for scope in ("session", "invocation"):
                    selected = await host.read_execution_context(ref.invocation_id, sequence=event.sequence, scope=scope)
                    self.assertEqual(selected["value"], thaw(getattr(state, scope).context), (event.event_name, scope))
                if event.event_name == "output.bound":
                    saw_pending = True
                    self.assertNotIn("last", state.session.context)
            self.assertTrue(saw_pending)

    async def test_fixed_position_survives_resume_and_later_workflow_in_same_session(self):
        """Later commits and another Workflow never leak into an older Invocation's selected Context."""
        async with self.host([self.workflow(), self.workflow("other")]) as host:
            ref = await self.start(host)
            target = (await self.events(host, ref))[-1]
            before = await host.read_execution_context(ref.invocation_id, sequence=target["sequence"], scope="session")
            waiting = await host.join(ref)
            await host.respond(ref.invocation_id, waiting.waits[0].id, {"text": "after"})
            await host.join(ref)
            later = await self.start(host, workflow="other")
            after = await host.read_execution_context(ref.invocation_id, sequence=target["sequence"], scope="session")
            self.assertEqual(before["value"], {"last": "before", "nullable": None})
            self.assertEqual(after["value"], before["value"])
            self.assertEqual(after["position"], before["position"])
            later_event = (await self.events(host, later))[0]
            with self.assertRaises(ValueError):
                await host.inspect_execution_state(ref.invocation_id, sequence=later_event["sequence"])

    async def test_pointer_escaping_null_missing_and_container_types(self):
        """Pointers distinguish null, absent fields, dictionary numeric keys and strict array indexes."""
        context = {"a/b": {"~key": [{"0": "zero", "value": None}]}, "foobar": "other"}
        async with self.host([self.workflow()]) as host:
            ref = await self.start(host, context=context)
            sequence = (await self.events(host, ref))[-1]["sequence"]
            for pointer, expected, available in (("/a~1b/~0key/0/0", "zero", True),
                ("/a~1b/~0key/0/value", None, True), ("/a~1b/~0key/01", None, False),
                ("/foo", None, False), ("/last/text", None, False), ("/a~1b/~0key/5", None, False)):
                result = await host.read_execution_context(ref.invocation_id, sequence=sequence, scope="session", pointer=pointer)
                self.assertEqual((result["available"], result["value"]), (available, expected), pointer)

    async def test_pointer_does_not_hydrate_large_sibling_and_budgets_are_explicit(self):
        """Small fields remain readable without decoding sibling Artifacts and oversized reads never truncate."""
        async with self.host([self.workflow()]) as host:
            ref = await self.start(host, context={"small": {"chosen": "yes"}, "large": "x" * (2 * 1024 * 1024)})
            sequence = (await self.events(host, ref))[-1]["sequence"]
            original = Storage.row
            async def reject_large(storage, conn, name, **identity):
                if name == "artifacts":
                    row = await original(storage, conn, name, **identity)
                    self.assertLess(len(row["body"]), 1024 * 1024, "Unrelated large Artifact hydrated")
                    return row
                return await original(storage, conn, name, **identity)
            with patch.object(Storage, "row", reject_large):
                result = await host.read_execution_context(ref.invocation_id, sequence=sequence, scope="session", pointer="/small/chosen")
                self.assertEqual(result["value"], "yes")
                summary = await host.inspect_execution_state(ref.invocation_id, sequence=sequence)
                self.assertNotIn("context", summary["state"])
            with self.assertRaises(HostRunConflict) as error:
                await host.read_execution_context(ref.invocation_id, sequence=sequence, scope="session")
            self.assertEqual(error.exception.code, "INSPECTION_LIMIT")

    async def test_read_only_reader_does_not_activate_flush_or_write(self):
        """A concurrent reader needs no code or Runtime and leaves Events/Checkpoints and writer queues untouched."""
        async with self.host([self.workflow()]) as writer:
            ref = await self.start(writer)
            sequence = (await self.events(writer, ref))[-1]["sequence"]
            before = await writer.list_checkpoints(ref.session_id)
            async with self.host(read_only=True) as reader:
                with patch("autoagent.host.host.Runtime", side_effect=AssertionError("No Runtime")), \
                     patch.object(reader._persistence, "flush", side_effect=AssertionError("No Flush")):
                    self.assertIsNone(reader._runtime)
                    await reader.inspect_execution_state(ref.invocation_id, sequence=sequence)
                    result = await reader.read_execution_context(ref.invocation_id, sequence=sequence, scope="session", pointer="/last")
                    self.assertEqual(result["value"], "before")
            after = await writer.list_checkpoints(ref.session_id)
            self.assertEqual(before, after)

    async def test_checkpoint_base_reads_only_selected_member_and_preserves_results(self):
        """A same-position Checkpoint supplies selected fields without replaying Events or loading Context siblings."""
        async with self.host([self.workflow()]) as host:
            ref = await self.start(host, context={"large": "x" * (2 * 1024 * 1024)})
            sequence = (await self.events(host, ref))[-1]["sequence"]
            checkpoint = await host.create_checkpoint(ref.session_id)
            with patch.object(host._persistence, "read_checkpoint_position", side_effect=AssertionError("No graph rebuild")):
                result = await host.read_execution_context(ref.invocation_id, sequence=sequence, scope="session", pointer="/last")
                self.assertEqual(result["value"], "before")
                self.assertEqual(result["source"]["id"], checkpoint["checkpoint_id"])
                self.assertEqual(result["read"]["scanned_events"], 0)
                self.assertEqual((await host.inspect_execution_state(ref.invocation_id, sequence=sequence))["state"]["status"], "waiting")

    async def test_fork_branch_context_is_inherited_then_independent(self):
        """Fork initialization is inspected as branch state and source Context stays immutable after continuation."""
        async with self.host([self.workflow()]) as host:
            ref = await self.start(host)
            source = (await self.events(host, ref))[-1]
            fork = await host.fork(ref.invocation_id, sequence=source["sequence"])
            waiting = await host.join(fork["invocation_id"])
            events = (await host.list_execution_events(fork["invocation_id"], limit=200)).items
            initial = await host.read_execution_context(fork["invocation_id"], sequence=events[0]["sequence"], scope="session")
            self.assertEqual(initial["value"]["last"], "before")
            await host.respond(fork["invocation_id"], waiting.waits[0].id, {"text": "branch"})
            await host.join(fork["invocation_id"])
            await host._persistence.flush()
            last = (await host.list_execution_events(fork["invocation_id"], limit=200)).items[-1]
            result = await host.read_execution_context(fork["invocation_id"], sequence=last["sequence"], scope="session", pointer="/last")
            self.assertEqual(result["value"], "branch")
            original = await host.read_execution_context(ref.invocation_id, sequence=source["sequence"], scope="session", pointer="/last")
            self.assertEqual(original["value"], "before")

    async def test_child_context_isolated_and_compaction_does_not_restore_released_context(self):
        """A Child reads its own Context, and compaction clears it rather than exposing a stale earlier copy."""
        child = Workflow("child", nodes=[Node("write", echo, output_binding=bind)])
        parent = Workflow("parent", nodes=[Node("spawn", Spawn(child, "write")), Node("await", Await())],
            edges=[Edge("spawn", "await", id="join")])
        async with self.host([parent]) as host:
            ref = await host.execute(parent.id, {"text": "child"}, entry_node_id="spawn", session_context={"owner": "parent"})
            await host.join(ref)
            await host._persistence.flush()
            child_id = (await host.list_child_executions(ref.invocation_id)).items[0]["invocation_id"]
            events = (await host.list_execution_events(child_id, limit=200)).items
            completed = next(e for e in events if e["event_name"] == "node_occurrence.completed")
            result = await host.read_execution_context(child_id, sequence=completed["sequence"], scope="invocation")
            self.assertEqual(result["value"], {"result": {"text": "child"}})
            compacted = next(e for e in events if e["event_name"] == "child_invocation.compacted")
            summary = await host.inspect_execution_state(child_id, sequence=compacted["sequence"])
            self.assertTrue(summary["state"]["compacted"])
            result = await host.read_execution_context(child_id, sequence=compacted["sequence"], scope="invocation")
            self.assertFalse(result["available"])
            self.assertEqual(result["reason"], "released")
            result = await host.read_execution_context(child_id, sequence=compacted["sequence"], scope="session")
            self.assertEqual(result["value"], {})

    async def test_pause_cancel_and_recovery_retain_exact_old_states(self):
        """Pause and cancellation reads stay historical when the same Invocation is recovered in place."""
        async with self.host([self.workflow()]) as host:
            ref = await self.start(host)
            await host.pause(ref.invocation_id)
            await host.join(ref)
            paused = (await self.events(host, ref))[-1]
            self.assertEqual((await host.inspect_execution_state(ref.invocation_id, sequence=paused["sequence"]))["state"]["control_state"], "paused")
            await host.cancel(ref.invocation_id, reason="test cancellation")
            await host.join(ref)
            cancelled = (await self.events(host, ref))[-1]
            self.assertEqual((await host.inspect_execution_state(ref.invocation_id, sequence=cancelled["sequence"]))["state"]["status"], "cancelled")
            await host.recover(ref.invocation_id)
            await host.join(ref)
            reopened = (await self.events(host, ref, "invocation.reopened"))[-1]
            result = await host.inspect_execution_state(ref.invocation_id, sequence=reopened["sequence"])
            self.assertEqual(result["state"]["recovery_count"], 1)
            self.assertEqual(result["state"]["status"], "running")
            self.assertEqual((await host.inspect_execution_state(ref.invocation_id, sequence=cancelled["sequence"]))["state"]["status"], "cancelled")

    async def test_imported_context_anchor_and_missing_early_history(self):
        """A later Invocation reads imported Context from its anchor and fails clearly if that anchor is unavailable."""
        wf = Workflow("imported", nodes=[Node("write", echo, output_binding=bind)])
        app = AutoAgentApp()
        try:
            original = await app.ainvoke(wf, {"text": "imported"})
            graph = await app.acapture_checkpoint(original.ref)
        finally:
            await app.aclose()
        later_workflow = Workflow("later", nodes=[Node("entry", echo)])
        async with self.host([wf, later_workflow]) as host:
            await host._persistence.import_checkpoint(graph)
            ref = await host.execute("later", {"text": "later"}, entry_node_id="entry", session_id=original.ref.session_id)
            await host.join(ref)
            event = (await self.events(host, ref))[0]
            result = await host.read_execution_context(ref.invocation_id, sequence=event["sequence"], scope="session", pointer="/last")
            self.assertEqual(result["value"], "imported")
            self.assertEqual(result["source"]["kind"], "checkpoint")
            summary = await host.inspect_execution_state(ref.invocation_id, sequence=event["sequence"])
            self.assertEqual(summary["state"]["workflow_id"], "later")
            async def corrupt_anchor():
                async with host._persistence._writer.storage.engine.begin() as conn:
                    await conn.execute(update(meta.tables["checkpoint_sessions"]).where(
                        meta.tables["checkpoint_sessions"].c.session_id == ref.session_id).values(sequence=999999))
            await host._persistence._writer.run(corrupt_anchor)
            with self.assertRaises(HostRunConflict) as error:
                await host.read_execution_context(ref.invocation_id, sequence=event["sequence"], scope="session")
            self.assertEqual(error.exception.code, "HISTORY_UNAVAILABLE")

    async def test_failed_recovery_preserves_prior_committed_context(self):
        """Failed and recovered rounds expose their own exact Context rather than the latest projection."""
        attempts = []
        def once(value: Value) -> Value:
            attempts.append(True)
            if len(attempts) == 1:
                raise ValueError("expected failure")
            return {"text": "recovered"}
        wf = Workflow("failure", nodes=[Node("write", echo, output_binding=bind),
            Node("fail", once, output_binding=bind, recovery_mode=Recovery("replay_safe"))], edges=[Edge("write", "fail", id="next")])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {"text": "committed"}, entry_node_id="write")
            await host.join(ref)
            failed = (await self.events(host, ref))[-1]
            self.assertEqual((await host.inspect_execution_state(ref.invocation_id, sequence=failed["sequence"]))["state"]["status"], "failed")
            await host.recover(ref.invocation_id)
            await host.join(ref)
            completed = (await self.events(host, ref))[-1]
            for event, expected in ((failed, "committed"), (completed, "recovered")):
                result = await host.read_execution_context(ref.invocation_id, sequence=event["sequence"], scope="session", pointer="/last")
                self.assertEqual(result["value"], expected)

    async def test_validation_and_scan_decode_result_budgets(self):
        """Invalid positions and explicit resource limits fail instead of returning misleading partial state."""
        async with self.host([self.workflow()]) as host:
            ref = await self.start(host)
            sequence = (await self.events(host, ref))[-1]["sequence"]
            for invalid in (0, -1, True, 1.5, 9007199254740992):
                with self.assertRaises(ValueError):
                    await host.inspect_execution_state(ref.invocation_id, sequence=invalid)
            with self.assertRaises(HostNotFoundError):
                await host.inspect_execution_state("missing", sequence=1)
            with self.assertRaises(ValueError):
                await host.read_execution_context(ref.invocation_id, sequence=sequence, scope=None)
            for option in ("inspection_scan_events", "inspection_read_bytes", "inspection_result_bytes"):
                settings = replace(self.settings, persistence=replace(self.settings.persistence, **{option: 1}))
                async with self.host(settings=settings, read_only=True) as reader:
                    with self.assertRaises(HostRunConflict) as error:
                        await reader.read_execution_context(ref.invocation_id, sequence=sequence, scope="session")
                    self.assertEqual(error.exception.code, "INSPECTION_LIMIT", option)

    async def test_missing_event_gap_is_not_silently_reconstructed(self):
        """A missing Event between the target and its deciding assignment is an explicit history error."""
        async with self.host([self.workflow()]) as host:
            ref = await self.start(host)
            events = await self.events(host, ref)
            target = events[-1]
            async def remove():
                async with host._persistence._writer.storage.engine.begin() as conn:
                    await conn.execute(update(meta.tables["runtime_events"]).where(
                        meta.tables["runtime_events"].c.event_id == events[-2]["event_id"]).values(sequence=999999))
            await host._persistence._writer.run(remove)
            with self.assertRaises(HostRunConflict) as error:
                await host.read_execution_context(ref.invocation_id, sequence=target["sequence"], scope="session")
            self.assertEqual(error.exception.code, "HISTORY_UNAVAILABLE")

    def test_environment_settings_validate_budgets(self):
        """Deployment budgets are configurable positive integers with documented environment names."""
        env = {"AUTOAGENT_INSPECTION_SCAN_EVENTS": "7", "AUTOAGENT_INSPECTION_READ_BYTES": "1234",
            "AUTOAGENT_INSPECTION_RESULT_BYTES": "567"}
        settings = HostSettings.from_environment(env).persistence
        self.assertEqual((settings.inspection_scan_events, settings.inspection_read_bytes, settings.inspection_result_bytes), (7, 1234, 567))
        for field in ("inspection_scan_events", "inspection_read_bytes", "inspection_result_bytes"):
            for invalid in (0, -1, True, 1.5):
                with self.assertRaises(ValueError):
                    PersistenceConfig(**{field: invalid})


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL is not configured")
class PostgreSQLStateInspectionTests(StateInspectionTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.database = PostgreSQLDatabase()
        self.settings = replace(self.settings, database_url=await self.database.create())

    async def asyncTearDown(self):
        try:
            await self.database.close()
        finally:
            await super().asyncTearDown()
