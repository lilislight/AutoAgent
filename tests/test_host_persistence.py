"""Persistence contract tests shared by async SQLite and PostgreSQL backends."""

from __future__ import annotations

import asyncio
import gc
import json
import os
import tempfile
import unittest
import weakref
from dataclasses import replace
from pathlib import Path
from uuid import uuid4
from tests.postgres import POSTGRES_TEST_URL

try:
    from sqlalchemy import select
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    raise unittest.SkipTest(
        "Install autoagent[persistence] to run Persistence tests."
    ) from exc
from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp,
    Await,
    ConditionContext,
    Edge,
    Node,
    Operator,
    Spawn,
    Wait,
    Workflow,
)
from autoagent.core.runtime import RuntimeEvent, SessionOpened
from autoagent.core.runtime.user_events import UserEvent
from autoagent.host.persistence import (
    CapacityError,
    ClosedError,
    ConflictError,
    DatabaseSink,
    PersistenceConfig,
    PersistenceError,
)
from autoagent.host.persistence.codec import EncodingMemo, assign, encode
from autoagent.host.persistence.schema import meta


class Value(TypedDict):
    blob: str


def identity(value: Value) -> Value:
    return value


def fail(value: Value) -> Value:
    raise ValueError("operator failed: " + value["blob"])


def never(value: ConditionContext) -> bool:
    return False


def shorten(value: Value) -> Value:
    return {"blob": value["blob"][1:]}


def again(value: ConditionContext) -> bool:
    return bool(value.output["blob"])


def finished(value: ConditionContext) -> bool:
    return not value.output["blob"]


class Capture:
    def __init__(self):
        self.events = []

    async def append(self, event):
        self.events.append(event)


class PersistenceContracts:
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.sinks, self.apps = [], []
        self.url = await self.database_url()

    async def asyncTearDown(self):
        for app in self.apps:
            app.close()
        for sink in self.sinks:
            try:
                await sink.close()
            except PersistenceError:
                pass
        await self.remove_database()
        self.directory.cleanup()

    async def database_url(self):
        return "sqlite+aiosqlite:///" + str(
            Path(self.directory.name) / "persistence.sqlite"
        )

    async def remove_database(self):
        pass

    async def sink(self, **config):
        result = await DatabaseSink.open(self.url, config=PersistenceConfig(**config))
        self.sinks.append(result)
        return result

    def workflow(self, *, handler=identity, branch=False, sink=None):
        operator = Operator(handler, id="echo", version="1")
        nodes = [Node("a", operator), Node("b", operator)]
        if branch:
            edges = [Edge("a", "b", condition=never)]
        else:
            edges = [Edge("a", "b")]
        workflow = Workflow("host-persistence", nodes=nodes, edges=edges)
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(workflow)
        return app, workflow, operator

    async def registered(self, sink, *, handler=identity, branch=False):
        app, workflow, operator = self.workflow(
            handler=handler, branch=branch, sink=sink
        )
        await sink.register_operator(operator)
        await sink.register_workflow(app.workflow_definition_snapshot(workflow.id))
        return app, workflow, operator

    async def rows(self, sink, name):
        async def query():
            async with sink._writer.storage.engine.connect() as conn:
                return [
                    dict(row)
                    for row in (
                        await conn.execute(select(meta.tables[name]))
                    ).mappings()
                ]

        return await sink._writer.run(query)

    async def test_real_core_roundtrip_details_and_rebuild(self):
        """Preserve real Core facts and historical details through projection rebuild."""
        sink = await self.sink()
        app, workflow, operator = await self.registered(sink)
        result = await app.ainvoke(workflow, {"blob": "hello"})
        self.assertEqual(result.status, "completed")
        invocation = await sink.get_invocation(result.ref.invocation_id)
        self.assertEqual(invocation["status"], "completed")
        nodes = (await sink.list_nodes(result.ref.invocation_id)).items
        self.assertEqual([n["status"] for n in nodes], ["completed", "completed"])
        detail = await sink.node_detail(result.ref.invocation_id, "a@root")
        self.assertEqual(detail["input"]["value"], {"blob": "hello"})
        self.assertEqual(detail["output"]["value"], {"blob": "hello"})
        self.assertEqual(detail["definition"]["id"], "a")
        call = (await sink.list_calls(result.ref.invocation_id)).items[0]
        cd = await sink.call_detail(result.ref.invocation_id, call["call_id"])
        self.assertEqual(cd["definition"]["operator_id"], operator.id)
        self.assertGreaterEqual(cd["execution_duration_ns"], 0)
        before = (await sink.graph_changes(result.ref.invocation_id)).items
        self.assertTrue(
            any(
                r["entity_type"] == "edge" and r["change"].get("selected")
                for r in before
            )
        )
        await sink.rebuild_projections()
        self.assertEqual(
            (await sink.graph_changes(result.ref.invocation_id)).items, before
        )
        self.assertEqual(
            (await sink.node_detail(result.ref.invocation_id, "a@root"))["output"],
            detail["output"],
        )
        self.assertEqual(
            sum(row["count"] for row in await sink.statistics("operator")), 2
        )

    async def test_artifacts_deduplicate_event_delta_and_checkpoint(self):
        """Store a shared MiB value once across original facts and checkpoints."""
        sink = await self.sink()
        app, workflow, _ = await self.registered(sink)
        value = {"blob": "x" * 1048576}
        result = await app.ainvoke(workflow, value)
        checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
        key = await sink.save_checkpoint(checkpoint)
        self.assertEqual(
            (await sink.get_checkpoint(key)).to_record(), checkpoint.to_record()
        )
        artifacts = await self.rows(sink, "artifacts")
        self.assertEqual(len(artifacts), 1)
        self.assertEqual(artifacts[0]["value_type"], "string")
        nodes = await self.rows(sink, "node_executions")
        self.assertNotIn("x" * 100, json.dumps(nodes))
        facts = (await sink.runtime_events(result.ref.session_id)).items
        for record in facts:
            self.assertEqual(
                (await sink.get_runtime_event(record["id"])).to_record(), record
            )
        self.assertEqual(
            (await sink.node_detail(result.ref.invocation_id, "a@root"))["output"][
                "value"
            ],
            value,
        )

    async def test_subthreshold_business_values_remain_inline(self):
        """Do not externalize structural Event wrappers containing small values."""
        sink = await self.sink()
        app, workflow, _ = await self.registered(sink)
        await app.ainvoke(workflow, {"blob": "x" * (768 * 1024)})
        self.assertEqual(await self.rows(sink, "artifacts"), [])

    async def test_idempotency_and_conflicting_event_identity(self):
        """Accept retries once and reject changed content or a sequence gap."""
        sink = await self.sink()
        event = RuntimeEvent("session", 1, SessionOpened({}))
        await sink.append(event)
        await sink.append(event)
        self.assertEqual(len((await sink.runtime_events("session")).items), 1)
        with self.assertRaises(ConflictError):
            await sink.append(replace(event, payload=SessionOpened({"changed": True})))
        with self.assertRaises(ConflictError):
            await sink.append(replace(event, id=str(uuid4())))
        with self.assertRaises(ConflictError):
            await sink.append(replace(event, sequence=3, id=str(uuid4())))
        self.assertEqual((await sink.health()).state, "open")

    async def test_operator_binding_survives_registry_update_and_retry(self):
        """Pin historical Call bindings even when the current Operator changes."""
        sink = await self.sink()
        app, workflow, operator = await self.registered(sink)
        result = await app.ainvoke(workflow, {"blob": "old"})
        before = (await sink.list_calls(result.ref.invocation_id)).items[0][
            "operator_definition_id"
        ]
        updated = await sink.register_operator(
            Operator(identity, id=operator.id, version="2")
        )
        self.assertNotEqual(updated, before)
        records = (await sink.runtime_events(result.ref.session_id)).items
        started = next(r for r in records if r["event_name"] == "operator_call.started")
        await sink.append(RuntimeEvent.from_record(started))
        await sink.rebuild_projections()
        self.assertEqual(
            (await sink.list_calls(result.ref.invocation_id)).items[0][
                "operator_definition_id"
            ],
            before,
        )
        with self.assertRaises(ConflictError):
            await sink.append_runtime_event(
                RuntimeEvent.from_record(started), operator_definition_id=updated
            )

    async def test_buffered_ack_queue_capacity_and_memory_release(self):
        """ACK memory acceptance, block at capacity, and release committed objects."""
        sink = await self.sink(mode="buffered", queue_count=2, batch_delay_ms=0)
        original = sink._writer.storage.write

        async def install():
            gate = asyncio.Event()
            entered = asyncio.Event()

            async def delayed(entries):
                entered.set()
                await gate.wait()
                return await original(entries)

            sink._writer.storage.write = delayed
            return gate, entered

        gate, entered = await sink._writer.run(install)
        a = RuntimeEvent("a", 1, SessionOpened({"blob": "shared"}))
        b = RuntimeEvent("b", 1, SessionOpened({}))
        c = RuntimeEvent("c", 1, SessionOpened({}))
        await sink.append(a)

        async def wait_entered():
            await entered.wait()

        await sink._writer.run(wait_entered)
        await asyncio.wait_for(sink.append(b), 2)
        task = asyncio.create_task(sink.append(c))
        await asyncio.sleep(0.05)
        self.assertFalse(task.done())
        health = await sink.health()
        self.assertEqual(health.queued_count, 2)
        self.assertGreater(health.retained_bytes, 0)

        async def release():
            gate.set()

        await sink._writer.run(release)
        await asyncio.wait_for(task, 2)
        await sink.flush()
        health = await sink.health()
        self.assertEqual((health.queued_count, health.retained_bytes), (0, 0))
        self.assertEqual(health.accepted_frontier, health.durable_frontier)

    async def test_durable_does_not_ack_before_commit(self):
        """A durable append waits for its actual transaction completion."""
        sink = await self.sink(batch_delay_ms=0)
        original = sink._writer.storage.write

        async def install():
            gate = asyncio.Event()
            entered = asyncio.Event()

            async def delayed(entries):
                entered.set()
                await gate.wait()
                return await original(entries)

            sink._writer.storage.write = delayed
            return gate, entered

        gate, entered = await sink._writer.run(install)
        task = asyncio.create_task(sink.append(RuntimeEvent("s", 1, SessionOpened({}))))

        async def wait():
            await entered.wait()

        await sink._writer.run(wait)
        self.assertFalse(task.done())

        async def release():
            gate.set()

        await sink._writer.run(release)
        await task
        self.assertEqual(len((await sink.runtime_events("s")).items), 1)

    async def test_cancelled_durable_waiter_cannot_cancel_the_write(self):
        """Cancellation after acceptance leaves the real writer running."""
        sink = await self.sink(durable_batch_delay_ms=200)
        event = RuntimeEvent("s", 1, SessionOpened({}))
        task = asyncio.create_task(sink.append(event))
        for _ in range(30):
            if (await sink.health()).queued_count:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await sink.flush()
        self.assertEqual(
            (await sink.get_runtime_event(event.id)).to_record(), event.to_record()
        )

    async def test_buffered_queue_and_byte_thresholds_bypass_large_window(self):
        """Queue utilization and batch bytes commit promptly despite a long buffer window."""
        for options in ({'queue_count': 10, 'batch_queue_ratio': .2}, {'batch_bytes': 1024}):
            with self.subTest(options=options):
                sink = await self.sink(mode='buffered', batch_delay_ms=5000, **options)
                prefix = uuid4().hex
                first = RuntimeEvent(prefix + '-a', 1, SessionOpened({'blob': 'x' * 4096}))
                await sink.append(first)
                await sink.append(RuntimeEvent(prefix + '-b', 1, SessionOpened({})))
                async def committed():
                    while await sink.get_runtime_event(first.id) is None:
                        await asyncio.sleep(.01)
                await asyncio.wait_for(committed(), 2)
                # A small remainder may wait for the next normal batch window.
                await sink.flush()
                self.assertEqual((await sink.health()).retained_bytes, 0)
                await sink.close()

    async def test_buffer_window_does_not_delay_durable_append(self):
        """Durable writes use their own window rather than the buffered delay."""
        sink = await self.sink(batch_delay_ms=5000, durable_batch_delay_ms=0)
        await asyncio.wait_for(sink.append(RuntimeEvent('durable', 1, SessionOpened({}))), 2)
        self.assertEqual((await sink.health()).queued_count, 0)

    async def test_oversized_record_rejected_before_ack(self):
        """Reject a record exceeding the explicit retained-data budget."""
        sink = await self.sink(mode="buffered", queue_bytes=4096)
        with self.assertRaises(CapacityError):
            await sink.append(RuntimeEvent("s", 1, SessionOpened({"blob": "x" * 8192})))
        self.assertEqual((await sink.health()).accepted_frontier, 0)

    async def test_failed_batch_stops_tail_and_close_reports_failure(self):
        """Do not persist later records past a failed buffered batch."""
        sink = await self.sink(mode="buffered", batch_delay_ms=100, batch_count=1)

        async def install():
            async def broken(entries):
                raise ValueError("injected permanent failure")

            sink._writer.storage.write = broken

        await sink._writer.run(install)
        await sink.append(RuntimeEvent("a", 1, SessionOpened({})))
        with self.assertRaises(PersistenceError):
            await sink.flush()
        with self.assertRaises(PersistenceError):
            await sink.append(RuntimeEvent("b", 1, SessionOpened({})))
        self.assertEqual((await sink.health()).state, "faulted")
        self.assertEqual((await sink.runtime_events("a")).items, ())
        with self.assertRaises(PersistenceError):
            await sink.close()
        self.assertFalse(sink._writer.thread.is_alive())

    async def test_checkpoint_cannot_advance_beyond_event_frontier(self):
        """Reject a normal checkpoint whose Event prefix was never persisted."""
        sink = await self.sink()
        app, wf, _ = self.workflow()
        await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result = await app.ainvoke(wf, {"blob": "missing-history"})
        checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
        with self.assertRaises(PersistenceError):
            await sink.save_checkpoint(checkpoint)
        self.assertEqual(await self.rows(sink, "checkpoints"), [])

    async def test_import_anchor_rebuild_and_retention_guard(self):
        """Import a complete checkpoint without fabricating historical Call data."""
        sink = await self.sink()
        app, wf, _ = self.workflow()
        await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result = await app.ainvoke(wf, {"blob": "checkpoint-only"})
        checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
        key = await sink.import_checkpoint(checkpoint)
        sessions = (await sink.list_sessions()).items
        self.assertEqual(
            sessions[0]["history_start_sequence"], checkpoint.sessions[0].sequence + 1
        )
        self.assertEqual((await sink.runtime_events(result.ref.session_id)).items, ())
        inv = await sink.get_invocation(result.ref.invocation_id)
        self.assertEqual(
            (await sink.resolve(inv["output_ref"]))["value"],
            {"blob": "checkpoint-only"},
        )
        await sink.rebuild_projections()
        self.assertEqual(
            (await sink.get_invocation(result.ref.invocation_id))["status"], "completed"
        )
        with self.assertRaises(ConflictError):
            await sink.delete_checkpoint(key)

    async def test_ordered_graph_pages_never_split_one_event(self):
        """Graph cursors return whole Event groups in deterministic sequence order."""
        sink = await self.sink()
        app, workflow, _ = await self.registered(sink)
        result = await app.ainvoke(workflow, {"blob": "pagination"})
        all_rows = (await sink.graph_changes(result.ref.invocation_id)).items
        rows, after = [], 0
        while True:
            page = await sink.graph_changes(
                result.ref.invocation_id, after_sequence=after, limit=1
            )
            rows.extend(page.items)
            if page.next_cursor is None:
                break
            after = page.next_cursor[0]
        self.assertEqual(tuple(rows), all_rows)

    async def test_user_events_are_independent_and_ordered(self):
        """User Events preserve a separate sequence and idempotency boundary."""
        sink = await self.sink()
        event = UserEvent(
            "s", "i", 1, "progress", {"value": None, "blob": "x" * 1048576}
        )
        await sink.append_user_event(event)
        await sink.append_user_event(event)
        self.assertEqual(
            (await sink.get_user_event(event.id)).to_record(), event.to_record()
        )
        self.assertEqual(len((await sink.user_events("i")).items), 1)
        self.assertEqual((await sink.runtime_events("s")).items, ())
        with self.assertRaises(ConflictError):
            await sink.append_user_event(replace(event, sequence=3, id=str(uuid4())))
        self.assertEqual(
            await sink.resolve(
                {"kind": "user_event", "id": event.id, "path": ["payload", "value"]}
            ),
            {"available": True, "value": None},
        )
        self.assertEqual(await sink.resolve(None), {"available": False, "value": None})

    async def test_failed_operator_retains_error_and_node_classification(self):
        """Persist failure details without duplicating large errors in projections."""
        sink = await self.sink()
        app, wf, _ = await self.registered(sink, handler=fail)
        result = await app.ainvoke(wf, {"blob": "failure"})
        self.assertEqual(result.status, "failed")
        node = await sink.node_detail(result.ref.invocation_id, "a@root")
        self.assertEqual(node["error_type"], "ValueError")
        self.assertIn("operator failed", node["error"]["value"]["message"])
        self.assertTrue(
            any(r["status"] == "failed" for r in await sink.statistics("operator"))
        )

    async def test_purge_removes_facts_projections_and_unreachable_artifacts(self):
        """Delete a stopped whole Graph atomically and GC its unreferenced data."""
        sink = await self.sink()
        app, wf, _operator = await self.registered(sink)
        result = await app.ainvoke(wf, {"blob": "x" * 1048576})
        cp = app.unload_session(result.ref, capture_checkpoint=True)
        await sink.save_checkpoint(cp)
        with self.assertRaises(ConflictError):
            await sink.delete_workflow(result.ref.workflow_revision_id)
        definition = (await sink.list_calls(result.ref.invocation_id)).items[0][
            "operator_definition_id"
        ]
        with self.assertRaises(ConflictError):
            await sink.delete_operator(definition)
        await sink.purge_root_graph(result.ref.invocation_id)
        self.assertIsNone(await sink.get_invocation(result.ref.invocation_id))
        self.assertEqual((await sink.runtime_events(result.ref.session_id)).items, ())
        self.assertEqual(await self.rows(sink, "artifacts"), [])
        await sink.delete_workflow(result.ref.workflow_revision_id)
        await sink.delete_operator(definition)

    async def test_single_writer_and_reopen_preserve_identity(self):
        """Exclude another writer and preserve durable facts after reopening."""
        sink = await self.sink()
        with self.assertRaises(ConflictError):
            await DatabaseSink.open(self.url)
        event = RuntimeEvent("s", 1, SessionOpened({}))
        await sink.append(event)
        await sink.close()
        reopened = await self.sink()
        await reopened.append(event)
        self.assertEqual(len((await reopened.runtime_events("s")).items), 1)
        with self.assertRaises(ClosedError):
            await sink.append(event)

    async def test_live_core_can_call_sink_from_different_event_loops(self):
        """Support Core's synchronous adapter without crossing SQLAlchemy loops."""
        sink = await self.sink()
        app, wf, _ = await self.registered(sink)
        result = await asyncio.to_thread(app.invoke, wf, {"blob": "thread"})
        self.assertEqual(result.status, "completed")
        self.assertEqual(
            (await sink.get_invocation(result.ref.invocation_id))["status"], "completed"
        )

    async def test_wait_checkpoint_reload_continues_same_identity(self):
        """Resume a persisted Wait after Core reload without replacing its Call."""
        sink = await self.sink()
        wf = Workflow("persist-wait", nodes=[Node("wait", Wait(Value, Value))])
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(wf)
        await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        waiting = await app.ainvoke(wf, {"blob": "request"})
        self.assertEqual(waiting.status, "waiting")
        before = (await sink.list_calls(waiting.ref.invocation_id)).items
        cp = app.unload_session(waiting.ref, capture_checkpoint=True)
        key = await sink.save_checkpoint(cp)
        await app.aload_checkpoint(await sink.get_checkpoint(key))
        await app.arecover(waiting.ref)
        final = await app.aresume(
            waiting.ref, waiting.waits[0].id, {"blob": "response"}
        )
        self.assertEqual(final.status, "completed")
        wait = await sink.wait_detail(waiting.ref.invocation_id, waiting.waits[0].id)
        self.assertEqual(wait["status"], "resumed")
        self.assertEqual(wait["request"]["value"], {"blob": "request"})
        self.assertEqual(wait["response"]["value"], {"blob": "response"})
        after = (await sink.list_calls(waiting.ref.invocation_id)).items
        self.assertEqual([c["call_id"] for c in before], [c["call_id"] for c in after])

    async def test_child_graph_ownership_checkpoint_rebuild_and_purge(self):
        """Persist real Parent-Child plans and compacted results as one Graph."""
        sink = await self.sink(mode="buffered")
        child = Workflow("persist-child", nodes=[Node("work", identity)])
        root = Workflow(
            "persist-root",
            nodes=[Node("spawn", Spawn(child, "work")), Node("await", Await())],
            edges=[Edge("spawn", "await")],
        )
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(root)
        for wf in (child, root):
            await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result = await app.ainvoke(root, {"blob": "child"})
        self.assertEqual(result.status, "completed")
        await sink.flush()
        invs = (
            await sink.list_invocations(
                root_invocation_id=result.ref.invocation_id, include_planned=True
            )
        ).items
        self.assertEqual(len(invs), 2)
        child_row = next(r for r in invs if r["parent_invocation_id"])
        self.assertEqual(child_row["parent_occurrence_id"], "spawn@root")
        self.assertEqual(child_row["ownership_phase"], "terminal")
        changes = (
            await sink.graph_changes(result.ref.invocation_id, root_graph=True)
        ).items
        self.assertEqual(
            {r["invocation_id"] for r in changes}, {r["invocation_id"] for r in invs}
        )
        cp = app.unload_session(result.ref, capture_checkpoint=True)
        key = await sink.save_checkpoint(cp)
        self.assertEqual((await sink.get_checkpoint(key)).to_record(), cp.to_record())
        await sink.rebuild_projections()
        self.assertEqual(
            (
                await sink.list_invocations(
                    root_invocation_id=result.ref.invocation_id, include_planned=True
                )
            ).items,
            invs,
        )
        await sink.purge_root_graph(result.ref.invocation_id)
        self.assertEqual((await sink.list_sessions()).items, ())

    async def test_loop_occurrences_and_conditions_remain_distinct(self):
        """Separate repeated Node occurrences and actual condition evaluations."""
        sink = await self.sink()
        wf = Workflow(
            "persist-loop",
            nodes=[
                Node("entry", identity),
                Node("step", shorten),
                Node("exit", identity),
            ],
            edges=[
                Edge("entry", "step"),
                Edge("step", "step", again, id="back"),
                Edge("step", "exit", finished, id="exit"),
            ],
        )
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(wf)
        await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result = await app.ainvoke(wf, {"blob": "abcd"})
        self.assertEqual(result.status, "completed")
        steps = [
            n
            for n in (await sink.list_nodes(result.ref.invocation_id)).items
            if n["node_id"] == "step"
        ]
        self.assertEqual(len(steps), 4)
        self.assertEqual(len({n["occurrence_id"] for n in steps}), 4)
        edge = await sink.edge_detail(result.ref.invocation_id, "back")
        self.assertEqual(len(edge["evaluations"]), 4)
        self.assertTrue(
            any(r["change"].get("selected") is False for r in edge["changes"])
        )
        await sink.rebuild_projections()
        self.assertEqual(
            (await sink.edge_detail(result.ref.invocation_id, "back"))["evaluations"],
            edge["evaluations"],
        )

    async def test_false_condition_is_a_committed_unselected_route(self):
        """Keep condition results separate from final routes and skipped Nodes."""
        sink = await self.sink()
        app, wf, _ = await self.registered(sink, branch=True)
        result = await app.ainvoke(wf, {"blob": "branch"})
        nodes = (await sink.list_nodes(result.ref.invocation_id)).items
        self.assertEqual(
            next(n for n in nodes if n["node_id"] == "b")["status"], "skipped"
        )
        edges = (await sink.get_workflow(result.ref.workflow_revision_id))[
            "definition"
        ]["edges"]
        detail = await sink.edge_detail(result.ref.invocation_id, edges[0]["id"])
        self.assertFalse(detail["evaluations"][0]["condition_result"])
        self.assertTrue(
            any(r["change"].get("selected") is False for r in detail["changes"])
        )

    async def test_accepted_view_reads_pending_graph_and_releases_tail(self):
        """Expose a fixed accepted frontier before commit and merge it once."""
        sink = await self.sink(mode="buffered", batch_delay_ms=5000, batch_count=1000)
        app, wf, _ = await self.registered(sink)
        result = await app.ainvoke(wf, {"blob": "tail"})
        self.assertIsNone(await sink.get_invocation(result.ref.invocation_id))
        inv = await sink.get_invocation(result.ref.invocation_id, view="accepted")
        self.assertEqual(inv["status"], "completed")
        detail = await sink.node_detail(
            result.ref.invocation_id, "a@root", view="accepted"
        )
        self.assertEqual(detail["output"]["value"], {"blob": "tail"})
        before = (
            await sink.graph_changes(result.ref.invocation_id, view="accepted")
        ).items
        await sink.flush()
        self.assertEqual(
            (await sink.graph_changes(result.ref.invocation_id)).items, before
        )
        self.assertEqual((await sink.health()).retained_bytes, 0)

    async def test_shared_artifact_gc_retains_other_graph_and_nested_dependencies(self):
        """Collect one Root without deleting a shared or indirectly used Artifact."""
        sink = await self.sink(artifact_threshold_bytes=1024)
        app, wf, _ = await self.registered(sink)
        value = {"blob": "x" * 3000}
        a = await app.ainvoke(wf, value)
        b = await app.ainvoke(wf, value)
        app.unload_session(a.ref)
        app.unload_session(b.ref)
        await sink.purge_root_graph(a.ref.invocation_id)
        self.assertTrue(await self.rows(sink, "artifacts"))
        self.assertEqual(
            (await sink.node_detail(b.ref.invocation_id, "a@root"))["output"]["value"],
            value,
        )
        user = UserEvent(
            "user-s",
            "user-i",
            1,
            "data",
            {
                "rows": [
                    {"small": "a" * 80, "big": "y" * 3000 if i == 0 else ""}
                    for i in range(40)
                ]
            },
        )
        await sink.append_user_event(user)
        self.assertTrue(await self.rows(sink, "artifact_dependencies"))
        await sink.gc_artifacts()
        self.assertEqual(
            (await sink.get_user_event(user.id)).to_record(), user.to_record()
        )

    async def test_byte_capacity_waits_and_shared_objects_are_counted_once(self):
        """Apply byte backpressure while retaining shared values only once."""
        sink = await self.sink(mode="buffered", queue_bytes=16000, batch_delay_ms=5000)
        shared = {"blob": "x" * 9000}
        a = RuntimeEvent("a", 1, SessionOpened(shared))
        b = RuntimeEvent("b", 1, SessionOpened(shared))
        await sink.append(a)
        first = (await sink.health()).retained_bytes
        await sink.append(b)
        second = (await sink.health()).retained_bytes
        self.assertLess(second - first, 4000)
        task = asyncio.create_task(
            sink.append(RuntimeEvent("c", 1, SessionOpened({"blob": "y" * 9000})))
        )
        await asyncio.sleep(0.03)
        self.assertFalse(task.done())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await sink.flush()
        self.assertEqual((await sink.runtime_events("c")).items, ())

    async def test_idempotency_preserves_json_number_and_boolean_types(self):
        """Distinguish equal Python scalars with different canonical JSON types."""
        sink = await self.sink(mode="buffered", batch_delay_ms=5000)
        event = RuntimeEvent("types", 1, SessionOpened({"value": 1}))
        await sink.append(event)
        with self.assertRaises(ConflictError):
            await sink.append(replace(event, payload=SessionOpened({"value": True})))
        await sink.flush()
        with self.assertRaises(ConflictError):
            await sink.append(replace(event, payload=SessionOpened({"value": 1.0})))

    async def test_commit_releases_source_objects_while_writer_remains_open(self):
        """Release actual source references after commit, beyond queue accounting."""

        class Blob(str):
            pass

        sink = await self.sink(mode="buffered")
        blob = Blob("x" * 1048576)
        reference = weakref.ref(blob)
        event = RuntimeEvent("memory", 1, SessionOpened({"blob": blob}))
        await sink.append(event)
        del blob, event
        await sink.flush()
        gc.collect()
        self.assertIsNone(reference())
        self.assertEqual((await sink.health()).state, "open")

    async def test_cancelled_close_still_drains_and_releases_writer(self):
        """Keep shutdown alive after cancellation and reject capacity waiters."""
        sink = await self.sink(mode="buffered", queue_count=1, batch_delay_ms=0)
        original = sink._writer.storage.write

        async def install():
            gate, entered = asyncio.Event(), asyncio.Event()

            async def delayed(entries):
                entered.set()
                await gate.wait()
                return await original(entries)

            sink._writer.storage.write = delayed
            return gate, entered

        gate, entered = await sink._writer.run(install)
        await sink.append(RuntimeEvent("a", 1, SessionOpened({})))

        async def started():
            await entered.wait()

        await sink._writer.run(started)
        blocked = asyncio.create_task(
            sink.append(RuntimeEvent("b", 1, SessionOpened({})))
        )
        await asyncio.sleep(0.03)
        closing = asyncio.create_task(sink.close())
        with self.assertRaises(ClosedError):
            await asyncio.wait_for(blocked, 2)
        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        sink._writer.loop.call_soon_threadsafe(gate.set)
        await asyncio.wait_for(asyncio.wrap_future(sink._writer._close_future), 2)
        await sink.close()
        self.assertFalse(sink._writer.thread.is_alive())
        reopened = await self.sink()
        self.assertEqual(len((await reopened.runtime_events("a")).items), 1)
        self.assertEqual((await reopened.runtime_events("b")).items, ())

    async def test_edge_evaluation_pages_follow_runtime_sequence(self):
        """Page actual condition results chronologically rather than by UUID."""
        sink = await self.sink()
        wf = Workflow(
            "edge-pagination",
            nodes=[
                Node("entry", identity),
                Node("step", shorten),
                Node("exit", identity),
            ],
            edges=[
                Edge("entry", "step"),
                Edge("step", "step", again, id="back"),
                Edge("step", "exit", finished, id="exit"),
            ],
        )
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(wf)
        await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result = await app.ainvoke(wf, {"blob": "abcd"})
        whole = await sink.edge_detail(result.ref.invocation_id, "back")
        values, cursor = [], None
        while True:
            page = await sink.edge_detail(
                result.ref.invocation_id, "back", limit=1, evaluations_cursor=cursor
            )
            values.extend(page["evaluations"])
            cursor = page["evaluations_cursor"]
            if cursor is None:
                break
        self.assertEqual(tuple(values), whole["evaluations"])
        self.assertEqual(
            [r["sequence"] for r in values], sorted(r["sequence"] for r in values)
        )

    async def test_settling_output_reference_is_cleared_on_cancellation(self):
        """Do not expose a discarded success result after cancellation wins."""
        sink = await self.sink()
        child = Workflow("cancel-child", nodes=[Node("wait", Wait(Value, Value))])
        root = Workflow("cancel-root", nodes=[Node("spawn", Spawn(child, "wait"))])
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(root)
        for wf in (root, child):
            await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result = await app.ainvoke(root, {"blob": "wait"})
        self.assertEqual(result.status, "settling")
        self.assertIsNotNone(
            (await sink.get_invocation(result.ref.invocation_id))["output_ref"]
        )
        cancelled = await app.acancel(result.ref, "stop")
        self.assertEqual(cancelled.status, "cancelled")
        self.assertIsNone(
            (await sink.get_invocation(result.ref.invocation_id))["output_ref"]
        )


class SQLitePersistenceTests(PersistenceContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(
    POSTGRES_TEST_URL,
    "isolated PostgreSQL URL not configured",
)
class PostgreSQLPersistenceTests(
    PersistenceContracts, unittest.IsolatedAsyncioTestCase
):
    async def database_url(self):
        import asyncpg
        from sqlalchemy.engine import make_url

        self.admin_url = make_url(POSTGRES_TEST_URL)
        self.database_name = "autoagent_test_" + uuid4().hex
        conn = await asyncpg.connect(
            self.admin_url.set(drivername="postgresql").render_as_string(
                hide_password=False
            )
        )
        try:
            await conn.execute('CREATE DATABASE "' + self.database_name + '"')
        finally:
            await conn.close()
        return self.admin_url.set(
            drivername="postgresql+asyncpg", database=self.database_name
        ).render_as_string(hide_password=False)

    async def remove_database(self):
        import asyncpg

        conn = await asyncpg.connect(
            self.admin_url.set(drivername="postgresql").render_as_string(
                hide_password=False
            )
        )
        try:
            await conn.execute(
                'DROP DATABASE "' + self.database_name + '" WITH (FORCE)'
            )
        finally:
            await conn.close()


class ArtifactCodecTests(unittest.TestCase):
    def roundtrip(self, value, *, threshold=1024):
        result = encode({"payload": value}, "user_event", threshold, EncodingMemo(4096))

        def load(digest):
            artifact = result.artifacts[digest]
            body = json.loads(artifact["body"])
            for path, child in artifact["deps"]:
                body = assign(body, path, load(child))
            return body

        body = result.record
        for path, digest in result.refs:
            body = assign(body, path, load(digest))
        self.assertEqual(body, {"payload": value})
        return result

    def test_user_keys_unicode_and_nested_manifests(self):
        """Handle normal user keys and nested manifests without sentinel collisions."""
        value = {
            "op": ["replace"],
            "path": ["value"],
            "artifact_id": "user",
            "unicode": "中文" * 1000,
            "rows": [
                {"small": "a" * 80, "index": i, "large": "z" * 2000 if i == 0 else ""}
                for i in range(40)
            ],
        }
        result = self.roundtrip(value)
        self.assertTrue(any(a["deps"] for a in result.artifacts.values()))

    def test_threshold_and_content_hash_order(self):
        """Apply the canonical-byte threshold and ignore dictionary key order."""
        self.assertEqual(len(self.roundtrip("x" * 1021).artifacts), 0)
        self.assertEqual(len(self.roundtrip("x" * 1022).artifacts), 1)
        a = self.roundtrip({"a": "x" * 600, "b": "y" * 600})
        b = self.roundtrip({"b": "y" * 600, "a": "x" * 600})
        self.assertEqual(set(a.artifacts), set(b.artifacts))

    def test_escaped_surrogates_roundtrip_as_json(self):
        """Preserve escaped surrogate values allowed by Core JSON records."""
        self.roundtrip({"value": "\ud800" * 2000})
