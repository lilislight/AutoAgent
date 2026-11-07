"""Payload-free accepted observation and cancellable waits on both SQL backends."""

import asyncio
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import httpx

from autoagent import ChildCase, Edge, InputMappingContext, Map, Node, RuntimeHandle, Select, SignalCase, SignalEndpoint, Spawn, Timer, Wait, Workflow
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.host.persistence import ClosedError, PersistenceConfig, PersistenceError
from autoagent.host.persistence.reading import SourceReader
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_server import Value, identity


async def delayed(value: Value) -> Value:
    await asyncio.sleep(0.3)
    return value


def approval_inputs(context: InputMappingContext) -> list[Value]:
    return [{"text": str(index)} for index in range(105)]


def child_handle(context: InputMappingContext) -> RuntimeHandle:
    return RuntimeHandle.model_validate(context.incoming["spawn"])


class ExecutionObservationTests(unittest.IsolatedAsyncioTestCase):
    async def test_terminal_core_version_reuses_flags_only_for_matching_history(self):
        """A matching settled Core graph saves SQL; stale hints retain normal validation."""
        from sqlalchemy import event
        wf = Workflow('observation-version-fence', nodes=[Node('work', identity)])
        async with self.host([wf], buffered=True, local_projections=False) as host:
            ref = await host.execute(wf.id, {'text': 'terminal'}, entry_node_id='work')
            await host.join(ref)
            await host._persistence.flush()
            snapshot = await host._runtime.app.acontrol_snapshot(ref)
            self.assertTrue(snapshot.settled)
            statements = []
            engine = host._persistence._writer.storage.engine.sync_engine
            def collect(conn, cursor, statement, parameters, context, many):
                if statement.startswith('SELECT'):
                    statements.append(statement)
            event.listen(engine, 'before_cursor_execute', collect)
            try:
                normal = await host._persistence.execution_state(ref.invocation_id)
                normal_count = len(statements)
                statements.clear()
                fast = await host._persistence.execution_state(ref.invocation_id,
                    settled_version=snapshot.graph_version)
                self.assertEqual(normal, fast)
                self.assertLess(len(statements), normal_count)
                statements.clear()
                stale = tuple((sid, iid, seq - 1) for sid, iid, seq in snapshot.graph_version)
                checked = await host._persistence.execution_state(ref.invocation_id, settled_version=stale)
                self.assertEqual(normal, checked)
                self.assertGreaterEqual(len(statements), normal_count)
            finally:
                event.remove(engine, 'before_cursor_execute', collect)

    async def test_notification_wait_wakes_on_writer_fault_and_close(self):
        """A failed or closing Writer wakes observers instead of leaving them blocked."""
        wf = Workflow('notification-fault', nodes=[Node('work', identity)])
        async with self.host([wf]) as host:
            sink, writer = host._persistence, host._persistence._writer
            async with sink.execution_notifications('selected') as (revision, changed):
                async def transition(state):
                    async with writer.condition:
                        writer.state = state
                        writer.fault = RuntimeError('test failure') if state == 'faulted' else None
                        writer.condition.notify_all()

                for state, error in [('faulted', PersistenceError), ('closing', ClosedError)]:
                    try:
                        await writer.run(transition, state)
                        with self.assertRaises(error):
                            await changed(revision, 1, 'selected')
                    finally:
                        await writer.run(transition, 'open')

    async def test_commit_during_snapshot_cannot_lose_terminal_notification(self):
        """A terminal commit after a stale snapshot wakes observation without waiting for fallback."""
        entered, release = threading.Event(), threading.Event()

        async def gated(value: Value) -> Value:
            entered.set()
            while not release.is_set():
                await asyncio.sleep(0.005)
            return value

        wf = Workflow('notification-race', nodes=[Node('work', gated)])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {'text': 'race'}, entry_node_id='work')
            if not await asyncio.to_thread(entered.wait, 2):
                release.set()
                self.fail('Operator did not start')
            original = host.get_execution_state
            first = True

            async def stale_snapshot(iid):
                nonlocal first
                state = await original(iid)
                if first:
                    first = False
                    self.assertFalse(state['settled'])
                    release.set()
                    await host.join(ref, timeout=2)
                    await host._persistence.flush()
                state['resident'] = False
                return state

            writer = host._persistence._writer
            wait = writer.wait_execution_change
            notifications = []

            async def checked(token, after, timeout, root_id):
                notifications.append(writer.execution_watches[token][1] != after)
                return await wait(token, after, timeout, root_id)

            with patch.object(host, '_resident_observation_ref', return_value=None), patch.object(
                host, 'get_execution_state', stale_snapshot), patch.object(writer, 'wait_execution_change', checked):
                result = await host.wait_execution(ref.invocation_id, until='terminal', timeout=2)
            self.assertEqual(result['observation'], 'terminal')
            self.assertEqual(notifications, [True])
            self.assertFalse(writer.execution_watches)

    async def test_live_wait_defers_sql_summary_until_the_core_boundary(self):
        """A live wait queries one final summary without polling SQL while work runs."""
        entered, release = threading.Event(), threading.Event()

        async def gated(value: Value) -> Value:
            entered.set()
            while not release.is_set():
                await asyncio.sleep(.005)
            return value

        wf = Workflow('core-observation', nodes=[Node('work', gated)])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {'text': 'live'}, entry_node_id='work')
            original = host.get_execution_state
            with patch.object(host, 'get_execution_state', wraps=original) as query:
                observer = asyncio.create_task(host.wait_execution(ref.invocation_id, until='terminal', timeout=3))
                try:
                    self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                    for _ in range(100):
                        if host._persistence._writer.execution_watches:
                            break
                        await asyncio.sleep(.001)
                    await asyncio.sleep(.02)
                    self.assertEqual(query.await_count, 0)
                    release.set()
                    result = await observer
                    self.assertEqual(result['observation'], 'terminal')
                    self.assertEqual(query.await_count, 1)
                finally:
                    release.set()
                    if not observer.done():
                        observer.cancel()
                    await asyncio.gather(observer, return_exceptions=True)

    async def test_unrelated_notifications_are_filtered_and_subscriptions_released(self):
        """Scope wakeups to selected executions and retain no observer history after exit."""
        wf = Workflow('notification-filter', nodes=[Node('work', identity)])
        async with self.host([wf]) as host:
            sink, writer = host._persistence, host._persistence._writer
            async with sink.execution_notifications('selected') as (revision, changed):
                async def notify(identities):
                    async with writer.condition:
                        writer.notify_executions(identities)
                        writer.condition.notify_all()

                await writer.run(notify, {'unrelated'})
                self.assertEqual(await changed(revision, 0.01, 'selected'), revision)
                await writer.run(notify, {'selected'})
                updated = await changed(revision, 1, 'selected')
                self.assertGreater(updated, revision)
                # Adding a Root must force a fresh snapshot even if its commit
                # happened before we learned the Child's Root identity.
                self.assertGreater(await changed(updated, 1, 'root'), updated)
            self.assertFalse(writer.execution_watches)

    async def test_cancelled_observer_releases_notification_subscription(self):
        """Cancellation removes the watch while leaving execution under Host control."""
        wf = Workflow('notification-cancel', nodes=[Node('work', delayed)])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {'text': 'cancel observer'}, entry_node_id='work')
            observer = asyncio.create_task(host.wait_execution(ref.invocation_id, until='terminal'))
            for _ in range(100):
                if host._persistence._writer.execution_watches:
                    break
                await asyncio.sleep(0.001)
            self.assertTrue(host._persistence._writer.execution_watches)
            observer.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await observer
            self.assertFalse(host._persistence._writer.execution_watches)
            await host.join(ref)

    async def test_cancelled_registration_releases_undelivered_subscription(self):
        """Cancellation before registration returns cannot leak a Writer-side watch."""
        wf = Workflow('notification-registration-cancel', nodes=[Node('work', identity)])
        async with self.host([wf]) as host:
            sink, writer = host._persistence, host._persistence._writer
            registered, release = asyncio.Event(), asyncio.Event()
            original_run = writer.run

            async def delayed_registration_result(function, *args, **kwargs):
                result = await original_run(function, *args, **kwargs)
                if function == writer.subscribe_execution:
                    registered.set()
                    await release.wait()
                return result

            async def observe():
                async with sink.execution_notifications('selected'):
                    self.fail('A cancelled registration must not enter the observer body.')

            with patch.object(writer, 'run', delayed_registration_result):
                observer = asyncio.create_task(observe())
                try:
                    await asyncio.wait_for(registered.wait(), 2)
                    self.assertTrue(writer.execution_watches)
                    observer.cancel()
                finally:
                    release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await observer
            self.assertFalse(writer.execution_watches)

    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.url = "sqlite+aiosqlite:///" + str(Path(self.temp.name) / "observation.db")

    async def asyncTearDown(self):
        self.temp.cleanup()

    def host(self, workflows, *, buffered=False, local_projections=True):
        return AutoAgentHost(workflows=workflows, settings=HostSettings(database_url=self.url,
            local_projections=local_projections,
            persistence=PersistenceConfig(mode="buffered" if buffered else "durable", batch_delay_ms=60_000)), environment={})

    async def test_state_and_waits_never_resolve_large_business_values(self):
        """Accepted summaries include buffered Wait identities while neither requests nor results are read."""
        wf = Workflow("observation-wait", nodes=[Node("ask", Wait(Value, Value))])
        async with self.host([wf], buffered=True) as host:
            ref = await host.execute(wf.id, {"text": "x" * (2 * 1024 * 1024)}, entry_node_id="ask")
            await host.join(ref)
            with patch.object(SourceReader, "read", side_effect=AssertionError("Metadata only")):
                state = await host.wait_execution(ref.invocation_id, timeout=1)
                self.assertEqual(state["observation"], "boundary")
                self.assertEqual(state["status"], "waiting")
                self.assertEqual(state["waits"][0]["kind"], "external")
                self.assertNotIn("request", state["waits"][0])
                self.assertNotIn("output", state)
                self.assertLess(len(str(state)), 3000)
                self.assertEqual((await host.wait_execution(ref.invocation_id, until="terminal", timeout=0))["observation"], "timeout")

    async def test_terminal_wait_crosses_timer_and_boundary_wait_stops_at_timer(self):
        """Timer waits are stable boundaries, but terminal observation stays until their automatic completion."""
        wf = Workflow("observation-timer", nodes=[Node("clock", Timer())])
        async with self.host([wf]) as host:
            ref = await host.execute_json(wf.id, {"delay_us": 250_000}, revision_id=(await host.list_workflows()).items[0]["revision_id"], entry_node_id="clock", require_new_session=True)
            state = await host.wait_execution(ref.invocation_id, timeout=2)
            self.assertEqual(state["observation"], "boundary")
            self.assertEqual(state["waits"][0]["kind"], "timer")
            result = await host.wait_execution(ref.invocation_id, until="terminal", timeout=3)
            self.assertEqual(result["status"], "completed")
            self.assertTrue(result["settled"])

    async def test_child_work_does_not_look_like_a_boundary_and_child_wait_targets_are_exact(self):
        """Observation waits through pure Child work and exposes Child approval IDs through its Root."""
        work = Workflow("observation-child-work", nodes=[Node("work", delayed)])
        approval = Workflow("observation-child-approval", nodes=[Node("ask", Wait(Value, Value))])
        parent = Workflow("observation-parent", nodes=[Node("spawn", Spawn(work, "work"))])
        waiting_parent = Workflow("observation-wait-parent", nodes=[Node("spawn", Spawn(approval, "ask"))])
        async with self.host([parent, waiting_parent]) as host:
            ref = await host.execute(parent.id, {"text": "slow"}, entry_node_id="spawn")
            self.assertEqual((await host.wait_execution(ref.invocation_id, timeout=0.03))["observation"], "timeout")
            self.assertEqual((await host.wait_execution(ref.invocation_id, until="terminal", timeout=3))["status"], "completed")
            ref = await host.execute(waiting_parent.id, {"text": "approval"}, entry_node_id="spawn")
            state = await host.wait_execution(ref.invocation_id, timeout=3)
            external = next(wait for wait in state["waits"] if wait["kind"] == "external")
            self.assertNotEqual(external["invocation_id"], ref.invocation_id)
            selected = await host.get_execution_state(external["invocation_id"])
            self.assertEqual(selected["invocation_id"], ref.invocation_id)
            await host.perform_execution_action(ref.invocation_id, "respond", wait_id=external["wait_id"],
                target_invocation_id=external["invocation_id"], payload={"text": "approved"})
            self.assertEqual((await host.wait_execution(ref.invocation_id, until="terminal", timeout=3))["status"], "completed")

    async def test_observer_timeout_and_disconnect_do_not_cancel_or_leak_host_requests(self):
        """Stopping a status observer releases its own work and leaves the admitted execution running."""
        wf = Workflow("observation-slow", nodes=[Node("work", delayed)])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {"text": "running"}, entry_node_id="work")
            observer = asyncio.create_task(host.wait_execution(ref.invocation_id, until="terminal"))
            await asyncio.sleep(0.02)
            observer.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await observer
            await asyncio.sleep(0.02)
            self.assertFalse(host._operations)
            self.assertEqual((await host.wait_execution(ref.invocation_id, until="terminal", timeout=3))["status"], "completed")

    async def test_http_state_is_bounded_and_validates_observation_arguments(self):
        """The HTTP observer returns a normal timeout snapshot without downloading source records."""
        wf = Workflow("observation-http", nodes=[Node("ask", Wait(Value, Value))])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {"text": "wait"}, entry_node_id="ask")
            await host.join(ref)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host, ui=False)), base_url="http://test") as client:
                with patch.object(SourceReader, "read", side_effect=AssertionError("Metadata only")):
                    response = await client.get("/api/v2/execution-state", params={"invocation_id": ref.invocation_id, "until": "terminal", "timeout": 0.01})
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(response.json()["observation"], "timeout")
                self.assertEqual((await client.get("/api/v2/execution-state", params={"invocation_id": "missing"})).status_code, 404)
                for changes in ({"until": "wrong"}, {"timeout": -1}, {"timeout": "nan"}):
                    self.assertEqual((await client.get("/api/v2/execution-state", params={"invocation_id": ref.invocation_id, **changes})).status_code, 422)

    async def test_stored_boundaries_do_not_restore_the_graph_or_load_payloads(self):
        """Reopening a Host can summarize an old Wait without loading its Checkpoint into Core."""
        wf = Workflow("observation-reopen", nodes=[Node("ask", Wait(Value, Value))])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {"text": "approval"}, entry_node_id="ask")
            await host.join(ref)
        async with self.host([wf]) as host:
            with patch.object(SourceReader, "read", side_effect=AssertionError("Metadata only")):
                state = await host.wait_execution(ref.invocation_id, timeout=0)
                self.assertEqual(state["observation"], "boundary")
                self.assertFalse(state["resident"])
                self.assertFalse(host._runtime.resident)

    async def test_large_wait_families_have_bounded_payload_free_summaries(self):
        """A mapped Spawn family caps returned Wait metadata without truncating the graph's boundary decision."""
        child = Workflow("observation-many-child", nodes=[Node("ask", Wait(Value, Value))])
        parent = Workflow("observation-many-parent", nodes=[Node("spawn", Spawn(child, "ask"),
            input_mapping=approval_inputs, map=Map(max_parallelism=8))])
        async with self.host([parent]) as host:
            ref = await host.execute(parent.id, {"text": "approvals"}, entry_node_id="spawn")
            await host.join(ref)
            with patch.object(SourceReader, "read", side_effect=AssertionError("Metadata only")):
                state = await host.wait_execution(ref.invocation_id, timeout=1)
                self.assertEqual(state["observation"], "boundary")
                self.assertEqual(len(state["waits"]), 100)
                self.assertTrue(state["waits_truncated"])

    async def test_paused_graph_is_a_boundary_but_not_a_terminal_outcome(self):
        """Pause observation uses control_state and cannot report a paused Wait as completed."""
        wf = Workflow("observation-pause", nodes=[Node("ask", Wait(Value, Value))])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {"text": "approval"}, entry_node_id="ask")
            await host.join(ref)
            await host.pause(ref)
            state = await host.wait_execution(ref.invocation_id, timeout=1)
            self.assertEqual(state["control_state"], "paused")
            self.assertEqual(state["observation"], "boundary")
            self.assertFalse(state["settled"])
            self.assertEqual((await host.wait_execution(ref.invocation_id, until="terminal", timeout=0))["observation"], "timeout")

    async def test_mixed_select_is_a_boundary_while_pure_child_select_waits_for_work(self):
        """Select with a Signal can yield while a Child runs; a Child-only Select cannot yield early."""
        child = Workflow("observation-select-child", nodes=[Node("work", delayed)])
        for mixed in (True, False):
            with self.subTest(mixed=mixed):
                cases = {"child": ChildCase(child_handle)}
                if mixed:
                    cases["signal"] = SignalCase("wake")
                parent = Workflow("observation-select-" + str(mixed), nodes=[Node("spawn", Spawn(child, "work")),
                    Node("select", Select(cases))], edges=[Edge("spawn", "select", id="spawn")],
                    signal_endpoints=[SignalEndpoint("wake", Value)])
                async with self.host([parent]) as host:
                    ref = await host.execute(parent.id, {"text": "child"}, entry_node_id="spawn")
                    # Wait until the Select is registered while its Child is live.
                    for _ in range(100):
                        state = await host.get_execution_state(ref.invocation_id)
                        if any(wait["kind"] == "any" for wait in state["waits"]):
                            break
                        await asyncio.sleep(.005)
                    self.assertTrue(any(wait["kind"] == "any" for wait in state["waits"]))
                    with patch.object(SourceReader, "read", side_effect=AssertionError("Metadata only")):
                        state = await host.wait_execution(ref.invocation_id, timeout=0)
                    self.assertEqual(state["observation"], "boundary" if mixed else "timeout")
                    self.assertEqual((await host.wait_execution(ref.invocation_id, until="terminal", timeout=3))["status"], "completed")


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL test URL is required.")
class PostgreSQLExecutionObservationTests(ExecutionObservationTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.database = PostgreSQLDatabase()
        self.url = await self.database.create()

    async def asyncTearDown(self):
        await self.database.close()
        await super().asyncTearDown()
