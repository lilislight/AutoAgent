"""Host operations and decorator/TOML projects through actual Core execution."""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import threading
import unittest
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4
from tests.postgres import POSTGRES_TEST_URL

try:
    import dotenv  # noqa: F401 -- optional dependency guard
    import sqlalchemy  # noqa: F401 -- optional dependency guard
except ModuleNotFoundError as exc:
    raise unittest.SkipTest(
        "Install autoagent[host] to run Host integration tests."
    ) from exc

from typing_extensions import TypedDict

from autoagent import (
    Capability,
    InputMappingContext,
    Edge,
    Node,
    Operator,
    ReceiveSignal,
    SignalEndpoint,
    Spawn,
    UserEventContext,
    UserEventSpec,
    Wait,
    Workflow,
)
from autoagent.core.app import AutoAgentApp
from autoagent.core.runtime import InMemoryUserEventJournal
from autoagent.host import (
    AutoAgentHost,
    HostError,
    HostSettings,
    ProjectError,
    load_project_environment,
)
from autoagent.host.persistence import DatabaseSink, PersistenceConfig


class Value(TypedDict):
    text: str


def identity(value: Value) -> Value:
    return value


def failed(value: Value) -> Value:
    raise ValueError(value["text"])


def observation(context: UserEventContext) -> UserEventSpec:
    return UserEventSpec(kind=context.node_id, payload={"text": context.value["text"]})


async def run(host, workflow_id, value, **options):
    return await host.join(await host.execute(workflow_id, value, **options))


async def respond(host, execution, value):
    result = await host.get_execution(execution)
    assert len(result.waits) == 1
    await host.respond(execution, result.waits[0].id, value)
    return await host.join(execution)


async def runtime_events(host, invocation_id, **options):
    row = await host._persistence.get_invocation(invocation_id, view=options.get("view", "durable"))
    return await host._persistence.runtime_events(row["session_id"], invocation_id=invocation_id, **options)


class HostTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.settings = HostSettings(
            database_url="sqlite+aiosqlite:///" + str(self.root / "runtime.db")
        )
        self.modules = []

    async def asyncTearDown(self):
        for name in tuple(sys.modules):
            if any(
                name == prefix or name.startswith(prefix + ".")
                for prefix in self.modules
            ):
                del sys.modules[name]
        self.temp.cleanup()

    def project_module(self, source, *, src=False):
        name = "host_fixture_" + uuid4().hex
        self.modules.append(name)
        base = self.root / "src" if src else self.root
        base.mkdir(exist_ok=True)
        (base / (name + ".py")).write_text(source)
        return name

    async def test_decorator_discovery_env_and_repeat_load(self):
        """Discover decorated factories without an entry module or global registry."""
        self.project_module(
            """
import os
from autoagent import Node, Workflow
from autoagent.host import workflow as discover
from typing_extensions import TypedDict
class Value(TypedDict):
    text: str
def work(value: Value) -> Value:
    return {"text": os.environ["HOST_TEST_VALUE"] + value["text"]}
@discover()
def factory():
    return Workflow("discovered", nodes=[Node("work", work)])
""",
            src=True,
        )
        (self.root / "unrelated.py").write_text('raise RuntimeError("must not import")')
        (self.root / ".env").write_text(
            'HOST_TEST_VALUE="dotenv-"\nAUTOAGENT_PERSISTENCE_MODE=buffered\n'
        )
        before = dict(os.environ)
        report = AutoAgentHost.check_project(self.root)
        self.assertTrue(report.ok)
        self.assertEqual(dict(os.environ), before)
        for _ in range(2):
            async with AutoAgentHost.from_project(
                self.root, settings=self.settings
            ) as host:
                result = await run(host, "discovered", {"text": "ok"})
                self.assertEqual(result.output, {"text": "dotenv-ok"})
        self.assertEqual(dict(os.environ), before)

    async def test_toml_is_explicit_and_check_does_not_open_resources(self):
        """Load TOML objects while checking without a database or Provider secrets."""
        name = self.project_module("""
from contextlib import asynccontextmanager
from autoagent import Workflow, Node
from typing_extensions import TypedDict
class Value(TypedDict):
    text: str
def work(value: Value) -> Value:
    return value
workflow = Workflow("explicit", nodes=[Node("work",work)])
@asynccontextmanager
async def resources(setup):
    raise RuntimeError("resource startup")
    yield
""")
        (self.root / "auto-agent.toml").write_text(f'''schema_version=1
[project]
name="example"
lifespan="{name}:resources"
[[workflows]]
entrypoint="{name}:workflow"
''')
        report = AutoAgentHost.check_project(self.root)
        self.assertTrue(report.ok)
        self.assertFalse((self.root / ".autoagent").exists())
        with self.assertRaisesRegex(RuntimeError, "resource startup"):
            async with AutoAgentHost.from_project(self.root, settings=self.settings):
                pass
        # Startup failure releases the single-writer database lease.
        async with await DatabaseSink.open(self.settings.database_url):
            pass

    async def test_environment_precedence_override_disable_and_settings(self):
        """Read dotenv syntax and honor process precedence, explicit files and settings."""
        (self.root / ".env").write_text(
            "HOST_TEST_VALUE=dotenv\nHOST_TEST_BASE=local\nHOST_TEST_EXPANDED=${HOST_TEST_BASE}-suffix\nAUTOAGENT_QUEUE_COUNT=7\n"
        )
        (self.root / "custom.env").write_text("HOST_TEST_VALUE=custom\n")
        with patch.dict(os.environ, {"HOST_TEST_VALUE": "process"}):
            env = load_project_environment(self.root)
            self.assertEqual(env["HOST_TEST_VALUE"], "process")
            self.assertEqual(env["HOST_TEST_EXPANDED"], "local-suffix")
            self.assertEqual(
                HostSettings.from_environment(env).persistence.queue_count, 7
            )
        self.assertEqual(
            load_project_environment(self.root, env_file="custom.env")[
                "HOST_TEST_VALUE"
            ],
            "custom",
        )
        self.assertNotIn(
            "HOST_TEST_BASE", load_project_environment(self.root, use_env_file=False)
        )
        with self.assertRaises(ProjectError):
            load_project_environment(self.root, env_file="missing.env")
        url = HostSettings().resolved_database_url(self.root)
        self.assertIn(str(self.root / ".autoagent" / "runtime.db"), url)
        with self.assertRaises(ValueError):
            HostSettings.from_environment({"AUTOAGENT_PERSISTENCE_MODE": "invalid"})

    async def test_default_residency_queries_and_historical_result(self):
        """Reuse warm Sessions and preserve historical results across replacement."""
        wf = Workflow("basic", nodes=[Node("work", identity)])
        async with AutoAgentHost(workflows=[wf], settings=self.settings) as host:
            first = await run(host, "basic", {"text": "first"}, session_id="session")
            self.assertTrue(first.ref.session_id in host._runtime.resident)
            second = await run(host, "basic", {"text": "second"}, session_id="session")
            self.assertEqual(
                (await host.join(first.invocation_id)).output, {"text": "first"}
            )
            with self.assertRaises(HostError):
                await host.cancel(first.invocation_id)
            self.assertEqual(len((await host._persistence.list_invocations()).items), 2)
            self.assertEqual((await host.get_execution(second.ref)).output, {"text": "second"})
            for forbidden in (
                "start",
                "close",
                "health",
                "flush",
                "unload",
                "load_checkpoint",
                "continue_execution",
            ):
                self.assertFalse(hasattr(host, forbidden))
        with self.assertRaises(HostError):
            await run(host, "basic", {"text": "closed"})

    async def test_wait_is_restored_automatically_after_shutdown(self):
        """Resume a persisted Wait by Invocation id without exposing checkpoints."""
        wf = Workflow("approval", nodes=[Node("wait", Wait(Value, Value))])
        async with AutoAgentHost(workflows=[wf], settings=self.settings) as host:
            waiting = await run(host, "approval", {"text": "request"})
        async with AutoAgentHost(workflows=[wf], settings=self.settings) as host:
            self.assertFalse(
                waiting.ref.session_id in host._runtime.resident
            )
            result = await respond(host, waiting.invocation_id, {"text": "approved"})
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"text": "approved"})

    async def test_child_graph_automatically_restores_and_cancels(self):
        """Restore complete Parent/Child ownership before a high-level cancel."""
        child = Workflow("child", nodes=[Node("wait", Wait(Value, Value))])
        root = Workflow("parent", nodes=[Node("spawn", Spawn(child, "wait"))])
        async with AutoAgentHost(workflows=[root], settings=self.settings) as host:
            result = await run(host, "parent", {"text": "child"})
            self.assertEqual(result.status, "settling")
        async with AutoAgentHost(workflows=[root], settings=self.settings) as host:
            cancelled = await host.cancel(result.invocation_id, reason="stop")
            self.assertEqual(cancelled.status, "cancelled")
            rows = (
                await host._persistence.list_invocations(root_invocation_id=result.invocation_id)
            ).items
            child_row = next(
                row for row in rows if row["parent_invocation_id"] is not None
            )
            self.assertTrue(
                child_row["status"] == "cancelled"
            )

    async def test_capability_resolvers_use_host_environment_without_changing_defaults(self):
        """Host strategies see parsed environment while other Capabilities retain defaults."""
        calls = []
        custom = Capability("custom", Operator(identity).contract)
        ordinary = Capability("ordinary", custom.contract)
        @asynccontextmanager
        async def resources(setup):
            async def selected(value: Value) -> Value:
                return {"text": setup.environment["MODEL"] + ":" + value["text"]}
            chosen = setup.register_operator(Operator(selected, id="selected"), capability_id="custom")
            setup.register_operator(Operator(identity, id="unused"), capability_id="custom", default=True)
            setup.register_operator(Operator(identity, id="ordinary"), capability_id="ordinary", default=True)
            async def resolve(capability, value):
                calls.append((capability.id, value["text"]))
                return chosen
            setup.register_capability_resolver("custom", resolve)
            yield
        workflow = Workflow("routing", nodes=[Node("choose", custom), Node("default", ordinary)], edges=[Edge("choose", "default")])
        async with AutoAgentHost(workflows=[workflow], settings=self.settings, lifespan=resources,
                                 environment={"MODEL": "configured-model"}) as host:
            result = await run(host, "routing", {"text": "hello"})
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(result.output, {"text": "configured-model:hello"})
        self.assertEqual(calls, [("custom", "hello")])

    async def test_retry_and_original_error_input_roundtrip_through_storage(self):
        """Both database backends retain Retry ordinals and pre-mapping input snapshots."""
        from autoagent import BackoffPolicy, ResourcePolicy, RetryPolicy
        from autoagent.core.runtime import RuntimeEvent, StateReducer
        calls = []
        async def fail(value: Value) -> Value:
            calls.append(value["text"])
            raise OSError("provider unavailable")
        def mapped(context: InputMappingContext) -> Value:
            return {"text": "mapped:" + context.invocation_input["text"]}
        def backup(context: InputMappingContext) -> Value:
            return context.incoming["failed"]["input"]["invocation_input"]
        workflow = Workflow("retry-storage", nodes=[Node("primary", fail, input_mapping=mapped,
            retry=RetryPolicy(backoff=BackoffPolicy(initial_seconds=0, max_seconds=0)), resource=ResourcePolicy(1)),
            Node("backup", identity, input_mapping=backup)], edges=[Edge("primary", "backup", on="error", id="failed")])
        async with AutoAgentHost(workflows=[workflow], settings=self.settings) as host:
            result = await run(host, "retry-storage", {"text": "original"})
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(result.output, {"text": "original"})
            row = await host._persistence.get_invocation(result.invocation_id)
            page = await host._persistence.runtime_events(row["session_id"], limit=1000)
            events = [RuntimeEvent.from_record(item) for item in page.items]
            starts = [e.payload for e in events if e.event_name == "operator_call.started"]
            self.assertEqual([e.retry_attempt for e in starts], [0, 1, 0])
            snapshot = next(e.payload.input for e in events if e.event_name == "node_occurrence.started")
            self.assertEqual(snapshot["invocation_input"]["text"], "original")
            state = StateReducer().reduce(events)
            self.assertEqual(dict(state.invocation.scheduler.node_executions), {"primary": 1, "backup": 1})
        self.assertEqual(calls, ["mapped:original", "mapped:original"])

    async def test_async_resources_share_operator_loop_and_cleanup(self):
        """Create, use and release asynchronous resources in Core's execution loop."""
        observations = []
        capability = Capability("resource", Operator(identity).contract)

        @asynccontextmanager
        async def resources(setup):
            loop = asyncio.get_running_loop()
            observations.append(("enter", loop))

            async def work(value: Value) -> Value:
                observations.append(("call", asyncio.get_running_loop()))
                return value

            setup.register_operator(
                Operator(work, id="resource-op"), capability_id="resource", default=True
            )
            try:
                yield
            finally:
                observations.append(("exit", asyncio.get_running_loop()))

        caller = asyncio.get_running_loop()
        async with AutoAgentHost(
            workflows=[Workflow("resources", nodes=[Node("work", capability)])],
            settings=self.settings,
            lifespan=resources,
        ) as host:
            result = await run(host, "resources", {"text": "value"})
            self.assertEqual(result.status, "completed")
        self.assertEqual([name for name, _ in observations], ["enter", "call", "exit"])
        self.assertIs(observations[0][1], observations[1][1])
        self.assertIs(observations[1][1], observations[2][1])
        self.assertIsNot(observations[0][1], caller)

    async def test_request_cancellation_detaches_without_cancelling_workflow(self):
        """A disconnected caller cannot cancel an admitted background execution."""

        started, release = threading.Event(), threading.Event()

        @asynccontextmanager
        async def resources(setup):
            async def delayed(value: Value) -> Value:
                started.set()
                await asyncio.to_thread(release.wait, 3)
                return value

            setup.register_workflow(Workflow("delayed", nodes=[Node("work", delayed)]))
            yield

        async with AutoAgentHost(settings=self.settings, lifespan=resources) as host:
            task = asyncio.create_task(run(host, "delayed", {"text": "finish"}))
            self.assertTrue(await asyncio.to_thread(started.wait, 3))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            row = (await host._persistence.list_invocations()).items[0]
            release.set()
            result = await host.join(row["invocation_id"])
            self.assertEqual(result.status, "completed")

    async def test_failure_does_not_prevent_other_workflows(self):
        """Report Operator failure while preserving Host's other operations."""
        workflows = [
            Workflow("fail", nodes=[Node("work", failed)]),
            Workflow("ok", nodes=[Node("work", identity)]),
        ]
        async with AutoAgentHost(workflows=workflows, settings=self.settings) as host:
            result = await run(host, "fail", {"text": "failure"})
            self.assertEqual(result.status, "failed")
            self.assertEqual(
                (await host.join(result.invocation_id)).error.type, "ValueError"
            )
            self.assertEqual((await run(host, "ok", {"text": "ok"})).status, "completed")

    async def test_invalid_duplicate_and_outside_import_rejected(self):
        """Reject invalid declarations before execution, including reused module names."""
        name = self.project_module("""
from autoagent import Node, Workflow
from autoagent.host import workflow
from typing_extensions import TypedDict
class Value(TypedDict):
    text: str
def work(value: Value) -> Value:
    return value
@workflow
def first(): return Workflow("duplicate", nodes=[Node("work", work)])
@workflow
def second(): return Workflow("duplicate", nodes=[Node("work", work)])
""")
        with self.assertRaisesRegex(ProjectError, "Duplicate Workflow"):
            AutoAgentHost.check_project(self.root)
        (self.root / "auto-agent.toml").write_text(
            f'schema_version=1\n[[workflows]]\nentrypoint="{name}:missing"\n'
        )
        with self.assertRaisesRegex(ProjectError, "does not exist"):
            AutoAgentHost.check_project(self.root)
        (self.root / "auto-agent.toml").write_text(
            'schema_version=1\n[[workflows]]\nentrypoint="autoagent:Workflow"\n'
        )
        with self.assertRaisesRegex(ProjectError, "another project"):
            AutoAgentHost.check_project(self.root)

    async def test_signal_control_uses_invocation_id(self):
        """Deliver an external Signal and continue the resident waiting graph."""
        wf = Workflow(
            "signals",
            nodes=[Node("receive", ReceiveSignal("message"))],
            signal_endpoints=[SignalEndpoint("message", Value)],
        )
        async with AutoAgentHost(workflows=[wf], settings=self.settings) as host:
            submission = await host.execute("signals", {"text": "start"})
            await host.signal(
                submission.invocation_id,
                "message",
                {"text": "signal"},
                source_id="test",
                sequence=1,
            )
            result = await host.join(submission.invocation_id)
            self.assertEqual(result.status, "completed")

    async def test_recovery_replays_events_without_checkpoint_and_after_anchor(self):
        """Restore full event history and a newer event tail without rerunning completed Nodes."""
        for anchored in (False, True):
            settings = self.settings
            # Keep each iteration independent even on PostgreSQL by using a
            # different Session and Workflow identity in the same test database.
            name = "events-anchor" if anchored else "events-only"
            wf = Workflow(
                name,
                nodes=[
                    Node(
                        "first",
                        identity,
                        user_events=observation,
                    ),
                    Node(
                        "wait",
                        Wait(Value, Value),
                        user_events=observation,
                    ),
                    Node(
                        "last",
                        Wait(Value, Value),
                        user_events=observation,
                    ),
                ],
                edges=[Edge("first", "wait"), Edge("wait", "last")],
            )
            async with await DatabaseSink.open(settings.database_url) as sink:
                journal = InMemoryUserEventJournal()
                app = AutoAgentApp(
                    runtime_event_sink=sink,
                    user_event_sink=sink,
                    user_event_journal=journal,
                )
                try:
                    app.register_workflow(wf)
                    await sink.register_operator(Operator(identity))
                    await sink.register_workflow(
                        app.workflow_definition_snapshot(wf.id)
                    )
                    waiting = await app.ainvoke(wf, {"text": "first"}, session_id=name)
                    if anchored:
                        checkpoint = app.unload_session(
                            waiting.ref, capture_checkpoint=True
                        )
                        await sink.save_checkpoint(checkpoint)
                        journal.continue_sequence(waiting.invocation_id, 1)
                        app.load_checkpoint(checkpoint)
                        await app.arecover(waiting.ref)
                        waiting = await app.aresume(
                            waiting.ref, waiting.waits[0].id, {"text": "middle"}
                        )
                finally:
                    # Simulate cleanly terminating the process without updating
                    # the database checkpoint, leaving an event-only durable tail.
                    app.close()
            async with AutoAgentHost(workflows=[wf], settings=settings) as host:
                result = await respond(host, waiting.invocation_id, {"text": "resumed"})
                if not anchored:
                    self.assertEqual(result.status, "waiting")
                    result = await respond(host, waiting.invocation_id, {"text": "done"})
                self.assertEqual(result.status, "completed")
                events = (await host._persistence.user_events(waiting.invocation_id)).items
                self.assertEqual([event["sequence"] for event in events], [1, 2, 3])
                self.assertEqual(events[0]["payload"], {"text": "first"})

    async def test_buffered_close_drains_automatically(self):
        """Persist buffered events and release the writer before a second Host opens."""
        settings = replace(
            self.settings,
            persistence=PersistenceConfig(mode="buffered", batch_delay_ms=100),
        )
        wf = Workflow("buffered", nodes=[Node("work", identity)])
        async with AutoAgentHost(workflows=[wf], settings=settings) as host:
            result = await run(host, 
                "buffered", {"text": "saved"}, session_id="buffered"
            )
        async with AutoAgentHost(workflows=[wf], settings=settings) as host:
            self.assertEqual(
                (await host.join(result.invocation_id)).output, {"text": "saved"}
            )
            second = await run(host, "buffered", {"text": "next"}, session_id="buffered")
            self.assertEqual(second.status, "completed")
            events = (await runtime_events(host, result.invocation_id)).items
            self.assertTrue(
                all(event["invocation_id"] == result.invocation_id for event in events)
            )

    async def test_resource_cleanup_failure_preserves_checkpoint_and_releases_lease(
        self,
    ):
        """Report resource cleanup failure while preserving state and releasing every owner."""
        wf = Workflow("cleanup", nodes=[Node("wait", Wait(Value, Value))])

        @asynccontextmanager
        async def resources(setup):
            yield
            raise ValueError("resource cleanup")

        with self.assertRaisesRegex(ValueError, "resource cleanup"):
            async with AutoAgentHost(
                workflows=[wf], settings=self.settings, lifespan=resources
            ) as host:
                result = await run(host, "cleanup", {"text": "wait"})
        async with AutoAgentHost(workflows=[wf], settings=self.settings) as host:
            self.assertEqual(
                (await respond(host, result.invocation_id, {"text": "after"})).status,
                "completed",
            )

    async def test_missing_revision_does_not_mutate_history(self):
        """Reject continuation using incompatible code while leaving history readable."""
        wf = Workflow("revision", nodes=[Node("wait", Wait(Value, Value))], version="1")
        async with AutoAgentHost(workflows=[wf], settings=self.settings) as host:
            waiting = await run(host, "revision", {"text": "before"})
        changed = Workflow(
            "revision", nodes=[Node("wait", Wait(Value, Value))], version="2"
        )
        async with AutoAgentHost(workflows=[changed], settings=self.settings) as host:
            with self.assertRaisesRegex(HostError, "Revision is unavailable"):
                await respond(host, waiting.invocation_id, {"text": "bad"})
            self.assertEqual(
                (await host.get_execution(waiting.invocation_id)).status, "waiting"
            )

    async def test_terminal_session_can_run_new_revision_after_restart(self):
        """Reuse completed Session context without requiring execution of archived code."""
        old = Workflow("upgrade", nodes=[Node("work", identity)], version="1")
        async with AutoAgentHost(workflows=[old], settings=self.settings) as host:
            first = await run(host, 
                "upgrade", {"text": "old"}, session_id="upgrade-session"
            )
        new = Workflow("upgrade", nodes=[Node("work", identity)], version="2")
        async with AutoAgentHost(workflows=[new], settings=self.settings) as host:
            second = await run(host, 
                "upgrade", {"text": "new"}, session_id="upgrade-session"
            )
            self.assertEqual(second.status, "completed")
            self.assertNotEqual(
                first.ref.workflow_revision_id, second.ref.workflow_revision_id
            )
            self.assertEqual(
                (await host.join(first.invocation_id)).output, {"text": "old"}
            )

    async def test_cancelled_exit_waiter_cannot_abort_shutdown(self):
        """Cancellation detaches a context-exit waiter without dropping checkpoint writes."""
        wf = Workflow("exit", nodes=[Node("wait", Wait(Value, Value))])
        host = AutoAgentHost(workflows=[wf], settings=self.settings)
        await host.__aenter__()
        result = await run(host, "exit", {"text": "before"})
        entered, release = asyncio.Event(), asyncio.Event()
        original = host._persistence.save_checkpoint

        async def delayed(checkpoint, **options):
            entered.set()
            await release.wait()
            return await original(checkpoint, **options)

        host._persistence.save_checkpoint = delayed
        waiter = asyncio.create_task(host.__aexit__(None, None, None))
        await asyncio.wait_for(entered.wait(), 3)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        with self.assertRaises(HostError):
            await run(host, "exit", {"text": "rejected"})
        release.set()
        await host.__aexit__(None, None, None)
        async with AutoAgentHost(workflows=[wf], settings=self.settings) as reopened:
            self.assertEqual(
                (await respond(reopened, result.invocation_id, {"text": "after"})).status,
                "completed",
            )

    async def test_cancelled_startup_waiter_releases_resources_and_environment(self):
        """A cancelled context entry still finishes cleanup of its shared startup."""
        started, release = threading.Event(), threading.Event()
        exits = []

        @asynccontextmanager
        async def resources(setup):
            started.set()
            await asyncio.to_thread(release.wait, 3)
            try:
                yield
            finally:
                exits.append("closed")

        wf = Workflow("entry", nodes=[Node("work", identity)])
        host = AutoAgentHost(
            workflows=[wf],
            settings=self.settings,
            lifespan=resources,
            environment={"HOST_STARTUP_TEST": "temporary"},
        )
        task = asyncio.create_task(host.__aenter__())
        self.assertTrue(await asyncio.to_thread(started.wait, 3))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        release.set()
        await host.__aexit__(None, None, None)
        self.assertEqual(exits, ["closed"])
        self.assertNotIn("HOST_STARTUP_TEST", os.environ)
        async with await DatabaseSink.open(self.settings.database_url):
            pass

    async def test_core_resource_prepare_is_shared_and_close_waits_for_it(self):
        """Serialize resource initialization with close and avoid double creation."""
        entered, release = threading.Event(), threading.Event()
        calls = []

        @asynccontextmanager
        async def resources():
            calls.append("enter")
            entered.set()
            await asyncio.to_thread(release.wait, 3)
            try:
                yield
            finally:
                calls.append("exit")

        app = AutoAgentApp(runtime_lifespan=resources)
        try:
            first = asyncio.create_task(app.aprepare())
            self.assertTrue(await asyncio.to_thread(entered.wait, 3))
            second = asyncio.create_task(app.aprepare())
            await asyncio.sleep(0)
            closing = asyncio.create_task(app.aclose(timeout=None))
            release.set()
            await asyncio.gather(first, second, closing)
            self.assertEqual(calls, ["enter", "exit"])
        finally:
            release.set()
            await app.aclose(timeout=None)


@unittest.skipUnless(
    POSTGRES_TEST_URL,
    "isolated PostgreSQL URL not configured",
)
class PostgreSQLHostTests(HostTests):
    async def asyncSetUp(self):
        import asyncpg
        from sqlalchemy.engine import make_url

        await super().asyncSetUp()
        self.admin_url = make_url(POSTGRES_TEST_URL)
        self.database_name = "autoagent_host_test_" + uuid4().hex
        connection = await asyncpg.connect(
            self.admin_url.set(drivername="postgresql").render_as_string(
                hide_password=False
            )
        )
        try:
            await connection.execute('CREATE DATABASE "' + self.database_name + '"')
        finally:
            await connection.close()
        self.settings = replace(
            self.settings,
            database_url=self.admin_url.set(
                database=self.database_name
            ).render_as_string(hide_password=False),
        )

    async def asyncTearDown(self):
        import asyncpg

        connection = await asyncpg.connect(
            self.admin_url.set(drivername="postgresql").render_as_string(
                hide_password=False
            )
        )
        try:
            await connection.execute(
                'DROP DATABASE "' + self.database_name + '" WITH (FORCE)'
            )
        finally:
            await connection.close()
            await super().asyncTearDown()


class HostConfigurationTests(unittest.TestCase):
    def test_transactional_projection_default_and_explicit_background(self):
        """SDK and environment defaults agree, while background remains explicitly selectable."""
        from autoagent.host.persistence import PersistenceConfig
        self.assertEqual(PersistenceConfig().projection_mode, 'transactional')
        self.assertEqual(HostSettings().persistence.projection_mode, 'transactional')
        self.assertEqual(HostSettings.from_environment({}).persistence.projection_mode, 'transactional')
        settings = HostSettings.from_environment({'AUTOAGENT_PROJECTION_MODE': 'background'})
        self.assertEqual(settings.persistence.projection_mode, 'background')

    def test_env_example_covers_every_host_and_agent_application_setting(self):
        """Document startup settings at their Host and Agent application parser locations."""
        from autoagent.agent_app import AgentApplication
        from autoagent.agent_app.resources import ManagedAgentResources
        from autoagent.host.providers import HostProviders
        from autoagent.host.project import Project

        names = {
            value
            for parser in (HostSettings.from_environment.__func__, ManagedAgentResources.skill_roots, HostProviders.__init__, Project.load,
                           AgentApplication.project_workflows, AgentApplication._configure_startup)
            for value in parser.__code__.co_consts
            if isinstance(value, str) and value.startswith("AUTOAGENT_") and value.isidentifier()
        }
        documented = {
            line.split("=", 1)[0]
            for line in (Path(__file__).resolve().parents[1] / ".env.example")
            .read_text()
            .splitlines()
            if line.startswith("AUTOAGENT_") and not line.startswith(("AUTOAGENT_SERVER_", "AUTOAGENT_LOG_"))
        }
        self.assertEqual(names, documented)

    def test_user_event_journal_continuation(self):
        """Restore observation sequence without manufacturing previous User Events."""
        journal = InMemoryUserEventJournal()
        journal.continue_sequence("invocation", 5)
        event = journal.emit(
            session_id="session",
            invocation_id="invocation",
            kind="progress",
            payload={},
            occurrence_id=None,
            occurred_at_us=1,
        )
        self.assertEqual(event.sequence, 6)
        with self.assertRaises(ValueError):
            journal.continue_sequence("invocation", 4)
        with self.assertRaises(ValueError):
            journal.continue_sequence("invocation", True)
