from __future__ import annotations

import asyncio
import sys
import tempfile
import textwrap
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from unittest.mock import patch

from autoagent import (
    AppCheckpoint,
    CheckpointLoadResult,
    InvocationRef,
    InvocationResult,
    RuntimeTransitionError,
)
from autoagent.host import (
    AutoAgentHost,
    HostOperationError,
    HostSettings,
    ProjectLoader,
    create_runtime_event_sink,
)
from autoagent.hosting import HttpRuntimeEventSink, SQLiteRuntimeStore


class HostLifecycleTests(unittest.TestCase):
    def test_sink_factory_builds_none_sqlite_and_http_modes(self) -> None:
        """Verify every configured sink mode has the expected lifecycle object."""

        self.assertIsNone(
            create_runtime_event_sink(HostSettings(runtime_event_sink="none"))
        )
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "runtime.db"
            sqlite = create_runtime_event_sink(
                HostSettings(sqlite_path=database)
            )
            self.assertIsInstance(sqlite, SQLiteRuntimeStore)
            self.assertTrue(database.is_file())
            assert sqlite is not None
            sqlite.close()

        http = create_runtime_event_sink(
            HostSettings(
                runtime_event_sink="http",
                http_sink_url="https://events.example.test/v1",
            )
        )
        self.assertIsInstance(http, HttpRuntimeEventSink)
        assert http is not None
        http.close()

    def test_sink_factory_closes_a_store_that_fails_to_start(self) -> None:
        """Verify failed SQLite startup cannot leak its dedicated workers."""

        events: list[str] = []

        class FailingStore:
            def __init__(self, _path, *, refresh_seconds=0.5):
                events.append("create")

            def start(self):
                events.append("start")
                raise RuntimeError("database unavailable")

            def close(self):
                events.append("close")

        with patch("autoagent.host.sinks.SQLiteRuntimeStore", FailingStore):
            with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                create_runtime_event_sink(
                    HostSettings(sqlite_path=Path("runtime.db"))
                )
        self.assertEqual(events, ["create", "start", "close"])

    def test_from_project_registers_nested_workflows_and_persists_snapshots(self) -> None:
        """Verify Host startup registers and stores the complete Workflow closure."""

        module = "host_lifecycle_nested"
        with self.project(
            module,
            """
            from typing_extensions import TypedDict
            from autoagent import Node, Workflow

            class Value(TypedDict):
                value: int

            def identity(value: Value) -> Value:
                return value

            child = Workflow("nested-child", nodes=[Node("work", identity)])
            workflow = Workflow("nested-root", nodes=[Node("child", child)])
            """,
        ) as root:
            database = root / "events.db"
            host = AutoAgentHost.from_project(
                root,
                environ={"AUTOAGENT_SQLITE_PATH": str(database)},
            )
            try:
                result = host.invoke(
                    "nested-root",
                    {"value": 3},
                    session_id="nested-session",
                )
                self.assertEqual(result.status, "completed")
                handles = host.child_handles(result.ref)
                self.assertEqual(len(handles), 1)
                self.assertEqual(host.child_status(handles[0]).status, "completed")
                store = host.runtime_store
                assert store is not None
                workflows = asyncio.run(store.list_workflows())
                self.assertEqual(
                    {item["workflow_id"] for item in workflows.items},
                    {"nested-root", "nested-child"},
                )
            finally:
                host.close()
        sys.modules.pop(module, None)

    def test_sync_invoke_submit_and_stream_delegate_to_core(self) -> None:
        """Verify synchronous Host execution preserves Core result contracts."""

        module = "host_lifecycle_sync"
        with self.project(
            module,
            """
            from typing_extensions import TypedDict
            from autoagent import Node, Workflow

            class Value(TypedDict):
                value: int

            def identity(value: Value) -> Value:
                return value

            workflow = Workflow("sync", nodes=[Node("work", identity)])
            """,
        ) as root:
            host = AutoAgentHost.from_project(
                root,
                environ={"AUTOAGENT_RUNTIME_EVENT_SINK": "none"},
            )
            try:
                invoked = host.invoke("sync", {"value": 1}, session_id="invoke")
                submitted = host.submit_invoke(
                    "sync", {"value": 2}, session_id="submit"
                )
                completed = host.wait(submitted.ref, timeout=1)
                streamed = list(
                    host.stream("sync", {"value": 3}, session_id="stream")
                )
                self.assertEqual(invoked.output, {"value": 1})
                self.assertEqual(completed.output, {"value": 2})
                self.assertIsInstance(streamed[-1], InvocationResult)
                self.assertEqual(streamed[-1].output, {"value": 3})
            finally:
                host.close()
        sys.modules.pop(module, None)

    def test_async_invoke_submit_and_stream_delegate_to_core(self) -> None:
        """Verify asynchronous Host execution preserves Core backpressure APIs."""

        module = "host_lifecycle_async"
        with self.project(
            module,
            """
            from typing_extensions import TypedDict
            from autoagent import Node, Workflow

            class Value(TypedDict):
                value: int

            async def identity(value: Value) -> Value:
                return value

            workflow = Workflow("async", nodes=[Node("work", identity)])
            """,
        ) as root:
            host = AutoAgentHost.from_project(
                root,
                environ={"AUTOAGENT_RUNTIME_EVENT_SINK": "none"},
            )

            async def run() -> tuple[InvocationResult, InvocationResult, list[object]]:
                invoked = await host.ainvoke(
                    "async", {"value": 4}, session_id="ainvoke"
                )
                submitted = await host.asubmit_invoke(
                    "async", {"value": 5}, session_id="asubmit"
                )
                completed = await host.await_result(submitted.ref, timeout=1)
                streamed = [
                    item
                    async for item in host.astream(
                        "async", {"value": 6}, session_id="astream"
                    )
                ]
                return invoked, completed, streamed

            try:
                invoked, completed, streamed = asyncio.run(run())
                self.assertEqual(invoked.output, {"value": 4})
                self.assertEqual(completed.output, {"value": 5})
                self.assertIsInstance(streamed[-1], InvocationResult)
                self.assertEqual(streamed[-1].output, {"value": 6})
            finally:
                host.close()
        sys.modules.pop(module, None)

    def test_sqlite_restore_loads_then_recovers_a_completed_session(self) -> None:
        """Verify persisted Events rebuild a checkpoint without replaying work."""

        module = "host_lifecycle_restore_completed"
        with self.project(
            module,
            """
            from typing_extensions import TypedDict
            from autoagent import Node, Workflow

            CALLS = 0

            class Value(TypedDict):
                value: int

            def count(value: Value) -> Value:
                global CALLS
                CALLS += 1
                return value

            workflow = Workflow("recover-completed", nodes=[Node("work", count)])
            """,
        ) as root:
            environment = {"AUTOAGENT_SQLITE_PATH": str(root / "events.db")}
            first = AutoAgentHost.from_project(root, environ=environment)
            result = first.invoke(
                "recover-completed",
                {"value": 7},
                session_id=" recover-completed-session ",
            )
            first.close()

            restored = AutoAgentHost.from_project(root, environ=environment)
            try:
                loaded = restored.restore_session(result.session_id)
                recovered = restored.recover(loaded.roots[0])
                self.assertEqual(recovered.status, "completed")
                self.assertEqual(recovered.output, {"value": 7})
                self.assertEqual(sys.modules[module].CALLS, 1)
            finally:
                restored.close()
        sys.modules.pop(module, None)

    def test_restore_rebuilds_child_closure_and_rejects_child_as_root(self) -> None:
        """Verify restore owns the full Child graph and accepts only its Root id."""

        module = "host_lifecycle_restore_child"
        with self.project(
            module,
            """
            from typing_extensions import TypedDict
            from autoagent import Node, Workflow

            class Value(TypedDict):
                value: int

            def identity(value: Value) -> Value:
                return value

            child = Workflow("restore-child", nodes=[Node("work", identity)])
            workflow = Workflow("restore-parent", nodes=[Node("child", child)])
            """,
        ) as root:
            environment = {"AUTOAGENT_SQLITE_PATH": str(root / "events.db")}
            first = AutoAgentHost.from_project(root, environ=environment)
            result = first.invoke(
                "restore-parent",
                {"value": 11},
                session_id="restore-parent-session",
            )
            child_session_id = first.child_handles(result.ref)[0]["session_id"]
            first.close()

            restored = AutoAgentHost.from_project(root, environ=environment)
            try:
                with self.assertRaises(HostOperationError) as captured:
                    restored.restore_session(child_session_id)
                self.assertEqual(captured.exception.code, "HOST_SESSION_NOT_ROOT")

                loaded = restored.restore_session(result.session_id)
                self.assertEqual(len(loaded.roots), 1)
                self.assertEqual(len(loaded.invocations), 2)
                recovered = restored.recover(loaded.roots[0])
                self.assertEqual(recovered.status, "completed")
            finally:
                restored.close()
        sys.modules.pop(module, None)

    def test_restore_requires_the_deployed_historical_revision(self) -> None:
        """Verify portable snapshots cannot replace missing executable code."""

        first_module = "host_lifecycle_revision_one"
        second_module = "host_lifecycle_revision_two"
        source = """
            from typing_extensions import TypedDict
            from autoagent import Node, Workflow

            class Value(TypedDict):
                value: int

            def identity(value: Value) -> Value:
                return value

            workflow = Workflow(
                "revisioned",
                version={version!r},
                nodes=[Node({node_id!r}, identity)],
            )
        """
        with self.project(
            first_module,
            source.format(version="1", node_id="first"),
        ) as root:
            environment = {"AUTOAGENT_SQLITE_PATH": str(root / "events.db")}
            first = AutoAgentHost.from_project(root, environ=environment)
            result = first.invoke(
                "revisioned",
                {"value": 12},
                session_id="revision-session",
            )
            first.close()
            sys.modules.pop(first_module, None)

            (root / "autoagent.toml").write_text(
                self.manifest(second_module),
                encoding="utf-8",
            )
            (root / f"{second_module}.py").write_text(
                textwrap.dedent(source.format(version="2", node_id="second")),
                encoding="utf-8",
            )
            second = AutoAgentHost.from_project(root, environ=environment)
            try:
                with self.assertRaises(RuntimeTransitionError):
                    second.restore_session(result.session_id)
            finally:
                second.close()
        sys.modules.pop(second_module, None)

    def test_restore_then_recover_keeps_wait_and_session_ownership(self) -> None:
        """Verify a restored Wait remains active and blocks replacement Invocation."""

        module = "host_lifecycle_restore_wait"
        with self.project(
            module,
            """
            from typing_extensions import TypedDict
            from autoagent import Node, Wait, Workflow

            class Value(TypedDict):
                value: int

            workflow = Workflow("recover-wait", nodes=[Node("approval", Wait(Value, Value))])
            """,
        ) as root:
            environment = {"AUTOAGENT_SQLITE_PATH": str(root / "events.db")}
            first = AutoAgentHost.from_project(root, environ=environment)
            waiting = first.invoke(
                "recover-wait",
                {"value": 8},
                session_id="recover-wait-session",
            )
            first.close()

            restored = AutoAgentHost.from_project(root, environ=environment)
            try:
                loaded = restored.restore_session(waiting.session_id)
                recovered = restored.recover(loaded.roots[0])
                self.assertEqual(recovered.status, "waiting")
                with self.assertRaises(RuntimeTransitionError):
                    restored.invoke(
                        "recover-wait",
                        {"value": 9},
                        session_id=waiting.session_id,
                    )
                completed = restored.resume(
                    recovered.ref,
                    recovered.waits[0].id,
                    {"value": 10},
                )
                self.assertEqual(completed.output, {"value": 10})
            finally:
                restored.close()
        sys.modules.pop(module, None)

    def test_restore_requires_sqlite_and_sync_api_rejects_async_context(self) -> None:
        """Verify unsupported restore modes and sync-in-loop calls fail clearly."""

        module = "host_lifecycle_restore_errors"
        with self.project(
            module,
            """
            from typing_extensions import TypedDict
            from autoagent import Node, Workflow

            class Value(TypedDict):
                value: int

            def identity(value: Value) -> Value:
                return value

            workflow = Workflow("restore-errors", nodes=[Node("work", identity)])
            """,
        ) as root:
            no_store = AutoAgentHost.from_project(
                root,
                environ={"AUTOAGENT_RUNTIME_EVENT_SINK": "none"},
            )
            try:
                with self.assertRaises(HostOperationError) as captured:
                    no_store.restore_session("missing")
                self.assertEqual(captured.exception.code, "HOST_RESTORE_UNAVAILABLE")
            finally:
                no_store.close()

            sqlite = AutoAgentHost.from_project(
                root,
                environ={"AUTOAGENT_SQLITE_PATH": str(root / "events.db")},
            )

            with self.assertRaises(HostOperationError) as missing_sync:
                sqlite.restore_session("missing")
            self.assertEqual(
                missing_sync.exception.code,
                "HOST_SESSION_NOT_FOUND",
            )

            async def call_missing_async_restore() -> str:
                with self.assertRaises(HostOperationError) as captured:
                    await sqlite.arestore_session("missing")
                return captured.exception.code

            async def call_sync_restore() -> str:
                with self.assertRaises(HostOperationError) as captured:
                    sqlite.restore_session("missing")
                return captured.exception.code

            try:
                self.assertEqual(
                    asyncio.run(call_missing_async_restore()),
                    "HOST_SESSION_NOT_FOUND",
                )
                self.assertEqual(
                    asyncio.run(call_sync_restore()),
                    "HOST_SYNC_API_IN_ASYNC_CONTEXT",
                )
            finally:
                sqlite.close()
        sys.modules.pop(module, None)

    def test_close_is_idempotent_and_orders_app_before_sink(self) -> None:
        """Verify Host closes Core exactly once before closing Event persistence."""

        module = "host_lifecycle_close"
        with self.project(
            module,
            """
            from autoagent import Workflow
            workflow = Workflow("close")
            """,
        ) as root:
            project = ProjectLoader().load(root)
        events: list[str] = []

        class FakeApp:
            def close(self, timeout=30.0):
                events.append("app")
                return AppCheckpoint()

        class FakeSink:
            async def append(self, _event):  # pragma: no cover - structural port
                return None

            def close(self):
                events.append("sink")

        host = AutoAgentHost(
            project=project,
            settings=HostSettings(runtime_event_sink="none"),
            app=FakeApp(),  # type: ignore[arg-type]
            event_sink=FakeSink(),  # type: ignore[arg-type]
        )
        with self.assertRaises(ValueError):
            host.close(timeout=10**1000)
        first = host.close()
        second = host.close()
        self.assertIs(first, second)
        self.assertEqual(events, ["app", "sink"])
        with self.assertRaises(HostOperationError):
            host.invoke("close", None)
        sys.modules.pop(module, None)

    def test_close_timeout_keeps_sink_open_until_app_close_succeeds(self) -> None:
        """Let the shared close coordinator finish after one caller times out."""

        events: list[str] = []
        started = threading.Event()
        release = threading.Event()
        sink_closed = threading.Event()

        class SlowCloseApp:
            def close(self, timeout=30.0):
                self.assert_timeout(timeout)
                started.set()
                release.wait(1)
                events.append("app")
                return AppCheckpoint()

            @staticmethod
            def assert_timeout(timeout):
                if timeout is not None:
                    raise AssertionError("Host coordinator must own the full close")

        class FakeSink:
            async def append(self, _event):  # pragma: no cover - structural port
                return None

            def close(self):
                events.append("sink")
                sink_closed.set()

        host = AutoAgentHost(
            project=object(),  # type: ignore[arg-type]
            settings=HostSettings(runtime_event_sink="none"),
            app=SlowCloseApp(),  # type: ignore[arg-type]
            event_sink=FakeSink(),  # type: ignore[arg-type]
        )
        with self.assertRaises(TimeoutError):
            host.close(timeout=0.01)
        self.assertTrue(started.is_set())
        self.assertEqual(events, [])
        with self.assertRaises(HostOperationError) as captured:
            host.invoke("anything", None)
        self.assertEqual(captured.exception.code, "HOST_CLOSING")
        release.set()
        self.assertTrue(sink_closed.wait(1))
        self.assertIsInstance(host.close(timeout=1), AppCheckpoint)
        self.assertEqual(events, ["app", "sink"])

    def test_close_waits_for_an_inflight_async_restore(self) -> None:
        """Verify Store closure cannot race checkpoint rebuild and Core loading."""

        events: list[str] = []
        ref = InvocationRef("root", "invocation")

        class SlowStore(SQLiteRuntimeStore):
            def __init__(self):
                self.entered = asyncio.Event()
                self.release = asyncio.Event()

            async def rebuild_checkpoint(self, _root_session_id):
                self.entered.set()
                await self.release.wait()
                return object()

            def close(self):
                events.append("sink")

        class FakeApp:
            async def aload_checkpoint(self, _checkpoint):
                events.append("load")
                return CheckpointLoadResult((ref,), (ref,))

            def close(self, timeout=30.0):
                events.append("app")
                return AppCheckpoint()

            async def aclose(self, timeout=30.0):
                return self.close(timeout)

        async def run() -> None:
            store = SlowStore()
            host = AutoAgentHost(
                project=object(),  # type: ignore[arg-type]
                settings=HostSettings(),
                app=FakeApp(),  # type: ignore[arg-type]
                event_sink=store,
            )
            restore = asyncio.create_task(host.arestore_session("root"))
            await store.entered.wait()
            closing = asyncio.create_task(host.aclose())
            try:
                await asyncio.sleep(0.01)
                self.assertFalse(closing.done())
                self.assertEqual(events, [])
            finally:
                store.release.set()
            loaded = await asyncio.wait_for(restore, 1)
            self.assertEqual(loaded.roots, (ref,))
            await asyncio.wait_for(closing, 1)

        asyncio.run(run())
        self.assertEqual(events, ["load", "app", "sink"])

    def test_cancelled_async_close_does_not_cancel_shared_shutdown(self) -> None:
        """Verify shutdown survives caller cancellation and event-loop teardown."""

        events: list[str] = []

        class SlowApp:
            def __init__(self) -> None:
                self.loop: asyncio.AbstractEventLoop | None = None
                self.started: asyncio.Event | None = None
                self.release = threading.Event()

            def close(self, timeout=30.0):
                assert self.loop is not None
                assert self.started is not None
                self.loop.call_soon_threadsafe(self.started.set)
                self.release.wait()
                events.append("app")
                return AppCheckpoint()

        class FakeSink:
            async def append(self, _event):  # pragma: no cover - structural port
                return None

            def close(self):
                events.append("sink")

        app = SlowApp()
        host = AutoAgentHost(
            project=object(),  # type: ignore[arg-type]
            settings=HostSettings(runtime_event_sink="none"),
            app=app,  # type: ignore[arg-type]
            event_sink=FakeSink(),  # type: ignore[arg-type]
        )

        async def detach() -> None:
            app.loop = asyncio.get_running_loop()
            app.started = asyncio.Event()
            detached = asyncio.create_task(host.aclose())
            assert app.started is not None
            await app.started.wait()
            detached.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await detached
            self.assertEqual(events, [])

        asyncio.run(detach())
        self.assertEqual(events, [])
        app.release.set()
        self.assertIsInstance(host.close(), AppCheckpoint)
        self.assertEqual(events, ["app", "sink"])

    def test_sink_close_failure_leaves_host_terminal(self) -> None:
        """Never advertise an already-closed Core as an open Host."""

        class ClosedApp:
            def close(self, timeout=30.0):
                return AppCheckpoint()

        class FailingSink:
            async def append(self, _event):  # pragma: no cover - structural port
                return None

            def close(self):
                raise RuntimeError("sink close failed")

        host = AutoAgentHost(
            project=object(),  # type: ignore[arg-type]
            settings=HostSettings(runtime_event_sink="none"),
            app=ClosedApp(),  # type: ignore[arg-type]
            event_sink=FailingSink(),  # type: ignore[arg-type]
        )
        with self.assertRaises(HostOperationError) as close_error:
            host.close()
        self.assertEqual(close_error.exception.code, "HOST_SINK_CLOSE_FAILED")
        self.assertIsInstance(close_error.exception.__cause__, RuntimeError)
        with self.assertRaises(HostOperationError) as captured:
            host.invoke("workflow", None)
        self.assertEqual(captured.exception.code, "HOST_CLOSED")
        self.assertIsInstance(host.close(), AppCheckpoint)

    def test_failed_registration_closes_app_before_sink(self) -> None:
        """Verify startup failure cannot leak a live App or persistence worker."""

        module = "host_lifecycle_startup_failure"
        with self.project(
            module,
            """
            from autoagent import Workflow
            workflow = Workflow("startup-failure")
            """,
        ) as root:
            events: list[str] = []

            class FailingApp:
                def __init__(self, **_kwargs):
                    pass

                def register_workflow(self, _workflow):
                    raise RuntimeError("registration failed")

                def close(self):
                    events.append("app")
                    return AppCheckpoint()

            class FakeSink:
                async def append(self, _event):  # pragma: no cover - port only
                    return None

                def close(self):
                    events.append("sink")

            with patch("autoagent.host.host.AutoAgentApp", FailingApp), patch(
                "autoagent.host.host.create_runtime_event_sink",
                return_value=FakeSink(),
            ):
                with self.assertRaisesRegex(RuntimeError, "registration failed"):
                    AutoAgentHost.from_project(root, environ={})

        self.assertEqual(events, ["app", "sink"])
        sys.modules.pop(module, None)

    @staticmethod
    @contextmanager
    def project(module: str, source: str) -> Iterator[Path]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "autoagent.toml").write_text(
                HostLifecycleTests.manifest(module),
                encoding="utf-8",
            )
            (root / f"{module}.py").write_text(
                textwrap.dedent(source),
                encoding="utf-8",
            )
            yield root

    @staticmethod
    def manifest(module: str) -> str:
        return textwrap.dedent(
            f"""
            schema_version = 1

            [project]
            name = "host-lifecycle"
            version = "1"

            [[workflows]]
            entrypoint = "{module}:workflow"
            """
        )


if __name__ == "__main__":
    unittest.main()
