"""Live Workflow registration uses only source files, project TOML and existing history."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
import tomllib
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from autoagent.host import AutoAgentHost, HostSettings, HostRunConflict, ProjectError
from autoagent.core.errors import WorkflowCompileError
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


def write_workflow(root, value=1, *, workflow_id="registered", delay=0):
    root.mkdir(parents=True, exist_ok=True)
    (root / "helper.py").write_text(f"NUMBER = {value}\n")
    (root / "flow.py").write_text(
        "import asyncio\nfrom typing_extensions import TypedDict\n"
        "from autoagent import Workflow, Node, Operator\n"
        "class Value(TypedDict):\n    value: int\n"
        "async def step(value: Value) -> Value:\n"
        f"    await asyncio.sleep({delay!r})\n"
        "    import helper\n    return {'value': value['value'] + helper.NUMBER}\n"
        f"def build():\n    return Workflow({workflow_id!r}, nodes=[Node('start', Operator(step))])\n")


class RegistrationContracts:
    async def database_url(self, root):
        return "sqlite+aiosqlite:///" + str(root / "runtime.db")

    @asynccontextmanager
    async def project(self, manifest="schema_version = 1\nworkflows = []\n", *, discovered=False):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root, source = base / "server", base / "workspace"
            root.mkdir(); source.mkdir()
            if manifest is not None:
                (root / "auto-agent.toml").write_text(manifest)
            write_workflow(source)
            if discovered:
                (root / ("existing_" + uuid4().hex + ".py")).write_text(
                    "from autoagent import Workflow, Node, Operator\nfrom autoagent.host import workflow\n"
                    "from typing_extensions import TypedDict\nclass Value(TypedDict):\n    value: int\n"
                    "def step(value: Value) -> Value: return value\n"
                    "@workflow\ndef build(): return Workflow('existing', nodes=[Node('start', Operator(step))])\n")
            options = dict(use_env_file=False, environment={"AUTOAGENT_BUILTIN_AGENT_ENABLED": "false"},
                settings=HostSettings(database_url=await self.database_url(root)))
            async with AutoAgentHost.from_project(root, **options) as host:
                yield host, source, options

    async def register(self, host, source):
        return await host.register_workflow(source_dir=source, entrypoint="flow:build")

    async def test_register_edit_requires_registration_and_old_calls_keep_code(self):
        """Source edits do not affect active or pinned executions until another registration succeeds."""
        async with self.project() as (host, source, _):
            write_workflow(source, delay=.15)
            first = await self.register(host, source)
            before = await host.execute("registered", {"value": 10})
            write_workflow(source, 2, delay=.15)
            # Even a deferred import uses the old generation until registration.
            old = await host.join(before)
            self.assertEqual((old.status, old.output), ("completed", {"value": 11}), old.error)
            old_ref = await host.execute("registered", {"value": 10})
            second = await self.register(host, source)
            self.assertNotEqual(first["revision_id"], second["revision_id"])
            old = await host.join(old_ref)
            new = await host.join(await host.execute("registered", {"value": 10}))
            self.assertEqual(old.output, {"value": 11}, old.error)
            self.assertEqual(new.output, {"value": 12}, new.error)
            pinned = await host.join(await host.execute("registered", {"value": 10}, revision_id=first["revision_id"]))
            self.assertEqual(pinned.output, {"value": 11}, pinned.error)
            self.assertNotEqual(host._runtime.workflow_definitions[first["revision_id"], "start"].definition_id,
                host._runtime.workflow_definitions[second["revision_id"], "start"].definition_id)

    async def test_restart_loads_current_disk_source_from_existing_manifest(self):
        """TOML persists the source directory; restart loads edits without a snapshot or registration table."""
        async with self.project() as (host, source, options):
            first = await self.register(host, source)
            root = host.root
            manifest = (root / "auto-agent.toml").read_text()
            write_workflow(source, 3)
            # Stop and restart against the same files and existing database.
            await host.__aexit__(None, None, None)
            async with AutoAgentHost.from_project(root, **options) as restarted:
                result = await restarted.join(await restarted.execute("registered", {"value": 1}))
                self.assertEqual((result.status, result.output), ("completed", {"value": 4}), result.error)
                self.assertNotEqual(first["revision_id"], restarted._runtime.workflow_ids["registered"])
            self.assertEqual((root / "auto-agent.toml").read_text(), manifest)

    async def test_nested_workflow_can_update_after_host_restart(self):
        """Startup restores source ownership for embedded child Workflows so the parent can be registered again."""
        async with self.project() as (host, source, options):
            path = source / "parent.py"
            path.write_text("from autoagent import Workflow, Node, Spawn\nfrom flow import build as child\n"
                "def build(): return Workflow('parent', nodes=[Node('spawn', Spawn(child(), 'start'))])\n")
            first = await host.register_workflow(source_dir=source, entrypoint="parent:build")
            root = host.root
            await host.__aexit__(None, None, None)
            async with AutoAgentHost.from_project(root, **options) as restarted:
                write_workflow(source, 4)
                current = await restarted.register_workflow(source_dir=source, entrypoint="parent:build")
                self.assertNotEqual(current["revision_id"], first["revision_id"])
                child = await restarted.join(await restarted.execute("registered", {"value": 1}))
                self.assertEqual(child.output, {"value": 5}, child.error)

    async def test_concurrent_duplicate_registration_preserves_toml_comments(self):
        """Concurrent identical requests publish once and preserve project text and unrelated settings."""
        text = '# Project comment\nschema_version=1\nworkflows = [] # keep this comment\n[project]\nname="Existing"\n'
        async with self.project(text) as (host, source, _):
            rows = await asyncio.gather(*(self.register(host, source) for _ in range(4)))
            self.assertEqual(sum(row["changed"] for row in rows), 1)
            saved = (host.root / "auto-agent.toml").read_text()
            self.assertIn("# Project comment", saved)
            self.assertIn("# keep this comment", saved)
            data = tomllib.loads(saved)
            self.assertEqual(data["project"]["name"], "Existing")
            self.assertEqual(len(data["workflows"]), 1)

    async def test_compile_and_persist_failure_leave_current_registration_unchanged(self):
        """Failed validation or persistence cannot replace the live version or edit TOML."""
        async with self.project() as (host, source, _):
            first = await self.register(host, source)
            path = host.root / "auto-agent.toml"
            saved = path.read_bytes()
            (source / "flow.py").write_text('from autoagent import Workflow\ndef build(): return Workflow("registered")\n')
            with self.assertRaises(WorkflowCompileError):
                await self.register(host, source)
            self.assertEqual(path.read_bytes(), saved)
            write_workflow(source, 2)
            with patch.object(host._runtime, "persist_registration", AsyncMock(side_effect=OSError("disk failure"))):
                with self.assertRaises(OSError):
                    await self.register(host, source)
            self.assertEqual(path.read_bytes(), saved)
            self.assertEqual(host._runtime.workflow_ids["registered"], first["revision_id"])

    async def test_publication_failure_rolls_back_manifest(self):
        """A publication error restores the exact prior TOML bytes."""
        async with self.project() as (host, source, _):
            await self.register(host, source)
            path = host.root / "auto-agent.toml"
            before = path.read_bytes()
            write_workflow(source, 2)
            with patch.object(host._runtime, "publish_registration", AsyncMock(side_effect=RuntimeError("publish failed"))):
                with self.assertRaises(RuntimeError):
                    await self.register(host, source)
            self.assertEqual(path.read_bytes(), before)

    async def test_conflicting_source_cannot_replace_workflow(self):
        """Another source with the same Workflow ID cannot take over the registered implementation."""
        async with self.project() as (host, source, _):
            first = await self.register(host, source)
            other = source / "other"
            write_workflow(other, 9)
            saved = (host.root / "auto-agent.toml").read_bytes()
            with self.assertRaises(HostRunConflict):
                await self.register(host, other)
            self.assertEqual(host._runtime.workflow_ids["registered"], first["revision_id"])
            self.assertEqual((host.root / "auto-agent.toml").read_bytes(), saved)

    async def test_catalog_notification_publishes_after_registration(self):
        """An observer receives the new executable catalog and duplicate registration does not notify."""
        async with self.project() as (host, source, _):
            old = await host.workflow_catalog_head()
            notification = asyncio.create_task(host.wait_workflow_catalog(old["version"], timeout=2))
            first = await self.register(host, source)
            head = await notification
            self.assertEqual(head["workflows"]["registered"], first["revision_id"])
            await self.register(host, source)
            self.assertEqual((await host.workflow_catalog_head())["version"], head["version"])

    async def test_changed_source_digest_does_not_import_or_register(self):
        """The execution boundary rejects source edits that happened after permission was evaluated."""
        from autoagent.host.source_loading import capture_sources
        async with self.project() as (host, source, _):
            approved = capture_sources(source)[1]
            write_workflow(source, 2)
            with self.assertRaisesRegex(ProjectError, "changed after authorization"):
                await host.register_workflow(source_dir=source, entrypoint="flow:build", expected_source_digest=approved)
            self.assertNotIn("registered", host._runtime.workflow_ids)

    async def test_external_manifest_edit_is_preserved(self):
        """An external TOML edit during persistence cannot be overwritten or publish a new executable."""
        async with self.project() as (host, source, _):
            first = await self.register(host, source)
            path = host.root / "auto-agent.toml"
            edited = path.read_bytes() + b"\n# external edit\n"
            write_workflow(source, 3)
            persist = host._runtime.persist_registration
            async def intervening(*args):
                await persist(*args)
                path.write_bytes(edited)
            with patch.object(host._runtime, "persist_registration", intervening):
                with self.assertRaises(HostRunConflict):
                    await self.register(host, source)
            self.assertEqual(path.read_bytes(), edited)
            self.assertEqual(host._runtime.workflow_ids["registered"], first["revision_id"])

    async def test_failed_first_publication_leaves_no_manifest(self):
        """A failed initial registration removes a newly created TOML rather than leaving a broken startup entry."""
        async with self.project(None) as (host, source, _):
            with patch.object(host._runtime, "publish_registration", AsyncMock(side_effect=RuntimeError("publish failed"))):
                with self.assertRaises(RuntimeError):
                    await self.register(host, source)
            self.assertFalse((host.root / "auto-agent.toml").exists())

    async def test_auto_discovered_workflows_remain_in_new_manifest(self):
        """Adding the first explicit registration retains existing decorator-discovered entries."""
        async with self.project(None, discovered=True) as (host, source, _):
            self.assertFalse((host.root / "auto-agent.toml").exists())
            await self.register(host, source)
            self.assertEqual(len(tomllib.loads((host.root / "auto-agent.toml").read_text())["workflows"]), 2)
            result = await host.join(await host.execute("existing", {"value": 7}))
            self.assertEqual(result.output, {"value": 7}, result.error)

    async def test_catalog_stream_notifies_and_drains_on_close(self):
        """Catalog SSE delivers current registration and releases its subscriber during Server shutdown."""
        import json
        from fastapi import APIRouter, Request, HTTPException
        from autoagent.server.catalog_watch import install_catalog_watch
        from autoagent.server.settings import ServerSettings
        async with self.project() as (host, source, _):
            router = APIRouter()
            async def query(operation, settings):
                return await operation
            watch = install_catalog_watch(router, host, ServerSettings(trace_max_subscribers=1), query)
            endpoint = next(route.endpoint for route in router.routes if route.path.endswith("/watch"))
            async def receive():
                await asyncio.Event().wait()
            request = Request({"type": "http", "method": "GET", "path": "/watch", "query_string": b"ui=workflow", "headers": []}, receive)
            response = await endpoint(request)
            iterator = response.body_iterator
            try:
                first = await anext(iterator)
                self.assertIn('"workflows":{}', first)
                with self.assertRaises(HTTPException) as capacity:
                    await endpoint(request)
                self.assertEqual(capacity.exception.status_code, 503)
                result = await self.register(host, source)
                event = await asyncio.wait_for(anext(iterator), 2)
                head = json.loads(event.split("data: ")[1])
                self.assertEqual(head["workflows"]["registered"], result["revision_id"])
                await watch.close()
                with self.assertRaises(StopAsyncIteration):
                    await anext(iterator)
                self.assertEqual(watch.count, 0)
            finally:
                await iterator.aclose()


class SQLiteRegistrationTests(RegistrationContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLRegistrationTests(RegistrationContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, root):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()


class ApplicationRegistrationContracts:
    from tests.test_builtin_agent import BuiltinApplicationContracts
    project = BuiltinApplicationContracts.project
    database_url = RegistrationContracts.database_url

    async def test_tool_approves_then_registers_into_current_host(self):
        """The Workspace tool uses the normal approval Wait and publishes into its existing Host."""
        from autoagent.agent import AgentApprovalRequest, AgentApprovalResponse, LLMResponse, LLMToolCall, LLMToolResult
        from autoagent.agent.context import restore
        from tests.test_agent_workflow import ScriptedProvider, answer
        provider = ScriptedProvider(LLMResponse(model="offline", finish_reason="tool_calls", output=(
            LLMToolCall(name="register_workflow", call_id="register", arguments='{"entrypoint":"flow:build"}'),)), answer())
        async with self.project(provider=provider) as (host, client):
            write_workflow(host.root)
            waiting = await host.join(await host.applications["agent"].start_conversation("register-session", "Register the workflow"))
            self.assertEqual(waiting.status, "waiting", waiting.error)
            self.assertNotIn("registered", host._runtime.workflow_ids)
            request = restore(AgentApprovalRequest, waiting.waits[0].request)
            reply = AgentApprovalResponse(decisions=tuple(item.respond(approved=True) for item in request.requests), revision=request.revision)
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply.model_dump(mode="json"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            observation = next(item for item in provider.requests[-1].input if isinstance(item, LLMToolResult))
            self.assertFalse(observation.is_error, str(observation))
            outcome = await host.join(await host.execute("registered", {"value": 4}))
            self.assertEqual(outcome.output, {"value": 5}, outcome.error)
            head = (await client.get("/api/v2/workflow-catalog/head?ui=workflow")).json()
            self.assertEqual(head["workflows"]["registered"], outcome.ref.workflow_revision_id)

    async def test_changed_source_cannot_use_stale_approval(self):
        """Editing source while approval is pending changes the authorization digest without registering it."""
        from autoagent.agent import AgentApprovalRequest, AgentApprovalResponse, LLMResponse, LLMToolCall
        from autoagent.agent.context import restore
        from tests.test_agent_workflow import ScriptedProvider, answer
        provider = ScriptedProvider(LLMResponse(model="offline", finish_reason="tool_calls", output=(
            LLMToolCall(name="register_workflow", call_id="register", arguments='{"entrypoint":"flow:build"}'),)), answer())
        async with self.project(provider=provider) as (host, _):
            write_workflow(host.root)
            waiting = await host.join(await host.applications["agent"].start_conversation("changed-session", "Register"))
            self.assertEqual(waiting.status, "waiting", waiting.error)
            request = restore(AgentApprovalRequest, waiting.waits[0].request)
            reply = AgentApprovalResponse(decisions=tuple(item.respond(approved=True) for item in request.requests), revision=request.revision)
            write_workflow(host.root, 7)
            with self.assertRaises(HostRunConflict):
                await host.respond(waiting.invocation_id, waiting.waits[0].id, reply.model_dump(mode="json"))
            self.assertNotIn("registered", host._runtime.workflow_ids)

    async def test_unbound_session_has_no_registration_tool(self):
        """Sessions without a Workspace do not receive the product registration capability."""
        from tests.test_agent_workflow import ScriptedProvider, answer
        provider = ScriptedProvider(answer())
        async with self.project(provider=provider) as (host, _):
            result = await host.join(await host.applications["agent"].start_conversation("unbound-session", "Hello", unbound=True))
            self.assertEqual(result.status, "completed", result.error)
            self.assertNotIn("register_workflow", {item.name for item in provider.requests[0].tools})


class SQLiteApplicationRegistrationTests(ApplicationRegistrationContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLApplicationRegistrationTests(ApplicationRegistrationContracts, unittest.IsolatedAsyncioTestCase):
    database_url = PostgreSQLRegistrationTests.database_url


class SourceProjectTests(unittest.TestCase):
    def test_new_relative_module_requires_registration(self):
        """A new package file is not visible to an already captured source generation."""
        from autoagent.host.source_loading import resolve_source_entrypoint
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "package"
            package.mkdir()
            (package / "__init__.py").write_text("")
            (package / "factory.py").write_text(
                "from autoagent import Workflow, Node\nfrom typing_extensions import TypedDict\n"
                "class Value(TypedDict):\n    value: int\n"
                "def step(value: Value) -> Value:\n    from . import late\n    return {'value': late.NUMBER}\n"
                "def build(): return Workflow('package', nodes=[Node('start', step)])\n")
            first = resolve_source_entrypoint("package.factory:build", root)()
            (package / "late.py").write_text("NUMBER=3\n")
            with self.assertRaises(ImportError):
                first.nodes[0].executable({"value": 0})
            second = resolve_source_entrypoint("package.factory:build", root)()
            self.assertEqual(second.nodes[0].executable({"value": 0}), {"value": 3})

    def test_relative_source_directory_and_package_imports(self):
        """An explicit manifest resolves source_dir beside itself and reloads package-local imports in isolation."""
        from autoagent.host.project import Project
        from autoagent.host.source_loading import resolve_source_entrypoint
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "workspace"
            write_workflow(source)
            package = source / "package"
            package.mkdir()
            (package / "__init__.py").write_text("from .factory import build\n")
            (package / "factory.py").write_text("from flow import build\n")
            manifest = root / "custom.toml"
            manifest.write_text('schema_version=1\n[[workflows]]\nentrypoint="package:build"\nsource_dir="workspace"\n')
            project = Project.find(manifest)
            first = project.load({})[0][0]
            self.assertEqual(project.source_dirs, (source.resolve(),))
            self.assertEqual(project.manifest_path, manifest)
            write_workflow(source, 2)
            second = resolve_source_entrypoint("package:build", source)()
            self.assertNotEqual(first.version, second.version)
            self.assertEqual([node.id for node in first.nodes], [node.id for node in second.nodes])

    def test_invalid_source_directory_is_rejected(self):
        """An empty or non-string TOML source directory is rejected before imports or database initialization."""
        from autoagent.host.project import Project
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "auto-agent.toml"
            for source in ('""', '12', 'false'):
                manifest.write_text('schema_version=1\n[[workflows]]\nentrypoint="flow:build"\nsource_dir=' + source + '\n')
                with self.assertRaises(ProjectError):
                    Project.find(manifest)
