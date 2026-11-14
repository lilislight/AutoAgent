"""Workspace discovery, immutable Session bindings, and Host restart behavior."""

from contextlib import asynccontextmanager
import asyncio
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from autoagent.agent import (
    AgentInput, LLMResponse, LLMToolCall, LocalWorkspace,
    SkillSet, ToolSet, WorkspaceBinding, WorkspaceError, WorkspaceRegistry,
    WorkspaceDiscovery, WorkspaceInfo,
)
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.host.persistence import ConflictError
from autoagent.agent_app import AgentApplication, builtin_agent_workflow
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, settings


def make_workspace(root, name="report", text="Use report conventions."):
    skill = root / ".agents" / "skills" / name
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(f"---\nname: {name}\ndescription: {name} instructions\n---\n{text}\n")
    (skill / "rules.txt").write_text("first\nsecond\nthird\n")
    return skill


class LocalWorkspaceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_root_to_working_directory_discovery_is_bounded(self):
        """Only ancestors inside the Workspace contribute instructions and Skill roots."""
        root = self.root / "workspace"
        nested = root / "packages" / "api"
        nested.mkdir(parents=True)
        (self.root / "AGENTS.md").write_text("Outside instructions")
        (root / "AGENTS.md").write_text("Root conventions")
        (nested / "AGENTS.md").write_text("API conventions")
        make_workspace(root)
        make_workspace(nested, "api")
        discovered = LocalWorkspace("project", root).discover("packages/api")
        self.assertEqual([item.path for item in discovered.instructions], ["AGENTS.md", "packages/api/AGENTS.md"])
        self.assertEqual([item.metadata.name for item in discovered.skills], ["report"])
        self.assertNotIn("Outside", str(discovered))

    def test_paths_symlinks_and_missing_directories_fail_closed(self):
        """Relative traversal and discovery symlinks cannot access a neighboring directory."""
        root = self.root / "workspace"
        root.mkdir()
        outside = self.root / "outside"
        outside.mkdir()
        (outside / "secret").write_text("secret")
        workspace = LocalWorkspace("project", root)
        for path in ("../outside", "/tmp", "a/../../b", "C:/data", "a\\b"):
            with self.assertRaises(ValueError):
                WorkspaceBinding(workspace_id="project", working_directory=path)
        (root / "AGENTS.md").symlink_to(outside / "secret")
        with self.assertRaises(WorkspaceError):
            workspace.discover()
        (root / "AGENTS.md").unlink()
        (root / "escape").symlink_to(outside, target_is_directory=True)
        with self.assertRaises(WorkspaceError):
            workspace.discover("escape")
        shutil.rmtree(root)
        with self.assertRaises(WorkspaceError):
            workspace.discover()
        self.assertFalse(root.exists())

    def test_skill_read_resource_pages_text_without_duplicating_body(self):
        """The three default tools read bounded text with continuation and no execution tool."""
        make_workspace(self.root)
        skills = SkillSet.discover([self.root / ".agents" / "skills"])
        tools = ToolSet(skills.as_tools())
        self.assertEqual([item.name for item in tools.definitions], ["skill_load", "skill_list_resources", "skill_read_resource"])
        read = tools.invoke(LLMToolCall(name="skill_read_resource", call_id="read", arguments=json.dumps(
            dict(name="report", path="rules.txt", max_lines=1))))
        self.assertFalse(read.is_error, read)
        self.assertEqual(read.content[0].text, "first\n")
        self.assertEqual(read.structured_content["next_line"], 2)
        self.assertNotIn("text", read.structured_content)

    def test_project_workspace_is_automatic_without_environment_configuration(self):
        """The local application registers its project root independently of SDK settings."""
        unbound = AutoAgentHost(root=self.root, environment={}, applications=(AgentApplication,))
        selected = AutoAgentHost(root=self.root / "other-project", environment={}, applications=(AgentApplication,))
        self.assertFalse(hasattr(unbound, "agent_context"))
        first = unbound.applications["agent"].context
        second = selected.applications["agent"].context
        self.assertEqual(first.workspaces.entries()[0].root, self.root)
        self.assertEqual(second.workspaces.entries()[0].root, self.root / "other-project")
        self.assertNotEqual(first.project_workspace_id, second.project_workspace_id)



class WorkspaceResolverTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolution_contract_does_not_require_host_filesystem_access(self):
        """A resolver can supply a Workspace through its discovery interface without a local root."""
        class RemoteWorkspace:
            info = WorkspaceInfo(id="remote", name="Remote project", backend="remote")
            async def avalidate(self, working_directory="."):
                if working_directory != ".":
                    raise WorkspaceError("Unknown directory")
            async def adiscover(self, working_directory="."):
                await self.avalidate(working_directory)
                return WorkspaceDiscovery(self.info, working_directory)
        class Resolver:
            async def aresolve(self, workspace_id):
                return RemoteWorkspace()
        registry = WorkspaceRegistry(resolver=Resolver())
        workspace = await registry.aresolve("remote")
        self.assertEqual((await workspace.adiscover()).workspace.backend, "remote")
        with self.assertRaises(WorkspaceError):
            await registry.aresolve("different")


class WorkspaceContracts:
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.first, self.second = self.root / "first", self.root / "second"
        self.first.mkdir()
        self.second.mkdir()
        self.url = await self.database_url()

    async def database_url(self):
        return "sqlite+aiosqlite:///" + str(self.root / "runtime.db")

    @asynccontextmanager
    async def host(self, provider, *, workspaces=(), defaults=None, environment=None, workflows=None, root=None):
        @asynccontextmanager
        async def lifespan(setup):
            service = setup.applications["agent"].context
            service.services.providers.register("offline", provider)
            service.set_defaults(defaults or settings())
            for workspace in workspaces:
                service.workspaces.register(workspace)
            yield
        async with AutoAgentHost(workflows=workflows or [builtin_agent_workflow(execution=False)], lifespan=lifespan,
            root=root or self.root, environment=environment or {}, settings=HostSettings(database_url=self.url)) as host:
            yield host

    async def run_agent(self, host, sid):
        ref = await host.execute("autoagent.agent", AgentInput(message="Help with this workspace"), session_id=sid)
        return await host.join(ref)

    async def test_workspace_context_and_skills_are_isolated_per_session(self):
        """The same Workflow receives only the bound directory's instructions and Skills."""
        (self.first / "AGENTS.md").write_text("FIRST_PROJECT_INSTRUCTIONS")
        (self.second / "AGENTS.md").write_text("SECOND_PROJECT_INSTRUCTIONS")
        make_workspace(self.first, "first-skill")
        make_workspace(self.second, "second-skill")
        provider = ScriptedProvider(answer(), answer(), answer())
        async with self.host(provider, workspaces=[LocalWorkspace("first", self.first), LocalWorkspace("second", self.second)]) as host:
            for sid, wid in (("one", "first"), ("two", "second")):
                await host.applications["agent"].context.configure(sid, settings().session_defaults(), workspace=WorkspaceBinding(workspace_id=wid))
                result = await self.run_agent(host, sid)
                self.assertEqual(result.status, "completed", result.error)
            self.assertEqual((await self.run_agent(host, "unbound")).status, "completed")
            first, second, unbound = [request.model_dump_json() for request in provider.requests]
            self.assertIn("FIRST_PROJECT_INSTRUCTIONS", first)
            self.assertNotIn("SECOND_PROJECT_INSTRUCTIONS", first)
            self.assertIn("second-skill", second)
            self.assertNotIn("first-skill", second)
            self.assertNotIn("skill_load", unbound)
            self.assertNotIn("PROJECT_INSTRUCTIONS", unbound)

    async def test_configuration_rejects_binding_changes_but_accepts_model_changes(self):
        """A configured Session cannot change its Workspace or detach from it."""
        async with self.host(ScriptedProvider(answer()), workspaces=[LocalWorkspace("first", self.first), LocalWorkspace("second", self.second)]) as host:
            original = settings().model_copy(update={"workspace": WorkspaceBinding(workspace_id="first")})
            record = await host.applications["agent"].context.configure("session", original.session_defaults(), expected_version=0, workspace=(original).workspace)
            updated = original.model_copy(update={"model": original.model.model_copy(update={"model": "model-b"})})
            record = await host.applications["agent"].context.configure("session", updated.session_defaults(), expected_version=record.version, workspace=(updated).workspace)
            for workspace in (None, WorkspaceBinding(workspace_id="second")):
                with self.assertRaises(ConflictError):
                    await host.applications["agent"].context.configure("session", updated.session_defaults(), expected_version=record.version, workspace=workspace)
            self.assertEqual((await host.applications["agent"].context.get_settings("session")).workspace, original.workspace)

    async def test_accepted_default_binding_survives_default_changes_and_restart(self):
        """A Session started without explicit configuration retains its original Workspace."""
        defaults = settings().model_copy(update={"workspace": WorkspaceBinding(workspace_id="first")})
        async with self.host(ScriptedProvider(answer()), workspaces=[LocalWorkspace("first", self.first)], defaults=defaults) as host:
            original = await self.run_agent(host, "session")
            self.assertEqual(original.status, "completed")
            host.applications["agent"].context.set_defaults(settings())
            with self.assertRaises(ConflictError):
                await host.applications["agent"].context.configure("session", settings().session_defaults(), workspace=None)
        provider = ScriptedProvider(answer())
        async with self.host(provider, workspaces=[LocalWorkspace("first", self.first)]) as host:
            await host.applications["agent"].context.get_snapshot(original.invocation_id)
            self.assertEqual((await host.applications["agent"].context.get_settings("session")).workspace, defaults.workspace)
            result = await self.run_agent(host, "session")
            self.assertEqual(result.status, "completed", result.error)
            snapshot = await host.applications["agent"].context.get_snapshot(result.invocation_id)
            self.assertEqual(snapshot.settings.workspace, defaults.workspace)

    async def test_missing_directory_blocks_execution_and_same_workspace_can_relocate(self):
        """Unavailable files do not silently fall back to Host cwd; stable IDs can be remapped."""
        defaults = settings().model_copy(update={"workspace": WorkspaceBinding(workspace_id="first")})
        async with self.host(ScriptedProvider(), workspaces=[LocalWorkspace("first", self.first)], defaults=defaults) as host:
            shutil.rmtree(self.first)
            result = await self.run_agent(host, "session")
            self.assertEqual(result.status, "failed", result.error)
            self.assertIn("Workspace", str(result.error))
        (self.second / "AGENTS.md").write_text("Restored workspace")
        provider = ScriptedProvider(answer())
        async with self.host(provider, defaults=defaults) as host:
            await host.applications["agent"].context.register_workspace(LocalWorkspace("first", self.second), replace=True)
            result = await self.run_agent(host, "session")
            self.assertEqual(result.status, "completed", result.error)
            self.assertIn("Restored workspace", provider.requests[0].model_dump_json())

    async def test_skill_versions_are_rechecked_after_model_calls(self):
        """Removing a Skill after advertisement prevents the stale tool call from reading it."""
        skill = make_workspace(self.first)
        def remove(request):
            shutil.rmtree(skill)
            return LLMResponse(model="offline", finish_reason="tool_calls", output=(LLMToolCall(
                name="skill_load", call_id="load", arguments='{"name":"report"}'),))
        provider = ScriptedProvider(remove, answer())
        defaults = settings().model_copy(update={"workspace": WorkspaceBinding(workspace_id="first")})
        async with self.host(provider, workspaces=[LocalWorkspace("first", self.first)], defaults=defaults) as host:
            result = await self.run_agent(host, "session")
            self.assertEqual(result.status, "completed", result.error)
            self.assertIn("tool_unavailable", provider.requests[1].model_dump_json())

    async def test_unbound_sessions_and_concurrent_configuration_cannot_rebind(self):
        """Accepted unbound Sessions stay unbound and competing first bindings are serialized."""
        async with self.host(ScriptedProvider(answer()), workspaces=[LocalWorkspace("first", self.first), LocalWorkspace("second", self.second)]) as host:
            self.assertEqual((await self.run_agent(host, "unbound")).status, "completed")
            candidates = [settings().model_copy(update={"workspace": WorkspaceBinding(workspace_id=wid)}) for wid in ("first", "second")]
            with self.assertRaises(ConflictError):
                await host.applications["agent"].context.configure("unbound", candidates[0].session_defaults(), workspace=(candidates[0]).workspace)
            results = await asyncio.gather(*(host.applications["agent"].context.configure("new", item.session_defaults(), expected_version=0, workspace=(item).workspace)
                                            for item in candidates), return_exceptions=True)
            self.assertEqual(sum(isinstance(item, ConflictError) for item in results), 1)
            self.assertEqual((await host.applications["agent"].context.get_settings("new")).version, 1)

    async def test_different_project_roots_share_database_without_rebinding_sessions(self):
        """Each startup directory retains its own default identity, path, and conversations."""
        from autoagent.agent_app import builtin_agent_workflow
        async with self.host(ScriptedProvider(answer()), root=self.first, workflows=[builtin_agent_workflow()]) as host:
            first_id = (await host.applications["agent"].context.workspace_catalog())["project_workspace_id"]
            ref = await host.applications["agent"].context.start_conversation("first-project-chat", "First project")
            self.assertEqual((await host.join(ref)).status, "completed")
        async with self.host(ScriptedProvider(answer()), root=self.second, workflows=[builtin_agent_workflow()]) as host:
            second_id = (await host.applications["agent"].context.workspace_catalog())["project_workspace_id"]
            self.assertNotEqual(first_id, second_id)
            ref = await host.applications["agent"].context.start_conversation("second-project-chat", "Second project")
            self.assertEqual((await host.join(ref)).status, "completed")
            items = {row["id"]: row for row in await host.applications["agent"].context.list_workspaces()}
            self.assertEqual(items[first_id]["path"], str(self.first))
            self.assertEqual(items[second_id]["path"], str(self.second))
            self.assertFalse(items[first_id]["is_project"])
            self.assertTrue(items[second_id]["is_project"])
            self.assertEqual((await host.applications["agent"].context.get_settings("first-project-chat")).workspace.workspace_id, first_id)
            for wid, sid in ((first_id, "first-project-chat"), (second_id, "second-project-chat")):
                rows = (await host.applications["agent"].context.list_conversation_sessions(wid)).items
                self.assertEqual([row["session_id"] for row in rows], [sid])
        async with self.host(ScriptedProvider(), root=self.first, workflows=[builtin_agent_workflow()]) as host:
            self.assertEqual((await host.applications["agent"].context.workspace_catalog())["project_workspace_id"], first_id)
            self.assertEqual(len(await host.applications["agent"].context.list_workspaces()), 2)

    async def test_default_workspace_reuses_persisted_directory_identity(self):
        """Opening an already registered directory preserves its ID and existing Session bindings."""
        from autoagent.agent_app import builtin_agent_workflow
        async with self.host(ScriptedProvider(), workflows=[builtin_agent_workflow()], workspaces=[LocalWorkspace("saved-directory", self.first)]) as host:
            await host.applications["agent"].context.configure("saved-chat", settings().session_defaults(), workspace=WorkspaceBinding(workspace_id="saved-directory"))
        async with self.host(ScriptedProvider(), workflows=[builtin_agent_workflow()], root=self.first) as host:
            self.assertEqual(host.applications["agent"].context.project_workspace_id, "saved-directory")
            items = await host.applications["agent"].context.list_workspaces()
            self.assertEqual(sum(row["path"] == str(self.first) for row in items), 1)
            self.assertEqual((await host.applications["agent"].context.get_settings("saved-chat")).workspace.workspace_id, "saved-directory")

    async def test_server_settings_and_resources_use_the_session_workspace(self):
        """The Agent API exposes Workspace identities and selects Skills by Session binding."""
        import httpx
        from autoagent.agent_app import builtin_agent_workflow
        from autoagent.server import create_app
        make_workspace(self.first)
        async with self.host(ScriptedProvider(), workspaces=[LocalWorkspace("first", self.first)],
                             workflows=[builtin_agent_workflow()]) as host:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host, ui=False)),
                                         base_url="http://test") as client:
                listed = await client.get("/api/v2/agent/workspaces")
                self.assertEqual(listed.status_code, 200, listed.text)
                self.assertEqual([item["id"] for item in listed.json()["items"]], [host.applications["agent"].context.project_workspace_id, "first"])
                self.assertEqual(listed.json()["items"][0]["path"], str(self.root))
                self.assertTrue(listed.json()["items"][0]["is_project"])
                payload = settings().session_defaults().model_dump(mode="json")
                await host.applications["agent"].context.configure("chat", settings().session_defaults(), workspace=WorkspaceBinding(workspace_id="first"))
                configured = await client.put("/api/v2/agent/settings", json={"session_id": "chat", "expected_version": 1, "settings": payload})
                self.assertEqual(configured.status_code, 200, configured.text)
                catalog = (await client.get("/api/v2/agent/catalog")).json()
                params = {"revision_id": catalog["agent"]["revision_id"], "session_id": "chat"}
                resources = await client.get("/api/v2/agent/resources", params=params)
                self.assertEqual(resources.status_code, 200, resources.text)
                self.assertEqual(resources.json()["skills"], [{"name": "report", "description": "report instructions", "origin": "Workspace"}])
                params["session_id"] = "other"
                self.assertEqual((await client.get("/api/v2/agent/resources", params=params)).json()["skills"], [])
                payload["workspace"] = None
                rejected = await client.put("/api/v2/agent/settings", json={"session_id": "chat", "expected_version": 1, "settings": payload})
                self.assertEqual(rejected.status_code, 422, rejected.text)

    async def test_subagents_inherit_workspace_without_global_skill_registration(self):
        """A child Session inherits the parent's Workspace and discovers its own scoped catalog."""
        from autoagent.agent import SubagentsConfig, SubagentTarget
        make_workspace(self.first)
        provider = ScriptedProvider(LLMResponse(model="offline", finish_reason="tool_calls", output=(
            LLMToolCall(name="run_agent", call_id="child", arguments='{"type":"agent","task":"child task"}'),)),
            answer("Child complete"), answer("Parent complete"))
        config = SubagentsConfig(enabled=True, targets={"worker": SubagentTarget(workflow_id="worker", description="Helper")})
        workflows = [builtin_agent_workflow(execution=False)]
        defaults = settings().model_copy(update={"workspace": WorkspaceBinding(workspace_id="first")})
        async with self.host(provider, workspaces=[LocalWorkspace("first", self.first)], defaults=defaults, workflows=workflows) as host:
            result = await self.run_agent(host, "parent")
            async with asyncio.timeout(20):
                while result.status == "waiting":
                    await asyncio.sleep(0.01)
                    result = await host.join(result.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(len(provider.requests), 3)
            self.assertTrue(all("report instructions" in request.model_dump_json() for request in provider.requests))
            children = await host.list_child_executions(result.invocation_id)
            self.assertEqual(len(children.items), 1)
            snapshot = await host.applications["agent"].context.get_snapshot(children.items[0]["invocation_id"])
            self.assertNotEqual(snapshot.session_id, "parent")
            self.assertEqual(snapshot.settings.workspace, defaults.workspace)

    @asynccontextmanager
    async def workspace_client(self, provider):
        import httpx
        from autoagent.agent_app import builtin_agent_workflow
        from autoagent.server import create_app
        async with self.host(provider, workflows=[builtin_agent_workflow()]) as host:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host, ui=False)), base_url="http://test") as client:
                yield host, client

    async def test_ui_workspace_creation_registers_existing_and_new_directories_durably(self):
        """The same request reuses existing directories or creates missing parents and persists absolute paths."""
        marker = self.first / "keep.txt"
        marker.write_text("preserve")
        async with self.workspace_client(ScriptedProvider()) as (host, client):
            existing = await client.post("/api/v2/agent/workspaces", json={"name": "Existing", "path": str(self.first)})
            self.assertEqual(existing.status_code, 201, existing.text)
            created = await client.post("/api/v2/agent/workspaces", json={"name": "New", "path": "new-directory/nested"})
            self.assertEqual(created.status_code, 201, created.text)
            self.assertTrue((self.root / "new-directory/nested").is_dir())
            self.assertEqual(created.json()["path"], str(self.root / "new-directory/nested"))
            duplicate = await client.post("/api/v2/agent/workspaces", json={"name": "Duplicate", "path": str(self.first)})
            self.assertEqual(duplicate.status_code, 400)
            conflict = self.root / "not-a-directory"
            conflict.write_text("preserve file")
            rejected = await client.post("/api/v2/agent/workspaces", json={"name": "Invalid", "path": str(conflict)})
            self.assertEqual(rejected.status_code, 400, rejected.text)
            self.assertEqual(conflict.read_text(), "preserve file")
            self.assertEqual(marker.read_text(), "preserve")
        async with self.workspace_client(ScriptedProvider()) as (host, client):
            items = (await client.get("/api/v2/agent/workspaces")).json()["items"]
            self.assertEqual({item["id"] for item in items}, {host.applications["agent"].context.project_workspace_id, existing.json()["id"], created.json()["id"]})
            self.assertEqual(next(item["path"] for item in items if item["id"] == created.json()["id"]), created.json()["path"])

    async def test_new_chat_pins_project_workspace_before_execution(self):
        """A product conversation explicitly saves the current project binding before starting Core."""
        (self.root / "AGENTS.md").write_text("PROJECT_WORKSPACE_GUIDANCE")
        provider = ScriptedProvider(answer())
        async with self.workspace_client(provider) as (host, client):
            invalid = await client.post("/api/v2/agent/messages", json={"session_id": "invalid", "message": "   "})
            self.assertEqual(invalid.status_code, 422, invalid.text)
            self.assertEqual((await host.applications["agent"].context.get_settings("invalid")).version, 0)
            preview = (await client.get("/api/v2/agent/settings", params={"session_id": "chat"})).json()
            self.assertEqual(preview["version"], 0)
            self.assertEqual(preview["workspace"]["workspace_id"], host.applications["agent"].context.project_workspace_id)
            receipt = await client.post("/api/v2/agent/messages", json={"session_id": "chat", "message": "Help"})
            self.assertEqual(receipt.status_code, 202, receipt.text)
            result = await host.join(receipt.json()["invocation_id"])
            self.assertEqual(result.status, "completed", result.error)
            saved = await host.applications["agent"].context.get_settings("chat")
            self.assertEqual(saved.version, 1)
            self.assertEqual(saved.workspace, WorkspaceBinding(workspace_id=host.applications["agent"].context.project_workspace_id))
            self.assertIn("PROJECT_WORKSPACE_GUIDANCE", provider.requests[0].model_dump_json())
            snapshot = await host.applications["agent"].context.get_snapshot(result.invocation_id)
            self.assertEqual(snapshot.settings_version, 1)

    async def test_workspace_filter_precedes_pagination_and_cursors_are_scoped(self):
        """Workspace pages exclude newer unrelated chats before limiting and reject foreign cursors."""
        async with self.workspace_client(ScriptedProvider(answer(), answer(), answer())) as (host, client):
            item = (await client.post("/api/v2/agent/workspaces", json={"name": "Other", "path": str(self.first)})).json()
            for sid, wid in (("project-old", host.applications["agent"].context.project_workspace_id), ("project-new", host.applications["agent"].context.project_workspace_id), ("other", item["id"])):
                receipt = await client.post("/api/v2/agent/messages", json={"session_id": sid, "message": sid, "workspace_id": wid})
                self.assertEqual(receipt.status_code, 202, receipt.text)
                self.assertEqual((await host.join(receipt.json()["invocation_id"])).status, "completed")
            first = await client.get("/api/v2/agent/sessions", params={"workspace_id": host.applications["agent"].context.project_workspace_id, "limit": 1})
            self.assertEqual(first.status_code, 200, first.text)
            self.assertEqual([row["session_id"] for row in first.json()["items"]], ["project-new"])
            cursor = first.json()["next_cursor"]
            second = await client.get("/api/v2/agent/sessions", params={"workspace_id": host.applications["agent"].context.project_workspace_id, "limit": 1, "cursor": cursor})
            self.assertEqual([row["session_id"] for row in second.json()["items"]], ["project-old"])
            mismatch = await client.get("/api/v2/agent/sessions", params={"workspace_id": item["id"], "cursor": cursor})
            self.assertEqual(mismatch.status_code, 400)
            rejected = await client.post("/api/v2/agent/messages", json={"session_id": "project-new", "message": "Wrong directory", "workspace_id": item["id"]})
            self.assertEqual(rejected.status_code, 409)

    async def test_failed_workspace_registration_removes_only_its_new_empty_directory(self):
        """Failed registration removes its newly created parent chain while preserving existing directories."""
        from unittest.mock import AsyncMock, patch
        async with self.workspace_client(ScriptedProvider()) as (host, client):
            with patch.object(host.applications["agent"].context._workspaces, "_publish", new=AsyncMock(side_effect=RuntimeError("write failed"))):
                with self.assertRaisesRegex(RuntimeError, "write failed"):
                    await host.applications["agent"].context.create_workspace(name="Failed", path="rollback/nested")
                marker = self.first / "keep.txt"
                marker.write_text("preserve")
                with self.assertRaisesRegex(RuntimeError, "write failed"):
                    await host.applications["agent"].context.create_workspace(name="Existing", path=str(self.first))
                self.assertEqual(marker.read_text(), "preserve")
            self.assertFalse((self.root / "rollback").exists())

    async def test_workspace_filter_includes_direct_product_executions_without_user_settings(self):
        """Direct executions of the product persist their explicit Workspace binding without a UI submission."""
        from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID
        async with self.workspace_client(ScriptedProvider(answer())) as (host, client):
            host.applications["agent"].context.set_defaults(settings().model_copy(update={"workspace": WorkspaceBinding(workspace_id=host.applications["agent"].context.project_workspace_id)}))
            result = await host.join(await host.execute(BUILTIN_AGENT_WORKFLOW_ID, AgentInput(message="SDK chat"), session_id="sdk"))
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual((await host.applications["agent"].context.get_settings("sdk")).version, 1)
            selected = await client.get("/api/v2/agent/sessions", params={"workspace_id": host.applications["agent"].context.project_workspace_id})
            self.assertEqual(selected.status_code, 200, selected.text)
            self.assertEqual([item["session_id"] for item in selected.json()["items"]], ["sdk"])
            unbound = await client.get("/api/v2/agent/sessions", params={"unbound": "true"})
            self.assertEqual(unbound.status_code, 200, unbound.text)
            self.assertEqual(unbound.json()["items"], [])


class SQLiteWorkspaceTests(WorkspaceContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL test database is not configured")
class PostgreSQLWorkspaceTests(WorkspaceContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        url = await database.create()
        self.addAsyncCleanup(database.close)
        return url
