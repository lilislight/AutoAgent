"""Atomic source publication, MCP resync and live immutable Skill directories."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from autoagent.agent import (
    AgentResourceRegistry, MCPResourceSource, MCPToolRegistry, SkillDirectorySource, Tool,
)
from tests import test_agent_mcp as mcp_tests
from tests.test_agent_mcp import WireServer, httpx2
from tests.test_agent_mcp_recovery import policy


def tool(name):
    def action(text: str) -> str:
        return text
    return Tool(action, name=name)


async def eventually(predicate):
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.01)


class SourcePublicationTests(unittest.TestCase):
    def test_source_collision_rolls_back_and_unchanged_bindings_keep_versions(self):
        """A failed complete catalog cannot partially replace or delete earlier bindings."""
        resources = AgentResourceRegistry()
        alpha = tool("alpha")
        resources.replace_source("one", {("tool", "alpha"): ("a", alpha)})
        original = resources.get("tool", "alpha")
        resources.register_tool(tool("other"))
        with self.assertRaises(ValueError):
            resources.replace_source("one", {("tool", "other"): ("b", tool("other"))})
        self.assertIs(resources.get("tool", "alpha"), original)
        resources.replace_source("one", {("tool", "alpha"): ("a", alpha)})
        self.assertEqual(resources.get("tool", "alpha").version, original.version)
        resources.set_enabled("tool", "alpha", False)
        resources.replace_source("one", {("tool", "alpha"): ("changed", alpha)})
        self.assertFalse(resources.has("tool", "alpha"))
        with self.assertRaises(ValueError):
            resources.register_tool(alpha, replace=True, version="replacement")

    def test_reintroduced_content_never_revives_an_advertised_version(self):
        """Changed content, reverted content and deletion all invalidate old bindings."""
        resources = AgentResourceRegistry()
        versions = []
        for fingerprint in ("a", "b", "a"):
            resources.replace_source("one", {("tool", "alpha"): (fingerprint, tool("alpha"))})
            versions.append(resources.get("tool", "alpha").version)
        self.assertEqual(len(set(versions)), 3)
        resources.remove_source("one")
        self.assertFalse(resources.has("tool", "alpha"))
        resources.replace_source("one", {("tool", "alpha"): ("a", tool("alpha"))})
        self.assertNotIn(resources.get("tool", "alpha").version, versions)


class SkillSourceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.resources = AgentResourceRegistry()

    def skill(self, name="sample", body="Use this skill."):
        directory = self.root / name
        directory.mkdir(exist_ok=True)
        (directory / "SKILL.md").write_text(f"---\nname: {name}\ndescription: Sample skill\n---\n{body}\n")
        return directory

    async def test_directory_changes_publish_frozen_documents_assets_and_deletions(self):
        """Background scans detect nested resource bytes and never mutate previously borrowed Skills."""
        directory = self.skill()
        (directory / "assets").mkdir()
        asset = directory / "assets" / "notes.txt"
        asset.write_text("old")
        async with SkillDirectorySource(self.resources, [self.root], interval_seconds=0.02) as source:
            old = self.resources.get("skill", "sample")
            asset.write_text("new")
            await eventually(lambda: self.resources.get("skill", "sample").version != old.version)
            self.assertEqual(old.value.read_text("assets/notes.txt").text, "old")
            self.assertEqual(self.resources.get("skill", "sample").value.read_text("assets/notes.txt").text, "new")
            self.skill("another")
            await eventually(lambda: self.resources.has("skill", "another"))
            shutil.rmtree(directory)
            await eventually(lambda: not self.resources.has("skill", "sample"))
            self.assertIsNone(source.last_error)
        self.assertFalse(self.resources.entries("skill"))
        self.assertTrue(source._task.done())

    async def test_invalid_scan_withdraws_source_and_recovers_without_reusing_versions(self):
        """An incomplete or invalid Skill is unavailable until a complete valid rescan."""
        directory = self.skill()
        async with SkillDirectorySource(self.resources, [self.root], interval_seconds=0.02) as source:
            old = self.resources.get("skill", "sample")
            (directory / "SKILL.md").write_text("broken")
            await eventually(lambda: source.last_error is not None)
            self.assertFalse(self.resources.has("skill", "sample"))
            self.skill()
            await eventually(lambda: self.resources.has("skill", "sample"))
            self.assertNotEqual(self.resources.get("skill", "sample").version, old.version)
            self.assertIsNone(source.last_error)

    async def test_duplicate_active_source_is_rejected_without_removing_owner(self):
        """Two watchers cannot own or clean up the same source name."""
        self.skill()
        async with SkillDirectorySource(self.resources, [self.root]):
            version = self.resources.get("skill", "sample").version
            with self.assertRaises(ValueError):
                async with SkillDirectorySource(self.resources, [self.root]):
                    self.fail("Duplicate source entered")
            self.assertEqual(self.resources.get("skill", "sample").version, version)


@unittest.skipIf(httpx2 is None, "MCP SDK not installed")
class MCPSourceTests(unittest.IsolatedAsyncioTestCase):
    client = mcp_tests.MCPHTTPTests.client

    async def test_stopped_listener_withdraws_tools_and_explicit_refresh_restarts_it(self):
        """A stopped subscription cannot leave advertised Tools silently unsynchronized."""
        from autoagent.agent import MCPSubscriptionUpdate
        client, _ = self.client(WireServer())
        resources, stopped, forever = AgentResourceRegistry(), asyncio.Event(), asyncio.Event()
        attempts = 0
        async def watch(**options):
            nonlocal attempts
            attempts += 1
            yield MCPSubscriptionUpdate(type="connected", generation=client.generation,
                resync_required=True, honored={"toolsListChanged": True})
            await (stopped if attempts == 1 else forever).wait()
        async with client:
            registry = MCPToolRegistry(client)
            with patch.object(registry, "watch", new=watch):
                async with MCPResourceSource(resources, registry) as source:
                    original = resources.get("tool", "add").version
                    stopped.set()
                    await eventually(lambda: source.last_error is not None)
                    self.assertFalse(resources.has("tool", "add"))
                    await source.refresh()
                    self.assertEqual(attempts, 2)
                    self.assertIsNone(source.last_error)
                    self.assertNotEqual(resources.get("tool", "add").version, original)
                with self.assertRaises(RuntimeError):
                    await source.refresh()
                self.assertFalse(resources.has("tool", "add"))

    async def test_legacy_sse_notifications_preserve_application_callback(self):
        """Legacy list-change notifications refresh off the SDK handler without replacing callbacks."""
        from autoagent.agent import MCPCallbacks
        seen = []
        async def observe(message):
            seen.append(message.method)
        server = WireServer(legacy=True)
        client, _ = self.client(server, sse=True, callbacks=MCPCallbacks(message_handler=observe))
        resources = AgentResourceRegistry()
        async with client:
            registry = MCPToolRegistry(client)
            async with MCPResourceSource(resources, registry):
                previous = resources.get("tool", "add")
                server.tool = {**server.tool, "description": "Legacy update"}
                server.queue.put_nowait({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})
                await eventually(lambda: resources.get("tool", "add").version != previous.version)
                self.assertEqual(resources.get("tool", "add").value.definition.description, "Legacy update")
                self.assertIn("notifications/tools/list_changed", seen)
            self.assertFalse(client._message_observers)
            self.assertFalse(resources.entries("tool"))

    async def test_notifications_reconnect_and_removal_sync_to_agent_registry(self):
        """A real SDK registry republishes changes and connection generations before reconnect returns."""
        from mcp_types import SubscriptionFilter
        from mcp.shared.subscriptions import ToolsListChanged
        queue = asyncio.Queue()
        class Changes:
            honored = SubscriptionFilter(toolsListChanged=True)
            def __aiter__(self):
                return self
            async def __anext__(self):
                return await queue.get()
        @asynccontextmanager
        async def changes(sdk, **options):
            yield Changes()
        server = WireServer(list_changed=True)
        client, http = self.client(server, reconnect_policy=policy())
        resources = AgentResourceRegistry()
        async with client:
            registry = MCPToolRegistry(client)
            with patch("mcp.Client.listen", new=changes):
                async with MCPResourceSource(resources, registry) as source:
                    first = resources.get("tool", "add")
                    server.tool = {**server.tool, "description": "Changed"}
                    await queue.put(ToolsListChanged())
                    await eventually(lambda: resources.get("tool", "add").version != first.version)
                    current = resources.get("tool", "add")
                    self.assertEqual(current.value.definition.description, "Changed")
                    await client.areconnect()
                    self.assertNotEqual(resources.get("tool", "add").version, current.version)
                    server.rpc_override = lambda message: {"result": {"tools": [], "resultType": "complete"}} if message["method"] == "tools/list" else None
                    await source.refresh()
                    self.assertFalse(resources.has("tool", "add"))
                self.assertTrue(source._task.done())
                self.assertFalse(registry._observers)
        self.assertFalse(http.is_closed)
