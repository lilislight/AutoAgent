"""Bounded MCP recovery, no Tool replay and resync-aware subscription lifetimes."""

import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from autoagent.agent import (
    MCPClient, MCPClientError, MCPReconnectPolicy, MCPToolRegistry,
    MCPStdioConfig, MCPSSEConfig, MCPStreamableHTTPConfig,
)
from tests import test_agent_mcp as mcp_tests
from tests.test_agent_mcp import WireServer, call, httpx2


def policy(**options):
    options.setdefault("initial_delay_seconds", 0)
    options.setdefault("max_delay_seconds", 0)
    return MCPReconnectPolicy(**options)


class MCPRecoveryTests(unittest.IsolatedAsyncioTestCase):
    client = mcp_tests.MCPHTTPTests.client

    async def test_unsubscribable_catalog_polls_changes_and_removals(self):
        """Peers without list-change support stay usable and refresh without subscription RPCs."""
        server = WireServer()
        client, _ = self.client(server)
        async with client:
            registry = MCPToolRegistry(client)
            watch = registry.watch(poll_interval_seconds=0.01)
            try:
                self.assertEqual((await anext(watch)).type, "connected")
                server.tool = {**server.tool, "description": "Updated by polling"}
                update = await asyncio.wait_for(anext(watch), 2)
                self.assertEqual(update.type, "notification")
                self.assertEqual(registry.definitions[0].description, "Updated by polling")
                server.rpc_override = lambda message: {"result": {"tools": [], "resultType": "complete"}} if message["method"] == "tools/list" else None
                await asyncio.wait_for(anext(watch), 2)
                self.assertEqual(registry.definitions, ())
                methods = [json.loads(request.content)["method"] for request in server.requests if request.method == "POST"]
                self.assertNotIn("subscriptions/listen", methods)
                self.assertEqual(server.calls, [])
            finally:
                await watch.aclose()
            self.assertFalse(client._message_observers)

    async def test_missing_subscription_method_falls_back_but_other_errors_fail(self):
        """Only unsupported subscriptions fall back; other server failures remain visible."""
        from mcp.shared.exceptions import MCPError
        for code in (-32601, -32603):
            with self.subTest(code=code):
                server = WireServer(list_changed=True)
                server.rpc_override = lambda message: {"error": {"code": code, "message": "Listen unavailable"}} if message["method"] == "subscriptions/listen" else None
                client, _ = self.client(server)
                async with client:
                    registry = MCPToolRegistry(client)
                    watch = registry.watch(poll_interval_seconds=0.01)
                    try:
                        if code == -32603:
                            with self.assertRaises(MCPError):
                                await anext(watch)
                        else:
                            self.assertEqual((await anext(watch)).type, "connected")
                            server.tool = {**server.tool, "description": "Fallback refresh"}
                            await asyncio.wait_for(anext(watch), 2)
                            self.assertEqual(registry.definitions[0].description, "Fallback refresh")
                    finally:
                        await watch.aclose()
                    self.assertEqual(client._active, 0)
                    self.assertFalse(client._message_observers)

    async def test_unhonored_subscription_polls_instead_of_stalling(self):
        """A subscription that omits Tools changes cannot silently disable catalog updates."""
        from mcp_types import SubscriptionFilter
        class Unhonored:
            honored = SubscriptionFilter()
        @asynccontextmanager
        async def listen(sdk, **options):
            yield Unhonored()
        server = WireServer(list_changed=True)
        client, _ = self.client(server)
        async with client:
            registry = MCPToolRegistry(client)
            with patch("mcp.Client.listen", new=listen):
                watch = registry.watch(poll_interval_seconds=0.01)
                try:
                    self.assertEqual((await anext(watch)).type, "connected")
                    server.tool = {**server.tool, "description": "Unhonored fallback"}
                    await asyncio.wait_for(anext(watch), 2)
                    self.assertEqual(registry.definitions[0].description, "Unhonored fallback")
                finally:
                    await watch.aclose()
            self.assertEqual(client._active, 0)

    async def test_polling_catalog_recovers_connection_without_tool_replay(self):
        """A polling discovery failure reconnects and publishes a resync without executing Tools."""
        server = WireServer()
        client, _ = self.client(server, reconnect_policy=policy())
        async with client:
            registry = MCPToolRegistry(client)
            watch = registry.watch(poll_interval_seconds=0.01)
            try:
                await anext(watch)
                failed = False
                def fail_once(message):
                    nonlocal failed
                    if message["method"] == "tools/list" and not failed:
                        failed = True
                        return {"error": {"code": -32000, "message": "Connection closed"}}
                server.rpc_override = fail_once
                server.tool = {**server.tool, "description": "Reconnected"}
                update = await asyncio.wait_for(anext(watch), 2)
                self.assertEqual(update.type, "connected")
                self.assertTrue(update.resync_required)
                self.assertEqual(client.generation, 2)
                self.assertEqual(registry.definitions[0].description, "Reconnected")
                self.assertEqual(server.calls, [])
            finally:
                await watch.aclose()


    async def test_reconnect_rediscovery_replaces_registry_and_rejects_old_tool_snapshots(self):
        """A new session rediscoveries Tools before dispatch, while old schemas cannot execute."""
        server = WireServer()
        client, http = self.client(server, reconnect_policy=policy())
        registry = MCPToolRegistry(client)
        async with client:
            old = await registry.refresh()
            owner = client._owner
            server.tool = {**server.tool, "description":"Updated"}
            await client.areconnect()
            self.assertEqual(client.generation, 2)
            self.assertTrue(owner.done())
            self.assertEqual(registry.definitions[0].description, "Updated")
            self.assertEqual(registry.revision, 2)
            self.assertTrue((await old.ainvoke(call())).is_error)
            self.assertEqual(server.calls, [])
            self.assertEqual((await registry.ainvoke(call())).structured_content, {"sum":6})
            self.assertEqual(len(server.calls), 1)
        self.assertFalse(http.is_closed)

    async def test_session_loss_is_reported_once_and_next_call_recovers(self):
        """A possibly executed Tool call is not replayed when its connection is lost."""
        server = WireServer()
        invocations = []
        def rpc(message):
            if message["method"] == "tools/call":
                invocations.append(message)
                if len(invocations) == 1:
                    return {"error":{"code":-32000, "message":"Connection closed"}}
        server.rpc_override = rpc
        client, _ = self.client(server, reconnect_policy=policy())
        async with client:
            with self.assertRaises(MCPClientError) as caught:
                await client.call_tool("add", {"a":2, "b":4})
            self.assertTrue(caught.exception.connection_lost)
            self.assertTrue(caught.exception.outcome_unknown)
            self.assertEqual(len(invocations), 1)
            self.assertEqual(client.generation, 1)
            result = await client.call_tool("add", {"a":2, "b":4})
            self.assertEqual(result.structured_content, {"sum":6})
            self.assertEqual(client.generation, 2)
            self.assertEqual(len(invocations), 2)

    async def test_protocol_errors_do_not_reconnect_or_retry(self):
        """A domain/protocol RPC failure does not masquerade as broken transport."""
        server = WireServer()
        server.rpc_override = lambda message: {"error":{"code":-32603, "message":"execution failed"}} if message["method"] == "tools/call" else None
        client, _ = self.client(server, reconnect_policy=policy())
        async with client:
            with self.assertRaises(MCPClientError) as caught:
                await client.call_tool("add", {})
            self.assertFalse(caught.exception.connection_lost)
            await client.list_tools()
            self.assertEqual(client.generation, 1)

    async def test_connection_attempts_are_bounded_and_auth_failures_are_not_retried(self):
        """Recovery has a finite connection budget and preserves permanent authentication errors."""
        for status in (None, 403):
            attempts = 0
            @asynccontextmanager
            async def failing(stack):
                nonlocal attempts
                attempts += 1
                if status is None:
                    raise ConnectionError("Offline")
                raise httpx2.HTTPStatusError("Denied", request=httpx2.Request("GET", "https://server.test"), response=httpx2.Response(status))
                yield
            client = MCPClient(MCPStreamableHTTPConfig(server_id="test", url="https://server.test/mcp"), reconnect_policy=policy(max_attempts=3))
            # Return a fresh context on each reconnect; the real owner enters/exits it.
            async def transport(stack):
                return failing(stack)
            with patch.object(client, "_transport", side_effect=transport):
                with self.assertRaises(MCPClientError):
                    await client.aprepare()
            self.assertEqual(attempts, 3 if status is None else 1)
            await client.aclose()

    async def test_concurrent_consumers_share_one_reconnect_generation(self):
        """Independent subscriptions requesting the same recovery do not replace each other."""
        client, _ = self.client(WireServer(), reconnect_policy=policy())
        async with client:
            generation = client.generation
            await asyncio.gather(*(client.areconnect(expected_generation=generation) for _ in range(4)))
            self.assertEqual(client.generation, generation + 1)

    async def test_reconnect_waits_for_inflight_calls_without_replaying_them(self):
        """Transport replacement waits for active application work to settle."""
        server = WireServer()
        client, _ = self.client(server, reconnect_policy=policy())
        async with client:
            entered, release = asyncio.Event(), asyncio.Event()
            original = client._sdk.call_tool
            async def blocking(**arguments):
                entered.set()
                await release.wait()
                return await original(**arguments)
            with patch.object(client._sdk, "call_tool", side_effect=blocking):
                task = asyncio.create_task(client.call_tool("add", {"a":2, "b":4}))
                await entered.wait()
                reconnect = asyncio.create_task(client.areconnect())
                await asyncio.sleep(0)
                self.assertFalse(reconnect.done())
                release.set()
                await task
                await reconnect
            self.assertEqual(len(server.calls), 1)
            self.assertEqual(client.generation, 2)

    async def test_cancelled_reconnect_can_be_completed_without_leaking_owner_tasks(self):
        """Cancelling connection replacement leaves resources reachable for cleanup/recovery."""
        client, _ = self.client(WireServer(), reconnect_policy=policy())
        async with client:
            delay = policy(initial_delay_seconds=0.5, max_delay_seconds=0.5)
            task = asyncio.create_task(client.areconnect(policy=delay))
            # Retiring the old owner happens before the configurable delay.
            while client._owner is not None:
                await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            await client.aprepare()
            self.assertEqual(client.generation, 2)
            self.assertEqual(len(await client.list_tools()), 1)

    async def test_legacy_resource_subscriptions_are_restored_and_unsubscribed_names_stay_removed(self):
        """Legacy subscription intent survives a new session without reviving removed entries."""
        import warnings
        server = WireServer(legacy=True)
        client, _ = self.client(server, reconnect_policy=policy())
        async with client:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                await client.subscribe_resource("asset://keep")
                await client.subscribe_resource("asset://remove")
                await client.unsubscribe_resource("asset://remove")
                await client.areconnect()
            sent = [json.loads(request.content) for request in server.requests if request.method == "POST"]
            names = [message["params"]["uri"] for message in sent if message.get("method") == "resources/subscribe"]
            self.assertEqual(names, ["asset://keep", "asset://remove", "asset://keep"])

    async def test_failed_rediscovery_retains_old_snapshot_but_prevents_stale_dispatch(self):
        """An invalid replacement catalog cannot silently keep executing the previous definitions."""
        server = WireServer()
        client, _ = self.client(server, reconnect_policy=policy())
        registry = MCPToolRegistry(client)
        async with client:
            old = await registry.refresh()
            server.tool = {**server.tool, "inputSchema":{"type":"string"}}
            with self.assertRaises(ExceptionGroup):
                await client.areconnect()
            with self.assertRaises(MCPClientError):
                _ = registry.snapshot
            self.assertIs(registry._snapshot, old)
            self.assertTrue((await old.ainvoke(call())).is_error)
            self.assertFalse(server.calls)


class MCPRecoveryTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_sse_reconnect_creates_a_new_session_and_leaves_borrowed_http_open(self):
        """Legacy SSE ownership scopes can be replaced on the same event loop."""
        server = WireServer(legacy=True)
        http = httpx2.AsyncClient(transport=httpx2.MockTransport(server.handle))
        client = MCPClient(MCPSSEConfig(server_id="sse", url="https://server.test/sse"), http_client=http)
        try:
            async with client:
                await client.areconnect()
                self.assertEqual(len(await client.list_tools()), 1)
                self.assertEqual(client.generation, 2)
            self.assertFalse(http.is_closed)
        finally:
            await http.aclose()

    async def test_stdio_reconnect_settles_old_subprocess_owner_and_rediscovers_tools(self):
        """Actual stdio processes are replaced without reusing an old AnyIO ownership scope."""
        fixture = str(Path(__file__).parent / "fixtures/mcp_tool_server.py")
        client = MCPClient(MCPStdioConfig(server_id="stdio", command=sys.executable, args=(fixture,)))
        async with client:
            registry = MCPToolRegistry(client)
            await registry.refresh()
            owner = client._owner
            await client.areconnect()
            self.assertTrue(owner.done())
            self.assertEqual(client.generation, 2)
            self.assertEqual((await registry.ainvoke(call())).structured_content, {"sum":6})


class MCPSubscriptionRecoveryTests(unittest.IsolatedAsyncioTestCase):
    client = mcp_tests.MCPHTTPTests.client

    async def test_acknowledgement_cannot_publish_a_subscription_from_a_retired_connection(self):
        """A delayed subscription ack must not be labeled with a replacement connection's generation."""
        from types import SimpleNamespace
        from mcp_types import SubscriptionFilter
        entered, release = asyncio.Event(), asyncio.Event()
        @asynccontextmanager
        async def delayed(sdk, **options):
            entered.set()
            await release.wait()
            yield SimpleNamespace(honored=SubscriptionFilter(toolsListChanged=True))
        client, _ = self.client(WireServer())
        async with client:
            async def opening():
                async with client.listen(tools_list_changed=True):
                    self.fail("A retired subscription must not be returned")
            with patch("mcp.Client.listen", new=delayed):
                task = asyncio.create_task(opening())
                await entered.wait()
                await client.areconnect()
                release.set()
                with self.assertRaises(MCPClientError) as caught:
                    await task
            self.assertTrue(caught.exception.connection_lost)
            self.assertEqual(client._active, 0)

    async def test_real_sdk_resubscription_announces_resync_and_restores_all_filters(self):
        """A lost subscription reopens through the SDK and exposes the notification gap."""
        server = WireServer()
        original = server.handle
        filters = []
        async def handle(request):
            if request.method == "POST":
                message = json.loads(request.content)
                if message.get("method") == "subscriptions/listen":
                    filters.append(message["params"]["notifications"])
                    meta = {"io.modelcontextprotocol/subscriptionId":message["id"]}
                    honored = {"toolsListChanged":True, "resourcesListChanged":True, "resourceSubscriptions":["asset://report"]}
                    frames = [{"jsonrpc":"2.0", "method":"notifications/subscriptions/acknowledged", "params":{"_meta":meta, "notifications":honored}}]
                    if len(filters) == 1:
                        frames.extend([
                            {"jsonrpc":"2.0", "method":"notifications/tools/list_changed", "params":{"_meta":meta}},
                            {"jsonrpc":"2.0", "method":"notifications/cancelled", "params":{"requestId":message["id"], "reason":"lost"}},
                        ])
                    else:
                        frames.extend([
                            {"jsonrpc":"2.0", "method":"notifications/resources/updated", "params":{"_meta":meta, "uri":"asset://report"}},
                            {"jsonrpc":"2.0", "id":message["id"], "result":{"cacheScope":"private", "ttlMs":0, "resultType":"complete"}},
                        ])
                    body = "".join("event: message\ndata: " + json.dumps(frame) + "\n\n" for frame in frames)
                    return httpx2.Response(200, content=body.encode(), headers={"content-type":"text/event-stream"})
            return await original(request)
        server.handle = handle
        client, _ = self.client(server)
        async with client:
            async with client.listen_resilient(tools_list_changed=True, resources_list_changed=True,
                                               resource_subscriptions=("asset://report",), policy=policy()) as subscription:
                connected = await anext(subscription)
                event = await anext(subscription)
                recovered = await anext(subscription)
                updated = await anext(subscription)
                self.assertEqual(connected.type, "connected")
                self.assertTrue(connected.resync_required)
                self.assertEqual(event.event.type, "tools_list_changed")
                self.assertEqual(recovered.generation, connected.generation + 1)
                self.assertTrue(recovered.resync_required)
                self.assertEqual(updated.event.uri, "asset://report")
                self.assertEqual(filters[0], filters[1])
                from autoagent import ValueContract
                from autoagent.agent import MCPSubscriptionUpdate
                contract = ValueContract.create(MCPSubscriptionUpdate, location="subscription-update")
                self.assertEqual(contract.restore(contract.to_record(recovered)), recovered)
                self.assertEqual(contract.restore(contract.to_record(updated)), updated)
                with self.assertRaises(StopAsyncIteration):
                    await anext(subscription)
            self.assertEqual(client._active, 0)

    async def test_repeated_subscription_loss_has_a_finite_recovery_budget(self):
        """Permanent subscription loss cannot create an unbounded reconnect loop."""
        from mcp.client.subscriptions import SubscriptionLost
        attempts = []
        @asynccontextmanager
        async def lost(sdk, **options):
            attempts.append(options)
            raise SubscriptionLost("Lost before acknowledgement")
            yield
        client, _ = self.client(WireServer())
        async with client:
            with patch("mcp.Client.listen", new=lost):
                with self.assertRaises(SubscriptionLost):
                    async with client.listen_resilient(tools_list_changed=True, policy=policy(max_attempts=2)):
                        self.fail("Lost subscription should not enter")
            self.assertEqual(len(attempts), 3)
            self.assertEqual(client._active, 0)
            self.assertEqual(client._requests, 0)

    async def test_cancelled_subscriber_releases_recovery_and_client_admission(self):
        """Cancelling an idle subscriber leaves no held subscription or owner task to leak."""
        from mcp_types import SubscriptionFilter
        entered = asyncio.Event()
        class Waiting:
            honored = SubscriptionFilter(toolsListChanged=True)
            def __aiter__(self):
                return self
            async def __anext__(self):
                entered.set()
                await asyncio.Future()
        @asynccontextmanager
        async def waiting(sdk, **options):
            yield Waiting()
        client, _ = self.client(WireServer())
        async with client:
            async def consume():
                async with client.listen_resilient(tools_list_changed=True, policy=policy()) as stream:
                    async for update in stream:
                        pass
            with patch("mcp.Client.listen", new=waiting):
                task = asyncio.create_task(consume())
                await entered.wait()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertEqual(client._active, 0)

    async def test_registry_watch_publishes_changed_tools_before_emitting_notification(self):
        """Watch consumers see an already refreshed catalog when change notifications arrive."""
        from mcp_types import SubscriptionFilter
        from mcp.shared.subscriptions import ToolsListChanged
        server = WireServer(list_changed=True)
        class Changes:
            honored = SubscriptionFilter(toolsListChanged=True)
            def __aiter__(self):
                return self
            async def __anext__(self):
                server.tool = {**server.tool, "description":"Watched"}
                return ToolsListChanged()
        @asynccontextmanager
        async def changes(sdk, **options):
            yield Changes()
        client, _ = self.client(server)
        async with client:
            registry = MCPToolRegistry(client)
            with patch("mcp.Client.listen", new=changes):
                watch = registry.watch(policy=policy())
                self.assertEqual((await anext(watch)).type, "connected")
                self.assertEqual((await anext(watch)).type, "notification")
                self.assertEqual(registry.definitions[0].description, "Watched")
                await watch.aclose()
            self.assertEqual(client._active, 0)
