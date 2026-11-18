"""Tool regression boundaries, MCP callbacks/progress and OAuth resources."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import stat
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from autoagent import (
    UserEventContext,
    UserEventSpec,
    Capability,
    Node,
    Operator,
    Stream,
    ValueContract,
    Workflow,
)
from autoagent.core.app import AutoAgentApp
from autoagent.agent import (
    ChatCompletionsAdapter, LLMRequest, LLMToolCall, LLMToolResult,
    MCPCallbacks, MCPClient, MCPClientError, MCPFileTokenStorage,
    MCPMemoryTokenStorage, MCPOAuth, MCPOAuthCallbackServer,
    MCPSSEConfig, MCPStreamableHTTPConfig, MCPTool, MCPToolRegistry,
    MCPToolStreamChunk, MCPToolStreamReducer, ResponsesAdapter, Tool, ToolOutput, ToolSet, tool,
)
from tests import test_agent_mcp as mcp_tests
from tests.test_agent_mcp import WireServer, call, httpx2

if httpx2 is not None:
    from mcp.shared.auth import OAuthClientInformationFull, OAuthClientMetadata, OAuthToken
    import mcp_types as types



def mcp_events(context: UserEventContext) -> UserEventSpec | None:
    if context.phase == "chunk":
        return UserEventSpec(kind="mcp.chunk", payload=context.value)
    return None


class NormalizedOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str

    @field_validator("name")
    @classmethod
    def normalize(cls, value):
        return value.strip().lower()


class ToolRegressionTests(unittest.TestCase):
    def test_mutated_output_emits_normalized_validated_value(self):
        """Use JSON validator normalization rather than the prevalidation record."""
        @tool
        def profile() -> NormalizedOutput:
            value = NormalizedOutput(name="initial")
            value.name = " UPDATED "
            return value

        observation = Tool(profile).invoke(call("profile", "{}"))
        self.assertEqual(observation.structured_content, {"name": "updated"})

    def test_structured_null_and_absence_survive_core_and_both_encoders(self):
        """Transmit explicit null while retaining an empty result as absent."""
        contract = ValueContract.create(LLMToolResult, location="null")
        for present in (False, True):
            with self.subTest(present=present):
                result = LLMToolResult(call_id="1", **({"structured_content": None} if present else {}))
                restored = contract.restore(contract.to_record(result))
                self.assertEqual(restored.structured_content_present, present)
                request = LLMRequest(model="model", input=(restored,))
                responses = ResponsesAdapter().encode_request(request, provider_id="test")["input"][0]["output"]
                chat = ChatCompletionsAdapter().encode_request(request, provider_id="test")["messages"][0]["content"]
                self.assertEqual(responses, [{"type": "input_text", "text": "null"}] if present else "")
                self.assertEqual(chat, "null" if present else "")
        with self.assertRaises(ValidationError):
            ToolOutput(structured_content=1, structured_content_present=False)

    def test_local_tooloutput_null_presence_is_forwarded(self):
        """Preserve explicit null from custom Tools through their result wrapper."""
        @tool
        def null() -> ToolOutput:
            return ToolOutput(structured_content=None)

        self.assertTrue(Tool(null).invoke(call("null", "{}")).structured_content_present)

    def test_tool_selection_keeps_snapshots_and_rejects_unknown_includes(self):
        """Select advertised names without mutating the original collection."""
        @tool
        def first() -> str:
            return "one"

        @tool
        def second() -> str:
            return "two"

        original = ToolSet((first, second))
        self.assertEqual([d.name for d in original.select(include=("second",)).definitions], ["second"])
        self.assertEqual([d.name for d in original.select(exclude=("second",)).definitions], ["first"])
        self.assertEqual(len(original), 2)
        with self.assertRaises(ValueError):
            original.select(include=("missing",))

    def test_progress_contract_and_reducer_require_a_final_correlated_result(self):
        """Progress is portable but cannot itself complete a Workflow node."""
        reducer = MCPToolStreamReducer()
        state = reducer.initial(None)
        chunk = MCPToolStreamChunk(type="progress", server_id="test", tool_name="add", call_id="1", progress=1.0)
        contract = ValueContract.create(MCPToolStreamChunk, location="progress")
        self.assertEqual(contract.restore(contract.to_record(chunk)), chunk)
        self.assertEqual(reducer.add(None, state, chunk), state)
        with self.assertRaises(ValueError):
            reducer.finish(None, state)
        terminal = MCPToolStreamChunk(type="completed", server_id="test", tool_name="add", call_id="1", result=LLMToolResult(call_id="1"))
        state = reducer.add(None, state, terminal)
        self.assertEqual(reducer.finish(None, state), terminal.result)
        with self.assertRaises(ValueError):
            reducer.add(None, state, terminal)
        with self.assertRaises(ValidationError):
            terminal.model_copy(update={"call_id": "wrong"}).model_validate(terminal.model_copy(update={"call_id": "wrong"}))


@unittest.skipUnless(httpx2 is not None, "Install autoagent[mcp] for SDK tests")
class ExtensionHTTPTests(unittest.IsolatedAsyncioTestCase):
    client = mcp_tests.MCPHTTPTests.client

    async def test_nested_context_rejection_leaves_outer_connection_usable(self):
        """Reject duplicate ownership before its cleanup can close an outer scope."""
        client, _ = self.client(WireServer())
        async with client:
            with self.assertRaisesRegex(RuntimeError, "already entered"):
                async with client:
                    self.fail("Nested scope accepted")
            self.assertEqual(len(await client.list_tools()), 1)
            self.assertFalse(client._closed)
        self.assertTrue(client._owner.done())

    async def test_connect_deadline_cleans_up_a_stalled_handshake(self):
        """Bound connection establishment independently of RPC response timeouts."""
        async def stall(request):
            await asyncio.Event().wait()

        async with httpx2.AsyncClient(transport=httpx2.MockTransport(stall)) as http:
            client = MCPClient(MCPStreamableHTTPConfig(server_id="slow", url="https://server.test/mcp", connect_timeout_seconds=0.03), http_client=http)
            with self.assertRaises(MCPClientError) as raised:
                async with client:
                    self.fail("Handshake should time out")
            self.assertEqual(raised.exception.operation, "connect")
            self.assertTrue(client._owner.done())
            self.assertIsNone(client._sdk)

    async def test_remote_null_is_preserved_without_protocol_metadata_workarounds(self):
        """Promote structured-content presence into the portable result contract."""
        server = WireServer(result={"content": [], "structuredContent": None, "resultType": "complete"})
        client, _ = self.client(server)
        async with client:
            observation = await (await client.toolset()).ainvoke(call())
        self.assertTrue(observation.structured_content_present)
        self.assertIsNone(observation.structured_content)

    async def test_remote_filter_runs_before_schema_compilation_and_checks_aliases(self):
        """An excluded unusable schema cannot prevent approved Tools being loaded."""
        server = WireServer()
        bad = {"name": "bad", "inputSchema": {"type": "object", "properties": {"x": {"type": "unknown"}}}}
        server.rpc_override = lambda message: {"result": {"tools": [server.tool, bad], "resultType": "complete"}} if message["method"] == "tools/list" else None
        client, _ = self.client(server)
        async with client:
            selected = await client.toolset(include=("add",), prefix="remote_", names={"add": "sum"})
            self.assertEqual(selected.definitions[0].name, "remote_sum")
            self.assertEqual(selected["remote_sum"].as_operator().contract.output.annotation, LLMToolResult)
            self.assertEqual((await selected.ainvoke(call("remote_sum"))).structured_content, {"sum": 6})
            with self.assertRaises(ValueError):
                await client.tools(include=("absent",))
            server.rpc_override = lambda message: {"result": {"tools": [server.tool, {**server.tool, "name": "other"}], "resultType": "complete"}} if message["method"] == "tools/list" else None
            with self.assertRaises(ValueError):
                await client.tools(names={"other": "add"})

    async def test_refresh_is_atomic_and_old_snapshots_keep_their_definition(self):
        """Failed discoveries cannot replace a previously usable Tool snapshot."""
        server = WireServer()
        client, _ = self.client(server)
        registry = MCPToolRegistry(client)
        with self.assertRaises(RuntimeError):
            _ = registry.snapshot
        async with client:
            original = await registry.refresh()
            server.tool = {**server.tool, "description": "Changed description"}
            replacement = await registry.refresh()
            self.assertEqual(original.definitions[0].description, "Remote addition")
            self.assertEqual(replacement.definitions[0].description, "Changed description")
            self.assertEqual(registry.revision, 2)
            server.rpc_override = lambda message: {"error": {"code": -32603, "message": "listing failed"}} if message["method"] == "tools/list" else None
            with self.assertRaises(MCPClientError):
                await registry.refresh()
            self.assertIs(registry.snapshot, replacement)
            self.assertEqual(registry.revision, 2)
            self.assertEqual((await registry.ainvoke(call())).structured_content, {"sum": 6})

    async def test_real_sdk_progress_stream_and_terminal_result(self):
        """Correlate actual wire progress with one final portable observation."""
        server = WireServer()
        original = server.handle

        async def handle(request):
            if request.method == "POST":
                message = json.loads(request.content)
                if message.get("method") == "tools/call":
                    token = message["params"]["_meta"]["progressToken"]
                    notifications = [{"jsonrpc": "2.0", "method": "notifications/progress", "params": {"progressToken": token, "progress": value, "total": 2, "message": "working"}} for value in (1, 2)]
                    body = "".join("event: message\ndata: " + json.dumps(value) + "\n\n" for value in (*notifications, server.rpc(message)))
                    return httpx2.Response(200, content=body.encode(), headers={"content-type": "text/event-stream"})
            return await original(request)

        server.handle = handle
        client, _ = self.client(server)
        async with client:
            remote = (await client.tools())[0]
            chunks = [chunk async for chunk in remote.astream(call())]
        self.assertEqual([c.type for c in chunks], ["progress", "progress", "completed"])
        self.assertEqual([c.progress for c in chunks[:-1]], [1, 2])
        self.assertTrue(all(c.call_id == "call-1" for c in chunks))
        self.assertEqual(chunks[-1].result.structured_content, {"sum": 6})
        self.assertEqual(len(server.calls), 1)

    async def test_closing_progress_generator_cancels_pending_rpc(self):
        """Early consumer exit settles the request before client closure."""
        server = WireServer()
        client, _ = self.client(server)
        async with client:
            remote = (await client.tools())[0]
            async def request(name, arguments, *, progress_callback):
                client._active += 1
                try:
                    await progress_callback(1.0, None, "working")
                    await asyncio.Event().wait()
                finally:
                    client._active -= 1
            with patch.object(client, "call_tool", side_effect=request):
                stream = remote.astream(call())
                self.assertEqual((await anext(stream)).type, "progress")
                await stream.aclose()
            self.assertEqual(client._active, 0)

    async def test_slow_progress_consumer_retains_latest_bounded_updates(self):
        """Progress bursts cannot block SDK notification tasks or grow the buffer."""
        client, _ = self.client(WireServer())
        async with client:
            remote = (await client.tools())[0]
            async def request(name, arguments, *, progress_callback):
                for value in range(100):
                    await progress_callback(float(value), 100.0, None)
                return types.CallToolResult(content=[])
            with patch.object(client, "call_tool", side_effect=request):
                chunks = [chunk async for chunk in remote.astream(call())]
            self.assertEqual(len(chunks), 33)
            self.assertEqual(chunks[0].progress, 68)
            self.assertEqual(chunks[-2].progress, 99)
            self.assertEqual(chunks[-1].type, "completed")

    async def test_legacy_callbacks_are_negotiated_and_reverse_roots_are_answered(self):
        """Install callbacks before negotiation and route real reverse requests."""
        server = WireServer(legacy=True)
        original = server.handle
        replies, seen, logs = [], [], []
        answered, notified = asyncio.Event(), asyncio.Event()

        async def handle(request):
            if request.method == "POST":
                message = json.loads(request.content)
                if "method" not in message:
                    replies.append(message)
                    answered.set()
                    return httpx2.Response(202)
            return await original(request)

        async def roots(context):
            return types.ListRootsResult(roots=[types.Root(uri="file:///workspace", name="Workspace")])

        async def log(params):
            logs.append(params.data)

        async def notify(message):
            seen.append(message.method)
            if message.method == "notifications/tools/list_changed":
                notified.set()

        server.handle = handle
        client, _ = self.client(server, sse=True, callbacks=MCPCallbacks(list_roots_callback=roots, logging_callback=log, message_handler=notify))
        async with client:
            initialize = next(json.loads(r.content) for r in server.requests if r.method == "POST" and json.loads(r.content).get("method") == "initialize")
            self.assertIn("roots", initialize["params"]["capabilities"])
            server.queue.put_nowait({"jsonrpc": "2.0", "id": "server-1", "method": "roots/list"})
            await asyncio.wait_for(answered.wait(), 1)
            self.assertEqual(replies[0]["result"]["roots"][0]["uri"], "file:///workspace")
            server.queue.put_nowait({"jsonrpc": "2.0", "method": "notifications/message", "params": {"level": "info", "data": "hello"}})
            server.queue.put_nowait({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})
            await asyncio.wait_for(notified.wait(), 1)
            self.assertEqual(logs, ["hello"])
            self.assertIn("notifications/message", seen)

    async def test_modern_input_required_invokes_sampling_elicitation_and_roots(self):
        """Fulfill modern embedded input requests through official SDK callbacks."""
        server = WireServer()
        rounds, seen = [], []

        async def sample(context, params):
            seen.append("sampling")
            return types.CreateMessageResult(role="assistant", content=types.TextContent(type="text", text="answer"), model="app-model")

        async def elicit(context, params):
            seen.append("elicitation")
            return types.ElicitResult(action="accept", content={"confirm": True})

        async def roots(context):
            seen.append("roots")
            return types.ListRootsResult(roots=[])

        def override(message):
            if message["method"] != "tools/call":
                return None
            rounds.append(message["params"])
            if "inputResponses" in message["params"]:
                return {"result": server.result}
            return {"result": {"resultType": "input_required", "requestState": "opaque-state", "inputRequests": {
                "sample": {"method": "sampling/createMessage", "params": {"messages": [{"role": "user", "content": {"type": "text", "text": "hello"}}], "maxTokens": 8}},
                "confirm": {"method": "elicitation/create", "params": {"mode": "form", "message": "Confirm", "requestedSchema": {"type": "object", "properties": {"confirm": {"type": "boolean"}}, "required": ["confirm"]}}},
                "roots": {"method": "roots/list"},
            }}}

        server.rpc_override = override
        client, _ = self.client(server, callbacks=MCPCallbacks(sampling_callback=sample, elicitation_callback=elicit, list_roots_callback=roots))
        async with client:
            result = await (await client.toolset()).ainvoke(call())
        self.assertEqual(result.structured_content, {"sum": 6})
        self.assertEqual(set(seen), {"sampling", "elicitation", "roots"})
        self.assertEqual(len(rounds), 2)
        self.assertEqual(rounds[1]["requestState"], "opaque-state")
        self.assertEqual(rounds[1]["inputResponses"]["sample"]["content"]["text"], "answer")
        self.assertEqual(rounds[1]["inputResponses"]["confirm"]["content"], {"confirm": True})

    async def test_modern_tool_list_subscription_delivers_typed_change_events(self):
        """Honor SDK subscription filters and settle lifecycle accounting on exit."""
        server = WireServer()
        original = server.handle

        async def handle(request):
            if request.method == "POST":
                message = json.loads(request.content)
                if message.get("method") == "subscriptions/listen":
                    meta = {"io.modelcontextprotocol/subscriptionId": message["id"]}
                    frames = (
                        {"jsonrpc": "2.0", "method": "notifications/subscriptions/acknowledged", "params": {"_meta": meta, "notifications": {"toolsListChanged": True}}},
                        {"jsonrpc": "2.0", "method": "notifications/tools/list_changed", "params": {"_meta": meta}},
                        {"jsonrpc": "2.0", "id": message["id"], "result": {"cacheScope": "private", "ttlMs": 0, "resultType": "complete"}},
                    )
                    body = "".join("event: message\ndata: " + json.dumps(frame) + "\n\n" for frame in frames)
                    return httpx2.Response(200, content=body.encode(), headers={"content-type": "text/event-stream"})
            return await original(request)

        server.handle = handle
        client, _ = self.client(server)
        async with client:
            async with client.listen_tools() as subscription:
                self.assertTrue(subscription.honored.tools_list_changed)
                self.assertEqual(client._active, 1)
                with self.assertRaises(RuntimeError):
                    await client.aclose()
                events = [event async for event in subscription]
                self.assertEqual(len(events), 1)
                self.assertEqual(type(events[0]).__name__, "ToolsListChanged")
            self.assertEqual(client._active, 0)

    async def test_core_streaming_tool_emits_progress_user_events_and_reduces_output(self):
        """Use Runtime lifespan, streaming contracts and the existing User Event sink."""
        events = []
        selected = []

        class Sink:
            async def append_user_event(self, event):
                events.append(event)

        @asynccontextmanager
        async def lifespan():
            server = WireServer()
            async with httpx2.AsyncClient(transport=httpx2.MockTransport(server.handle)) as http:
                client = MCPClient(MCPStreamableHTTPConfig(server_id="core", url="https://server.test/mcp"), http_client=http)
                async with client:
                    remote = (await client.tools())[0]
                    original = client.call_tool
                    async def progress_call(name, arguments, *, progress_callback=None):
                        if progress_callback is not None:
                            await progress_callback(1.0, 2.0, "working")
                        return await original(name, arguments)
                    with patch.object(client, "call_tool", side_effect=progress_call):
                        selected.append(client)
                        app.register_operator(remote.as_operator(id="mcp", streaming=True), capability_id="mcp")
                        yield

        async def placeholder(call: LLMToolCall):
            yield MCPToolStreamChunk(type="completed", server_id="core", tool_name=call.name, call_id=call.call_id, result=LLMToolResult(call_id=call.call_id))

        # Core needs a module-resolvable chunk annotation at construction.
        from collections.abc import AsyncIterator
        placeholder.__annotations__["return"] = AsyncIterator[MCPToolStreamChunk]
        prototype = Operator(placeholder, id="mcp")
        app = AutoAgentApp(runtime_lifespan=lifespan, user_event_sink=Sink())
        try:
            await app.aprepare()
            result = await app.ainvoke(Workflow("progress", nodes=[Node("tool", Capability("mcp", prototype.contract), stream=Stream(MCPToolStreamReducer()), user_events=mcp_events)]), call())
            self.assertEqual(result.status, "completed", result.error)
            observation = ValueContract.create(LLMToolResult, location="tool").restore(result.output)
            self.assertEqual(observation.structured_content, {"sum": 6})
            chunks = [e.to_record()["payload"] for e in events if e.kind == "mcp.chunk"]
            self.assertEqual([c["type"] for c in chunks], ["progress", "completed"])
        finally:
            await app.aclose()
        self.assertTrue(selected[0]._owner.done())


@unittest.skipUnless(httpx2 is not None, "Install autoagent[mcp] for OAuth SDK tests")
class OAuthResourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_file_storage_restarts_preserve_registration_secrets_and_expiry(self):
        """Atomic credential persistence retains absolute expiry across new instances."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private" / "server.json"
            storage = MCPFileTokenStorage(path)
            token = OAuthToken(access_token="token", refresh_token="refresh", expires_in=60)
            info = OAuthClientInformationFull(client_id="client", client_secret="secret")
            with patch("autoagent.agent.mcp.storage.time.time", return_value=1000):
                await asyncio.gather(storage.set_tokens(token), storage.set_client_info(info))
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            restored = MCPFileTokenStorage(path)
            with patch("autoagent.agent.mcp.storage.time.time", return_value=1020):
                self.assertEqual((await restored.get_tokens()).expires_in, 40)
            with patch("autoagent.agent.mcp.storage.time.time", return_value=1100):
                expired = await restored.get_tokens()
                self.assertEqual(expired.expires_in, 0)
                self.assertEqual(expired.refresh_token, "refresh")
            self.assertEqual((await restored.get_client_info()).client_secret, "secret")
            self.assertNotIn("secret", repr(restored))
            self.assertEqual(list(path.parent.glob(".mcp-*")), [])

    async def test_file_storage_does_not_silently_replace_invalid_credentials(self):
        """Corrupt storage surfaces an error instead of discarding registration."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.json"
            path.write_text('{"version":9}')
            storage = MCPFileTokenStorage(path)
            with self.assertRaises(ValueError):
                await storage.set_tokens(OAuthToken(access_token="token"))
            self.assertEqual(json.loads(path.read_text()), {"version": 9})

    async def test_cancelled_file_write_holds_lock_until_filesystem_work_settles(self):
        """Cancellation cannot let a later writer overwrite unfinished token storage."""
        with tempfile.TemporaryDirectory() as directory:
            storage = MCPFileTokenStorage(Path(directory) / "server.json")
            started = asyncio.Event()
            release = threading.Event()
            loop = asyncio.get_running_loop()
            original = storage._write
            def delayed(record):
                loop.call_soon_threadsafe(started.set)
                release.wait(timeout=2)
                original(record)
            try:
                with patch.object(storage, "_write", side_effect=delayed):
                    writing = asyncio.create_task(storage.set_tokens(OAuthToken(access_token="token")))
                    await asyncio.wait_for(started.wait(), 1)
                    writing.cancel()
                    await asyncio.sleep(0)
                    self.assertFalse(writing.done())
                    self.assertTrue(storage._lock.locked())
                    registration = asyncio.create_task(storage.set_client_info(OAuthClientInformationFull(client_id="client")))
                    release.set()
                    with self.assertRaises(asyncio.CancelledError):
                        await writing
                    await registration
            finally:
                release.set()
            self.assertEqual((await storage.get_tokens()).access_token, "token")
            self.assertEqual((await storage.get_client_info()).client_id, "client")

    async def test_memory_storage_does_not_extend_access_token_lifetime(self):
        """Reading an existing token adjusts expires_in to its remaining lifetime."""
        storage = MCPMemoryTokenStorage()
        with patch("autoagent.agent.mcp.auth.time.time", return_value=1000):
            await storage.set_tokens(OAuthToken(access_token="token", expires_in=60))
        with patch("autoagent.agent.mcp.auth.time.time", return_value=1100):
            self.assertEqual((await storage.get_tokens()).expires_in, 0)

    async def test_oauth_callback_handler_has_an_independent_deadline(self):
        """Interactive login cannot hang indefinitely when SDK request timeouts pause."""
        async def callback():
            await asyncio.Event().wait()
        oauth = MCPOAuth(client_metadata=OAuthClientMetadata(redirect_uris=["http://127.0.0.1/callback"]), storage=MCPMemoryTokenStorage(), callback_handler=callback, login_timeout_seconds=0.02)
        sdk_auth = oauth.build("https://server.test/mcp")
        with self.assertRaises(TimeoutError):
            await sdk_auth.context.callback_handler()

    async def send_callback(self, server, query):
        uri = urlsplit(server.redirect_uri)
        reader, writer = await asyncio.open_connection(uri.hostname, uri.port)
        try:
            writer.write(f"GET {uri.path}?{query} HTTP/1.1\r\nHost: {uri.netloc}\r\n\r\n".encode())
            await writer.drain()
            return await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_loopback_callback_validates_state_and_preserves_sdk_issuer(self):
        """Wrong state cannot settle login; valid code/state/iss reach the SDK."""
        server = MCPOAuthCallbackServer(timeout_seconds=1)
        async with server:
            with self.assertRaises(RuntimeError):
                async with server:
                    self.fail("Nested callback context accepted")
            await server.redirect_handler("https://auth.test/authorize?state=expected")
            self.assertIn("state=expected", await server.wait_for_authorization_url())
            response = await self.send_callback(server, "code=code&state=wrong")
            self.assertIn(b"400 Bad Request", response)
            response = await self.send_callback(server, "code=code&state=expected&iss=https%3A%2F%2Fauth.test")
            self.assertIn(b"200 OK", response)
            result = await server.callback_handler()
            self.assertEqual((result.code, result.state, result.iss), ("code", "expected", "https://auth.test"))
            self.assertIn(b"409 Conflict", await self.send_callback(server, "code=again&state=expected"))
        self.assertEqual(server._handlers, set())
        with self.assertRaises(RuntimeError):
            _ = server.redirect_uri

    async def test_callback_timeout_cleans_up_pending_login(self):
        """A missing browser redirect ends login with a bounded timeout."""
        async with MCPOAuthCallbackServer(timeout_seconds=0.02) as server:
            await server.redirect_handler("https://auth.test/authorize?state=test")
            with self.assertRaises(TimeoutError):
                await server.callback_handler()
            self.assertTrue(server._result.cancelled())

    async def test_callback_authorization_denial_is_an_explicit_login_failure(self):
        """A state-matched denial settles login without echoing provider details."""
        async with MCPOAuthCallbackServer(timeout_seconds=1) as server:
            await server.redirect_handler("https://auth.test/authorize?state=test")
            response = await self.send_callback(server, "error=access_denied&error_description=secret&state=test")
            self.assertIn(b"200 OK", response)
            self.assertNotIn(b"secret", response)
            with self.assertRaisesRegex(ValueError, "denied"):
                await server.callback_handler()


if __name__ == "__main__":
    unittest.main()
