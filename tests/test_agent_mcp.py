"""Real SDK transports, OAuth flow and MCP Tool/Runtime boundaries."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from pydantic import ValidationError

from autoagent import AutoAgentApp, Node, ValueContract, Workflow
from autoagent.agent import (
    LLMToolCall, LLMToolResult, MCPBearerAuth, MCPClient, MCPClientError,
    MCPMemoryTokenStorage, MCPOAuth, MCPSSEConfig, MCPStdioConfig,
    MCPStreamableHTTPConfig, MCPTool, ToolOutput, ToolOutputError, ToolSet,
)

try:
    import httpx2
    from mcp.client.auth import AuthorizationCodeResult
    from mcp.shared.auth import OAuthClientInformationFull, OAuthClientMetadata, OAuthToken
    from mcp_types import CallToolResult, Tool as SDKTool
except ImportError:
    httpx2 = None


def call(name="add", arguments='{"a":2,"b":4}'):
    return LLMToolCall(name=name, arguments=arguments, call_id="call-1")


class MCPModelTests(unittest.TestCase):
    def test_transport_configs_and_secrets_have_explicit_boundaries(self):
        """Validate transport selection and keep credentials out of representations."""
        stdio = MCPStdioConfig(server_id="local", command="python", env={"TOKEN": "secret"})
        http = MCPStreamableHTTPConfig(server_id="remote", url="https://server.test/mcp", headers={"X-Key": "secret"})
        auth = MCPBearerAuth(token="secret")
        for value in (stdio, http, auth):
            self.assertNotIn("secret", repr(value))
        with self.assertRaises(ValueError):
            MCPClient(stdio, auth=auth)
        with self.assertRaises(ValueError):
            MCPClient(MCPSSEConfig(server_id="remote", url="https://server.test/sse", headers={"authorization": "Bearer another"}), auth=auth)
        for url in ("file:///tmp/mcp", "relative", "https:///mcp"):
            with self.subTest(url=url), self.assertRaises(ValidationError):
                MCPStreamableHTTPConfig(server_id="bad", url=url)
        for token in ("", "contains space", "token\n"):
            with self.subTest(token=token), self.assertRaises(ValidationError):
                MCPBearerAuth(token=token)

    def test_connection_config_is_detached(self):
        """Changing caller dictionaries cannot mutate a configured connection."""
        config = MCPSSEConfig(server_id="remote", url="https://server.test/sse", headers={"X-Region": "one"})
        client = MCPClient(config)
        config.headers["X-Region"] = "two"
        self.assertEqual(client.config.headers["X-Region"], "one")
        self.assertIsNone(client._owner)

    def test_arbitrary_json_structured_results_round_trip_in_core(self):
        """Preserve modern MCP arrays and scalars without wrapping them in objects."""
        contract = ValueContract.create(LLMToolResult, location="mcp-result")
        for value in ([1, {"a": True}], 3, "text", False, None):
            with self.subTest(value=value):
                result = LLMToolResult(call_id="1", structured_content=value, tool_metadata={"mcp": {"server_id": "remote"}})
                self.assertEqual(contract.restore(contract.to_record(result)), result)
                self.assertEqual(ToolOutput(structured_content=value).structured_content, value)
        with self.assertRaises(ValidationError):
            LLMToolResult(call_id="1", structured_content=[float("nan")])


class SSEStream(httpx2.AsyncByteStream if httpx2 is not None else object):
    def __init__(self, queue):
        self.queue = queue
        self.closed = False

    async def __aiter__(self):
        yield b"event: endpoint\ndata: /messages/?session_id=test\n\n"
        while True:
            data = await self.queue.get()
            yield ("event: message\ndata: " + json.dumps(data) + "\n\n").encode()

    async def aclose(self):
        self.closed = True


class WireServer:
    """A JSON-RPC wire peer; transports and sessions are the actual MCP SDK."""
    def __init__(self, *, legacy=False, bearer=None, result=None):
        self.legacy = legacy
        self.bearer = bearer
        self.requests = []
        self.calls = []
        self.queue = asyncio.Queue()
        self.stream = SSEStream(self.queue)
        self.result = result if result is not None else {"content": [{"type": "text", "text": "6"}], "structuredContent": {"sum": 6}, "isError": False, "resultType": "complete"}
        self.tool = {"name": "add", "description": "Remote addition", "inputSchema": {"type": "object", "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}}, "required": ["a", "b"], "additionalProperties": False}}
        self.rpc_override = None
        self.sse_post_responses = False

    def rpc(self, message):
        method = message["method"]
        if "id" not in message:
            return None
        if self.rpc_override is not None:
            override = self.rpc_override(message)
            if override is not None:
                if not self.legacy and "result" in override:
                    override["result"] = {"cacheScope": "private", "ttlMs": 0, **override["result"]}
                return {"jsonrpc": "2.0", "id": message["id"], **override}
        if method == "server/discover":
            if self.legacy:
                return {"jsonrpc": "2.0", "id": message["id"], "error": {"code": -32601, "message": "Method not found"}}
            result = {"supportedVersions": ["2026-07-28"], "capabilities": {"tools": {}, "resources": {}, "prompts": {}}, "resultType": "complete"}
        elif method == "initialize":
            result = {"protocolVersion": "2025-11-25", "capabilities": {"tools": {}, "resources": {}, "prompts": {}}, "serverInfo": {"name": "test", "version": "1"}}
        elif method == "tools/list":
            result = {"tools": [self.tool], "resultType": "complete"}
        elif method == "tools/call":
            self.calls.append(message["params"])
            result = self.result
        elif method == "resources/list":
            result = {"resources": [{"name": "report", "uri": "asset://report"}], "resultType": "complete"}
        elif method == "resources/read":
            result = {"contents": [{"uri": "asset://report", "text": "report text"}], "resultType": "complete"}
        elif method == "prompts/list":
            result = {"prompts": [{"name": "summary"}], "resultType": "complete"}
        elif method == "prompts/get":
            result = {"messages": [{"role": "user", "content": {"type": "text", "text": "Summarize"}}], "resultType": "complete"}
        else:
            result = {}
        if not self.legacy:
            result = {"cacheScope": "private", "ttlMs": 0, **result}
        return {"jsonrpc": "2.0", "id": message["id"], "result": result}

    async def handle(self, request):
        self.requests.append(request)
        if self.bearer is not None and request.headers.get("authorization") != "Bearer " + self.bearer:
            return httpx2.Response(401, headers={"WWW-Authenticate": "Bearer"})
        if request.method == "DELETE":
            return httpx2.Response(200)
        if request.method == "GET":
            if request.url.path == "/sse":
                return httpx2.Response(200, stream=self.stream, headers={"content-type": "text/event-stream"})
            return httpx2.Response(405)
        message = json.loads(request.content)
        reply = self.rpc(message)
        if request.url.path == "/messages/":
            if reply is not None:
                self.queue.put_nowait(reply)
            return httpx2.Response(202)
        if reply is None:
            return httpx2.Response(202)
        headers = {"content-type": "application/json"}
        if self.legacy:
            headers["mcp-session-id"] = "test-session"
        if self.sse_post_responses:
            return httpx2.Response(200, content=("event: message\ndata: " + json.dumps(reply) + "\n\n").encode(), headers={**headers, "content-type": "text/event-stream"})
        return httpx2.Response(200, json=reply, headers=headers)


@unittest.skipUnless(httpx2 is not None, "Install autoagent[mcp] for SDK tests")
class MCPHTTPTests(unittest.IsolatedAsyncioTestCase):
    def client(self, server, *, sse=False, **kwargs):
        http = httpx2.AsyncClient(transport=httpx2.MockTransport(server.handle))
        config_type = MCPSSEConfig if sse else MCPStreamableHTTPConfig
        config = config_type(server_id="test", url="https://server.test/sse" if sse else "https://server.test/mcp", request_timeout_seconds=2)
        client = MCPClient(config, http_client=http, **kwargs)
        self.addAsyncCleanup(http.aclose)
        self.addAsyncCleanup(client.aclose)
        return client, http

    async def test_streamable_http_json_and_sse_responses(self):
        """Discover and call tools through both legal Streamable HTTP response forms."""
        for use_sse in (False, True):
            with self.subTest(response_sse=use_sse):
                server = WireServer()
                server.sse_post_responses = use_sse
                client, _ = self.client(server)
                tools = await client.toolset(prefix="web_")
                self.assertEqual(tools.definitions[0].name, "web_add")
                result = await tools.ainvoke(call("web_add"))
                self.assertEqual(result.structured_content, {"sum": 6})
                self.assertEqual(result.call_id, "call-1")
                self.assertEqual(server.calls[0]["name"], "add")
                self.assertEqual(result.tool_metadata["mcp"]["server_id"], "test")

    async def test_legacy_sse_transport_and_borrowed_http_cleanup(self):
        """Use endpoint/message SSE framing and close its stream without closing borrowed HTTP."""
        server = WireServer(legacy=True)
        client, http = self.client(server, sse=True)
        async with client:
            tools = await client.toolset()
            self.assertEqual((await tools.ainvoke(call())).content[0].text, "6")
        self.assertTrue(server.stream.closed)
        self.assertFalse(http.is_closed)
        self.assertTrue(any(request.url.path == "/messages/" for request in server.requests))

    async def test_legacy_streamable_session_is_terminated(self):
        """Negotiate a legacy session and issue DELETE on normal connection shutdown."""
        server = WireServer(legacy=True)
        client, _ = self.client(server)
        async with client:
            await client.list_tools()
        self.assertTrue(any(request.method == "DELETE" for request in server.requests))

    async def test_bearer_auth_applies_to_both_http_transports(self):
        """Send Bearer on SSE GET/message POST and Streamable HTTP requests."""
        actual_factory = httpx2.AsyncClient
        for sse in (False, True):
            server = WireServer(legacy=sse, bearer="test-token")
            created = []

            def factory(**kwargs):
                value = actual_factory(transport=httpx2.MockTransport(server.handle), **kwargs)
                created.append(value)
                return value

            config_type = MCPSSEConfig if sse else MCPStreamableHTTPConfig
            config = config_type(server_id="auth", url="https://server.test/sse" if sse else "https://server.test/mcp")
            client = MCPClient(config, auth=MCPBearerAuth(token="test-token"))
            with patch("httpx2.AsyncClient", side_effect=factory):
                async with client:
                    tools = await client.toolset()
                    await tools.ainvoke(call())
            self.assertTrue(created[0].is_closed)
            self.assertTrue(all(request.headers["authorization"] == "Bearer test-token" for request in server.requests))

    async def test_pagination_and_aliases_keep_remote_names(self):
        """Collect all pages and route model-safe aliases to original MCP tool names."""
        server = WireServer()
        first = {**server.tool, "name": "remote.add"}
        second = {**server.tool, "name": "other"}

        def pages(message):
            if message["method"] == "tools/list":
                if not message.get("params", {}).get("cursor"):
                    return {"result": {"tools": [first], "nextCursor": "page-2", "resultType": "complete"}}
                return {"result": {"tools": [second], "resultType": "complete"}}

        server.rpc_override = pages
        client, _ = self.client(server)
        tools = await client.toolset(prefix="remote_", names={"remote.add": "add"})
        self.assertEqual(tuple(item.name for item in tools.definitions), ("remote_add", "remote_other"))
        await tools.ainvoke(call("remote_add"))
        self.assertEqual(server.calls[0]["name"], "remote.add")

    async def test_cursor_cycles_fail_instead_of_looping(self):
        """Reject a server that keeps returning the same pagination cursor."""
        server = WireServer()
        server.rpc_override = lambda message: {"result": {"tools": [server.tool], "nextCursor": "repeat", "resultType": "complete"}} if message["method"] == "tools/list" else None
        client, _ = self.client(server)
        with self.assertRaises(MCPClientError) as caught:
            await client.list_tools()
        self.assertEqual(caught.exception.operation, "list_tools")

    async def test_input_schema_errors_do_not_dispatch_remote_effects(self):
        """Check required fields, unknown keys, duplicate JSON keys and numeric types locally."""
        server = WireServer()
        client, _ = self.client(server)
        tools = await client.toolset()
        for arguments in ("[]", '{"a":2}', '{"a":2,"b":4,"extra":1}', '{"a":true,"b":4}', '{"a":2,"b":4,"b":5}', '{"a":NaN,"b":4}'):
            with self.subTest(arguments=arguments):
                result = await tools.ainvoke(call(arguments=arguments))
                self.assertTrue(result.is_error)
                self.assertEqual(result.structured_content["error"]["kind"], "invalid_arguments")
        self.assertEqual(server.calls, [])

    async def test_tool_schema_external_refs_are_not_fetched(self):
        """Do not perform external network retrieval while validating server-supplied schemas."""
        server = WireServer()
        server.tool["inputSchema"]["properties"]["a"] = {"$ref": "https://schemas.test/value.json"}
        client, _ = self.client(server)
        tools = await client.toolset()
        from autoagent.agent import ToolExecutionError
        with self.assertRaises(ToolExecutionError):
            await tools.ainvoke(call())
        self.assertEqual(server.calls, [])

    async def test_business_errors_and_protocol_errors_have_distinct_outcomes(self):
        """Keep isError observations while retaining JSON-RPC error codes as failures."""
        server = WireServer(result={"content": [{"type": "text", "text": "not found"}], "isError": True, "resultType": "complete"})
        client, _ = self.client(server)
        tools = await client.toolset()
        result = await tools.ainvoke(call())
        self.assertTrue(result.is_error)
        server.rpc_override = lambda message: {"error": {"code": -32001, "message": "server failure"}} if message["method"] == "tools/call" else None
        with self.assertRaises(MCPClientError) as caught:
            await client.call_tool("add", {"a": 2, "b": 4})
        self.assertEqual(caught.exception.code, -32001)

    async def test_sdk_validates_remote_output_schema(self):
        """Reject a successful server response whose structured data violates its schema."""
        server = WireServer()
        server.tool["outputSchema"] = {"type": "object", "properties": {"sum": {"type": "string"}}, "required": ["sum"]}
        client, _ = self.client(server)
        await client.list_tools()
        with self.assertRaises(MCPClientError):
            await client.call_tool("add", {"a": 2, "b": 4})

    async def test_metadata_and_multimodal_content_are_preserved(self):
        """Map image/audio/resource blocks without fetching files or duplicating binary data."""
        result = {"content": [
            {"type": "text", "text": "Preview", "annotations": {"audience": ["user"]}},
            {"type": "image", "data": "YQ==", "mimeType": "image/png"},
            {"type": "audio", "data": "YQ==", "mimeType": "audio/wav"},
            {"type": "resource_link", "uri": "asset://report", "name": "report.pdf", "mimeType": "application/pdf"},
            {"type": "resource", "resource": {"uri": "asset://text", "text": "Document text", "mimeType": "text/plain"}},
            {"type": "resource", "resource": {"uri": "asset://blob", "blob": "YQ==", "mimeType": "application/pdf"}},
        ], "structuredContent": [1, 2], "_meta": {"trace": "remote"}, "resultType": "complete"}
        server = WireServer(result=result)
        client, _ = self.client(server)
        tools = await client.toolset()
        observation = await tools.ainvoke(call())
        self.assertEqual(tuple(item.type for item in observation.content), ("text", "image", "audio", "file", "text", "file"))
        self.assertEqual(observation.structured_content, [1, 2])
        self.assertEqual(observation.content[3].source.uri, "asset://report")
        metadata = observation.tool_metadata["mcp"]
        self.assertEqual(metadata["result"]["_meta"], {"trace": "remote"})
        self.assertEqual(metadata["content"][0]["annotations"]["audience"], ["user"])
        self.assertNotIn("data", metadata["content"][1])
        self.assertNotIn("blob", metadata["content"][5]["resource"])
        contract = ValueContract.create(LLMToolResult, location="observation")
        self.assertEqual(contract.restore(contract.to_record(observation)), observation)

    async def test_invalid_binary_content_is_an_output_error(self):
        """Refuse malformed media bytes even if the SDK accepted the result envelope."""
        server = WireServer(result={"content": [{"type": "image", "data": "invalid!", "mimeType": "image/png"}], "resultType": "complete"})
        client, _ = self.client(server)
        tools = await client.toolset()
        with self.assertRaises(ToolOutputError):
            await tools.ainvoke(call())

    async def test_resources_and_prompts_share_the_connection(self):
        """Expose SDK resource and prompt APIs through the same managed transport."""
        client, _ = self.client(WireServer())
        self.assertEqual((await client.list_resources())[0].name, "report")
        self.assertEqual((await client.read_resource("asset://report")).contents[0].text, "report text")
        self.assertEqual((await client.list_prompts())[0].name, "summary")
        self.assertEqual((await client.get_prompt("summary")).messages[0].content.text, "Summarize")

    async def test_concurrent_callers_share_one_connection_owner(self):
        """Connect once, then issue multiple calls from separate tasks and close elsewhere."""
        server = WireServer()
        client, http = self.client(server)
        await asyncio.gather(client.aprepare(), client.aprepare())
        owner = client._owner
        results = await asyncio.gather(client.call_tool("add", {"a": 2, "b": 4}), client.call_tool("add", {"a": 2, "b": 4}))
        self.assertEqual(len(results), 2)
        self.assertIs(client._owner, owner)
        await asyncio.create_task(client.aclose())
        self.assertTrue(owner.done())
        self.assertFalse(http.is_closed)
        with self.assertRaises(RuntimeError):
            await client.list_tools()

    async def test_cancellation_releases_admission_and_blocks_premature_close(self):
        """Preserve caller cancellation and keep shutdown from interrupting active requests."""
        server = WireServer()
        entered = asyncio.Event()
        gate = asyncio.Event()

        async def handle(request):
            if request.method == "POST" and json.loads(request.content).get("method") == "tools/call":
                entered.set()
                await gate.wait()
            return await server.handle(request)

        http = httpx2.AsyncClient(transport=httpx2.MockTransport(handle))
        client = MCPClient(MCPStreamableHTTPConfig(server_id="cancel", url="https://server.test/mcp"), http_client=http)
        try:
            await client.list_tools()
            task = asyncio.create_task(client.call_tool("add", {"a": 2, "b": 4}))
            await entered.wait()
            with self.assertRaises(RuntimeError):
                await client.aclose()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(client._active, 0)
        finally:
            gate.set()
            await client.aclose()
            await http.aclose()

    async def test_connection_failure_keeps_status_and_releases_resources(self):
        """Normalize authentication failure and unwind a partially entered connection."""
        actual_factory = httpx2.AsyncClient
        created = []

        def factory(**kwargs):
            value = actual_factory(transport=httpx2.MockTransport(lambda request: httpx2.Response(403)), **kwargs)
            created.append(value)
            return value

        client = MCPClient(MCPStreamableHTTPConfig(server_id="denied", url="https://server.test/mcp"))
        with patch("httpx2.AsyncClient", side_effect=factory):
            with self.assertRaises(MCPClientError):
                async with client:
                    self.fail("Connection should not succeed")
        self.assertTrue(created[0].is_closed)
        self.assertTrue(client._owner.done())

    async def test_cancelled_connection_attempt_closes_owned_http(self):
        """Unwind the SDK and HTTP contexts when initialization is cancelled."""
        entered = asyncio.Event()
        gate = asyncio.Event()
        actual_factory = httpx2.AsyncClient
        created = []

        async def handle(request):
            entered.set()
            await gate.wait()
            return httpx2.Response(503)

        def factory(**kwargs):
            value = actual_factory(transport=httpx2.MockTransport(handle), **kwargs)
            created.append(value)
            return value

        client = MCPClient(MCPStreamableHTTPConfig(server_id="startup", url="https://server.test/mcp"))

        async def connect():
            async with client:
                self.fail("A cancelled connection must not enter user code")

        with patch("httpx2.AsyncClient", side_effect=factory):
            task = asyncio.create_task(connect())
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(created[0].is_closed)
        self.assertTrue(client._owner.done())

    async def test_cancelled_close_can_be_awaited_again(self):
        """Keep the owner task closing resources after a shutdown caller is cancelled."""
        entered = asyncio.Event()
        gate = asyncio.Event()
        server = WireServer()

        class DelayedTransport(httpx2.MockTransport):
            async def aclose(self):
                entered.set()
                await gate.wait()
                await super().aclose()

        actual_factory = httpx2.AsyncClient
        created = []

        def factory(**kwargs):
            value = actual_factory(transport=DelayedTransport(server.handle), **kwargs)
            created.append(value)
            return value

        client = MCPClient(MCPStreamableHTTPConfig(server_id="shutdown", url="https://server.test/mcp"))
        with patch("httpx2.AsyncClient", side_effect=factory):
            await client.aprepare()
            closing = asyncio.create_task(client.aclose())
            await entered.wait()
            closing.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await closing
            self.assertFalse(client._owner.done())
            gate.set()
            await client.aclose()
        self.assertTrue(client._owner.done())
        self.assertTrue(created[0].is_closed)

    async def test_request_timeout_and_cross_loop_use_are_explicit_failures(self):
        """Bound a nonresponding RPC and reject reuse on Core's different event loop."""
        server = WireServer()
        client, _ = self.client(server)
        await client.aprepare()
        app = AutoAgentApp()

        async def on_other_loop(incoming: LLMToolCall) -> LLMToolResult:
            await client.list_tools()
            return LLMToolResult(call_id=incoming.call_id)

        try:
            result = await app.ainvoke(Workflow("wrong-mcp-loop", nodes=[Node("tool", on_other_loop)]), call())
            self.assertEqual(result.status, "failed")
            self.assertIn("owning event loop", result.error.message)
        finally:
            await app.aclose()

        # The SDK receives an HTTP acknowledgment but no JSON-RPC response.
        async def no_response(request):
            if request.method == "POST" and json.loads(request.content)["method"] == "tools/call":
                return httpx2.Response(202)
            return await server.handle(request)

        http = httpx2.AsyncClient(transport=httpx2.MockTransport(no_response))
        timed = MCPClient(MCPStreamableHTTPConfig(server_id="timeout", url="https://server.test/mcp", request_timeout_seconds=0.05), http_client=http)
        try:
            await timed.list_tools()
            with self.assertRaises(MCPClientError):
                await timed.call_tool("add", {"a": 2, "b": 4})
            self.assertEqual(timed._active, 0)
        finally:
            await timed.aclose()
            await http.aclose()

    async def test_core_lifespan_owns_connection_and_mcp_tool_node(self):
        """Discover and execute MCP tools on Core's loop, retaining only portable results."""
        client, http = self.client(WireServer())
        selected = []

        @asynccontextmanager
        async def lifespan():
            try:
                async with client:
                    selected.append(await client.toolset())
                    app.register_operator(selected[0].as_operator(id=operator.id), capability_id=operator.id)
                    yield
            finally:
                await http.aclose()

        from autoagent import Capability
        operator = ToolSet().as_operator()

        app = AutoAgentApp(runtime_lifespan=lifespan)
        try:
            await app.aprepare()
            result = await app.ainvoke(Workflow("mcp", nodes=[Node("tool", Capability(operator.id, operator.contract))]), call())
            self.assertEqual(result.status, "completed", result.error)
            observation = ValueContract.create(LLMToolResult, location="result").restore(result.output)
            self.assertEqual(observation.structured_content, {"sum": 6})
            self.assertIsNot(client._loop, asyncio.get_running_loop())
        finally:
            await app.aclose()
        self.assertTrue(client._owner.done())
        self.assertTrue(http.is_closed)


@unittest.skipUnless(httpx2 is not None, "Install autoagent[mcp] for SDK tests")
class MCPStdioTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_stdio_subprocess_discovery_call_env_and_shutdown(self):
        """Launch an actual SDK server, pass explicit env and close its subprocess transport."""
        fixture = str(Path(__file__).parent / "fixtures/mcp_tool_server.py")
        client = MCPClient(MCPStdioConfig(server_id="stdio", command=sys.executable, args=(fixture,), env={"AUTOAGENT_MCP_TEST_VALUE": "explicit"}, request_timeout_seconds=3))
        async with client:
            tools = await client.toolset()
            self.assertEqual((await tools.ainvoke(call())).structured_content, {"sum": 6})
            env = await tools.ainvoke(call("secret_from_env", "{}"))
            self.assertEqual(env.content[0].text, "explicit")
            media = await tools.ainvoke(call("media", "{}"))
            self.assertEqual(tuple(item.type for item in media.content), ("text", "image", "audio"))
            self.assertTrue((await tools.ainvoke(call("domain_error", "{}"))).is_error)
        self.assertTrue(client._owner.done())
        self.assertIsNone(client._sdk)


@unittest.skipUnless(httpx2 is not None, "Install autoagent[mcp] for OAuth tests")
class MCPOAuthTests(unittest.IsolatedAsyncioTestCase):
    async def test_memory_storage_copies_registration_and_tokens(self):
        """Preserve SDK token and registration objects without leaking mutable references."""
        storage = MCPMemoryTokenStorage()
        token = OAuthToken(access_token="token", refresh_token="refresh")
        info = OAuthClientInformationFull(client_id="client", client_secret="secret")
        await storage.set_tokens(token)
        await storage.set_client_info(info)
        token.access_token = "changed"
        info.client_secret = "changed"
        self.assertEqual((await storage.get_tokens()).access_token, "token")
        self.assertEqual((await storage.get_client_info()).client_secret, "secret")
        self.assertNotIn("secret", repr(storage))

    async def oauth_flow(self, *, sse=False, wrong_state=False, wrong_issuer=False, expire=False):
        server = WireServer(legacy=sse)
        endpoint = "https://server.test/sse" if sse else "https://server.test/mcp"
        storage = MCPMemoryTokenStorage()
        redirects = []
        token_requests = []
        registration = []
        authorized = []

        async def redirect(url):
            redirects.append(parse_qs(urlparse(url).query))

        async def callback():
            return AuthorizationCodeResult(code="test-code", state="wrong" if wrong_state else redirects[-1]["state"][0], iss="https://wrong.test" if wrong_issuer else "https://auth.test")

        async def handle(request):
            path = request.url.path
            if path == "/.well-known/oauth-protected-resource":
                return httpx2.Response(200, json={"resource": endpoint, "authorization_servers": ["https://auth.test"], "scopes_supported": ["tools"]})
            if path == "/.well-known/oauth-authorization-server":
                return httpx2.Response(200, json={"issuer": "https://auth.test", "authorization_endpoint": "https://auth.test/authorize", "token_endpoint": "https://auth.test/token", "registration_endpoint": "https://auth.test/register", "response_types_supported": ["code"], "grant_types_supported": ["authorization_code", "refresh_token"], "code_challenge_methods_supported": ["S256"], "token_endpoint_auth_methods_supported": ["none"], "authorization_response_iss_parameter_supported": True})
            if path == "/register":
                metadata = json.loads(request.content)
                registration.append(metadata)
                return httpx2.Response(201, json={**metadata, "client_id": "client-1", "token_endpoint_auth_method": "none"})
            if path == "/token":
                form = parse_qs(request.content.decode())
                token_requests.append(form)
                refreshing = form["grant_type"] == ["refresh_token"]
                return httpx2.Response(200, json={"access_token": "refreshed" if refreshing else "token-1", "token_type": "Bearer", "expires_in": 3600 if refreshing or not expire else 0, "refresh_token": "refresh-1", "scope": "tools"})
            if not request.headers.get("authorization", "").startswith("Bearer "):
                return httpx2.Response(401, headers={"WWW-Authenticate": 'Bearer resource_metadata="https://server.test/.well-known/oauth-protected-resource", scope="tools"'})
            authorized.append(request.headers["authorization"])
            return await server.handle(request)

        actual_factory = httpx2.AsyncClient
        created = []

        def factory(**kwargs):
            value = actual_factory(transport=httpx2.MockTransport(handle), **kwargs)
            created.append(value)
            return value

        oauth = MCPOAuth(client_metadata=OAuthClientMetadata(redirect_uris=["http://127.0.0.1:3030/callback"], client_name="AutoAgent test", token_endpoint_auth_method="none"), storage=storage, redirect_handler=redirect, callback_handler=callback)
        config_type = MCPSSEConfig if sse else MCPStreamableHTTPConfig
        client = MCPClient(config_type(server_id="oauth", url=endpoint, request_timeout_seconds=3), auth=oauth)
        with patch("httpx2.AsyncClient", side_effect=factory):
            if wrong_state or wrong_issuer:
                with self.assertRaises(MCPClientError):
                    async with client:
                        self.fail("Invalid callback should be rejected")
            else:
                async with client:
                    tools = await client.toolset()
                    self.assertEqual((await tools.ainvoke(call())).structured_content, {"sum": 6})
        self.assertTrue(created[0].is_closed)
        self.assertIsNone(oauth.client_metadata.scope)
        return redirects, token_requests, registration, authorized, storage

    async def test_oauth_discovery_registration_pkce_and_token_exchange_for_both_transports(self):
        """Use the SDK's complete authorization-code flow for both HTTP transports."""
        for sse in (False, True):
            with self.subTest(sse=sse):
                redirects, tokens, registration, authorized, storage = await self.oauth_flow(sse=sse)
                self.assertEqual(len(registration), 1)
                self.assertEqual(redirects[0]["code_challenge_method"], ["S256"])
                self.assertIn("code_challenge", redirects[0])
                self.assertIn("code_verifier", tokens[0])
                self.assertEqual(tokens[0]["grant_type"], ["authorization_code"])
                self.assertEqual((await storage.get_tokens()).access_token, "token-1")
                self.assertEqual((await storage.get_client_info()).client_id, "client-1")
                self.assertTrue(all(value == "Bearer token-1" for value in authorized))

    async def test_oauth_refresh_does_not_repeat_interactive_login(self):
        """Refresh an expired access token through the SDK while retaining registration."""
        redirects, tokens, registration, authorized, storage = await self.oauth_flow(expire=True)
        self.assertEqual(len(redirects), 1)
        self.assertEqual(len(registration), 1)
        self.assertEqual(tokens[-1]["grant_type"], ["refresh_token"])
        self.assertIn("Bearer refreshed", authorized)
        self.assertEqual((await storage.get_tokens()).access_token, "refreshed")

    async def test_oauth_rejects_state_and_issuer_mismatches(self):
        """Do not exchange callback codes whose state or issuer fails SDK validation."""
        for bad in ({"wrong_state": True}, {"wrong_issuer": True}):
            with self.subTest(bad=bad):
                _, tokens, _, _, storage = await self.oauth_flow(**bad)
                self.assertEqual(tokens, [])
                self.assertIsNone(await storage.get_tokens())


if __name__ == "__main__":
    unittest.main()
