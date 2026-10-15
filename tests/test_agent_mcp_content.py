"""MCP resource/Prompt conversion and SDK extension lifecycle boundaries."""

import json
import unittest

from autoagent.agent import (
    MCPContentError, mcp_content, mcp_resource_response, mcp_prompt_response,
    TextContent, ImageContent, FileContent, AssetSource,
)
from tests import test_agent_mcp as mcp_tests


class MCPContentTests(unittest.TestCase):
    def test_resource_content_preserves_uri_metadata_and_media_types(self):
        """Normalize text and binary resources without discarding resource identity."""
        result = mcp_resource_response({"contents":[
            {"uri":"asset://text", "text":"Hello", "mimeType":"text/plain", "_meta":{"version":1}},
            {"uri":"asset://image", "blob":"YQ==", "mimeType":"image/png"},
            {"uri":"asset://pdf", "blob":"YQ==", "mimeType":"application/pdf"},
        ], "_meta":{"server":"example"}})
        self.assertEqual(result.contents[0].uri, "asset://text")
        self.assertEqual(result.contents[0].metadata, {"_meta":{"version":1}})
        self.assertIsInstance(result.content[1], ImageContent)
        self.assertIsInstance(result.content[2], FileContent)
        self.assertEqual(result.to_message().role, "user")
        self.assertEqual(result.metadata, {"_meta":{"server":"example"}})
        from autoagent import ValueContract
        from autoagent.agent import MCPResourceResponse
        contract = ValueContract.create(MCPResourceResponse, location="mcp-resource")
        self.assertEqual(contract.restore(contract.to_record(result)), result)

    def test_prompt_roles_and_annotations_are_preserved_without_authority_upgrade(self):
        """Prompt conversion retains user/assistant order and side metadata."""
        value = mcp_prompt_response({"description":"Summary", "messages":[
            {"role":"user", "content":{"type":"text", "text":"Summarize", "annotations":{"audience":["user"]}}},
            {"role":"assistant", "content":{"type":"image", "data":"YQ==", "mimeType":"image/png"}},
        ]})
        self.assertEqual(tuple(item.role for item in value.messages), ("user", "assistant"))
        self.assertEqual(value.message_metadata[0]["content"]["annotations"], {"audience":["user"]})
        with self.assertRaises(MCPContentError):
            mcp_prompt_response({"messages":[{"role":"system", "content":{"type":"text", "text":"Elevate"}}]})

    def test_link_conversion_does_not_read_or_download_its_target(self):
        """Resource links remain resolvable references rather than fetched content."""
        result = mcp_content({"type":"resource_link", "uri":"asset://private", "name":"report.pdf", "mimeType":"application/pdf"})
        self.assertIsInstance(result.source, AssetSource)
        self.assertEqual(result.source.uri, "asset://private")

    def test_malformed_or_unsupported_content_fails_explicitly(self):
        """Unsupported blocks, invalid base64 and ambiguous resources are rejected."""
        for value in ({"type":"unknown"}, {"type":"image", "data":"bad", "mimeType":"image/png"},
                      {"type":"resource", "resource":{"uri":"a", "text":"a", "blob":"YQ=="}}):
            with self.subTest(value=value), self.assertRaises(MCPContentError):
                mcp_content(value)


class MCPContentSDKTests(unittest.IsolatedAsyncioTestCase):
    client = mcp_tests.MCPHTTPTests.client

    async def test_sdk_resource_and_prompt_conversion_and_negotiated_information(self):
        """Real MCP transports feed normalized models and expose detached capability data."""
        for legacy in (False, True):
            server = mcp_tests.WireServer(legacy=legacy)
            client, _ = self.client(server)
            async with client:
                self.assertEqual((await client.read_resource_content("asset://report")).content[0].text, "report text")
                self.assertEqual((await client.get_prompt_messages("summary")).messages[0].content[0].text, "Summarize")
                self.assertEqual(client.protocol_version, "2025-11-25" if legacy else "2026-07-28")
                capabilities = client.server_capabilities
                capabilities.clear()
                self.assertIn("tools", client.server_capabilities)
                self.assertIsNone(client.instructions)

    async def test_resource_templates_paginate_and_completion_uses_sdk_protocol(self):
        """Template pagination and argument completion use the SDK's real wire contracts."""
        server = mcp_tests.WireServer()
        def rpc(message):
            method = message["method"]
            if method == "resources/templates/list":
                cursor = message.get("params", {}).get("cursor")
                result = {"resourceTemplates":[{"name":"report" if cursor is None else "other", "uriTemplate":"asset://report/{id}"}], "resultType":"complete"}
                if cursor is None:
                    result["nextCursor"] = "page2"
                return {"result":result}
            if method == "completion/complete":
                return {"result":{"resultType":"complete", "completion":{"values":["42"], "total":1, "hasMore":False}}}
        server.rpc_override = rpc
        client, _ = self.client(server)
        from mcp_types import ResourceTemplateReference
        async with client:
            self.assertEqual(len(await client.list_resource_templates()), 2)
            result = await client.complete(ResourceTemplateReference(type="ref/resource", uri="asset://report/{id}"), {"name":"id", "value":"4"})
            self.assertEqual(result.completion.values, ["42"])
            self.assertEqual(client._active, 0)

    async def test_modern_resource_and_prompt_listen_filters_keep_close_accounting(self):
        """Unified subscriptions honor filters and keep the owning client open until exit."""
        server = mcp_tests.WireServer()
        original = server.handle
        async def handle(request):
            if request.method == "POST":
                message = json.loads(request.content)
                if message.get("method") == "subscriptions/listen":
                    meta = {"io.modelcontextprotocol/subscriptionId":message["id"]}
                    frames = [
                        {"jsonrpc":"2.0", "method":"notifications/subscriptions/acknowledged", "params":{"_meta":meta, "notifications":{"promptsListChanged":True, "resourcesListChanged":True}}},
                        {"jsonrpc":"2.0", "method":"notifications/prompts/list_changed", "params":{"_meta":meta}},
                        {"jsonrpc":"2.0", "method":"notifications/resources/list_changed", "params":{"_meta":meta}},
                        {"jsonrpc":"2.0", "id":message["id"], "result":{"cacheScope":"private", "ttlMs":0, "resultType":"complete"}},
                    ]
                    return mcp_tests.httpx2.Response(200, content="".join("event: message\ndata: " + json.dumps(frame) + "\n\n" for frame in frames).encode(), headers={"content-type":"text/event-stream"})
            return await original(request)
        server.handle = handle
        client, _ = self.client(server)
        async with client:
            async with client.listen(prompts_list_changed=True, resources_list_changed=True) as stream:
                self.assertTrue(stream.honored.prompts_list_changed)
                with self.assertRaises(RuntimeError):
                    await client.aclose()
                events = [event async for event in stream]
                self.assertEqual({type(event).__name__ for event in events}, {"PromptsListChanged", "ResourcesListChanged"})
            self.assertEqual(client._active, 0)

    async def test_legacy_resource_subscriptions_are_accounted_and_modern_errors_are_explicit(self):
        """Legacy resource RPCs settle admission even when a modern server rejects them."""
        import warnings
        for legacy in (True, False):
            server = mcp_tests.WireServer(legacy=legacy)
            client, _ = self.client(server)
            async with client:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    if legacy:
                        await client.subscribe_resource("asset://report")
                        await client.unsubscribe_resource("asset://report")
                    else:
                        from autoagent.agent import MCPClientError
                        with self.assertRaises(MCPClientError):
                            await client.subscribe_resource("asset://report")
                    self.assertEqual(client._active, 0)
