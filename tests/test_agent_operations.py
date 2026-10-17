"""Portable Skill/MCP requests used as Nodes and inside custom Operators."""

from contextlib import asynccontextmanager
import sys
import unittest
from unittest.mock import patch

from autoagent import AutoAgentApp, Node, ValueContract, Workflow
from autoagent.agent import (
    MCPGetPromptRequest, MCPOperations, MCPReadResourceRequest, MCPResourceResponse,
    MemorySkillSource, Skill, SkillSet, SkillOperations, SkillLoadRequest, SkillDocument,
    SkillListResourcesRequest, SkillReadResourceRequest, SkillReadTextRequest,
    SkillRunScriptRequest, SkillScriptSpec, SubprocessSkillRunner, SkillScriptResult,
    SkillScriptError, SkillResourceError,
)
from tests import test_agent_mcp as mcp_tests


class SkillOperationTests(unittest.IsolatedAsyncioTestCase):
    def fixture(self):
        files = {"SKILL.md": "---\nname: report\ndescription: Reports\ndisable-model-invocation: true\n---\nUse the rules.",
                 "references/rules.txt": "first\nsecond\n",
                 "scripts/run.py": "import sys\nprint(sys.argv[1], sys.stdin.read())\n"}
        skills = SkillSet((Skill(MemorySkillSource("report", files)),))
        runner = SubprocessSkillRunner((SkillScriptSpec(skill_name="report", path="scripts/run.py", command=(sys.executable,)),))
        return SkillOperations(skills, runner=runner)

    async def test_dynamic_skill_reads_run_as_typed_nodes_on_owned_workers(self):
        """Dynamic reads need no handwritten wrappers and preserve application-only access."""
        operations = self.fixture()
        self.assertEqual(operations.skills.model_metadata, ())
        cases = (
            ("load", SkillLoadRequest(name="report"), "instructions"),
            ("list_resources", SkillListResourcesRequest(name="report", prefix="references"), "resources"),
            ("read_resource", SkillReadResourceRequest(name="report", path="references/rules.txt"), "content"),
            ("read_text", SkillReadTextRequest(name="report", path="references/rules.txt", start_line=2), "text"),
        )
        app = AutoAgentApp(max_operator_concurrency=1)
        try:
            with patch("asyncio.to_thread", side_effect=AssertionError("Default executor used")):
                for operation, request, field in cases:
                    with self.subTest(operation=operation):
                        result = await app.ainvoke(Workflow(operation, nodes=[Node("read", operations.as_operator(operation))]), request)
                        self.assertEqual(result.status, "completed", result.error)
                        self.assertIn(field, result.output)
                        if operation == "read_text":
                            self.assertEqual(result.output["text"], "second\n")
        finally:
            await app.aclose()

    async def test_custom_operator_and_standalone_calls_share_the_same_request(self):
        """The async API supports composition without serializing its borrowed SkillSet."""
        operations = self.fixture()
        request = SkillLoadRequest(name="report")
        direct = await operations.aload(request)
        async def custom(value: SkillLoadRequest) -> SkillDocument:
            return await operations.aload(value)
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(Workflow("custom", nodes=[Node("read", custom)]), request)
            self.assertEqual(result.status, "completed", result.error)
            restored = ValueContract.create(SkillDocument, location="skill").restore(result.output)
            self.assertEqual(restored, direct)
        finally:
            await app.aclose()

    async def test_script_node_uses_explicit_runner_and_returns_portable_result(self):
        """Script Nodes retain their execution binding, stdin, arguments and snapshot revision."""
        operations = self.fixture()
        app = AutoAgentApp()
        try:
            request = SkillRunScriptRequest(name="report", path="scripts/run.py", arguments=("hello",), stdin="input")
            result = await app.ainvoke(Workflow("script", nodes=[Node("run", operations.as_operator("run_script"))]), request)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(result.output["stdout"], "hello input\n")
            self.assertIsNotNone(result.output["snapshot_revision"])
        finally:
            await app.aclose()

    async def test_unconfigured_invalid_and_mismatched_scripts_are_rejected(self):
        """Typed adapters cannot bypass script namespace or returned identity checks."""
        operations = self.fixture()
        with self.assertRaises(ValueError):
            SkillOperations(operations.skills).as_operator("run_script")
        with self.assertRaises(ValueError):
            operations.as_operator("unknown")
        class Wrong:
            async def arun(self, skill, path, **kwargs):
                return SkillScriptResult(skill_name="other", path=path, revision="a" * 64, returncode=0, stdout="", stderr="")
        wrong = SkillOperations(operations.skills, runner=Wrong())
        with self.assertRaises(ValueError):
            await wrong.arun_script(SkillRunScriptRequest(name="report", path="scripts/run.py"))
        with self.assertRaises(SkillScriptError):
            await wrong.arun_script(SkillRunScriptRequest(name="report", path="references/rules.txt"))
        with self.assertRaises(SkillResourceError):
            await operations.aread_resource(SkillReadResourceRequest(name="report", path="../outside"))

    async def test_request_models_round_trip_through_core(self):
        """All adapter inputs have durable schemas without embedding runtime resources."""
        requests = (
            SkillLoadRequest(name="report"), SkillListResourcesRequest(name="report"),
            SkillReadResourceRequest(name="report", path="references/rules.txt"),
            SkillReadTextRequest(name="report", path="references/rules.txt"),
            SkillRunScriptRequest(name="report", path="scripts/run.py", arguments=("one",)),
            MCPReadResourceRequest(uri="asset://report"), MCPGetPromptRequest(name="summary", arguments={"topic": "one"}),
        )
        for request in requests:
            contract = ValueContract.create(type(request), location="request")
            self.assertEqual(contract.restore(contract.to_record(request)), request)


@unittest.skipUnless(mcp_tests.httpx2 is not None, "Install autoagent[mcp] for SDK tests")
class MCPOperationTests(unittest.IsolatedAsyncioTestCase):
    client = mcp_tests.MCPHTTPTests.client

    async def test_resource_and_prompt_nodes_use_actual_sdk_lifespan(self):
        """Both typed MCP operations run on the client loop and retain normalized data."""
        client, http = self.client(mcp_tests.WireServer())
        operations = MCPOperations(client)
        @asynccontextmanager
        async def resources():
            try:
                async with client:
                    yield
            finally:
                await http.aclose()
        app = AutoAgentApp(runtime_lifespan=resources)
        try:
            await app.aprepare()
            resource = await app.ainvoke(Workflow("resource", nodes=[Node("read", operations.as_operator("read_resource"))]),
                                        MCPReadResourceRequest(uri="asset://report"))
            self.assertEqual(resource.status, "completed", resource.error)
            self.assertEqual(resource.output["contents"][0]["content"]["text"], "report text")
            async def custom(request: MCPReadResourceRequest) -> MCPResourceResponse:
                return await operations.aread_resource(request)
            composed = await app.ainvoke(Workflow("custom_resource", nodes=[Node("read", custom)]),
                                        MCPReadResourceRequest(uri="asset://report"))
            self.assertEqual(composed.status, "completed", composed.error)
            self.assertEqual(composed.output, resource.output)
            prompt = await app.ainvoke(Workflow("prompt", nodes=[Node("get", operations.as_operator("get_prompt"))]),
                                      MCPGetPromptRequest(name="summary"))
            self.assertEqual(prompt.status, "completed", prompt.error)
            self.assertEqual(prompt.output["messages"][0]["content"][0]["text"], "Summarize")
        finally:
            await app.aclose()
        self.assertTrue(http.is_closed)

    async def test_standalone_reads_and_unknown_operation_validation(self):
        """Standalone calls return normalized content and invalid adapters fail early."""
        client, _http = self.client(mcp_tests.WireServer())
        operations = MCPOperations(client)
        async with client:
            direct = await operations.aread_resource(MCPReadResourceRequest(uri="asset://report"))
            self.assertEqual(direct.content[0].text, "report text")
        with self.assertRaises(ValueError):
            operations.as_operator("unknown")
