"""Local Skill format, filesystem boundaries, Tools and real Provider integration."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from pydantic import ValidationError

from autoagent import AutoAgentApp, Node, ValueContract, Workflow
from autoagent.agent import (
    AudioContent, FileContent, ImageContent, LLMMessage, LLMRequest, LLMToolCall,
    LLMToolResult, OpenAIChatCompletionsProvider, OpenAIProviderConfig, OpenAIResponsesProvider,
    Skill, SkillChangedError, SkillDocument, SkillError, SkillFormatError,
    SkillLimitError, SkillLimits, SkillMetadata, SkillNotFoundError, SkillResource,
    SkillResourceContent, SkillResourceError, SkillSet, TextContent, Tool, ToolSet,
    VideoContent, tool,
)

try:
    import yaml
except ImportError:
    yaml = None
try:
    import httpx
    from openai import AsyncOpenAI
except ImportError:
    httpx = None
    AsyncOpenAI = None


def make_skill(root: Path, *, name="inventory-report", header=None, body="Use the supplied inventory rules.", resources=None):
    directory = root / name
    directory.mkdir(parents=True, exist_ok=True)
    header = header if header is not None else f"name: {name}\ndescription: Analyze inventory and replenishment risk."
    (directory / "SKILL.md").write_text(f"---\n{header}\n---\n{body}\n", encoding="utf-8")
    for path, data in (resources or {}).items():
        target = directory / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    return directory


def call(name="skill_load", arguments=None):
    return LLMToolCall(name=name, call_id="call-1", arguments=json.dumps(arguments or {"name": "inventory-report"}))


class SkillModelTests(unittest.TestCase):
    def test_metadata_enforces_standard_names_lengths_and_string_metadata(self):
        """Reject unsupported names, empty descriptions and implicit metadata coercion."""
        for name in ("", "Upper", "with space", "-name", "name-", "two--parts", "a" * 65, "report\n"):
            with self.subTest(name=name), self.assertRaises(ValidationError):
                SkillMetadata(name=name, description="Use for reports.")
        for fields in ({"description": " "}, {"description": "a" * 1025}, {"compatibility": "a" * 501}, {"metadata": {"version": 1}}, {"extensions": {"number": float("nan")}}):
            with self.subTest(fields=fields), self.assertRaises(ValidationError):
                SkillMetadata.model_validate({"name": "report", "description": "Reports", **fields})

    def test_loaded_models_round_trip_through_core_without_runtime_resources(self):
        """Serialize documents, resource bytes and limits through strict Core contracts."""
        metadata = SkillMetadata(name="report", description="Reports", extensions={"flag": True})
        resource = SkillResource(path="references/rules.md", size_bytes=3, mime_type="text/plain")
        document = SkillDocument(metadata=metadata, instructions="Follow rules.", revision="a" * 64)
        content = SkillResourceContent(skill_name="report", resource=resource, revision="b" * 64, content=(TextContent(text="abc"),))
        for value in (metadata, resource, document, content, SkillLimits()):
            with self.subTest(model=type(value).__name__):
                contract = ValueContract.create(type(value), location="skill")
                self.assertEqual(contract.restore(contract.to_record(value)), value)
        self.assertEqual(document.to_message().role, "user")
        self.assertEqual(document.to_message(role="developer").role, "developer")
        self.assertEqual(json.loads(document.to_message().content[0].text)["instructions"], document.instructions)

    def test_import_and_construction_need_no_optional_sdks_or_yaml(self):
        """Keep package imports and empty collections independent of integration extras."""
        root = Path(__file__).resolve().parents[1]
        source = f"""
import importlib.abc
import sys
sys.path.insert(0, {str(root)!r})
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {{'yaml', 'openai', 'mcp', 'mcp_types', 'httpx2', 'jsonschema', 'referencing'}}:
            raise ImportError('blocked optional dependency')
sys.meta_path.insert(0, Block())
from autoagent.agent import SkillSet, SkillMetadata, SkillLimits
assert SkillSet().catalog_message() is None
assert SkillSet().as_tools() == ()
SkillMetadata(name='report', description='Reports')
SkillLimits()
"""
        result = subprocess.run([sys.executable, "-I", "-c", source], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)


@unittest.skipUnless(yaml is not None, "Install autoagent[skills] for YAML tests")
class SkillFilesystemTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_standard_frontmatter_optional_fields_and_extensions_are_preserved(self):
        """Read standard YAML keys while retaining client-specific JSON fields."""
        directory = make_skill(self.root, header="""name: inventory-report
description: >-
  Analyze inventory and
  replenishment risk.
license: MIT
compatibility: Requires inventory data.
metadata:
  author: autoagent
  version: "1"
allowed-tools: inventory_get skill_read_resource
disable-model-invocation: true
custom:
  labels: [one, two]
""")
        skill = Skill.from_directory(directory)
        self.assertEqual(skill.metadata.allowed_tools, "inventory_get skill_read_resource")
        self.assertEqual(skill.metadata.metadata, {"author": "autoagent", "version": "1"})
        self.assertEqual(skill.metadata.extensions["custom"], {"labels": ["one", "two"]})
        self.assertEqual(skill.load().metadata, skill.metadata)

    def test_bom_crlf_and_markdown_body_are_supported(self):
        """Extract Markdown after frontmatter without confusing later separators."""
        directory = make_skill(self.root)
        data = b"\xef\xbb\xbf---\r\nname: inventory-report\r\ndescription: Reports\r\n---\r\n# Instructions\r\n\r\n---\r\nMore\r\n"
        (directory / "SKILL.md").write_bytes(data)
        document = Skill(directory).load()
        self.assertEqual(document.instructions, "# Instructions\r\n\r\n---\r\nMore")
        self.assertEqual(document.revision, hashlib.sha256(data).hexdigest())

    def test_malformed_yaml_missing_fields_and_unsafe_yaml_fail(self):
        """Reject ambiguous metadata, duplicate keys, aliases and arbitrary constructors."""
        headers = (
            "name: inventory-report", "name: other\ndescription: Reports",
            "name: inventory-report\ndescription: first\ndescription: second",
            "name: inventory-report\ndescription: [invalid]",
            "name: inventory-report\ndescription: !!python/object/apply:os.system [echo bad]",
            "name: inventory-report\ndescription: &desc Reports\ncustom: *desc",
            "name: inventory-report\ndescription: Reports\ncustom: .nan",
            "name: inventory-report\ndescription: Reports\ncustom: 2026-10-14",
            "name: inventory-report\ndescription: Reports\nmetadata:\n  author: one\n  author: two",
            "[name, description]", "name: inventory-report\ndescription: Reports\n? [key]\n: value",
        )
        for header in headers:
            with self.subTest(header=header):
                directory = make_skill(self.root, header=header)
                with self.assertRaises(SkillFormatError):
                    Skill(directory)

    def test_missing_delimiters_and_invalid_encoding_fail(self):
        """Require explicit UTF-8 YAML frontmatter rather than guessing file structure."""
        directory = make_skill(self.root)
        for data in (b"# Instructions", b"---\nname: inventory-report", b"\xff"):
            with self.subTest(data=data):
                (directory / "SKILL.md").write_bytes(data)
                with self.assertRaises(SkillFormatError):
                    Skill(directory)

    def test_yaml_nesting_and_node_counts_are_bounded(self):
        """Prevent deeply nested or excessively large YAML graphs during parsing."""
        for extra in ("custom: " + "[" * 40 + "1" + "]" * 40,
                      "custom: [" + ",".join("1" for _ in range(5000)) + "]"):
            with self.subTest(kind=extra[:30]):
                directory = make_skill(self.root, header="name: inventory-report\ndescription: Reports\n" + extra)
                with self.assertRaises(SkillFormatError):
                    Skill(directory)

    def test_missing_yaml_dependency_has_an_actionable_error(self):
        """Load YAML lazily and explain the extra needed when it is unavailable."""
        directory = make_skill(self.root)
        with patch.dict(sys.modules, {"yaml": None}), self.assertRaisesRegex(ImportError, r"autoagent\[skills\]"):
            Skill(directory)

    def test_discovery_uses_explicit_roots_and_direct_children_only(self):
        """Ignore ordinary entries and nested packages while keeping stable name ordering."""
        make_skill(self.root, name="z-report")
        make_skill(self.root, name="a-report")
        make_skill(self.root / "nested", name="hidden-report")
        (self.root / "README.md").write_text("Not a Skill")
        skills = SkillSet.discover([self.root, self.root])
        self.assertEqual([value.name for value in skills.metadata], ["a-report", "z-report"])
        self.assertEqual(json.loads(skills.catalog_message().content[0].text)["available_skills"][0]["name"], "a-report")
        with self.assertRaises(TypeError):
            SkillSet.discover(self.root)
        with self.assertRaises(SkillResourceError):
            SkillSet.discover([self.root / "missing"])
        with self.assertRaises(SkillLimitError):
            SkillSet.discover([self.root], limits=SkillLimits(max_scan_entries=1))

    def test_duplicate_names_and_selection_have_explicit_boundaries(self):
        """Reject collisions and preserve the original collection when selecting Skills."""
        first = Skill(make_skill(self.root / "one"))
        second = Skill(make_skill(self.root / "two"))
        with self.assertRaises(SkillError):
            SkillSet((first, second))
        with self.assertRaises(SkillError):
            SkillSet.discover([self.root / "one", self.root / "two"])
        other = Skill(make_skill(self.root, name="another"))
        skills = SkillSet((first, other))
        selected = skills.select(include=("inventory-report",))
        self.assertEqual(len(selected), 1)
        self.assertEqual(len(skills), 2)
        self.assertEqual(len(skills.select(exclude=("inventory-report",))), 1)
        with self.assertRaises(SkillNotFoundError):
            skills.select(include=("unknown",))
        self.assertIsNone(SkillSet().catalog_message())
        self.assertEqual(SkillSet().as_tools(), ())

    def test_discovery_and_loading_do_not_read_resource_bodies(self):
        """Keep reference content on demand even when full Skill instructions are loaded."""
        directory = make_skill(self.root, resources={"references/rules.md": b"\xff"})
        with patch("autoagent.agent.skills.sources.LocalSkillSource.iter_resources", side_effect=AssertionError("Unexpected enumeration")):
            skills = SkillSet.discover([self.root])
            skills.as_tools()
            source = skills["inventory-report"].source
            with patch.object(source, "open_binary", wraps=source.open_binary) as reads:
                document = skills.load("inventory-report")
                self.assertEqual(document.instructions, "Use the supplied inventory rules.")
                self.assertTrue(all(args.args[0] == "SKILL.md" for args in reads.call_args_list))
        with self.assertRaises(SkillResourceError):
            skills.read_resource("inventory-report", "references/rules.md")

    def test_body_versions_and_loaded_values_remain_detached(self):
        """Changing files affects future reads while preserving previously loaded documents."""
        directory = make_skill(self.root, resources={"references/rules.md": b"old"})
        skill = Skill(directory)
        old = skill.load()
        resource = skill.read_resource("references/rules.md")
        make_skill(self.root, body="New instructions.", resources={"references/rules.md": b"new"})
        new = skill.load()
        self.assertNotEqual(old.revision, new.revision)
        self.assertEqual(old.instructions, "Use the supplied inventory rules.")
        self.assertEqual(resource.content[0].text, "old")
        self.assertEqual(skill.read_resource("references/rules.md").content[0].text, "new")

    def test_metadata_changes_require_rediscovery(self):
        """A stale catalog cannot silently activate a Skill with changed metadata."""
        directory = make_skill(self.root)
        old = SkillSet.discover([self.root])
        make_skill(self.root, header="name: inventory-report\ndescription: Changed purpose.")
        with self.assertRaises(SkillChangedError):
            old.load("inventory-report")
        new = SkillSet.discover([self.root])
        self.assertEqual(new.load("inventory-report").metadata.description, "Changed purpose.")
        self.assertNotEqual(old.metadata, new.metadata)

    def test_metadata_and_definitions_are_detached_from_callers(self):
        """Mutating an exposed metadata copy cannot alter an existing catalog or Tool schema."""
        directory = make_skill(self.root, header='name: inventory-report\ndescription: Reports\nmetadata: {author: original}')
        skill = Skill(directory)
        metadata = skill.metadata
        metadata.metadata["author"] = "changed"
        self.assertEqual(skill.metadata.metadata["author"], "original")
        executable = SkillSet((skill,)).as_tools()[0]
        definition = executable.definition
        definition.parameters["properties"]["name"]["enum"].append("fake")
        self.assertEqual(executable.definition.parameters["properties"]["name"]["enum"], ["inventory-report"])

    def test_relative_paths_reject_traversal_and_absolute_locations(self):
        """Resource access cannot escape the Skill by path syntax."""
        skill = Skill(make_skill(self.root, resources={"rules.md": b"safe"}))
        for path in ("", " ", ".", "..", "../rules.md", "/etc/passwd", "references/../rules.md", "references//rules.md", "C:/data", "C:\\data", "rules.md\x00"):
            with self.subTest(path=path), self.assertRaises(SkillResourceError):
                skill.read_resource(path)
        with self.assertRaises(SkillResourceError):
            skill.read_resource("missing.md")
        with self.assertRaises(SkillResourceError):
            skill.read_resource(".")

    @unittest.skipUnless(hasattr(os, "symlink"), "Symlink support required")
    def test_symlinks_are_constrained_and_skill_root_links_are_not_discovered(self):
        """Allow internal file links while rejecting links outside the selected Skill."""
        directory = make_skill(self.root, resources={"rules.md": b"safe"})
        skill = Skill(directory)
        (self.root / "outside.md").write_text("secret")
        (directory / "internal.md").symlink_to("rules.md")
        self.assertEqual(skill.read_resource("internal.md").content[0].text, "safe")
        (directory / "external.md").symlink_to(self.root / "outside.md")
        with self.assertRaises(SkillResourceError):
            skill.read_resource("external.md")
        with self.assertRaises(SkillResourceError):
            skill.list_resources()
        (self.root / "linked").symlink_to(directory, target_is_directory=True)
        self.assertEqual(len(SkillSet.discover([self.root])), 1)

    def test_replacing_the_root_requires_explicit_rediscovery(self):
        """Do not read from a replacement directory under a previously bound Skill root."""
        directory = make_skill(self.root, resources={"rules.md": b"old"})
        skill = Skill(directory)
        directory.rename(self.root / "moved")
        make_skill(self.root, resources={"rules.md": b"replacement"})
        with self.assertRaises(SkillChangedError):
            skill.read_resource("rules.md")

    @unittest.skipUnless(os.open in os.supports_dir_fd and hasattr(os, "O_NOFOLLOW"), "POSIX descriptor traversal required")
    def test_symlink_swap_between_resolution_and_read_cannot_escape(self):
        """A concurrent file-link swap is rejected by descriptor-relative access."""
        directory = make_skill(self.root, resources={"rules.md": b"safe"})
        outside = self.root / "outside.md"
        outside.write_text("secret")
        skill = Skill(directory)
        from autoagent.agent.skills.files import resolve_resource
        def swap(root, relative):
            resolved = resolve_resource(root, relative)
            resolved.unlink()
            resolved.symlink_to(outside)
            return resolved
        with patch("autoagent.agent.skills.files.resolve_resource", side_effect=swap), self.assertRaises(SkillResourceError):
            skill.read_resource("rules.md")

    def test_portable_file_access_fallback_preserves_normal_read_behavior(self):
        """Platforms without descriptor-relative access still perform bounded regular reads."""
        skill = Skill(make_skill(self.root, resources={"rules.md": b"safe"}))
        with patch("autoagent.agent.skills.files.os.supports_dir_fd", set()):
            self.assertEqual(skill.read_resource("rules.md").content[0].text, "safe")

    @unittest.skipUnless(hasattr(os, "mkfifo"), "FIFO support required")
    def test_nonregular_resources_are_rejected_without_blocking(self):
        """A FIFO is rejected before read rather than waiting indefinitely for a writer."""
        directory = make_skill(self.root)
        skill = Skill(directory)
        os.mkfifo(directory / "pipe")
        with self.assertRaises(SkillResourceError):
            skill.read_resource("pipe")
        with self.assertRaises(SkillResourceError):
            skill.list_resources()

    def test_document_resource_listing_and_depth_limits_fail_explicitly(self):
        """Bound bytes, file counts and traversal depth instead of truncating instructions."""
        directory = make_skill(self.root, resources={"one.md": b"12345", "deep/nested/two.md": b"two"})
        with self.assertRaises(SkillLimitError):
            Skill(directory, limits=SkillLimits(max_document_bytes=8))
        limited = Skill(directory, limits=SkillLimits(max_resource_bytes=4))
        with self.assertRaises(SkillLimitError):
            limited.read_resource("one.md")
        for limits in (SkillLimits(max_scan_entries=1), SkillLimits(max_resource_depth=1)):
            with self.subTest(limits=limits), self.assertRaises(SkillLimitError):
                Skill(directory, limits=limits).list_resources()
        paged = Skill(directory, limits=SkillLimits(max_resources=1))
        self.assertEqual(paged.load().instructions, "Use the supplied inventory rules.")
        self.assertIsNotNone(paged.list_resources().next_cursor)

    def test_resource_prefix_is_a_directory_boundary_and_order_is_stable(self):
        """Filtering references does not match similarly named sibling directories."""
        skill = Skill(make_skill(self.root, resources={"references/b.md": b"b", "references/a.md": b"a", "references-other/x.md": b"x", "assets/template.md": b"t"}))
        self.assertEqual([value.path for value in skill.list_resources(prefix="references/").resources], ["references/a.md", "references/b.md"])
        with self.assertRaises(SkillResourceError):
            skill.list_resources(prefix="../references")

    def test_multimodal_and_unknown_binary_resources_preserve_bytes(self):
        """Map media and files into existing content models without extracting or uploading."""
        data = b"sample bytes"
        resources = {"assets/" + name: data for name in ("image.png", "clip.wav", "video.mp4", "report.pdf", "custom.blob")}
        resources["references/rules.json"] = b'{"rule": true}'
        skill = Skill(make_skill(self.root, resources=resources))
        for name, kind in (("image.png", ImageContent), ("clip.wav", AudioContent), ("video.mp4", VideoContent), ("report.pdf", FileContent), ("custom.blob", FileContent)):
            with self.subTest(name=name):
                result = skill.read_resource("assets/" + name)
                self.assertIsInstance(result.content[0], kind)
                self.assertEqual(base64.b64decode(result.content[0].source.data), data)
                self.assertEqual(result.revision, hashlib.sha256(data).hexdigest())
        self.assertEqual(skill.read_resource("references/rules.json").content[0].text, '{"rule": true}')

    def test_empty_text_is_supported_but_empty_media_is_rejected(self):
        """Keep empty text exact without constructing an invalid empty base64 media source."""
        skill = Skill(make_skill(self.root, resources={"empty.md": b"", "empty.pdf": b""}))
        self.assertEqual(skill.read_resource("empty.md").content[0].text, "")
        with self.assertRaises(SkillResourceError):
            skill.read_resource("empty.pdf")

    def test_scripts_are_read_as_source_without_execution(self):
        """Loading and reading a bundled script never runs its code."""
        marker = self.root / "executed"
        source = f"from pathlib import Path\nPath({str(marker)!r}).touch()\n"
        skill = Skill(make_skill(self.root, resources={"scripts/run.py": source.encode()}))
        self.assertEqual(skill.list_resources().resources[0].path, "scripts/run.py")
        self.assertEqual(skill.read_resource("scripts/run.py").content[0].text, source)
        self.assertFalse(marker.exists())

    def test_skill_tools_use_existing_sync_execution_and_preserve_call_identity(self):
        """Expose bounded Skill reads through ordinary Tool definitions and results."""
        make_skill(self.root, resources={"references/rules.md": b"rules", "assets/report.pdf": b"%PDF sample"})
        tools = SkillSet.discover([self.root]).as_tools()
        self.assertTrue(all(isinstance(value, Tool) and not value.is_async for value in tools))
        self.assertEqual([value.definition.name for value in tools], ["skill_load", "skill_list_resources", "skill_read_resource", "skill_read_text"])
        loaded = tools[0].invoke(call())
        self.assertFalse(loaded.is_error)
        self.assertEqual(loaded.call_id, "call-1")
        self.assertEqual(json.loads(loaded.content[0].text)["skill"]["name"], "inventory-report")
        listing = tools[1].invoke(call("skill_list_resources", {"name": "inventory-report", "prefix": "references"}))
        self.assertEqual(listing.structured_content["resources"][0]["path"], "references/rules.md")
        pdf = tools[2].invoke(call("skill_read_resource", {"name": "inventory-report", "path": "assets/report.pdf"}))
        self.assertIsInstance(pdf.content[0], FileContent)
        self.assertEqual(pdf.tool_metadata["skill"]["path"], "assets/report.pdf")

    def test_skill_tool_failures_are_model_observations(self):
        """Unknown Skills, forbidden paths and invalid arguments do not execute arbitrary reads."""
        make_skill(self.root)
        tools = SkillSet.discover([self.root]).as_tools()
        invocations = (
            (tools[0], call(arguments={"name": "unknown"})),
            (tools[0], call(arguments={"name": 1})),
            (tools[0], call(arguments={"name": "inventory-report", "extra": True})),
            (tools[2], call("skill_read_resource", {"name": "inventory-report", "path": "../secret"})),
        )
        for executable, incoming in invocations:
            with self.subTest(arguments=incoming.arguments):
                result = executable.invoke(incoming)
                self.assertTrue(result.is_error)
                self.assertEqual(result.call_id, incoming.call_id)
        with self.assertRaises(TypeError):
            SkillSet.discover([self.root]).as_tools(prefix="invalid prefix ")


@unittest.skipUnless(yaml is not None, "Install autoagent[skills] for integration tests")
class SkillIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        make_skill(self.root, resources={"references/rules.md": b"Inventory must come from actual data.", "assets/report.pdf": b"%PDF sample"})

    async def test_skill_tools_integrate_with_core_operator_contracts(self):
        """Core executes local reads through its sync Operator pool and restores portable output."""
        tools = ToolSet(SkillSet.discover([self.root]).as_tools())
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(Workflow("skill-read", nodes=[Node("read", tools.as_operator())]), call())
            self.assertEqual(result.status, "completed", result.error)
            restored = ValueContract.create(LLMToolResult, location="skill").restore(result.output)
            self.assertEqual(json.loads(restored.content[0].text)["skill"]["name"], "inventory-report")
        finally:
            await app.aclose()

    @unittest.skipUnless(AsyncOpenAI is not None, "Install autoagent[agent] for Provider tests")
    async def test_real_sdk_replays_loaded_skill_for_both_protocols(self):
        """Round-trip Skill load calls through actual OpenAI SDK encoding and Tool execution."""
        from tests.test_agent_providers import chat_body, response_body, output_message
        skills = SkillSet.discover([self.root])
        tools = ToolSet(skills.as_tools())
        for protocol in ("responses", "chat"):
            with self.subTest(protocol=protocol):
                captured = []
                async def handle(request):
                    payload = json.loads(request.content)
                    captured.append(payload)
                    if len(captured) == 1:
                        arguments = '{"name":"inventory-report"}'
                        if protocol == "responses":
                            reply = response_body(output=[{"type": "function_call", "id": "fc-1", "call_id": "call-1", "name": "skill_load", "arguments": arguments}])
                        else:
                            reply = chat_body(message={"role": "assistant", "content": None, "tool_calls": [{"id": "call-1", "type": "function", "function": {"name": "skill_load", "arguments": arguments}}]})
                    else:
                        if protocol == "responses":
                            record = next(item for item in payload["input"] if item.get("type") == "function_call_output")
                            content = record["output"][0]["text"]
                            reply = response_body(output=[output_message("Skill loaded")])
                        else:
                            record = next(item for item in payload["messages"] if item.get("role") == "tool")
                            content = record["content"]
                            reply = chat_body(message={"role": "assistant", "content": "Skill loaded"})
                        self.assertEqual(json.loads(content)["instructions"], skills.load("inventory-report").instructions)
                    return httpx.Response(200, json=reply)

                async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
                    sdk = AsyncOpenAI(api_key="test", max_retries=0, http_client=http)
                    sdk._platform = "Linux"
                    provider_type = OpenAIResponsesProvider if protocol == "responses" else OpenAIChatCompletionsProvider
                    provider = provider_type(OpenAIProviderConfig(provider_id="test"), client=sdk)
                    try:
                        initial = LLMRequest(model="model", input=(skills.catalog_message(), LLMMessage(role="user", content=(TextContent(text="Analyze inventory."),))), tools=tools.definitions)
                        response = await provider.ainvoke(initial)
                        observation = await tools.ainvoke(response.tool_calls[0])
                        final = await provider.ainvoke(LLMRequest(model="model", input=(*initial.input, *response.output, observation), tools=tools.definitions))
                        self.assertEqual(final.text, "Skill loaded")
                        definition = captured[0]["tools"][0]
                        if protocol == "chat":
                            definition = definition["function"]
                        self.assertEqual(definition["parameters"]["properties"]["name"]["enum"], ["inventory-report"])
                    finally:
                        await provider.aclose()
                        await sdk.close()

    @unittest.skipUnless(AsyncOpenAI is not None, "Install autoagent[agent] for Provider tests")
    async def test_loaded_pdf_encodes_through_responses_as_a_tool_result(self):
        """Skill binary resources become real SDK PDF file inputs with intact bytes."""
        from tests.test_agent_providers import response_body
        tools = ToolSet(SkillSet.discover([self.root]).as_tools())
        incoming = call("skill_read_resource", {"name": "inventory-report", "path": "assets/report.pdf"})
        result = await tools.ainvoke(incoming)
        captured = []
        async def handle(request):
            captured.append(json.loads(request.content))
            return httpx.Response(200, json=response_body())
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            sdk = AsyncOpenAI(api_key="test", http_client=http)
            sdk._platform = "Linux"
            provider = OpenAIResponsesProvider(client=sdk)
            try:
                await provider.ainvoke(LLMRequest(model="model", input=(incoming, result)))
                encoded = captured[0]["input"][1]["output"][0]
                self.assertEqual(encoded["type"], "input_file")
                self.assertEqual(encoded["filename"], "report.pdf")
                self.assertEqual(base64.b64decode(encoded["file_data"].split(",", 1)[1]), b"%PDF sample")
            finally:
                await provider.aclose()
                await sdk.close()

    async def test_skill_tools_and_mcp_tools_share_a_toolset(self):
        """Combine local Skill access and remote MCP calls without coupling their lifetimes."""
        try:
            import httpx2
            from autoagent.agent import MCPClient, MCPStreamableHTTPConfig
            from tests.test_agent_mcp import WireServer
        except ImportError:
            self.skipTest("Install autoagent[mcp] for remote tool tests")
        server = WireServer()
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(server.handle)) as http:
            async with MCPClient(MCPStreamableHTTPConfig(server_id="remote", url="https://server.test/mcp"), http_client=http) as client:
                skills = SkillSet.discover([self.root])
                tools = ToolSet((*skills.as_tools(), *await client.tools(prefix="remote_")))
                loaded = await tools.ainvoke(call())
                remote = await tools.ainvoke(call("remote_add", {"a": 2, "b": 4}))
                self.assertFalse(loaded.is_error)
                self.assertEqual(remote.structured_content, {"sum": 6})
                self.assertEqual(len(tools), 5)

    async def test_offline_example_completes_explicit_and_tool_activation(self):
        """Run both standalone application flows and retain missing-data handling."""
        from examples.agent_skills_demo.run import OfflineProvider, run_demo
        closed = []
        original = OfflineProvider.aclose
        async def close(provider):
            await original(provider)
            closed.append(provider.closed)
        with patch.object(OfflineProvider, "aclose", new=close):
            explicit = await run_demo(activation="explicit")
            dynamic = await run_demo(activation="tools")
        self.assertEqual(explicit.text, dynamic.text)
        self.assertIn("预计可售 4 天", explicit.text)
        self.assertIn("最低补货数量 15", explicit.text)
        self.assertIn("C 缺少 daily_sales", explicit.text)
        self.assertEqual(closed, [True, True])

    async def test_offline_tool_example_needs_no_provider_or_mcp_sdk(self):
        """Execute the full offline Tool flow while optional integration SDK imports are blocked."""
        root = Path(__file__).resolve().parents[1]
        source = f"""
import importlib.abc
import sys
sys.path.insert(0, {str(root)!r})
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {{'openai', 'mcp', 'mcp_types', 'httpx2', 'jsonschema', 'referencing'}}:
            raise ImportError('blocked optional dependency')
sys.meta_path.insert(0, Block())
import asyncio
from examples.agent_skills_demo.run import run_demo
response = asyncio.run(run_demo(activation='tools'))
assert '最低补货数量 15' in response.text
"""
        result = await asyncio.to_thread(subprocess.run, [sys.executable, "-I", "-c", source], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    async def test_example_call_limit_still_closes_the_provider(self):
        """A bounded demo cannot continue indefinitely or leak its Provider."""
        from examples.agent_skills_demo.run import OfflineProvider, run_demo
        provider = OfflineProvider()
        with patch("examples.agent_skills_demo.run.OfflineProvider", return_value=provider):
            with self.assertRaisesRegex(RuntimeError, "model-call limit"):
                await run_demo(activation="tools", max_rounds=1)
        self.assertTrue(provider.closed)

    async def test_example_keeps_errors_from_repeated_resource_tool_calls(self):
        """A later successful read cannot hide an earlier failure with the same Tool name."""
        from examples.agent_skills_demo.run import run_demo
        # The rules file is absent while the later template read succeeds.
        directory = self.root / "inventory-report"
        (directory / "references/rules.md").unlink()
        (directory / "assets/report-template.md").write_text("# Report template")
        with self.assertRaisesRegex(RuntimeError, "demo Tool failed"):
            await run_demo(activation="tools", skill_root=self.root)

    @unittest.skipUnless(AsyncOpenAI is not None, "Install autoagent[agent] for live-mode wiring tests")
    async def test_live_example_wires_both_providers_to_real_sdk_mock_transports(self):
        """Verify the live-mode path and request content without contacting paid endpoints."""
        from examples.agent_skills_demo.run import run_demo
        from tests.test_agent_providers import response_body, chat_body
        for protocol in ("responses", "chat"):
            with self.subTest(protocol=protocol):
                captured = []
                async def handle(request):
                    captured.append(json.loads(request.content))
                    return httpx.Response(200, json=response_body() if protocol == "responses" else chat_body())
                async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
                    sdk = AsyncOpenAI(api_key="test", http_client=http)
                    sdk._platform = "Linux"
                    provider_type = OpenAIResponsesProvider if protocol == "responses" else OpenAIChatCompletionsProvider
                    provider = provider_type(client=sdk)
                    target = "OpenAIResponsesProvider" if protocol == "responses" else "OpenAIChatCompletionsProvider"
                    try:
                        with patch.dict(os.environ, {"OPENAI_API_KEY": "test", "LLM_MODEL": "live-model"}), patch("examples.agent_skills_demo.run." + target, return_value=provider):
                            result = await run_demo(mode="live", protocol=protocol)
                        self.assertEqual(result.text, "Answer")
                        self.assertEqual(captured[0]["model"], "live-model")
                        encoded = json.dumps(captured[0], ensure_ascii=False)
                        self.assertIn("inventory-report", encoded)
                        self.assertIn("缺货风险", encoded)
                        self.assertTrue(provider._closed)
                    finally:
                        await provider.aclose()
                        await sdk.close()


if __name__ == "__main__":
    unittest.main()
