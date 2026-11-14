"""File permission boundaries, real mutations and independently concurrent callers."""

import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from autoagent.agent import (
    AgentResources, FileTools, AgentInput, FileToolsConfig, LLMToolCall, LocalFileService,
    Tool, ToolApprovalRequired, ToolContext, ToolDecision, ToolOperation, ToolOperationPolicy,
    ToolPolicy, ToolSet, agent_workflow, file_tools,
    AgentApprovalRequest, AgentApprovalResponse, LLMResponse, LLMToolResult,
)
from autoagent.agent.context import restore
from autoagent.host import AutoAgentHost, HostSettings
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, settings


def call(name, **arguments):
    return LLMToolCall(name=name, call_id=name, arguments=json.dumps(arguments))


def path_rule(pattern, decision="ask", actions=("read", "write", "delete")):
    return dict(when={"all": [dict(field="operation.attributes.path", op="glob", value=pattern),
        dict(field="operation.action", op="in", value=["file." + action for action in actions])]}, decision=decision)


def file_config(*rules, external=None):
    selected = list(rules)
    if external is not None:
        selected.append(dict(when=dict(field="operation.attributes.scope", op="equals", value="external"), decision=external))
    return FileToolsConfig(policy=ToolPolicy(rules=selected))


class FileToolsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "workspace"
        self.root.mkdir()
        self.service = LocalFileService(self.root, config=file_config(path_rule(".env"), external="ask"))
        self.tools = file_tools(self.service, context=ToolContext(session_id="one"))

    def invoke(self, name, **arguments):
        return self.tools.invoke(call(name, **arguments))

    def test_catalog_retains_six_typed_tools(self):
        """All six definitions have schemas and file management is discriminated."""
        definitions = {value.name: value for value in self.tools.definitions}
        self.assertEqual(set(definitions), {"list_files", "search_files", "read_file", "write_file", "edit_file", "manage_files"})
        self.assertIn("oneOf", str(definitions["manage_files"].parameters))

    def test_file_operation_contract_is_normalized_and_transfers_have_independent_targets(self):
        """File Tools declare consistent Operation facts without advertising them as model arguments."""
        from jsonschema import validate
        from autoagent.agent import FILE_OPERATION_SCHEMA
        (self.root / 'source').write_text('content')
        proposal = call('manage_files', request={'operation': 'copy', 'path': 'source', 'destination': 'target'})
        operations = self.tools._prepare(proposal, None).context.operations
        self.assertEqual([op.action for op in operations], ['file.read', 'file.write'])
        self.assertEqual([op.attributes['path'] for op in operations], ['source', 'target'])
        for operation in operations:
            validate(operation.model_dump(mode='json'), FILE_OPERATION_SCHEMA)
            self.assertTrue(Path(operation.resource).is_absolute())
            self.assertEqual(operation.attributes['scope'], 'workspace')
        for tool in self.tools._tools.values():
            self.assertEqual(tool.operation_schema, FILE_OPERATION_SCHEMA)
            self.assertNotIn('operation_schema', tool.definition.parameters['properties'])

    def test_pages_allow_new_limit_and_report_exact_search_totals_only_when_known(self):
        """Changing page size preserves cursors while partial counts never pretend to be totals."""
        for name in ("a", "b", "c"):
            (self.root / name).write_text("needle\n")
        first = self.invoke("list_files", limit=1).structured_content
        self.assertTrue(first["has_more"])
        second = self.invoke("list_files", limit=10, cursor=first["cursor"]).structured_content
        self.assertEqual([row["path"] for row in second["entries"]], ["b", "c"])
        self.assertFalse(second["has_more"])
        self.assertFalse(second["truncated"])
        first = self.invoke("search_files", query="needle", mode="count", limit=1).structured_content
        self.assertTrue(first["has_more"])
        self.assertEqual(first["count_scope"], "page")
        self.assertNotIn("total_count", first)
        last = self.invoke("search_files", query="needle", mode="count", limit=10, cursor=first["cursor"]).structured_content
        self.assertFalse(last["has_more"])
        self.assertNotIn("total_count", last)
        full = self.invoke("search_files", query="needle", mode="count").structured_content
        self.assertEqual(full["total_count"], 3)
        (self.root / ".env").write_text("needle\n")
        protected = self.invoke("search_files", query="needle", mode="count", include_hidden=True).structured_content
        self.assertNotIn("total_count", protected)

    def test_read_metadata_and_failed_mutation_guidance_preserve_file(self):
        """Reads expose snapshot metadata and ambiguous or nonempty mutations give corrective feedback."""
        target = self.root / "a"
        target.write_text("same\nsame\n")
        result = self.invoke("read_file", path="a").structured_content
        self.assertEqual(result["size_bytes"], len(target.read_bytes()))
        self.assertEqual(result["modified_at_ns"], target.stat().st_mtime_ns)
        result = self.invoke("edit_file", path="a", edits=[dict(old_text="same", new_text="other")])
        self.assertIn("2 times", result.structured_content["error"]["message"])
        self.assertEqual(target.read_text(), "same\nsame\n")
        (self.root / "directory").mkdir()
        (self.root / "directory" / "keep").touch()
        result = self.invoke("manage_files", request=dict(operation="delete", path="directory"))
        self.assertEqual(result.structured_content["error"]["kind"], "directory_not_empty")
        self.assertTrue((self.root / "directory" / "keep").exists())

    def test_cursor_failure_distinguishes_query_change_from_directory_change(self):
        """Continuation failures identify what changed without silently restarting or duplicating rows."""
        for name in ("a", "b"):
            (self.root / name).touch()
        first = self.invoke("list_files", limit=1).structured_content
        changed = self.invoke("list_files", pattern="*.py", cursor=first["cursor"])
        self.assertIn("filters", changed.structured_content["error"]["message"])
        (self.root / "c").touch()
        changed = self.invoke("list_files", cursor=first["cursor"])
        self.assertIn("Directory contents changed", changed.structured_content["error"]["message"])

    def test_oversized_file_reports_actual_limit_and_a_supported_next_step(self):
        """A file-size rejection explains why smaller read pages cannot solve the problem."""
        (self.root / "large").write_bytes(b"x" * 20)
        tools = file_tools(LocalFileService(self.root, config=FileToolsConfig(max_file_bytes=10)),
            context=ToolContext(session_id="one"))
        result = tools.invoke(call("read_file", path="large"))
        self.assertEqual(result.structured_content["error"]["kind"], "limit_exceeded")
        self.assertIn("20 bytes", result.structured_content["error"]["message"])
        self.assertIn("max_file_bytes=10", result.structured_content["error"]["message"])

    def test_search_does_not_claim_exact_total_when_directory_access_blocks_traversal(self):
        """An unreadable subtree is labelled without claiming its contents were searched."""
        (self.root / "locked").mkdir()
        (self.root / "locked" / "hidden.txt").write_text("needle")
        tools = file_tools(LocalFileService(self.root, config=file_config(path_rule("locked", actions=("list",), decision="deny"))), context=ToolContext(session_id="one"))
        value = tools.invoke(call("search_files", query="needle")).structured_content
        self.assertEqual(value["match_count"], 0)
        self.assertNotIn("total_count", value)
        self.assertEqual(value["protected_directories"], [dict(path="locked", list="deny", content_searched=False)])

    def test_listing_labels_protected_names_without_reading_them(self):
        """Ask and deny files remain visible with access labels, never content."""
        (self.root / ".env").write_text("SECRET_VALUE")
        (self.root / "denied.txt").write_text("DENIED_VALUE")
        service = LocalFileService(self.root, config=file_config(path_rule(".env"), path_rule("denied.txt", decision="deny")))
        result = file_tools(service).invoke(call("list_files", include_hidden=True))
        rows = {value["path"]: value for value in result.structured_content["entries"]}
        self.assertEqual(rows[".env"]["permissions"]["read"], "ask")
        self.assertEqual(rows["denied.txt"]["permissions"]["read"], "deny")
        self.assertNotIn("SECRET_VALUE", result.model_dump_json())
        self.assertNotIn("DENIED_VALUE", result.model_dump_json())

    def test_protected_search_does_not_leak_matching_or_content(self):
        """Protected search candidates are listed even when the query would not match."""
        (self.root / ".env").write_text("SECRET_VALUE")
        result = self.invoke("search_files", query="absent", include_hidden=True)
        self.assertEqual(result.structured_content["match_count"], 0)
        self.assertFalse(result.structured_content["protected_files"][0]["content_searched"])
        self.assertNotIn("SECRET_VALUE", result.model_dump_json())

    def test_secret_and_external_access_require_exact_approval(self):
        """External and sensitive reads use approvals bound to the resolved operation."""
        (self.root / ".env").write_text("SECRET_VALUE")
        outside = self.root.parent / "outside.txt"
        outside.write_text("OUTSIDE")
        for path in (".env", str(outside)):
            request = call("read_file", path=path)
            decision = self.tools.check(request)
            self.assertEqual(decision.action, "ask")
            self.assertTrue(decision.approval_request.operations)
            with self.assertRaises(ToolApprovalRequired):
                self.tools.invoke(request)
            result = self.tools.invoke(request, approval=decision.approval_request.respond(approved=True))
            self.assertFalse(result.is_error)
        mismatch = self.tools.invoke(call("read_file", path=str(outside)),
            approval=self.tools.check(call("read_file", path=".env")).approval_request.respond(approved=True))
        self.assertTrue(mismatch.is_error)

    def test_explicit_secret_search_can_be_approved(self):
        """An approved exact-file search reads the selected sensitive content."""
        (self.root / ".env").write_text("SECRET_VALUE")
        request = call("search_files", path=".env", query="SECRET")
        decision = self.tools.check(request)
        self.assertEqual(decision.action, "ask")
        result = self.tools.invoke(request, approval=decision.approval_request.respond(approved=True))
        self.assertEqual(result.structured_content["results"][0]["text"], "SECRET_VALUE")

    def test_deny_cannot_be_overridden_by_bypass(self):
        """Bypass may skip ask but cannot override file policy deny."""
        service = LocalFileService(self.root, config=file_config(path_rule("blocked", decision="deny")))
        tools = file_tools(service)
        tools.approval_mode = "bypass"
        self.assertEqual(tools.check(call("read_file", path="blocked")).action, "deny")

    def test_text_pages_unicode_bom_and_next_line(self):
        """Numbered bounded reads expose continuation and preserve Unicode text."""
        (self.root / "text").write_bytes(b"\xef\xbb\xbf" + "你好\nworld\nlast".encode())
        result = self.invoke("read_file", path="text", max_lines=1)
        self.assertEqual(result.structured_content["next_line"], 2)
        self.assertIn("version", result.structured_content)
        self.assertNotIn("encoding", result.structured_content)
        self.assertIn("encoding", result.tool_metadata["file"])
        self.assertIn("你好", result.content[0].text)
        next_page = self.invoke("read_file", path="text", start_line=2, max_lines=2)
        self.assertIsNone(next_page.structured_content["next_line"])

    def test_create_overwrite_and_repeated_edit_need_no_read_or_version(self):
        """Creation is non-clobbering while explicit replacement and exact edits need no read round-trip."""
        self.assertFalse(self.invoke("write_file", path="a", content="old").is_error)
        self.assertTrue(self.invoke("write_file", path="a", content="new").is_error)
        self.assertFalse(self.invoke("write_file", path="a", content="new", overwrite=True).is_error)
        self.assertFalse(self.invoke("write_file", path="a", content="one two", overwrite=True).is_error)
        self.assertFalse(self.invoke("edit_file", path="a", edits=[{"old_text":"one", "new_text":"ONE"}]).is_error)
        self.assertFalse(self.invoke("edit_file", path="a", edits=[{"old_text":"two", "new_text":"TWO"}]).is_error)
        self.assertFalse(self.invoke("write_file", path="a", content="new", overwrite=True).is_error)
        self.assertEqual((self.root / "a").read_text(), "new")

    def test_unique_edits_commit_together_and_reject_overlap(self):
        """Invalid multi-edits leave the file intact; valid replacements commit once."""
        path = self.root / "a"
        path.write_text("alpha beta gamma")
        self.invoke("read_file", path="a")
        invalid = self.invoke("edit_file", path="a", edits=[
            {"old_text": "alpha beta", "new_text": "x"}, {"old_text": "beta", "new_text": "y"}])
        self.assertTrue(invalid.is_error)
        self.assertEqual(path.read_text(), "alpha beta gamma")
        result = self.invoke("edit_file", path="a", edits=[
            {"old_text": "alpha", "new_text": "one"}, {"old_text": "gamma", "new_text": "three"}])
        self.assertFalse(result.is_error)
        self.assertEqual(path.read_text(), "one beta three")
        self.assertEqual(set(result.structured_content), {"path", "version", "change"})
        self.assertIn("+one beta three", result.tool_metadata["file"]["diff"])

    def test_explicit_version_does_not_create_a_missing_file(self):
        """An explicit existing-content precondition cannot silently turn into file creation."""
        result = self.invoke("write_file", path="missing", content="new", expected_version="stale", overwrite=True)
        self.assertEqual(result.structured_content["error"]["kind"], "version_conflict")
        self.assertFalse((self.root / "missing").exists())

    def test_full_overwrite_does_not_require_old_content_to_be_text(self):
        """Replacing a regular file with UTF-8 content does not attempt to edit or decode its old bytes."""
        (self.root / "a").write_bytes(b"\x00\xff\x00")
        result = self.invoke("write_file", path="a", content="replacement", overwrite=True)
        self.assertFalse(result.is_error, result)
        self.assertEqual((self.root / "a").read_text(), "replacement")
        self.assertIsNone(result.tool_metadata["file"]["diff"])

    def test_ambiguous_and_absent_matches_are_distinct(self):
        """Ambiguity and absence return actionable errors without partial writes."""
        (self.root / "a").write_text("same same")
        self.invoke("read_file", path="a")
        for text, code in (("same", "ambiguous_match"), ("absent", "no_match")):
            result = self.invoke("edit_file", path="a", edits=[{"old_text": text, "new_text": "x"}])
            self.assertEqual(result.structured_content["error"]["kind"], code)

    def test_two_sessions_cannot_silently_overwrite_the_same_version(self):
        """Concurrent callers that read the same version produce one conflict."""
        (self.root / "a").write_text("original")
        second = file_tools(LocalFileService(self.root), context=ToolContext(session_id="two"))
        observed = self.invoke("read_file", path="a").structured_content["version"]
        second.invoke(call("read_file", path="a"))
        request = call("edit_file", path="a", edits=[{"old_text": "original", "new_text": "changed"}], expected_version=observed)
        with ThreadPoolExecutor(2) as executor:
            futures = [executor.submit(tools.invoke, request) for tools in (self.tools, second)]
            results = [value.result(timeout=10) for value in futures]
        self.assertEqual(sum(value.is_error for value in results), 1)
        error = next(value for value in results if value.is_error)
        self.assertEqual(error.structured_content["error"]["kind"], "version_conflict")

    def test_explicit_stale_version_fails_but_omitted_version_has_no_hidden_read_precondition(self):
        """Only an explicitly supplied version constrains later edits after a successful mutation."""
        (self.root / "a").write_text("one two")
        observed = self.invoke("read_file", path="a").structured_content["version"]
        self.invoke("edit_file", path="a", edits=[{"old_text": "one", "new_text": "ONE"}])
        result = self.invoke("edit_file", path="a", edits=[{"old_text": "two", "new_text": "TWO"}], expected_version=observed)
        self.assertEqual(result.structured_content["error"]["kind"], "version_conflict")
        self.assertFalse(self.invoke("edit_file", path="a", edits=[{"old_text": "two", "new_text": "TWO"}]).is_error)

    def test_big_endian_utf16_and_crlf_are_preserved(self):
        """Precise edits preserve the original BOM, byte order and untouched newlines."""
        path = self.root / "a"
        path.write_bytes(b"\xfe\xff" + "one\r\ntwo\r\n".encode("utf-16-be"))
        self.invoke("read_file", path="a")
        result = self.invoke("edit_file", path="a", edits=[{"old_text": "one", "new_text": "ONE"}])
        self.assertFalse(result.is_error)
        self.assertEqual(path.read_bytes(), b"\xfe\xff" + "ONE\r\ntwo\r\n".encode("utf-16-be"))

    def test_gitignore_nested_rules_and_glob(self):
        """Git ignore negation and nested sources filter traversal consistently."""
        (self.root / ".gitignore").write_text("*.log\n!keep.log\n")
        for name in ("skip.log", "keep.log", "a.py"):
            (self.root / name).write_text("hello")
        result = self.invoke("list_files", pattern="**/*.log", recursive=True)
        self.assertEqual([value["path"] for value in result.structured_content["entries"]], ["keep.log"])
        search = self.invoke("search_files", query="HELLO", case_sensitive=False, include=["**/*.py"])
        self.assertEqual(search.structured_content["results"][0]["path"], "a.py")

    def test_listing_cursor_detects_changes(self):
        """A cursor cannot silently paginate a modified directory snapshot."""
        for name in ("a", "b", "c"):
            (self.root / name).write_text(name)
        result = self.invoke("list_files", limit=1)
        cursor = result.structured_content["cursor"]
        self.assertIsNotNone(cursor)
        (self.root / "d").write_text("d")
        changed = self.invoke("list_files", limit=1, cursor=cursor)
        self.assertEqual(changed.structured_content["error"]["kind"], "listing_changed")

    def test_management_and_nonrecursive_directory_delete(self):
        """Explicit moves/copies never overwrite; nonempty directories cannot be deleted."""
        self.invoke("manage_files", request={"operation": "mkdir", "path": "sub"})
        self.invoke("write_file", path="sub/a", content="one")
        copied = self.invoke("manage_files", request={"operation": "copy", "path": "sub/a", "destination": "sub/b"})
        self.assertFalse(copied.is_error)
        self.assertTrue(self.invoke("manage_files", request={"operation": "delete", "path": "sub"}).is_error)
        self.invoke("read_file", path="sub/a")
        moved = self.invoke("manage_files", request={"operation": "move", "path": "sub/a", "destination": "sub/c"})
        self.assertFalse(moved.is_error)
        self.assertFalse((self.root / "sub/a").exists())

    def test_symlink_outside_workspace_is_ask_and_swap_invalidates_approval(self):
        """A symlink cannot disguise an external read or reuse approval after retargeting."""
        external = self.root.parent / "external"
        external.write_text("SECRET")
        link = self.root / "link"
        link.symlink_to(external)
        request = call("read_file", path="link")
        decision = self.tools.check(request)
        self.assertEqual(decision.action, "ask")
        link.unlink()
        (self.root / "safe").write_text("safe")
        link.symlink_to(self.root / "safe")
        result = self.tools.invoke(request, approval=decision.approval_request.respond(approved=True))
        self.assertTrue(result.is_error)

    def test_write_cannot_leak_original_content_when_read_is_denied(self):
        """Overwrite diff generation requires read permission as well as write permission."""
        (self.root / "secret").write_text("SECRET")
        service = LocalFileService(self.root, config=file_config(path_rule("secret", actions=("read",), decision="deny")))
        result = file_tools(service).invoke(call("write_file", path="secret", content="new",
            overwrite=True, expected_version="anything"))
        self.assertTrue(result.is_error)
        self.assertNotIn("SECRET", result.model_dump_json())

    def test_nested_search_respects_parent_gitignore(self):
        """Directly searching a subdirectory retains ancestor ignore rules."""
        (self.root / ".gitignore").write_text("*.log\n")
        (self.root / "sub").mkdir()
        (self.root / "sub/hidden.log").write_text("needle")
        result = self.invoke("search_files", path="sub", query="needle")
        self.assertEqual(result.structured_content["results"], [])

    def test_declarative_rules_label_and_block_discovered_search_content(self):
        """Session argument rules are applied to each discovered file, not only the search root."""
        (self.root / "secret.txt").write_text("SECRET")
        tools = ToolSet(FileTools(service=self.service).tools(), policy=ToolPolicy(rules=[path_rule("secret.txt", "deny", ("read",))]),
                        context=ToolContext(session_id="one"))
        result = tools.invoke(call("search_files", query="SECRET"))
        self.assertEqual(result.structured_content["protected_files"][0]["read"], "deny")
        self.assertEqual(result.structured_content["results"], [])

    def test_frozen_sibling_writes_accept_new_parent_and_reject_replaced_anchor(self):
        """Creating a shared parent preserves authorization; replacing its anchor does not."""
        proposals = [call("write_file", path=f"new/nested/{name}", content=name, create_parents=True)
            for name in ("a", "b")]
        contexts = [ToolContext(session_id="one", authorization={"tool_binding": {"operations":
            [op.model_dump(mode="json") for op in self.tools._prepare(proposal, None).context.operations]}})
            for proposal in proposals]
        for proposal, context in zip(proposals, contexts):
            result = self.tools.invoke(proposal, context=context)
            self.assertFalse(result.is_error, result)
        self.assertEqual((self.root / "new/nested/b").read_text(), "b")
        directory = self.root / "anchor"
        directory.mkdir()
        proposal = call("write_file", path="anchor/new/file", content="no", create_parents=True)
        operations = self.tools._prepare(proposal, None).context.operations
        directory.rename(self.root / "old-anchor")
        directory.mkdir()
        context = ToolContext(session_id="one", authorization={"tool_binding": {
            "operations": [op.model_dump(mode="json") for op in operations]}})
        result = self.tools.invoke(proposal, context=context)
        self.assertTrue(result.is_error)
        self.assertFalse((directory / "new/file").exists())

    def test_search_merges_context_without_repeating_source_lines(self):
        """Nearby matches share one context block while retaining exact match positions."""
        (self.root / "a").write_text("one\nmatch a\nthree\nmatch b\nfive\nsix\nseven\neight\nmatch c\nten\n")
        result = self.invoke("search_files", query="match", context_lines=1).structured_content
        self.assertEqual(result["match_count"], 3)
        self.assertEqual([row["line"] for row in result["results"]], [2, 4, 9])
        self.assertEqual([(row["start_line"], row["end_line"]) for row in result["contexts"]], [(1, 5), (8, 10)])
        self.assertEqual(result["contexts"][0]["lines"], ["one", "match a", "three", "match b", "five"])
        self.assertTrue(all("text" not in row for row in result["results"]))

    def test_preflight_version_cannot_rebase_after_another_read(self):
        """An execution plan pins the preflight version even if Session observations advance."""
        path = self.root / "a"
        path.write_text("one two")
        observed = self.invoke("read_file", path="a").structured_content["version"]
        request = call("edit_file", path="a", edits=[{"old_text": "two", "new_text": "TWO"}], expected_version=observed)
        prepared = self.tools._prepare(request, None)
        operations = [value.model_dump(mode="json") for value in prepared.context.operations]
        path.write_text("ONE two")
        self.invoke("read_file", path="a")
        context = ToolContext(session_id="one", authorization={"tool_binding": {"operations": operations}})
        result = self.tools.invoke(request, context=context)
        self.assertEqual(result.structured_content["error"]["kind"], "version_conflict")

    def test_regex_timeout_and_invalid_patterns_are_classified(self):
        """Untrusted regexes have bounded matching time and invalid syntax is an observation."""
        (self.root / "a").write_text("a" * 20000 + "!")
        service = LocalFileService(self.root, config=FileToolsConfig(regex_timeout_seconds=0.001))
        tools = file_tools(service)
        invalid = tools.invoke(call("search_files", query="[", regex=True))
        self.assertEqual(invalid.structured_content["error"]["kind"], "invalid_regex")
        result = tools.invoke(call("search_files", query="(a|aa)+$", regex=True))
        self.assertEqual(result.structured_content["error"]["kind"], "regex_timeout")

    def test_policy_and_approval_records_validate_from_plain_dicts(self):
        """HTTP-style dictionaries accept new tuple fields represented as JSON arrays."""
        from autoagent.agent import ToolApprovalRequest
        policy = ToolPolicy.from_dict({"rules": [path_rule("*.secret")]})
        self.assertEqual(policy.rules[0].decision, "ask")
        (self.root / ".env").write_text("secret")
        approval = self.tools.check(call("read_file", path=".env")).approval_request
        self.assertEqual(ToolApprovalRequest.model_validate(approval.model_dump(mode="json")), approval)

    def test_portable_fallback_reads_lists_and_edits_without_descriptor_traversal(self):
        """Backends lacking scandir-fd and dir-fd APIs retain the basic file contract."""
        import os
        (self.root / "a").write_text("old")
        with patch.object(os, "supports_fd", set()), patch.object(os, "supports_dir_fd", set()):
            self.assertFalse(self.invoke("list_files").is_error)
            self.assertFalse(self.invoke("read_file", path="a").is_error)
            result = self.invoke("edit_file", path="a", edits=[{"old_text": "old", "new_text": "new"}])
            self.assertFalse(result.is_error)
        self.assertEqual((self.root / "a").read_text(), "new")

    def test_whitespace_only_replacements_are_valid(self):
        """Indentation and blank-space edits remain expressible without empty matches."""
        (self.root / "a").write_text("    value\n")
        self.invoke("read_file", path="a")
        result = self.invoke("edit_file", path="a", edits=[{"old_text": "    ", "new_text": "  "}])
        self.assertFalse(result.is_error)
        self.assertEqual((self.root / "a").read_text(), "  value\n")


    def test_recursive_scan_budget_resumes_without_duplicates(self):
        """A tree larger than the per-call scan budget is fully reachable by cursor."""
        expected = []
        for directory in ("a", "b", "c"):
            (self.root / directory).mkdir()
            expected.append(directory)
            for index in range(5):
                name = f"{directory}/{index}.txt"
                (self.root / name).write_text("needle\n")
                expected.append(name)
        tools = file_tools(LocalFileService(self.root, config=FileToolsConfig(max_entries=3)))
        cursor, actual, seen = None, [], set()
        for _ in range(30):
            result = tools.invoke(call("list_files", recursive=True, limit=200, cursor=cursor))
            self.assertFalse(result.is_error, result)
            value = result.structured_content
            actual.extend(row["path"] for row in value["entries"])
            cursor = value["cursor"]
            if cursor is None:
                break
            self.assertNotIn(cursor, seen)
            seen.add(cursor)
        else:
            self.fail("Listing did not finish")
        self.assertEqual(actual, expected)

    def test_flat_listing_crosses_directory_batch_boundaries(self):
        """Sorted pages cross internal directory batches without losing the last entries."""
        expected = [f"{index:04}.txt" for index in range(270)]
        for name in reversed(expected):
            (self.root / name).touch()
        actual, cursor = [], None
        for _ in range(5):
            value = self.invoke("list_files", limit=130, cursor=cursor).structured_content
            actual.extend(row["path"] for row in value["entries"])
            cursor = value["cursor"]
            if cursor is None:
                break
        self.assertEqual(actual, expected)

    def test_filtered_entries_do_not_exhaust_scan_budget(self):
        """Hidden and ignored names do not spend the visible-entry traversal budget."""
        for index in range(10):
            (self.root / f".{index}").touch()
            (self.root / f"{index}.log").touch()
        (self.root / ".gitignore").write_text("*.log\n")
        (self.root / "visible.txt").touch()
        tools = file_tools(LocalFileService(self.root, config=FileToolsConfig(max_entries=1)))
        value = tools.invoke(call("list_files")).structured_content
        self.assertEqual([row["path"] for row in value["entries"]], ["visible.txt"])
        self.assertIsNone(value["cursor"])

    def test_generated_directories_are_explicitly_opted_into(self):
        """Generated directories remain discoverable without recursively flooding normal listings."""
        (self.root / "node_modules").mkdir()
        (self.root / "node_modules/a.js").touch()
        value = self.invoke("list_files", recursive=True).structured_content
        self.assertEqual([row["path"] for row in value["entries"]], ["node_modules"])
        value = self.invoke("list_files", recursive=True, include_generated=True).structured_content
        self.assertEqual([row["path"] for row in value["entries"]], ["node_modules", "node_modules/a.js"])
        value = self.invoke("list_files", path="node_modules", recursive=True).structured_content
        self.assertEqual([row["path"] for row in value["entries"]], ["a.js"])

    def test_depth_limit_returns_useful_results_and_paths(self):
        """Depth limits report omitted subtrees without discarding accessible entries."""
        (self.root / "deep/nested").mkdir(parents=True)
        (self.root / "root.txt").touch()
        value = self.invoke("list_files", recursive=True, max_depth=0).structured_content
        self.assertEqual([row["path"] for row in value["entries"]], ["deep", "root.txt"])
        self.assertEqual(value["reason"], "max_depth")
        self.assertEqual(value["depth_limited_paths"], ["deep"])

    def test_pattern_can_resume_after_an_empty_partial_page(self):
        """Nonmatching files can fill the scan budget without hiding later matches forever."""
        for name in ("a.log", "b.log", "c.txt"):
            (self.root / name).touch()
        tools = file_tools(LocalFileService(self.root, config=FileToolsConfig(max_entries=2)))
        first = tools.invoke(call("list_files", pattern="*.txt")).structured_content
        self.assertEqual(first["entries"], [])
        self.assertEqual(first["reason"], "max_entries")
        second = tools.invoke(call("list_files", pattern="*.txt", cursor=first["cursor"])).structured_content
        self.assertEqual(second["entries"][0]["path"], "c.txt")

    def test_search_pages_resume_within_files_and_across_subtrees(self):
        """Match pages preserve exact file/line identity even when a file spans pages."""
        (self.root / "a").mkdir()
        (self.root / "a/one.txt").write_text("needle\n" * 5)
        (self.root / "z.txt").write_text("needle\n" * 3)
        actual, cursor = [], None
        for _ in range(10):
            result = self.invoke("search_files", query="needle", limit=2, cursor=cursor)
            self.assertFalse(result.is_error, result)
            value = result.structured_content
            actual.extend((row["path"], row["line"]) for row in value["results"])
            cursor = value.get("cursor")
            if not cursor:
                break
        self.assertEqual(actual, [("a/one.txt", line) for line in range(1, 6)] + [("z.txt", line) for line in range(1, 4)])

    def test_search_counts_and_file_pages_do_not_duplicate(self):
        """Count pages account for each line once while file mode emits each path once."""
        for name in ("a", "b"):
            (self.root / name).write_text("needle\n" * 3)
        for mode in ("count", "files"):
            cursor, rows, count = None, [], 0
            for _ in range(10):
                result = self.invoke("search_files", query="needle", mode=mode, limit=1, max_matches=2, cursor=cursor)
                self.assertFalse(result.is_error, result)
                value = result.structured_content
                rows.extend(value["results"])
                count += value["match_count"]
                cursor = value.get("cursor")
                if not cursor:
                    break
            self.assertEqual(count, 6 if mode == "count" else 2)
            if mode == "files":
                self.assertEqual([row["path"] for row in rows], ["a", "b"])

    def test_search_byte_budget_resumes_and_skips_oversized_files(self):
        """An oversized file and per-page byte cap never hide later small matches."""
        (self.root / "a").write_text("x" * 30)
        (self.root / "b").write_text("needle\n")
        (self.root / "c").write_text("needle\n")
        tools = file_tools(LocalFileService(self.root, config=FileToolsConfig(max_file_bytes=20, max_scan_bytes=10)))
        first = tools.invoke(call("search_files", query="needle")).structured_content
        self.assertEqual(first["skipped"], [{"path": "a", "reason": "file_too_large"}])
        self.assertEqual(first["results"][0]["path"], "b")
        self.assertEqual(first["reason"], "max_scan_bytes")
        second = tools.invoke(call("search_files", query="needle", cursor=first["cursor"])).structured_content
        self.assertEqual([row["path"] for row in second["results"]], ["c"])
        self.assertNotIn("cursor", second)

    def test_search_cursor_rejects_changed_partial_file(self):
        """A resumed search cannot silently mix line positions from different file versions."""
        path = self.root / "a"
        path.write_text("needle\nneedle\n")
        first = self.invoke("search_files", query="needle", limit=1).structured_content
        path.write_text("changed\nneedle\n")
        result = self.invoke("search_files", query="needle", limit=1, cursor=first["cursor"])
        self.assertEqual(result.structured_content["error"]["kind"], "listing_changed")

    def test_long_lines_read_with_utf8_safe_column_continuation(self):
        """Oversized Unicode lines can be reconstructed from bounded fragments without loss."""
        original = "你好🙂" * 100
        (self.root / "long.txt").write_text(original)
        line, column, fragments = 1, 1, []
        for _ in range(100):
            result = self.invoke("read_file", path="long.txt", start_line=line, start_column=column, max_bytes=48)
            self.assertFalse(result.is_error, result)
            text = result.content[0].text
            self.assertLessEqual(len(text.encode()), 48)
            fragments.append(text.split(": ", 1)[1])
            value = result.structured_content
            line, column = value["next_line"], value.get("next_column", 1)
            if line is None:
                break
        self.assertEqual("".join(fragments), original)

    def test_long_search_match_is_clipped_around_the_match(self):
        """Minified source yields a bounded useful snippet instead of an output-size error."""
        (self.root / "long.txt").write_text("a" * 10000 + "needle" + "b" * 10000)
        value = self.invoke("search_files", query="needle").structured_content
        row = value["results"][0]
        self.assertTrue(row["text_truncated"])
        self.assertIn("needle", row["text"])
        self.assertEqual(row["column"], 10001)
        self.assertLessEqual(len(row["text"].encode()), 4096)

    def test_file_contract_publishes_actual_configured_limits(self):
        """Model-facing definitions expose runtime limits instead of unrelated static defaults."""
        tools = file_tools(LocalFileService(self.root, config=FileToolsConfig(max_output_bytes=1024, max_entries=7, max_depth=4)))
        definitions = {value.name: value for value in tools.definitions}
        self.assertEqual(definitions["read_file"].parameters["properties"]["max_bytes"]["maximum"], 1024)
        for name in ("list_files", "search_files"):
            self.assertEqual(definitions[name].parameters["properties"]["max_depth"]["anyOf"][0]["maximum"], 4)
            self.assertIn("7 eligible entries", definitions[name].description)
            self.assertIn("cursor", definitions[name].description)
            self.assertIn("limit may change", definitions[name].parameters["properties"]["cursor"]["description"])
        self.assertIn("page-local", definitions["search_files"].parameters["properties"]["mode"]["description"])
        self.assertIn("line labels", definitions["edit_file"].parameters["properties"]["edits"]["description"])

    def test_cursor_rejects_changed_query_and_parent_directory_escape(self):
        """Untrusted continuation state cannot change query scope or inspect parent directories."""
        for name in ("a", "b"):
            (self.root / name).touch()
        first = self.invoke("list_files", limit=1).structured_content
        result = self.invoke("list_files", limit=1, pattern="*.py", cursor=first["cursor"])
        self.assertEqual(result.structured_content["error"]["kind"], "listing_changed")
        value = json.loads(base64.urlsafe_b64decode(first["cursor"]))
        value["directories"] = {"../outside": []}
        cursor = base64.urlsafe_b64encode(json.dumps(value).encode()).decode()
        result = self.invoke("list_files", limit=1, cursor=cursor)
        self.assertEqual(result.structured_content["error"]["kind"], "listing_changed")

    def test_files_search_honors_max_matches_with_continuation(self):
        """File mode honors both match and page caps while exposing the next matching path."""
        for name in ("a", "b"):
            (self.root / name).write_text("needle\n")
        first = self.invoke("search_files", query="needle", mode="files", max_matches=1).structured_content
        self.assertEqual([row["path"] for row in first["results"]], ["a"])
        self.assertEqual(first["reason"], "max_matches")
        second = self.invoke("search_files", query="needle", mode="files", max_matches=1, cursor=first["cursor"]).structured_content
        self.assertEqual([row["path"] for row in second["results"]], ["b"])


class OperationPolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_generic_parameters_bind_approval_to_operation_targets(self):
        """A non-file Tool can evaluate structured parameters without changing Core."""
        target = ["first"]
        def command(program: str, arguments: list[str]) -> str:
            return program
        tool = Tool(command, operations=lambda arguments, context: (
            ToolOperation(action="command.execute", resource=target[0], attributes={"program": arguments["program"]}),))
        policy = ToolOperationPolicy(lambda operation, context: ToolDecision(
            action="ask" if operation.attributes["program"] == "ssh" else "allow"))
        tools = ToolSet([tool], policy=policy)
        request = call("command", program="ssh", arguments=["host"])
        decision = await tools.acheck(request)
        target[0] = "second"
        result = await tools.ainvoke(request, approval=decision.approval_request.respond(approved=True))
        self.assertEqual(result.structured_content["error"]["kind"], "tool_approval_mismatch")


class HostFileContracts:
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.url = await self.database_url()

    @asynccontextmanager
    async def host(self, provider, *, workflow=None):
        @asynccontextmanager
        async def lifespan(setup):
            setup.providers.register("offline", provider)
            setup.extensions["agent"].context.set_defaults(settings())
            yield
        async with AutoAgentHost(workflows=[workflow or agent_workflow(resources=AgentResources(tools=FileTools(self.root, config=file_config(path_rule(".env"))).tools()))], root=self.root,
            environment={}, lifespan=lifespan, settings=HostSettings(database_url=self.url)) as host:
            yield host

    async def test_bound_workspace_advertises_files_and_unbound_does_not(self):
        """Explicit SDK file Tools are bound to their directory, independently of App Workspaces."""
        provider = ScriptedProvider(answer(), answer())
        async with self.host(provider) as host:
            await host.extensions["agent"].context.configure("bound", settings().session_defaults())
            for session in ("bound", "unbound"):
                ref = await host.execute("agent", AgentInput(message="hello"), session_id=session)
                result = await host.join(ref)
                self.assertEqual(result.status, "completed", result.error)
        self.assertIn("edit_file", [value.name for value in provider.requests[0].tools])
        self.assertIn("edit_file", [value.name for value in provider.requests[1].tools])

    async def test_session_preferences_reject_tool_policy_overrides(self):
        """Parameter rules belong to shared resources or Host startup, never Session preferences."""
        from pydantic import ValidationError
        from autoagent.agent import AgentSessionSettings
        with self.assertRaises(ValidationError):
            AgentSessionSettings.model_validate_json('{"access":{"policy":{}}}')

    async def test_files_can_be_disabled_per_workflow(self):
        """Omitting the file Tool group leaves the Agent without file Tools."""
        provider = ScriptedProvider(answer())
        async with self.host(provider, workflow=agent_workflow()) as host:
            await host.extensions["agent"].context.configure("one", settings().session_defaults())
            result = await host.join(await host.execute("agent", AgentInput(message="hello"), session_id="one"))
            self.assertEqual(result.status, "completed", result.error)
        self.assertNotIn("read_file", [value.name for value in provider.requests[0].tools])

    async def test_secret_approval_round_trip_through_native_wait(self):
        """Actual file reads pause before exposure and resume through Host batch approval."""
        (self.root / ".env").write_text("SECRET_RESULT")
        provider = ScriptedProvider(LLMResponse(model="offline", finish_reason="tool_calls",
            output=(call("read_file", path=".env"),)), answer())
        async with self.host(provider) as host:
            await host.extensions["agent"].context.configure("one", settings().session_defaults())
            ref = await host.execute("agent", AgentInput(message="read the env"), session_id="one")
            waiting = await host.join(ref)
            self.assertEqual(waiting.status, "waiting", waiting.error)
            request = restore(AgentApprovalRequest, waiting.waits[0].request)
            self.assertIn(str(self.root / ".env"), str(request.requests[0].operations))
            self.assertNotIn("SECRET_RESULT", request.model_dump_json())
            reply = AgentApprovalResponse(decisions=tuple(value.respond(approved=True) for value in request.requests),
                revision=request.revision)
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply.model_dump(mode="json"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            observations = [value for value in provider.requests[-1].input if isinstance(value, LLMToolResult)]
            self.assertIn("SECRET_RESULT", observations[-1].model_dump_json())

    async def test_mapped_writes_create_shared_parent_without_invalidating_preflight(self):
        """The real Agent Map commits all sibling files authorized before their parent exists."""
        proposals = tuple(call("write_file", path=f"batch/data/{index}.txt", content=str(index),
            create_parents=True).model_copy(update={"call_id": f"write-{index}"}) for index in range(3))
        provider = ScriptedProvider(LLMResponse(model="offline", finish_reason="tool_calls", output=proposals), answer())
        async with self.host(provider) as host:
            await host.extensions["agent"].context.configure("one", settings().session_defaults())
            result = await host.join(await host.execute("agent", AgentInput(message="create sibling files"), session_id="one"))
            self.assertEqual(result.status, "completed", result.error)
            results = [item for item in provider.requests[-1].input if isinstance(item, LLMToolResult)]
            self.assertEqual([item.call_id for item in results], [f"write-{index}" for index in range(3)])
            self.assertTrue(all(not item.is_error for item in results), results)
            for index in range(3):
                self.assertEqual((self.root / f"batch/data/{index}.txt").read_text(), str(index))

    async def test_actual_file_edit_uses_session_read_version(self):
        """The Agent loop carries a read observation into a later mapped edit call."""
        (self.root / "code.py").write_text("old = 1\n")
        provider = ScriptedProvider(
            LLMResponse(model="offline", finish_reason="tool_calls", output=(call("read_file", path="code.py"),)),
            LLMResponse(model="offline", finish_reason="tool_calls", output=(call("edit_file", path="code.py",
                edits=[{"old_text": "old = 1", "new_text": "new = 2"}]),)), answer())
        async with self.host(provider) as host:
            await host.extensions["agent"].context.configure("one", settings().session_defaults())
            result = await host.join(await host.execute("agent", AgentInput(message="edit code"), session_id="one"))
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual((self.root / "code.py").read_text(), "new = 2\n")


class SQLiteHostFileTests(HostFileContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        return "sqlite+aiosqlite:///" + str(self.root / "runtime.db")


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL test database is not configured")
class PostgreSQLHostFileTests(HostFileContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
