"""Expanded Skill access, diagnostics, paging, sources and script ownership."""

from __future__ import annotations

import asyncio
import base64
from contextlib import contextmanager
import hashlib
import importlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

from pydantic import ValidationError

from autoagent import ValueContract
from autoagent.agent import (
    LocalSkillSource, MemorySkillSource, PackageSkillSource, Skill, SkillSet,
    SkillChangedError, SkillDiagnostic, SkillFormatError,
    SkillLimitError, SkillLimits, SkillResource, SkillResourceError,
    SkillResourcePage, SkillScriptError, SkillScriptResult, SkillScriptSpec,
    SkillTextContent, SubprocessSkillRunner, ToolSet,
)
from tests.test_agent_skills import make_skill, call, yaml


def memory_source(*, name="report", body="Report instructions.", extra="", files=None):
    return MemorySkillSource(name, {"SKILL.md": f"---\nname: {name}\ndescription: Reports\n{extra}---\n{body}", **(files or {})})


@unittest.skipUnless(yaml is not None, "Install autoagent[skills] for Skill tests")
class SkillExpansionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_skill_md_limits_apply_to_every_read_entry(self):
        """Resource reads cannot bypass a changed document's configured byte limit."""
        directory = make_skill(self.root, body="short")
        skill = Skill(directory, limits=SkillLimits(max_document_bytes=200, max_resource_bytes=1024))
        make_skill(self.root, body="x" * 400)
        for operation in (skill.load, lambda: skill.read_resource("SKILL.md"),
                          lambda: skill.read_text("SKILL.md"), lambda: skill.list_resources()):
            with self.subTest(operation=operation), self.assertRaises(SkillLimitError):
                operation()
        result = SkillSet((skill,)).as_tools()[2].invoke(call("skill_read_resource", {"name": "inventory-report", "path": "SKILL.md"}))
        self.assertTrue(result.is_error)
        self.assertEqual(result.structured_content["error"]["kind"], "SkillLimitError")

    def test_changed_metadata_blocks_resource_and_text_reads(self):
        """All access paths require rediscovery after metadata or permissions change."""
        directory = make_skill(self.root, resources={"rules.md": b"rules"})
        skill = Skill(directory)
        make_skill(self.root, header="name: inventory-report\ndescription: Changed purpose.")
        for operation in (lambda: skill.read_resource("SKILL.md"), lambda: skill.read_resource("rules.md"),
                          lambda: skill.read_text("rules.md"), lambda: skill.list_resources()):
            with self.subTest(operation=operation), self.assertRaises(SkillChangedError):
                operation()

    def test_invalid_resource_basename_is_a_tool_error_observation(self):
        """Blank path components fail before content conversion instead of crashing the Tool."""
        directory = make_skill(self.root, resources={"assets/ ": b"binary"})
        skill = Skill(directory)
        with self.assertRaises(SkillResourceError):
            skill.read_resource("assets/ ")
        executable = SkillSet((skill,)).as_tools()[2]
        result = executable.invoke(call("skill_read_resource", {"name": "inventory-report", "path": "assets/ "}))
        self.assertTrue(result.is_error)

    def test_invalid_source_media_metadata_is_an_expected_resource_failure(self):
        """Invalid media MIME data is normalized through the Skill error boundary."""
        source = memory_source(files={"asset.bin": b"data"})
        source.resource_info = lambda path: SkillResource(path=path, mime_type="invalid", size_bytes=4)
        skill = Skill(source)
        with self.assertRaises(SkillResourceError):
            skill.read_resource("asset.bin")

    def test_document_loading_never_requires_resource_enumeration(self):
        """A small document remains loadable even when its resource catalog is large or invalid."""
        source = memory_source(files={f"assets/{i}.md": "text" for i in range(300)})
        skill = Skill(source, limits=SkillLimits(max_resources=1))
        with patch.object(source, "iter_resources", side_effect=SkillResourceError("unavailable")):
            self.assertEqual(skill.load().instructions, "Report instructions.")
            result = SkillSet((skill,)).as_tools()[0].invoke(call(arguments={"name": "report"}))
            self.assertFalse(result.is_error)

    def test_resource_pages_cover_a_large_catalog_once(self):
        """Page limits constrain returned resources without rejecting the full catalog."""
        skill = Skill(memory_source(files={f"assets/{i:03}.md": "text" for i in range(300)}))
        paths, cursor = [], None
        while True:
            page = skill.list_resources(limit=41, cursor=cursor)
            self.assertLessEqual(len(page.resources), 41)
            paths.extend(value.path for value in page.resources)
            cursor = page.next_cursor
            if cursor is None:
                break
        self.assertEqual(len(paths), 300)
        self.assertEqual(len(set(paths)), 300)
        self.assertEqual(paths, sorted(paths))

    def test_cursor_detects_listing_changes_and_rejects_wrong_scope(self):
        """A cursor cannot silently skip changed resources or switch directory prefixes."""
        directory = make_skill(self.root, resources={"refs/a.md": b"a", "refs/b.md": b"b"})
        skill = Skill(directory)
        page = skill.list_resources(prefix="refs", limit=1)
        for cursor in ("bad", page.revision + ":999999"):
            with self.subTest(cursor=cursor), self.assertRaises(SkillResourceError):
                skill.list_resources(prefix="refs", cursor=cursor, limit=1)
        with self.assertRaises(SkillChangedError):
            skill.list_resources(cursor=page.next_cursor, limit=1)
        (directory / "refs/b.md").write_bytes(b"changed size")
        with self.assertRaises(SkillChangedError):
            skill.list_resources(prefix="refs", cursor=page.next_cursor, limit=1)

    def test_prefix_prunes_unrelated_invalid_directories(self):
        """Listing references neither scans unrelated assets nor fails on their broken links."""
        directory = make_skill(self.root, resources={"references/a.md": b"a"})
        (directory / "assets").mkdir()
        (directory / "assets/bad").symlink_to(self.root / "outside")
        skill = Skill(directory)
        self.assertEqual([value.path for value in skill.list_resources(prefix="references").resources], ["references/a.md"])
        self.assertEqual(skill.list_resources(prefix="missing").resources, ())
        with self.assertRaises(SkillResourceError):
            skill.list_resources()

    def test_prefix_keeps_page_limits_and_directory_depth_limits(self):
        """Selective traversal still enforces explicit scan, depth and page bounds."""
        directory = make_skill(self.root, resources={"refs/a.md": b"a", "refs/deep/b.md": b"b"})
        with self.assertRaises(SkillLimitError):
            Skill(directory, limits=SkillLimits(max_resource_depth=1)).list_resources(prefix="refs/deep")
        for limit in (0, -1, 257, True):
            with self.subTest(limit=limit), self.assertRaises(SkillLimitError):
                Skill(directory).list_resources(limit=limit)

    def test_diagnostics_preserve_valid_skills_and_identify_bad_fields(self):
        """Skip mode reports metadata locations and fields while retaining valid capabilities."""
        make_skill(self.root, name="good")
        bad = make_skill(self.root, name="bad", header="name: bad")
        with self.assertRaises(SkillFormatError):
            SkillSet.discover([self.root])
        skills = SkillSet.discover([self.root], errors="skip")
        self.assertEqual([value.name for value in skills.metadata], ["good"])
        self.assertEqual(len(skills.diagnostics), 1)
        self.assertEqual(skills.diagnostics[0].location, str(bad / "SKILL.md"))
        self.assertEqual(skills.diagnostics[0].field, "description")
        self.assertEqual(skills.select(include=("good",)).diagnostics, skills.diagnostics)

    def test_diagnostics_report_yaml_line_without_echoing_values(self):
        """Parse errors give actionable locations while keeping source values out of messages."""
        make_skill(self.root, name="bad", header="name: bad\ndescription: [secret")
        skills = SkillSet.discover([self.root], errors="skip")
        self.assertIsNotNone(skills.diagnostics[0].line)
        self.assertNotIn("secret", skills.diagnostics[0].message)

    def test_skip_mode_reports_duplicates_missing_roots_and_scan_exhaustion(self):
        """Diagnostics do not hide collisions or bypass global discovery bounds."""
        make_skill(self.root / "one")
        make_skill(self.root / "two")
        skills = SkillSet.discover([self.root / "missing", self.root / "one", self.root / "two"], errors="skip")
        self.assertEqual(len(skills), 1)
        self.assertEqual(len(skills.diagnostics), 2)
        limited = SkillSet.discover([self.root], errors="skip", limits=SkillLimits(max_scan_entries=1))
        self.assertEqual(limited.diagnostics[0].code, "SkillLimitError")
        with self.assertRaises(ValueError):
            SkillSet.discover([self.root], errors="unknown")

    def test_disabled_skills_are_manual_only_across_catalog_schema_and_execution(self):
        """Model-disabled Skills remain manually readable but cannot be called through any Tool."""
        enabled = Skill(memory_source(name="enabled", files={"rules.md": "rules"}))
        disabled = Skill(memory_source(name="disabled", extra="disable-model-invocation: true\n", files={"rules.md": "rules"}))
        skills = SkillSet((enabled, disabled))
        self.assertEqual(skills.load("disabled").metadata.name, "disabled")
        self.assertEqual(skills.read_resource("disabled", "rules.md").content[0].text, "rules")
        self.assertNotIn("disabled", skills.catalog_message().content[0].text)
        self.assertEqual(len(skills.for_model()), 1)
        for executable in skills.as_tools():
            self.assertEqual(executable.definition.parameters["properties"]["name"]["enum"], ["enabled"])
            args = {"name": "disabled"}
            if executable.definition.name == "skill_read_resource":
                args["path"] = "rules.md"
            result = executable.invoke(call(executable.definition.name, args))
            self.assertTrue(result.is_error)
            self.assertEqual(result.structured_content["error"]["kind"], "SkillNotFoundError")
        self.assertIsNone(SkillSet((disabled,)).catalog_message())
        self.assertEqual(SkillSet((disabled,)).as_tools(), ())

    def test_permission_edits_require_rediscovery_before_model_use(self):
        """A once-enabled Tool refuses content after its source disables model invocation."""
        directory = make_skill(self.root)
        executable = SkillSet.discover([self.root]).as_tools()[0]
        make_skill(self.root, header="name: inventory-report\ndescription: Analyze inventory and replenishment risk.\ndisable-model-invocation: true")
        result = executable.invoke(call())
        self.assertTrue(result.is_error)
        self.assertEqual(result.structured_content["error"]["kind"], "SkillChangedError")
        self.assertEqual(SkillSet.discover([self.root]).as_tools(), ())

    def test_model_invocation_flag_requires_a_boolean(self):
        """A string flag cannot accidentally change invocation access by Python truthiness."""
        with self.assertRaises(SkillFormatError):
            Skill(memory_source(extra='disable-model-invocation: "false"\n'))

    def test_text_ranges_preserve_utf8_and_continue_on_line_boundaries(self):
        """Text pages return intact Unicode and an exact continuation line."""
        skill = Skill(memory_source(files={"rules.md": "第一行\n第二行\n第三行"}))
        first = skill.read_text("rules.md", max_lines=2)
        second = skill.read_text("rules.md", start_line=first.next_line, max_lines=2)
        self.assertEqual(first.text, "第一行\n第二行\n")
        self.assertEqual((first.start_line, first.end_line, first.next_line), (1, 2, 3))
        self.assertEqual(second.text, "第三行")
        self.assertIsNone(second.next_line)
        self.assertEqual(first.revision, hashlib.sha256(first.text.encode()).hexdigest())

    def test_text_byte_budget_never_splits_or_loses_the_next_line(self):
        """Output byte exhaustion returns accepted lines and resumes the dropped line next time."""
        skill = Skill(memory_source(files={"rules.md": "one\ntwo\nthree\n"}))
        first = skill.read_text("rules.md", max_bytes=5)
        second = skill.read_text("rules.md", start_line=first.next_line, max_bytes=5)
        self.assertEqual(first.text, "one\n")
        self.assertEqual(second.text, "two\n")
        with self.assertRaises(SkillLimitError):
            skill.read_text("rules.md", start_line=3, max_bytes=5)

    def test_text_scan_budget_empty_eof_and_media_rejection(self):
        """Range reads bound skipped input and distinguish empty text from binary media."""
        skill = Skill(memory_source(files={"rules.md": "a\n" * 50, "empty.md": "", "image.png": b"PNG"}), limits=SkillLimits(max_resource_bytes=20))
        self.assertEqual(skill.read_text("empty.md").text, "")
        self.assertEqual(skill.read_text("rules.md", max_lines=2).text, "a\na\n")
        with self.assertRaises(SkillLimitError):
            skill.read_text("rules.md", start_line=20)
        with self.assertRaises(SkillResourceError):
            skill.read_text("image.png")
        eof = Skill(memory_source(files={"rules.md": "one\n"})).read_text("rules.md", start_line=10)
        self.assertEqual((eof.end_line, eof.next_line, eof.text), (9, None, ""))

    def test_text_bounds_and_invalid_utf8_are_expected_failures(self):
        """Reject invalid ranges and encodings through SkillError rather than conversion exceptions."""
        skill = Skill(memory_source(files={"rules.md": b"\xff"}))
        for kwargs in ({"start_line": 0}, {"max_lines": 0}, {"max_lines": 201}, {"max_bytes": 0}, {"max_bytes": 65537}):
            with self.subTest(kwargs=kwargs), self.assertRaises((SkillResourceError, SkillLimitError)):
                skill.read_text("rules.md", **kwargs)
        with self.assertRaises(SkillResourceError):
            skill.read_text("rules.md")

    def test_memory_source_is_detached_and_source_imports_are_lazy(self):
        """External data is copied into immutable source bytes with no remote SDK requirements."""
        files = {"SKILL.md": "---\nname: report\ndescription: Reports\n---\nInstructions", "rules.md": b"old"}
        source = MemorySkillSource("report", files)
        files["rules.md"] = b"new"
        skill = Skill.from_source(source)
        self.assertEqual(skill.read_resource("rules.md").content[0].text, "old")
        self.assertEqual(len(SkillSet.from_sources((source,))), 1)
        with self.assertRaises(SkillResourceError):
            MemorySkillSource("report", {"../escape": "bad"})

    def test_catalogs_exclude_generated_artifacts_across_source_types(self):
        """Python bytecode and dependency directories do not pollute Skill resource pages."""
        files = {"scripts/run.py": b"pass", "scripts/__pycache__/run.pyc": b"compiled", "node_modules/pkg/file": b"dependency"}
        directory = make_skill(self.root, resources=files)
        local = Skill(directory)
        memory = Skill(memory_source(files=files))
        for skill in (local, memory):
            with self.subTest(source=type(skill.source).__name__):
                self.assertEqual([value.path for value in skill.list_resources().resources], ["scripts/run.py"])

    def test_application_source_works_and_source_failures_become_diagnostics(self):
        """SkillSource supports application adapters without coupling their storage to Skill."""
        class Source:
            name, location = "report", "application:report"
            @contextmanager
            def open_binary(self, path):
                with io.BytesIO(b"---\nname: report\ndescription: Reports\n---\nCustom instructions") as stream:
                    yield stream
            def resource_info(self, path):
                return SkillResource(path=path, mime_type="text/plain")
            def iter_resources(self, *, prefix, limits):
                return iter(())
        skill = Skill(Source())
        self.assertEqual(skill.load().instructions, "Custom instructions")
        invalid = MemorySkillSource("bad", {"SKILL.md": "not YAML"})
        result = SkillSet.from_sources((Source(), invalid), errors="skip")
        self.assertEqual(len(result), 1)
        self.assertEqual(result.diagnostics[0].location, "memory:bad")

    def test_package_source_reads_from_an_importable_zip(self):
        """Package Skills work without assuming resources are real filesystem directories."""
        archive = self.root / "bundled.zip"
        with zipfile.ZipFile(archive, "w") as bundle:
            bundle.writestr("skill_test_bundle/__init__.py", "")
            bundle.writestr("skill_test_bundle/skills/report/SKILL.md", "---\nname: report\ndescription: Reports\n---\nBundled instructions")
            bundle.writestr("skill_test_bundle/skills/report/references/rules.md", "Bundled rules")
        sys.path.insert(0, str(archive))
        self.addCleanup(lambda: sys.path.remove(str(archive)))
        self.addCleanup(lambda: sys.modules.pop("skill_test_bundle", None))
        importlib.invalidate_caches()
        skill = Skill(PackageSkillSource("skill_test_bundle", "skills/report"))
        self.assertEqual(skill.load().instructions, "Bundled instructions")
        self.assertEqual(skill.read_resource("references/rules.md").content[0].text, "Bundled rules")
        self.assertEqual(skill.read_text("references/rules.md").text, "Bundled rules")
        self.assertEqual(skill.list_resources(prefix="references").resources[0].path, "references/rules.md")
        with self.assertRaises(SkillResourceError):
            skill.read_resource("../escape")

    def test_new_models_round_trip_through_core(self):
        """Pages, diagnostics, ranges and script results are portable Runtime values."""
        resource = SkillResource(path="rules.md", mime_type="text/plain", size_bytes=None)
        values = (
            SkillResourcePage(resources=(resource,), revision="a" * 64, next_cursor="a" * 64 + ":1"),
            SkillTextContent(skill_name="report", resource=resource, start_line=1, end_line=1, next_line=2, text="one\n", revision="b" * 64),
            SkillDiagnostic(location="memory:report", code="SkillFormatError", message="Invalid metadata", field="name"),
            SkillScriptSpec(skill_name="report", path="scripts/run.py", command=(sys.executable,)),
            SkillScriptResult(skill_name="report", path="scripts/run.py", revision="c" * 64, returncode=0, stdout="ok", stderr=""),
        )
        for value in values:
            with self.subTest(model=type(value).__name__):
                contract = ValueContract.create(type(value), location="skill-extension")
                self.assertEqual(contract.restore(contract.to_record(value)), value)


@unittest.skipUnless(yaml is not None, "Install autoagent[skills] for runner tests")
class SkillRunnerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def fixture(self, code, *, resources=None, **runner_options):
        skill = Skill(memory_source(files={"scripts/run.py": code, **(resources or {})}))
        spec = SkillScriptSpec(skill_name="report", path="scripts/run.py", command=(sys.executable,), resources=tuple((resources or {}).keys()))
        return skill, SubprocessSkillRunner((spec,), **runner_options)

    async def assert_process_exited(self, pid, *, timeout=1.0):
        """Allow signal delivery to settle, but fail if the process stays live."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        status = Path(f"/proc/{pid}/stat")
        while True:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            try:
                state = status.read_text().rpartition(")")[2].split()[0]
            except (FileNotFoundError, ProcessLookupError):
                state = "unavailable"
            # Container PID 1 may retain orphan zombies, which no longer execute.
            if state in {"Z", "X"}:
                return
            remaining = deadline - loop.time()
            if remaining <= 0:
                self.fail(f"Process {pid} did not exit within {timeout}s (state={state}).")
            await asyncio.sleep(min(0.01, remaining))

    async def test_bound_scripts_receive_arguments_stdin_resources_and_working_directory(self):
        """Application bindings stage selected files and return exact process outputs."""
        code = "import json,os,sys\nfrom pathlib import Path\nprint(json.dumps({'arg':sys.argv[1], 'stdin':sys.stdin.read(), 'resource':Path('data.txt').read_text(), 'env':os.environ.get('SKILL_TEST_VALUE'), 'cwd':str(Path.cwd())}))\n"
        skill, runner = self.fixture(code, resources={"data.txt": "resource data"}, env={"SKILL_TEST_VALUE": "configured"})
        result = await runner.arun(skill, "scripts/run.py", arguments=("argument",), stdin="input")
        record = json.loads(result.stdout)
        self.assertEqual((record["arg"], record["stdin"], record["resource"], record["env"]), ("argument", "input", "resource data", "configured"))
        self.assertEqual(result.returncode, 0)
        self.assertFalse(Path(record["cwd"]).exists())
        self.assertEqual(result.revision, hashlib.sha256(code.encode()).hexdigest())

    async def test_unbound_scripts_and_invalid_requests_fail_before_process_creation(self):
        """Skill declarations never grant execution beyond configured script bindings."""
        skill, runner = self.fixture("print('ok')")
        with patch("asyncio.create_subprocess_exec", side_effect=AssertionError("Unexpected spawn")):
            for path in ("scripts/unbound.py", "../escape"):
                with self.subTest(path=path), self.assertRaises((SkillResourceError, SkillScriptError)):
                    await runner.arun(skill, path)
            with self.assertRaises(SkillScriptError):
                await runner.arun(skill, "scripts/run.py", timeout_seconds=31)
            with self.assertRaises(SkillScriptError):
                await runner.arun(skill, "scripts/run.py", arguments=("bad\x00",))

    async def test_input_and_staging_budgets_are_enforced_before_spawn(self):
        """Bound stdin and staged source bytes before launching the interpreter."""
        skill, runner = self.fixture("print('ok')", max_input_bytes=2, max_staging_bytes=2)
        with patch("asyncio.create_subprocess_exec", side_effect=AssertionError("Unexpected spawn")):
            with self.assertRaises(SkillLimitError):
                await runner.arun(skill, "scripts/run.py", stdin="large")
            with self.assertRaises(SkillLimitError):
                await runner.arun(skill, "scripts/run.py")

    async def test_argument_count_and_combined_input_bytes_are_bounded(self):
        """Large or numerous command arguments cannot bypass the process input budget."""
        skill, runner = self.fixture("pass", max_input_bytes=4, max_arguments=2)
        with patch("asyncio.create_subprocess_exec", side_effect=AssertionError("Unexpected spawn")):
            with self.assertRaises(SkillLimitError):
                await runner.arun(skill, "scripts/run.py", arguments=("one", "two", "three"))
            with self.assertRaises(SkillLimitError):
                await runner.arun(skill, "scripts/run.py", arguments=("arg",), stdin="in")

    async def test_nonzero_exit_is_preserved_as_a_tool_error_result(self):
        """A business script exit retains stdout, stderr and return code for the model."""
        skill, runner = self.fixture("import sys\nprint('partial')\nprint('problem',file=sys.stderr)\nsys.exit(3)")
        tools = ToolSet(SkillSet((skill,)).as_tools(runner=runner))
        result = await tools.ainvoke(call("skill_run_script", {"name": "report", "path": "scripts/run.py"}))
        self.assertTrue(result.is_error)
        self.assertEqual(result.structured_content["returncode"], 3)
        self.assertEqual(result.structured_content["stdout"], "partial\n")
        self.assertEqual(result.structured_content["stderr"], "problem\n")

    async def test_output_overflow_kills_the_process_and_cleans_its_workspace(self):
        """Unbounded stdout cannot outgrow the configured process-output budget."""
        marker = self.root / "workspace"
        code = f"from pathlib import Path\nimport sys,time\nPath({str(marker)!r}).write_text(str(Path.cwd()))\nprint('x'*100000,flush=True)\ntime.sleep(30)"
        skill, runner = self.fixture(code, max_output_bytes=1024)
        with self.assertRaises(SkillLimitError):
            await asyncio.wait_for(runner.arun(skill, "scripts/run.py"), 3)
        self.assertFalse(Path(marker.read_text()).exists())

    async def test_execution_timeout_is_an_expected_error_and_cleans_workspace(self):
        """A script deadline terminates execution and returns no live working directory."""
        marker = self.root / "workspace"
        code = f"from pathlib import Path\nimport time\nPath({str(marker)!r}).write_text(str(Path.cwd()))\ntime.sleep(30)"
        skill, runner = self.fixture(code)
        with self.assertRaises(SkillScriptError):
            await runner.arun(skill, "scripts/run.py", timeout_seconds=0.15)
        self.assertFalse(Path(marker.read_text()).exists())

    async def test_cancellation_reclaims_owned_processes_and_workspace(self):
        """Cancellation stays cancellation after the subprocess and its workspace are reclaimed."""
        marker = self.root / "running"
        code = f"from pathlib import Path\nimport json,os,time\nPath({str(marker)!r}).write_text(json.dumps({{'pid':os.getpid(),'cwd':str(Path.cwd())}}))\ntime.sleep(30)"
        skill, runner = self.fixture(code)
        task = asyncio.create_task(runner.arun(skill, "scripts/run.py"))
        try:
            async with asyncio.timeout(2):
                while not marker.exists():
                    await asyncio.sleep(0.01)
            data = json.loads(marker.read_text())
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertFalse(Path(data["cwd"]).exists())
            if os.name == "posix":
                with self.assertRaises(ProcessLookupError):
                    os.kill(data["pid"], 0)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_custom_runner_is_injectable_and_model_disabled_skills_cannot_run(self):
        """Applications can provide execution adapters without bypassing model access checks."""
        seen = []
        class Runner:
            async def arun(self, skill, path, **options):
                seen.append((skill.metadata.name, path))
                return SkillScriptResult(skill_name=skill.metadata.name, path=path, revision="a" * 64,
                                         returncode=0, stdout="custom", stderr="")
        enabled = Skill(memory_source())
        disabled = Skill(memory_source(name="disabled", extra="disable-model-invocation: true\n"))
        tools = ToolSet(SkillSet((enabled, disabled)).as_tools(runner=Runner()))
        good = await tools.ainvoke(call("skill_run_script", {"name": "report", "path": "scripts/run.py"}))
        bad = await tools.ainvoke(call("skill_run_script", {"name": "disabled", "path": "scripts/run.py"}))
        self.assertFalse(good.is_error)
        self.assertTrue(bad.is_error)
        self.assertEqual(seen, [("report", "scripts/run.py")])

    async def test_optional_runner_is_absent_without_application_configuration(self):
        """Reading scripts never registers an execution Tool implicitly."""
        skills = SkillSet((Skill(memory_source()),))
        self.assertNotIn("skill_run_script", [value.definition.name for value in skills.as_tools()])

    async def test_cancelled_staging_settles_before_workspace_removal(self):
        """Cancellation cannot remove a working directory while its staging thread still writes."""
        skill, runner = self.fixture("print('ok')")
        started, release = asyncio.Event(), threading.Event()
        loop = asyncio.get_running_loop()
        seen = []
        original = runner._stage
        def stage(skill, spec, directory):
            seen.append(directory)
            loop.call_soon_threadsafe(started.set)
            release.wait(timeout=2)
            return original(skill, spec, directory)
        with patch.object(runner, "_stage", side_effect=stage):
            task = asyncio.create_task(runner.arun(skill, "scripts/run.py"))
            try:
                await asyncio.wait_for(started.wait(), 1)
                task.cancel()
                await asyncio.sleep(0)
                self.assertFalse(task.done())
                self.assertTrue(seen[0].exists())
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertFalse(seen[0].exists())
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)

    async def test_cancellation_during_process_creation_still_reclaims_the_process(self):
        """A process created before its handle is delivered remains owned on cancellation."""
        skill, runner = self.fixture("import time\ntime.sleep(30)")
        started, release = asyncio.Event(), asyncio.Event()
        original = asyncio.create_subprocess_exec
        processes = []
        async def spawn(*args, **kwargs):
            process = await original(*args, **kwargs)
            processes.append(process)
            started.set()
            await release.wait()
            return process
        with patch("asyncio.create_subprocess_exec", side_effect=spawn):
            task = asyncio.create_task(runner.arun(skill, "scripts/run.py"))
            try:
                await asyncio.wait_for(started.wait(), 2)
                task.cancel()
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertIsNotNone(processes[0].returncode)
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    @unittest.skipUnless(os.name == "posix", "POSIX process groups required")
    async def test_timeout_reclaims_a_child_even_after_its_parent_exits(self):
        """A child keeping stdout open cannot outlive the runner's process-group cleanup."""
        marker = self.root / "child-pid"
        code = f"import subprocess,sys\nfrom pathlib import Path\nchild=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'])\nPath({str(marker)!r}).write_text(str(child.pid))\n"
        skill, runner = self.fixture(code)
        with self.assertRaises(SkillScriptError):
            await asyncio.wait_for(runner.arun(skill, "scripts/run.py", timeout_seconds=0.2), 3)
        pid = int(marker.read_text())
        # SIGKILL delivery to an orphan can finish after the direct parent was
        # reaped. Confirm eventual exit with a deadline instead of one sample.
        await self.assert_process_exited(pid)

    @unittest.skipUnless(os.name == "posix", "POSIX process checks required")
    async def test_exit_confirmation_rejects_a_live_process(self):
        """The bounded exit check still fails when a process remains alive."""
        process = await asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(30)")
        try:
            with self.assertRaisesRegex(self.failureException, "did not exit"):
                await self.assert_process_exited(process.pid, timeout=0.05)
        finally:
            if process.returncode is None:
                process.kill()
            await process.wait()

    async def test_runner_launch_failure_is_an_expected_skill_error(self):
        """A missing interpreter returns a script error instead of losing the error boundary."""
        skill = Skill(memory_source(files={"scripts/run.py": "print('ok')"}))
        runner = SubprocessSkillRunner((SkillScriptSpec(skill_name="report", path="scripts/run.py", command=(str(self.root / "missing"),)),))
        with self.assertRaises(SkillScriptError):
            await runner.arun(skill, "scripts/run.py")

    async def test_wrong_identity_from_custom_runner_is_an_execution_failure(self):
        """Custom adapters cannot return a result belonging to a different Skill."""
        from autoagent.agent import ToolExecutionError
        class Runner:
            async def arun(self, skill, path, **options):
                return SkillScriptResult(skill_name="other", path=path, revision="a" * 64,
                                         returncode=0, stdout="", stderr="")
        tools = ToolSet(SkillSet((Skill(memory_source()),)).as_tools(runner=Runner()))
        with self.assertRaises(ToolExecutionError):
            await tools.ainvoke(call("skill_run_script", {"name": "report", "path": "scripts/run.py"}))

    async def test_custom_runner_cannot_receive_paths_outside_the_script_namespace(self):
        """Generic script Tools constrain relative paths before invoking application adapters."""
        class Runner:
            async def arun(self, *args, **kwargs):
                raise AssertionError("Unexpected adapter invocation")
        tools = ToolSet(SkillSet((Skill(memory_source()),)).as_tools(runner=Runner()))
        for path in ("../outside.py", "references/rules.md"):
            result = await tools.ainvoke(call("skill_run_script", {"name": "report", "path": path}))
            self.assertTrue(result.is_error)

    async def test_runner_tool_integrates_with_core_and_portable_output(self):
        """Core can await a configured runner without changing the Tool Operator contract."""
        from autoagent import AutoAgentApp, Node, Workflow
        from autoagent.agent import LLMToolResult
        skill, runner = self.fixture("print('core script')")
        tools = ToolSet(SkillSet((skill,)).as_tools(runner=runner))
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(Workflow("skill-script", nodes=[Node("run", tools.as_operator())]),
                                      call("skill_run_script", {"name": "report", "path": "scripts/run.py"}))
            self.assertEqual(result.status, "completed", result.error)
            observation = ValueContract.create(LLMToolResult, location="script").restore(result.output)
            self.assertEqual(observation.structured_content["stdout"], "core script\n")
        finally:
            await app.aclose()

    async def test_full_offline_capability_example_exercises_all_source_types_and_runner(self):
        """The standalone example completes paging, ranges, filtering, diagnostics and real execution."""
        from examples.agent_skills_demo.capabilities import run_capabilities
        result = await run_capabilities()
        self.assertEqual(result["model_skills"], ["inventory-report"])
        self.assertEqual(result["manual_skill"], "manual-only")
        self.assertEqual(result["diagnostics"][0]["field"], "description")
        self.assertTrue(result["source_revisions_match"])
        self.assertEqual(result["calculation"][0]["minimum_replenishment"], 15)
        self.assertEqual(len(result["resources"]), 3)

    async def test_mcp_prefetch_is_detached_and_preserves_text_and_blob_mime(self):
        """Fetch a Skill over the real MCP SDK, then use its files after connection shutdown."""
        try:
            import httpx2
            from autoagent.agent import MCPClient, MCPStreamableHTTPConfig
            from tests.test_agent_mcp import WireServer
        except ImportError:
            self.skipTest("Install autoagent[mcp] for MCP source tests")
        server = WireServer()
        document = b"---\nname: report\ndescription: Reports\n---\nRemote instructions"
        def override(message):
            if message["method"] != "resources/read":
                return None
            uri = message["params"]["uri"]
            content = ({"uri": uri, "text": document.decode()} if uri.endswith("SKILL.md") else
                       {"uri": uri, "blob": base64.b64encode(b"%PDF remote").decode(), "mimeType": "application/pdf"})
            return {"result": {"contents": [content], "resultType": "complete"}}
        server.rpc_override = override
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(server.handle)) as http:
            async with MCPClient(MCPStreamableHTTPConfig(server_id="remote", url="https://server.test/mcp"), http_client=http) as client:
                source = await MemorySkillSource.from_mcp(client, "report", {
                    "SKILL.md": "skill://report/SKILL.md", "assets/document": "skill://report/pdf",
                })
        skill = Skill(source)
        self.assertEqual(skill.load().instructions, "Remote instructions")
        result = skill.read_resource("assets/document")
        self.assertEqual(result.content[0].source.mime_type, "application/pdf")
        self.assertEqual(base64.b64decode(result.content[0].source.data), b"%PDF remote")

    async def test_mcp_prefetch_bounds_files_and_rejects_ambiguous_or_invalid_contents(self):
        """Remote prefetch cannot publish oversize files or silently combine distinct contents."""
        class Client:
            async def read_resource(self, uri):
                return SimpleNamespace(contents=[SimpleNamespace(text="x" * 100, blob=None)])
        with self.assertRaises(SkillLimitError):
            await MemorySkillSource.from_mcp(Client(), "report", {"SKILL.md": "uri"}, limits=SkillLimits(max_document_bytes=10))
        for items in ([], [SimpleNamespace(blob="!", text=None)], [SimpleNamespace(text="a"), SimpleNamespace(text="b")]):
            class BadClient:
                async def read_resource(self, uri):
                    return SimpleNamespace(contents=items)
            with self.subTest(items=items), self.assertRaises(SkillResourceError):
                await MemorySkillSource.from_mcp(BadClient(), "report", {"SKILL.md": "uri"})

    async def test_cancelled_remote_prefetch_returns_no_partial_source(self):
        """Cancellation settles the fetch rather than returning an incomplete Skill package."""
        started = asyncio.Event()
        class Client:
            async def read_resource(self, uri):
                started.set()
                await asyncio.Event().wait()
        task = asyncio.create_task(MemorySkillSource.from_mcp(Client(), "report", {"SKILL.md": "uri"}))
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task


if __name__ == "__main__":
    unittest.main()
