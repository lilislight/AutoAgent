"""Verified Skill capture, frozen reads and script resources from one version."""

import base64
from contextlib import contextmanager
import json
import sys
import unittest

from pydantic import ValidationError

from autoagent import ValueContract
from autoagent.agent import (
    Skill, SkillSet, SkillSnapshot, SkillChangedError, SkillLimitError,
    SkillLimits, SkillResourceError, SkillScriptSpec, SubprocessSkillRunner, ToolSet, LLMToolCall,
)
from tests.test_agent_skill_extensions import memory_source


class SkillSnapshotTests(unittest.TestCase):
    def skill(self, files=None, limits=None):
        return Skill(memory_source(files=files or {"refs/rules.txt":"first\nsecond\n"}), limits=limits)

    def test_frozen_reads_survive_live_document_and_resource_changes(self):
        """Every read from a captured version remains fixed when the original source changes."""
        live = self.skill()
        snapshot = live.snapshot()
        frozen = snapshot.to_skill()
        live.source._files["refs/rules.txt"] = b"different\nversion\n"
        live.source._files["SKILL.md"] = b"broken"
        self.assertEqual(frozen.read_text("refs/rules.txt", start_line=2).text, "second\n")
        self.assertEqual(frozen.load().snapshot_revision, snapshot.revision)
        self.assertEqual(frozen.read_resource("refs/rules.txt").snapshot_revision, snapshot.revision)
        self.assertEqual(frozen.list_resources().snapshot_revision, snapshot.revision)

    def test_content_changes_invalidate_version_even_when_size_is_unchanged(self):
        """Whole-version hashes cover file bytes rather than relying on listing metadata."""
        live = self.skill({"refs/rules.txt":"one"})
        old = live.snapshot()
        live.source._files["refs/rules.txt"] = b"two"
        self.assertNotEqual(old.revision, live.snapshot().revision)
        with self.assertRaises(SkillChangedError):
            live.snapshot(expected_revision=old.revision)

    def test_capture_detects_file_changes_between_its_two_reads(self):
        """A source that changes during capture cannot publish an apparently stable version."""
        source = memory_source(files={"refs/rules.txt":"one"})
        original = source.open_binary
        reads = 0
        @contextmanager
        def changing(path):
            nonlocal reads
            if path == "refs/rules.txt":
                reads += 1
                if reads == 2:
                    source._files[path] = b"two"
            with original(path) as stream:
                yield stream
        source.open_binary = changing
        with self.assertRaises(SkillChangedError):
            Skill(source).snapshot()

    def test_capture_detects_new_files_during_final_listing(self):
        """Publication fails if the captured resource membership changed."""
        source = memory_source(files={"a.txt":"one"})
        original = source.iter_resources
        scans = 0
        def changing(*, prefix, limits):
            nonlocal scans
            scans += 1
            if scans == 2:
                source._files["b.txt"] = b"two"
            yield from original(prefix=prefix, limits=limits)
        source.iter_resources = changing
        with self.assertRaises(SkillChangedError):
            Skill(source).snapshot()

    def test_selected_capture_omits_unbound_files_and_keeps_document(self):
        """Explicit script selections cannot accidentally include unrelated resources."""
        live = self.skill({"scripts/run.py":"print('ok')", "private.txt":"secret"})
        snapshot = live.snapshot(paths=("scripts/run.py",))
        self.assertEqual(tuple(item.resource.path for item in snapshot.files), ("SKILL.md", "scripts/run.py"))
        with self.assertRaises(SkillResourceError):
            snapshot.to_skill().read_bytes("private.txt")

    def test_snapshot_restore_verifies_bytes_and_manifest_and_round_trips_in_core(self):
        """Portable restore rejects both file corruption and a forged version hash."""
        snapshot = self.skill().snapshot()
        contract = ValueContract.create(SkillSnapshot, location="skill-snapshot")
        restored = contract.restore(contract.to_record(snapshot))
        self.assertEqual(restored.to_skill().read_bytes("refs/rules.txt"), b"first\nsecond\n")
        raw = json.loads(snapshot.model_dump_json())
        raw["files"][0]["data"] = base64.b64encode(b"changed").decode()
        with self.assertRaises(ValidationError):
            SkillSnapshot.model_validate_json(json.dumps(raw))
        raw = json.loads(snapshot.model_dump_json())
        raw["revision"] = "0" * 64
        with self.assertRaises(ValidationError):
            SkillSnapshot.model_validate_json(json.dumps(raw))

    def test_capture_and_restore_obey_total_and_file_limits(self):
        """Snapshot memory budgets apply to both source capture and persisted restore."""
        live = self.skill()
        with self.assertRaises(SkillLimitError):
            Skill(live.source, limits=SkillLimits(max_snapshot_bytes=1)).snapshot()
        with self.assertRaises(SkillLimitError):
            live.snapshot().to_skill(limits=SkillLimits(max_snapshot_bytes=1))
        with self.assertRaises(SkillLimitError):
            Skill(live.source, limits=SkillLimits(max_scan_entries=1)).snapshot()

    def test_equivalent_sources_and_repeated_freezes_share_the_content_version(self):
        """Versions identify captured content independent of provenance and frozen source location."""
        first = self.skill().snapshot()
        self.assertEqual(first.revision, self.skill().snapshot().revision)
        self.assertEqual(first.revision, first.to_skill().snapshot().revision)

    def test_pagination_cursor_cannot_cross_snapshot_versions(self):
        """A cursor from old content cannot continue against a new snapshot with the same sizes."""
        live = self.skill({"a.txt":"one", "b.txt":"two"})
        old = live.freeze()
        page = old.list_resources(limit=1)
        live.source._files["a.txt"] = b"new"
        with self.assertRaises(SkillChangedError):
            live.freeze().list_resources(limit=1, cursor=page.next_cursor)

    def test_snapshot_source_identity_and_manifest_cannot_be_reassigned(self):
        """Frozen sources cannot change their advertised name or version after publication."""
        from dataclasses import FrozenInstanceError
        source = self.skill().freeze().source
        for field, value in (("name", "other"), ("snapshot_revision", "0" * 64), ("snapshot", None)):
            with self.subTest(field=field), self.assertRaises(FrozenInstanceError):
                setattr(source, field, value)


class SkillSnapshotAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_frozen_skill_tools_use_the_same_version_after_source_changes(self):
        """A model-facing SkillSet serves captured instructions and resources throughout a turn."""
        live = Skill(memory_source(files={"refs/rules.txt":"original"}))
        frozen = SkillSet([live]).freeze()
        live.source._files["refs/rules.txt"] = b"changed"
        tools = ToolSet(frozen.as_tools())
        result = await tools.ainvoke(LLMToolCall(name="skill_read_resource", call_id="1", arguments='{"name":"report","path":"refs/rules.txt"}'))
        self.assertEqual(result.content[0].text, "original")
        self.assertEqual(result.structured_content["snapshot_revision"], frozen["report"].snapshot_revision)

    async def test_script_and_companion_resource_execute_from_the_frozen_version(self):
        """Script execution cannot combine new source files with a previously frozen document."""
        code = "from pathlib import Path\nprint(Path('assets/value.txt').read_text())\n"
        live = Skill(memory_source(files={"scripts/run.py":code, "assets/value.txt":"original"}))
        frozen = live.freeze()
        live.source._files["assets/value.txt"] = b"changed"
        live.source._files["scripts/run.py"] = b"raise RuntimeError('changed')"
        runner = SubprocessSkillRunner([SkillScriptSpec(skill_name="report", path="scripts/run.py", command=(sys.executable,), resources=("assets/value.txt",))])
        result = await runner.arun(frozen, "scripts/run.py")
        self.assertEqual(result.stdout, "original\n")
        self.assertEqual(result.snapshot_revision, frozen.snapshot_revision)

    async def test_live_script_execution_reports_its_captured_version(self):
        """The default Runner snapshots selected live resources before process launch."""
        live = Skill(memory_source(files={"scripts/run.py":"print('ok')", "unbound.txt":"ignored"}))
        expected = live.snapshot(paths=("scripts/run.py",)).revision
        runner = SubprocessSkillRunner([SkillScriptSpec(skill_name="report", path="scripts/run.py", command=(sys.executable,))])
        result = await runner.arun(live, "scripts/run.py")
        self.assertEqual(result.snapshot_revision, expected)
        self.assertEqual(result.stdout, "ok\n")
