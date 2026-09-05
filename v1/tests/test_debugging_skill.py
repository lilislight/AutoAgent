from __future__ import annotations

import re
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from autoagent.cli import build_parser


SKILL_ROOT = (
    Path(__file__).resolve().parents[1]
    / "skills"
    / "autoagent-debug-invocation"
)
DEBUG_EVAL_ROOT = (
    Path(__file__).resolve().parents[1]
    / "skill-evals"
    / "04-debug-invocation"
)
DEBUG_HARNESS_ROOT = (
    Path(__file__).resolve().parents[1]
    / "skill-evals"
    / "harness"
)


class DebuggingSkillTests(unittest.TestCase):
    def test_skill_has_portable_structure_and_no_placeholders(self) -> None:
        skill = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
        metadata = (SKILL_ROOT / "agents" / "openai.yaml").read_text(
            encoding="utf-8"
        )

        self.assertTrue(skill.startswith("---\nname: autoagent-debug-invocation\n"))
        self.assertNotIn("TODO", skill)
        self.assertNotIn("TODO", metadata)
        self.assertIn("$autoagent-debug-invocation", metadata)
        self.assertLess(len(skill.splitlines()), 500)

    def test_skill_routes_each_reference_to_an_existing_file(self) -> None:
        skill = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
        for name in ("cli.md", "evidence.md", "repair.md"):
            self.assertIn(f"references/{name}", skill)
            self.assertTrue((SKILL_ROOT / "references" / name).is_file())

    def test_documented_query_kinds_match_the_public_cli(self) -> None:
        parser = build_parser()
        invocation_id = "00000000-0000-0000-0000-000000000001"
        references = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((SKILL_ROOT / "references").glob("*.md"))
        )
        singular = {
            "node": "00000000-0000-0000-0000-000000000002",
            "edge": "00000000-0000-0000-0000-000000000003",
            "operator-call": "00000000-0000-0000-0000-000000000004",
            "runtime-event": "1",
            "user-event": "1",
        }
        kinds = (
            "nodes",
            "node",
            "edges",
            "edge",
            "operator-calls",
            "operator-call",
            "runtime-events",
            "runtime-event",
            "user-events",
            "user-event",
            "runtime-state",
        )

        for kind in kinds:
            arguments = ["invocation", "query", invocation_id, kind]
            if kind in singular:
                arguments.append(singular[kind])
            parsed = parser.parse_args(arguments)
            self.assertEqual(kind, parsed.kind)
            self.assertIn(kind, references)

    def test_skill_is_report_first_and_keeps_rerun_boundary_explicit(self) -> None:
        skill = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")

        first_cli_call = skill.index("autoagent invocation report")
        self.assertNotIn("autoagent ", skill[:first_cli_call])
        self.assertLess(
            first_cli_call,
            skill.index("Form one evidence question"),
        )
        self.assertIn("Do not weaken, delete, or rewrite", skill)
        self.assertIn("Rerun starts at the entry Node", skill)
        self.assertIn("it is not Replay or Fork", skill)
        self.assertIn("Do not mutate the original Invocation", skill)

    def test_debug_eval_starter_project_is_valid_and_keeps_oracle_hidden(
        self,
    ) -> None:
        real_cli = shutil.which("autoagent")
        self.assertIsNotNone(real_cli)
        for command, expected in (
            (("project", "check"), "RESULT valid"),
            (
                ("workflow", "check", "fulfillment_review"),
                "RESULT valid",
            ),
            (
                ("eval", "check", "fulfillment_review"),
                "CASES 4",
            ),
        ):
            completed = self._run(
                [
                    str(real_cli),
                    "--project",
                    str(DEBUG_EVAL_ROOT),
                    *command,
                ],
                cwd=DEBUG_EVAL_ROOT,
            )
            self.assertEqual(
                0,
                completed.returncode,
                completed.stdout + completed.stderr,
            )
            self.assertIn(expected, completed.stdout)

        requirements = (DEBUG_EVAL_ROOT / "REQUIREMENTS.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("<INVOCATION_ID>", requirements)
        self.assertNotIn("requires_manual_review", requirements)
        self.assertNotIn("EVALUATION.md", requirements)

    def test_debug_eval_harness_audits_the_complete_repair_flow(self) -> None:
        real_cli = shutil.which("autoagent")
        self.assertIsNotNone(real_cli)
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / "debug-eval"
            shutil.copytree(
                DEBUG_EVAL_ROOT,
                workspace,
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
            )
            prepared = self._run(
                [
                    sys.executable,
                    str(DEBUG_HARNESS_ROOT / "prepare_debug_eval.py"),
                    "--workspace",
                    str(workspace),
                    "--real-cli",
                    str(real_cli),
                ],
                cwd=workspace,
            )
            self.assertEqual(0, prepared.returncode, prepared.stderr)
            source_match = re.search(
                r"^SOURCE_INVOCATION ([0-9a-f-]+)$",
                prepared.stdout,
                re.MULTILINE,
            )
            self.assertIsNotNone(source_match, prepared.stdout)
            source_id = source_match.group(1)
            audited_cli = workspace / ".evaluation" / "bin" / "autoagent"

            self._assert_cli_ok(
                audited_cli,
                workspace,
                "invocation",
                "report",
                source_id,
                "--source",
                "database",
            )
            self._assert_cli_ok(
                audited_cli,
                workspace,
                "invocation",
                "query",
                source_id,
                "edges",
                "--source",
                "database",
                "--limit",
                "20",
            )

            workflow_path = workspace / "workflows" / "fulfillment.py"
            workflow_source = workflow_path.read_text(encoding="utf-8")
            incorrect = (
                "request.flagged\n"
                "        and request.amount > 1_000\n"
                "        and request.account_age_days < 30"
            )
            corrected = (
                "request.flagged\n"
                "        or request.amount > 1_000\n"
                "        or request.account_age_days < 30"
            )
            self.assertEqual(1, workflow_source.count(incorrect))
            workflow_path.write_text(
                workflow_source.replace(incorrect, corrected),
                encoding="utf-8",
            )

            self._assert_cli_ok(audited_cli, workspace, "project", "check")
            self._assert_cli_ok(
                audited_cli,
                workspace,
                "workflow",
                "check",
                "fulfillment_review",
            )
            self._assert_cli_ok(
                audited_cli,
                workspace,
                "eval",
                "check",
                "fulfillment_review",
            )
            rerun = self._assert_cli_ok(
                audited_cli,
                workspace,
                "invocation",
                "rerun",
                source_id,
                "--store",
                "database",
            )
            candidate_match = re.search(
                r"^CANDIDATE ([0-9a-f-]+)$",
                rerun.stdout,
                re.MULTILINE,
            )
            self.assertIsNotNone(candidate_match, rerun.stdout)
            candidate_id = candidate_match.group(1)
            self._assert_cli_ok(
                audited_cli,
                workspace,
                "invocation",
                "compare",
                source_id,
                candidate_id,
                "--source",
                "database",
            )
            evaluation = self._assert_cli_ok(
                audited_cli,
                workspace,
                "eval",
                "run",
                "fulfillment_review",
            )
            self.assertIn("RESULT passed", evaluation.stdout)

            reviewed = self._run(
                [
                    sys.executable,
                    str(DEBUG_HARNESS_ROOT / "review_debug_eval.py"),
                    "--workspace",
                    str(workspace),
                ],
                cwd=workspace,
            )
            self.assertEqual(0, reviewed.returncode, reviewed.stdout + reviewed.stderr)
            self.assertIn("CHECK report_first passed", reviewed.stdout)
            self.assertIn("CHECK debug_command_order passed", reviewed.stdout)
            self.assertIn("REVIEW_RESULT passed", reviewed.stdout)

    def _assert_cli_ok(
        self,
        executable: Path,
        workspace: Path,
        *arguments: str,
    ) -> subprocess.CompletedProcess[str]:
        completed = self._run(
            [str(executable), "--project", str(workspace), *arguments],
            cwd=workspace,
        )
        self.assertEqual(
            0,
            completed.returncode,
            completed.stdout + completed.stderr,
        )
        return completed

    @staticmethod
    def _run(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            command,
            cwd=cwd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=90,
            check=False,
        )


if __name__ == "__main__":
    unittest.main()
