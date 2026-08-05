from __future__ import annotations

from pathlib import Path
import unittest

from autoagent.cli import build_parser


SKILL_ROOT = (
    Path(__file__).resolve().parents[1]
    / "skills"
    / "autoagent-debug-invocation"
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

        self.assertLess(
            skill.index("autoagent invocation report"),
            skill.index("Form one evidence question"),
        )
        self.assertIn("Rerun starts at the entry Node", skill)
        self.assertIn("it is not Replay or Fork", skill)
        self.assertIn("Do not mutate the original Invocation", skill)


if __name__ == "__main__":
    unittest.main()
