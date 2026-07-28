from __future__ import annotations

import re
from pathlib import Path
import unittest

import autoagent
import autoagent.ai


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SKILL_ROOT = (
    REPOSITORY_ROOT
    / ".agents"
    / "skills"
    / "autoagent-author-workflow"
)
REFERENCES_ROOT = SKILL_ROOT / "references"
EXPECTED_REFERENCES = {
    "ai-workflows.md",
    "cli.md",
    "diagnostics.md",
    "hook-contracts.md",
    "policies.md",
    "project-contract.md",
    "public-api.md",
    "sample-index.md",
    "testing.md",
    "workflow-design.md",
}


class AuthoringSkillTests(unittest.TestCase):
    def test_skill_has_portable_expected_structure(self) -> None:
        self.assertTrue((SKILL_ROOT / "SKILL.md").is_file())
        self.assertEqual(
            {path.name for path in REFERENCES_ROOT.glob("*.md")},
            EXPECTED_REFERENCES,
        )
        self.assertFalse((SKILL_ROOT / "README.md").exists())
        self.assertFalse((SKILL_ROOT / "scripts").exists())
        self.assertFalse((SKILL_ROOT / "assets").exists())

    def test_skill_frontmatter_defines_only_name_and_description(self) -> None:
        text = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
        match = re.match(r"\A---\n(?P<header>.*?)\n---\n", text, re.DOTALL)
        self.assertIsNotNone(match)
        fields = {
            line.split(":", 1)[0]
            for line in match.group("header").splitlines()
            if ":" in line
        }
        self.assertEqual(fields, {"name", "description"})
        self.assertIn("name: autoagent-author-workflow", match.group("header"))

    def test_skill_routes_every_reference_directly(self) -> None:
        text = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
        linked = {
            Path(target).name
            for target in re.findall(
                r"\]\((references/[^)#]+\.md)(?:#[^)]+)?\)",
                text,
            )
        }
        self.assertEqual(linked, EXPECTED_REFERENCES)
        for target in linked:
            self.assertTrue((REFERENCES_ROOT / target).is_file())

    def test_public_api_reference_tracks_both_authoring_contracts(self) -> None:
        text = (REFERENCES_ROOT / "public-api.md").read_text(encoding="utf-8")
        for module in (autoagent, autoagent.ai):
            for name in module.__all__:
                with self.subTest(module=module.__name__, name=name):
                    self.assertIn(f"`{name}`", text)

    def test_skill_contains_no_template_placeholders(self) -> None:
        files = [SKILL_ROOT / "SKILL.md", *REFERENCES_ROOT.glob("*.md")]
        for path in files:
            with self.subTest(path=path.name):
                text = path.read_text(encoding="utf-8")
                self.assertNotIn("[TODO", text)
                self.assertNotIn("Autoagent Author Workflow", text)

    def test_sample_index_points_to_existing_files(self) -> None:
        text = (REFERENCES_ROOT / "sample-index.md").read_text(encoding="utf-8")
        paths = set(
            re.findall(
                r"(?:examples/authoring|tests)/[A-Za-z0-9_./-]+\.(?:py|json|md)",
                text,
            )
        )
        self.assertGreaterEqual(len(paths), 10)
        for relative in paths:
            with self.subTest(path=relative):
                self.assertTrue((REPOSITORY_ROOT / relative).is_file())


if __name__ == "__main__":
    unittest.main()
