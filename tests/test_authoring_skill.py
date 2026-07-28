from __future__ import annotations

import re
from pathlib import Path
import unittest

import autoagent
import autoagent.ai


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
AUTHORING_EXAMPLE_ROOT = REPOSITORY_ROOT / "examples" / "authoring"
SKILL_ROOT = (
    REPOSITORY_ROOT
    / ".agents"
    / "skills"
    / "autoagent-author-workflow"
)
REFERENCES_ROOT = SKILL_ROOT / "references"
SKILL_EVAL_ROOT = REPOSITORY_ROOT / "skill-evals"
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
                r"^(?:workflows|inputs|expected|tests)/"
                r"[A-Za-z0-9_./-]+\.(?:py|json|md)$|"
                r"^mock_openai_provider\.py$",
                text,
                re.MULTILINE,
            )
        )
        self.assertGreaterEqual(len(paths), 10)
        for relative in paths:
            with self.subTest(path=relative):
                self.assertTrue((AUTHORING_EXAMPLE_ROOT / relative).is_file())

    def test_skill_locates_installed_package_examples(self) -> None:
        skill = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
        samples = (REFERENCES_ROOT / "sample-index.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("Check that AutoAgent is installed", skill)
        self.assertIn("from importlib.resources import files", samples)
        self.assertIn("package mismatch", samples)

    def test_project_contract_checks_package_version_and_location(self) -> None:
        text = (REFERENCES_ROOT / "project-contract.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("importlib.metadata", text)
        self.assertIn("autoagent.__file__", text)
        self.assertIn("autoagent --version", text)
        self.assertIn("when the task supplies an AutoAgent Wheel", text)
        self.assertIn("Do not guess by", text)

    def test_framework_source_is_diagnostic_not_an_authoring_dependency(
        self,
    ) -> None:
        skill = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
        diagnostics = (REFERENCES_ROOT / "diagnostics.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("inspecting the installed", skill)
        self.assertIn("Reading framework source is allowed", diagnostics)
        self.assertIn("do not modify AutoAgent framework source", diagnostics)
        self.assertIn("do not copy internal implementation", diagnostics)

    def test_skill_translates_framework_free_business_requests(self) -> None:
        text = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
        self.assertIn("Assume the requester does not know AutoAgent", text)
        self.assertIn("Do not ask the requester to choose Nodes", text)
        self.assertIn("Create these checks and tests even when", text)

    def test_skill_documents_authoring_boundaries_found_by_forward_review(
        self,
    ) -> None:
        required_text = {
            "project-contract.md": "Package availability and version",
            "workflow-design.md": "Invocation and Node data flow",
            "public-api.md": "Durable values",
            "ai-workflows.md": "local OpenAI-compatible mock HTTP service",
            "testing.md": "registration out of Workflow source",
        }
        for name, expected in required_text.items():
            with self.subTest(reference=name):
                text = (REFERENCES_ROOT / name).read_text(encoding="utf-8")
                self.assertIn(expected, text)

    def test_eval_requests_contain_business_language_only(self) -> None:
        requirements = sorted(SKILL_EVAL_ROOT.glob("*/REQUIREMENTS.md"))
        self.assertEqual(len(requirements), 3)
        forbidden = re.compile(
            r"\b(?:AutoAgent|Workflow|Node|Edge|Operator|Invocation|Runtime|"
            r"Manifest|compiler|ReActWorkflow|LLMCall|SQLite|CLI)\b|"
            r"wait_key|event mode",
            re.IGNORECASE,
        )
        for path in requirements:
            with self.subTest(path=path.parent.name):
                text = path.read_text(encoding="utf-8")
                self.assertIsNone(forbidden.search(text))

    def test_eval_oracle_is_separate_from_agent_requirements(self) -> None:
        evaluator = SKILL_EVAL_ROOT / "EVALUATION.md"
        self.assertTrue(evaluator.is_file())
        readme = (SKILL_EVAL_ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("Do not copy or show `EVALUATION.md`", readme)
        self.assertIn("evaluator-only", evaluator.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
