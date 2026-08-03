from __future__ import annotations

import re
from pathlib import Path
import unittest

import autoagent
import autoagent.ai
import autoagent.evaluation


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

    def test_public_api_reference_tracks_workflow_authoring_contract(self) -> None:
        text = (REFERENCES_ROOT / "public-api.md").read_text(encoding="utf-8")
        for name in autoagent.__all__:
            with self.subTest(module=autoagent.__name__, name=name):
                self.assertIn(f"`{name}`", text)

        ai_authoring_names = {
            "LLMMessage",
            "LLMRequest",
            "LLMResponse",
            "LLMResponseFormat",
            "LLMToolCall",
            "LLMToolDefinition",
            "LLMUsage",
            "llm_call_node",
            "react_workflow",
            "response_format_from_type",
            "tool",
        }
        for name in ai_authoring_names:
            with self.subTest(module=autoagent.ai.__name__, name=name):
                self.assertIn(f"`{name}`", text)

        self.assertNotIn("### Chat Completions Provider", text)
        self.assertIn("Provider construction and Operator registration", text)
        self.assertIn("not Workflow-authoring APIs", text)

        evaluation_names = {
            "EvalCase",
            "Evaluation",
            "EvaluationContext",
            "Evaluator",
            "EvaluatorResult",
        }
        for name in evaluation_names:
            with self.subTest(module=autoagent.evaluation.__name__, name=name):
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
                r"^(?:workflows|evals|tests)/[A-Za-z0-9_./-]+\.py$|"
                r"^mock_chat_completions_provider\.py$",
                text,
                re.MULTILINE,
            )
        )
        self.assertEqual(len(paths), 8)
        for relative in paths:
            with self.subTest(path=relative):
                self.assertTrue((AUTHORING_EXAMPLE_ROOT / relative).is_file())

    def test_skill_locates_installed_package_examples(self) -> None:
        skill = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
        samples = (REFERENCES_ROOT / "sample-index.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("Ensure the selected Python environment", skill)
        self.assertIn("from importlib.resources import files", samples)
        self.assertIn("repeat the package availability procedure", samples)

    def test_project_contract_prefers_installed_then_local_wheel_then_index(
        self,
    ) -> None:
        text = (REFERENCES_ROOT / "project-contract.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("importlib.metadata", text)
        self.assertIn("autoagent.__file__", text)
        self.assertIn("autoagent --version", text)
        installed = text.index("already contains AutoAgent")
        wheel = text.index("search the target project directory")
        package_index = text.index("configured package index")
        self.assertLess(installed, wheel)
        self.assertLess(wheel, package_index)
        self.assertIn("python -m pip install <path-to-autoagent-wheel>", text)
        self.assertIn("python -m pip install autoagent", text)

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
        self.assertIn("Create the static checks and relevant Eval Cases", text)

    def test_skill_uses_eval_for_business_behavior_without_duplicate_tests(
        self,
    ) -> None:
        skill = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
        testing = (REFERENCES_ROOT / "testing.md").read_text(encoding="utf-8")
        cli = (REFERENCES_ROOT / "cli.md").read_text(encoding="utf-8")

        self.assertIn("autoagent eval check <suite-id>", skill)
        self.assertIn("autoagent eval run <suite-id>", skill)
        self.assertIn("Do not duplicate the same business scenario", testing)
        self.assertIn("--report-file", cli)
        self.assertIn("does not persist an Eval Result", cli)

    def test_skill_documents_authoring_boundaries_found_by_forward_review(
        self,
    ) -> None:
        required_text = {
            "project-contract.md": "Package availability and version",
            "workflow-design.md": "Invocation and Node data flow",
            "public-api.md": "Durable values",
            "ai-workflows.md": "local Chat Completions-compatible HTTP service",
            "testing.md": "Use Evaluation for Workflow behavior",
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
