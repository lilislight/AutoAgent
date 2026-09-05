from __future__ import annotations

from contextlib import contextmanager, redirect_stdout
from io import StringIO
import importlib.util
import os
from pathlib import Path
import sys
import unittest
from collections.abc import Iterator, Mapping
from unittest.mock import patch

import httpx
from openai import AsyncOpenAI

from autoagent.cli import main as cli_main
from autoagent.evaluation.loader import EvaluationLoader
from autoagent.project import ProjectCompiler, ProjectLoader


PROJECT_ROOT = Path(__file__).resolve().parents[1] / "examples" / "authoring"


class AuthoringExamplesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.project = ProjectLoader().load(PROJECT_ROOT)

    def test_manifest_exports_three_workflows_and_evaluations(self) -> None:
        self.assertEqual(
            {"release_review", "human_approval", "weather_assistant"},
            {loaded.workflow.id for loaded in self.project.workflows},
        )
        self.assertEqual(
            {"release_review", "human_approval", "weather_assistant"},
            {locator.id for locator in self.project.eval_suites},
        )
        for loaded in self.project.workflows:
            result = ProjectCompiler().compile(loaded.workflow)
            self.assertTrue(result.ok, result.diagnostics)
        checked = EvaluationLoader().check_all(self.project)
        self.assertEqual(3, len(checked))

    def test_release_review_evaluation_covers_both_business_paths(self) -> None:
        code, output = self._run_cli(
            "eval",
            "run",
            "release_review",
            "--store",
            "memory",
        )

        self.assertEqual(0, code, output)
        self.assertIn(
            "CASE eval_high_risk_change_requires_specialist_revision",
            output,
        )
        self.assertIn(
            "CASE eval_low_risk_change_is_approved_automatically",
            output,
        )
        self.assertIn("RESULT passed", output)

    def test_human_approval_evaluation_uses_invoke_and_resume_steps(self) -> None:
        code, output = self._run_cli(
            "eval",
            "run",
            "human_approval",
            "--store",
            "memory",
        )

        self.assertEqual(0, code, output)
        self.assertEqual(2, output.count("STEP 1 invoke"))
        self.assertEqual(2, output.count("STEP 2 resume"))
        self.assertIn("RESULT passed", output)

    def test_weather_evaluation_uses_normal_chat_completions_provider(self) -> None:
        with _chat_completions_transport() as base_url:
            code, output = self._run_cli(
                "eval",
                "run",
                "weather_assistant",
                "--store",
                "memory",
                environment={
                    "AUTOAGENT_LLM_PROVIDER": "chat_completions",
                    "AUTOAGENT_LLM_BASE_URL": base_url,
                    "AUTOAGENT_LLM_API_KEY": "mock",
                    "AUTOAGENT_LLM_MODEL": "mock-weather-model",
                    "AUTOAGENT_LLM_STRUCTURED_OUTPUT_MODE": "json_schema",
                },
            )

        self.assertEqual(0, code, output)
        self.assertIn("CASE eval_tokyo_weather_uses_verified_tool_data", output)
        self.assertIn("EVALUATOR invocation_result passed", output)
        self.assertIn("RESULT passed", output)

    def test_focused_project_unit_tests_pass(self) -> None:
        test_path = PROJECT_ROOT / "tests" / "test_project_functions.py"
        spec = importlib.util.spec_from_file_location(
            "authoring_project_function_tests",
            test_path,
        )
        if spec is None or spec.loader is None:
            self.fail(f"Cannot import authoring project tests: {test_path}")
        module = importlib.util.module_from_spec(spec)
        with patch.object(sys, "path", [str(PROJECT_ROOT), *sys.path]):
            spec.loader.exec_module(module)

        suite = unittest.defaultTestLoader.loadTestsFromModule(module)
        result = unittest.TestResult()
        suite.run(result)

        self.assertEqual(2, result.testsRun)
        self.assertTrue(
            result.wasSuccessful(),
            f"failures={result.failures!r} errors={result.errors!r}",
        )

    def _run_cli(
        self,
        *arguments: str,
        environment: Mapping[str, str] | None = None,
    ) -> tuple[int, str]:
        output = StringIO()
        clean_environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("AUTOAGENT_")
        }
        clean_environment.update(environment or {})
        with patch.dict(
            os.environ,
            clean_environment,
            clear=True,
        ), redirect_stdout(output):
            code = cli_main(
                (
                    "--project",
                    str(PROJECT_ROOT),
                    "--no-env-file",
                    *arguments,
                )
            )
        return code, output.getvalue()


@contextmanager
def _chat_completions_transport() -> Iterator[str]:
    module_path = PROJECT_ROOT / "mock_chat_completions_provider.py"
    spec = importlib.util.spec_from_file_location(
        "authoring_mock_chat_completions_provider",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot import mock Provider: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def create_client(**kwargs: object) -> AsyncOpenAI:
        return AsyncOpenAI(
            **kwargs,
            http_client=httpx.AsyncClient(
                transport=httpx.ASGITransport(app=module.app),
                base_url="http://authoring-example",
            ),
        )

    with patch(
        "autoagent.ai.providers.chat_completions.provider.AsyncOpenAI",
        side_effect=create_client,
    ):
        yield "http://authoring-example/v1"


if __name__ == "__main__":
    unittest.main()
