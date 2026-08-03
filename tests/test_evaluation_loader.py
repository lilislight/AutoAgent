from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sys
import tempfile
import textwrap
import unittest
from uuid import uuid4

from autoagent.evaluation.loader import EvaluationLoader
from autoagent.project import ProjectLoadError, ProjectLoader, load_project_manifest


class EvaluationManifestTests(unittest.TestCase):
    def test_manifest_parses_eval_suite_locator(self) -> None:
        with _project(
            eval_entries=(
                ("inventory_regression", "inventory", "evaluation:InventoryEvaluation"),
            )
        ) as (root, modules):
            manifest = load_project_manifest(root / "auto-agent.toml")

        locator = manifest.eval_suites[0]
        self.assertEqual("inventory_regression", locator.id)
        self.assertEqual("inventory", locator.workflow_id)
        self.assertEqual(modules["evaluation"], locator.module_name)
        self.assertEqual("InventoryEvaluation", locator.object_path)

    def test_manifest_rejects_duplicate_suite_ids(self) -> None:
        with _project(
            eval_entries=(
                ("same", "inventory", "first:Evaluation"),
                ("same", "inventory", "second:Evaluation"),
            )
        ) as (root, _):
            with self.assertRaises(ProjectLoadError) as captured:
                load_project_manifest(root / "auto-agent.toml")

        self.assertEqual("PROJECT_MANIFEST_INVALID", captured.exception.diagnostics[0].code)
        self.assertIn("Duplicate Eval Suite id", captured.exception.diagnostics[0].message)


class EvaluationLoaderTests(unittest.TestCase):
    def test_normal_project_loading_does_not_import_evaluation_module(self) -> None:
        with _project(
            eval_entries=(("broken", "inventory", "evaluation:Broken"),),
            evaluation_source="raise RuntimeError('evaluation imported')",
        ) as (root, modules):
            project = ProjectLoader().load(root)
            self.assertNotIn(modules["evaluation"], sys.modules)

            with self.assertRaises(ProjectLoadError) as captured:
                EvaluationLoader().load(project, "broken")

        self.assertEqual(
            "EVAL_MODULE_IMPORT_FAILED",
            captured.exception.diagnostics[0].code,
        )

    def test_loads_valid_evaluation_and_preserves_case_order(self) -> None:
        source = """
            from autoagent.evaluation import Evaluation

            class InventoryEvaluation(Evaluation):
                async def eval_out_of_stock(self, case):
                    pass

                async def eval_available(self, case):
                    pass
        """
        with _project(
            eval_entries=(
                ("inventory_regression", "inventory", "evaluation:InventoryEvaluation"),
            ),
            evaluation_source=source,
        ) as (root, _):
            project = ProjectLoader().load(root)
            loaded = EvaluationLoader().load(project, "inventory_regression")

        self.assertEqual("inventory_regression", loaded.locator.id)
        self.assertEqual(
            ("eval_out_of_stock", "eval_available"),
            loaded.case_ids,
        )

    def test_reports_unknown_suite_and_workflow_without_importing_code(self) -> None:
        with _project(
            eval_entries=(("wrong", "missing", "evaluation:Evaluation"),),
            evaluation_source="raise RuntimeError('must not import')",
        ) as (root, modules):
            project = ProjectLoader().load(root)

            with self.assertRaises(ProjectLoadError) as missing_suite:
                EvaluationLoader().load(project, "unknown")
            with self.assertRaises(ProjectLoadError) as missing_workflow:
                EvaluationLoader().load(project, "wrong")

            self.assertNotIn(modules["evaluation"], sys.modules)

        self.assertEqual("EVAL_SUITE_NOT_FOUND", missing_suite.exception.diagnostics[0].code)
        self.assertEqual(
            "EVAL_WORKFLOW_NOT_FOUND",
            missing_workflow.exception.diagnostics[0].code,
        )

    def test_validates_export_type_constructor_and_case_methods(self) -> None:
        sources = {
            "invalid_type": "Evaluation = object()",
            "constructor": """
                from autoagent.evaluation import Evaluation
                class Invalid(Evaluation):
                    def __init__(self, required):
                        self.required = required
                    async def eval_case(self, case):
                        pass
            """,
            "sync": """
                from autoagent.evaluation import Evaluation
                class Invalid(Evaluation):
                    def eval_case(self, case):
                        pass
            """,
            "signature": """
                from autoagent.evaluation import Evaluation
                class Invalid(Evaluation):
                    async def eval_case(self, case, extra):
                        pass
            """,
            "missing": """
                from autoagent.evaluation import Evaluation
                class Invalid(Evaluation):
                    pass
            """,
        }
        expected = {
            "invalid_type": "EVAL_OBJECT_INVALID",
            "constructor": "EVAL_CONSTRUCTOR_INVALID",
            "sync": "EVAL_CASE_NOT_ASYNC",
            "signature": "EVAL_CASE_SIGNATURE_INVALID",
            "missing": "EVAL_CASES_MISSING",
        }

        for label, source in sources.items():
            with self.subTest(label=label):
                object_name = "Evaluation" if label == "invalid_type" else "Invalid"
                with _project(
                    eval_entries=((label, "inventory", f"evaluation:{object_name}"),),
                    evaluation_source=source,
                ) as (root, _):
                    project = ProjectLoader().load(root)
                    with self.assertRaises(ProjectLoadError) as captured:
                        EvaluationLoader().load(project, label)
                self.assertEqual(expected[label], captured.exception.diagnostics[0].code)


@contextmanager
def _project(
    *,
    eval_entries: tuple[tuple[str, str, str], ...],
    evaluation_source: str = "",
):
    token = uuid4().hex
    workflow_module = f"workflow_{token}"
    evaluation_module = f"evaluation_{token}"
    remapped_entries = tuple(
        (
            suite_id,
            workflow_id,
            entrypoint.replace("evaluation:", f"{evaluation_module}:"),
        )
        for suite_id, workflow_id, entrypoint in eval_entries
    )
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / f"{workflow_module}.py").write_text(
            "from autoagent import Workflow\nworkflow = Workflow(id='inventory')\n",
            encoding="utf-8",
        )
        if evaluation_source:
            (root / f"{evaluation_module}.py").write_text(
                textwrap.dedent(evaluation_source).strip() + "\n",
                encoding="utf-8",
            )
        lines = [
            "schema_version = 1",
            "",
            "[project]",
            'name = "evaluation-test"',
            'version = "1"',
            "",
            "[[workflows]]",
            f'entrypoint = "{workflow_module}:workflow"',
        ]
        for suite_id, workflow_id, entrypoint in remapped_entries:
            lines.extend(
                (
                    "",
                    "[[eval_suites]]",
                    f'id = "{suite_id}"',
                    f'workflow_id = "{workflow_id}"',
                    f'entrypoint = "{entrypoint}"',
                )
            )
        (root / "auto-agent.toml").write_text("\n".join(lines) + "\n", encoding="utf-8")
        modules = {"workflow": workflow_module, "evaluation": evaluation_module}
        try:
            yield root, modules
        finally:
            sys.modules.pop(workflow_module, None)
            sys.modules.pop(evaluation_module, None)


if __name__ == "__main__":
    unittest.main()
