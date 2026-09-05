from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import sys
import tempfile
import textwrap
import unittest
from collections.abc import Iterator

from autoagent.project import (
    ProjectLoadError,
    ProjectLoader,
    find_project_manifest,
    load_project_manifest,
)


class ProjectLoaderTests(unittest.TestCase):
    def test_authoring_example_manifest_matches_v1_schema(self) -> None:
        repository_root = Path(__file__).resolve().parents[1]

        manifest = load_project_manifest(
            repository_root / "examples" / "authoring" / "auto-agent.toml"
        )

        self.assertEqual(1, manifest.schema_version)
        self.assertEqual("autoagent-authoring-samples", manifest.project.name)
        self.assertEqual(3, len(manifest.workflows))

    def test_manifest_parses_project_and_entrypoint(self) -> None:
        with self.project(
            """
            schema_version = 1

            [project]
            name = "weather-agent"
            version = "0.1.0"
            description = "Weather workflows"

            [[workflows]]
            entrypoint = "workflows.weather:workflow"
            """
        ) as root:
            manifest = load_project_manifest(root / "auto-agent.toml")

        self.assertEqual("weather-agent", manifest.project.name)
        self.assertEqual("0.1.0", manifest.project.version)
        self.assertEqual("workflows.weather", manifest.workflows[0].module_name)
        self.assertEqual("workflow", manifest.workflows[0].object_path)

    def test_manifest_rejects_runtime_configuration_and_unknown_fields(self) -> None:
        with self.project(
            """
            schema_version = 1

            [project]
            name = "invalid"
            version = "1"

            [server]
            port = 8000

            [[workflows]]
            entrypoint = "workflow:workflow"
            """
        ) as root:
            with self.assertRaises(ProjectLoadError) as captured:
                load_project_manifest(root / "auto-agent.toml")

        diagnostic = captured.exception.diagnostics[0]
        self.assertEqual("PROJECT_MANIFEST_INVALID", diagnostic.code)
        self.assertEqual("server", diagnostic.field)

    def test_manifest_rejects_invalid_and_duplicate_entrypoints(self) -> None:
        with self.project(
            """
            schema_version = 1

            [project]
            name = "invalid"
            version = "1"

            [[workflows]]
            entrypoint = "not an entrypoint"
            """
        ) as root:
            with self.assertRaises(ProjectLoadError) as captured:
                load_project_manifest(root / "auto-agent.toml")
        self.assertEqual(
            "workflows.0.entrypoint",
            captured.exception.diagnostics[0].field,
        )

        with self.project(
            """
            schema_version = 1

            [project]
            name = "duplicate"
            version = "1"

            [[workflows]]
            entrypoint = "workflow:workflow"

            [[workflows]]
            entrypoint = "workflow:workflow"
            """
        ) as root:
            with self.assertRaises(ProjectLoadError) as captured:
                load_project_manifest(root / "auto-agent.toml")
        self.assertEqual("PROJECT_MANIFEST_INVALID", captured.exception.diagnostics[0].code)

    def test_find_manifest_walks_up_from_nested_directory(self) -> None:
        with self.project(self.valid_manifest("workflow:workflow")) as root:
            nested = root / "nested" / "deeper"
            nested.mkdir(parents=True)

            found = find_project_manifest(nested)

        self.assertEqual(root / "auto-agent.toml", found)

    def test_loader_imports_only_explicit_workflow_objects(self) -> None:
        with self.project(
            self.valid_manifest(
                "weather_workflows:weather",
                "weather_workflows:translation",
            ),
            modules={
                "weather_workflows.py": """
                    from autoagent import Workflow

                    weather = Workflow(id="weather", version="1")
                    translation = Workflow(id="translation", version="2")
                    not_listed = Workflow(id="hidden", version="1")
                """,
            },
        ) as root:
            definition = ProjectLoader().load(root / "auto-agent.toml")

        self.assertEqual(root.resolve(), definition.root)
        self.assertEqual(
            ["weather", "translation"],
            [item.workflow.id for item in definition.workflows],
        )
        self.assertEqual("translation", definition.workflow_by_id("translation").id)
        with self.assertRaises(KeyError):
            definition.workflow_by_id("hidden")

    def test_loader_reports_missing_module_object_and_invalid_type(self) -> None:
        with self.project(
            self.valid_manifest(
                "missing_module:workflow",
                "broken_exports:missing",
                "broken_exports:not_workflow",
            ),
            modules={
                "broken_exports.py": """
                    not_workflow = object()
                """,
            },
        ) as root:
            with self.assertRaises(ProjectLoadError) as captured:
                ProjectLoader().load(root / "auto-agent.toml")

        self.assertEqual(
            [
                "WORKFLOW_MODULE_IMPORT_FAILED",
                "WORKFLOW_OBJECT_INVALID",
                "WORKFLOW_OBJECT_NOT_FOUND",
            ],
            sorted(item.code for item in captured.exception.diagnostics),
        )
        self.assertTrue(
            all(item.path == str(root / "auto-agent.toml") for item in captured.exception.diagnostics)
        )

    def test_loader_rejects_duplicate_workflow_ids(self) -> None:
        with self.project(
            self.valid_manifest("first:workflow", "second:workflow"),
            modules={
                "first.py": """
                    from autoagent import Workflow
                    workflow = Workflow(id="same")
                """,
                "second.py": """
                    from autoagent import Workflow
                    workflow = Workflow(id="same")
                """,
            },
        ) as root:
            with self.assertRaises(ProjectLoadError) as captured:
                ProjectLoader().load(root)

        diagnostic = captured.exception.diagnostics[0]
        self.assertEqual("WORKFLOW_ID_DUPLICATE", diagnostic.code)
        self.assertEqual(
            ["first:workflow", "second:workflow"],
            diagnostic.metadata["entrypoints"],
        )

    def test_loader_loads_standalone_workflow_file_and_nested_object(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "workflow_helper.py").write_text(
                "WORKFLOW_ID = 'standalone'\n",
                encoding="utf-8",
            )
            workflow_file = root / "draft-workflow.py"
            workflow_file.write_text(
                textwrap.dedent(
                    """
                    from autoagent import Workflow
                    from workflow_helper import WORKFLOW_ID

                    def execute(value: str) -> str:
                        return value.upper()

                    class Exports:
                        pass

                    exports = Exports()
                    exports.review = Workflow(id=WORKFLOW_ID)
                    exports.review.add_node(execute, node_id="execute")
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            try:
                definition = ProjectLoader().load_workflow_file(
                    workflow_file,
                    object_path="exports.review",
                )
            finally:
                sys.modules.pop("workflow_helper", None)

        self.assertIsNone(definition.manifest_path)
        self.assertEqual(root.resolve(), definition.root)
        self.assertEqual("standalone", definition.workflows[0].workflow.id)
        self.assertEqual(
            f"{workflow_file.resolve()}:exports.review",
            definition.workflows[0].locator.entrypoint,
        )

    def test_standalone_workflow_file_reports_import_and_export_errors(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            broken = root / "broken.py"
            broken.write_text("raise RuntimeError('broken import')\n", encoding="utf-8")
            with self.assertRaises(ProjectLoadError) as imported:
                ProjectLoader().load_workflow_file(broken)

            exports = root / "exports.py"
            exports.write_text("not_workflow = object()\n", encoding="utf-8")
            with self.assertRaises(ProjectLoadError) as missing:
                ProjectLoader().load_workflow_file(exports)
            with self.assertRaises(ProjectLoadError) as invalid:
                ProjectLoader().load_workflow_file(
                    exports,
                    object_path="not_workflow",
                )

        self.assertEqual(
            "WORKFLOW_FILE_IMPORT_FAILED",
            imported.exception.diagnostics[0].code,
        )
        self.assertEqual(
            "WORKFLOW_OBJECT_NOT_FOUND",
            missing.exception.diagnostics[0].code,
        )
        self.assertEqual(
            "WORKFLOW_OBJECT_INVALID",
            invalid.exception.diagnostics[0].code,
        )

    def test_manifest_errors_are_machine_readable(self) -> None:
        with self.project(
            """
            schema_version = 2

            [project]
            name = ""
            version = "1"

            workflows = []
            """
        ) as root:
            with self.assertRaises(ProjectLoadError) as captured:
                ProjectLoader().load(root)

        fields = {item.field for item in captured.exception.diagnostics}
        self.assertIn("schema_version", fields)
        self.assertIn("project.name", fields)
        self.assertIn("workflows", fields)
        for diagnostic in captured.exception.diagnostics:
            self.assertIsNotNone(diagnostic.code)
            self.assertIsNotNone(diagnostic.message)
            self.assertIsNotNone(diagnostic.hint)

    @staticmethod
    def valid_manifest(*entrypoints: str) -> str:
        workflow_blocks = "\n\n".join(
            f'[[workflows]]\nentrypoint = "{entrypoint}"'
            for entrypoint in entrypoints
        )
        return (
            'schema_version = 1\n\n'
            "[project]\n"
            'name = "test-project"\n'
            'version = "1.0.0"\n\n'
            f"{workflow_blocks}\n"
        )

    @contextmanager
    def project(
        self,
        manifest: str,
        *,
        modules: dict[str, str] | None = None,
    ) -> Iterator[Path]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "auto-agent.toml").write_text(
                textwrap.dedent(manifest).strip() + "\n",
                encoding="utf-8",
            )
            module_names: list[str] = []
            for relative_path, source in (modules or {}).items():
                target = root / relative_path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(
                    textwrap.dedent(source).strip() + "\n",
                    encoding="utf-8",
                )
                if target.suffix == ".py":
                    module_names.append(
                        ".".join(target.relative_to(root).with_suffix("").parts)
                    )

            original_cwd = Path.cwd()
            try:
                os.chdir(root)
                yield root
            finally:
                os.chdir(original_cwd)
                for module_name in module_names:
                    sys.modules.pop(module_name, None)


if __name__ == "__main__":
    unittest.main()
