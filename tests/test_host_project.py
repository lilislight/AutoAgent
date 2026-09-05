from __future__ import annotations

import os
import sys
import tempfile
import textwrap
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Iterator
from unittest.mock import patch

from pydantic import ValidationError

from autoagent.host import (
    HostSettings,
    HostSettingsError,
    ProjectLoadError,
    ProjectLoader,
    load_host_settings,
    load_project_environment,
    load_project_manifest,
    project_environment_scope,
    resolve_manifest_path,
)


class HostProjectTests(unittest.TestCase):
    def test_manifest_discovery_uses_the_nearest_existing_project(self) -> None:
        """Find the nearest ancestor manifest while keeping explicit paths strict."""

        with self.project(self.manifest("workflow:workflow")) as root:
            nested_project = root / "nested"
            nested_project.mkdir()
            nested_manifest = nested_project / "autoagent.toml"
            nested_manifest.write_text(
                self.manifest("workflow:workflow"),
                encoding="utf-8",
            )
            working_directory = nested_project / "src" / "package"
            working_directory.mkdir(parents=True)

            self.assertEqual(
                resolve_manifest_path(working_directory),
                nested_manifest.resolve(),
            )
            with patch(
                "autoagent.host.manifest.Path.cwd",
                return_value=working_directory,
            ):
                self.assertEqual(
                    resolve_manifest_path(),
                    nested_manifest.resolve(),
                )
            self.assertEqual(
                resolve_manifest_path(root / "autoagent.toml"),
                (root / "autoagent.toml").resolve(),
            )
            explicit_missing = working_directory / "autoagent.toml"
            with self.assertRaises(ProjectLoadError) as captured:
                load_project_manifest(explicit_missing)
            self.assertEqual(
                captured.exception.diagnostics[0].code,
                "PROJECT_MANIFEST_NOT_FOUND",
            )
            self.assertEqual(
                captured.exception.diagnostics[0].path,
                str(explicit_missing.resolve()),
            )

            wrong_file = root / "project.toml"
            wrong_file.write_text("", encoding="utf-8")
            with self.assertRaises(ProjectLoadError) as captured:
                resolve_manifest_path(wrong_file)
            self.assertEqual(
                captured.exception.diagnostics[0].code,
                "PROJECT_MANIFEST_FILENAME_INVALID",
            )

            with self.assertRaises(ProjectLoadError) as captured:
                resolve_manifest_path(root / "missing" / "project")
            self.assertEqual(
                captured.exception.diagnostics[0].code,
                "PROJECT_MANIFEST_NOT_FOUND",
            )

    def test_manifest_parses_the_standard_strict_schema(self) -> None:
        """Verify autoagent.toml exposes immutable project and locator values."""

        with self.project(self.manifest("host_valid_workflow:workflow")) as root:
            manifest = load_project_manifest(root)

        self.assertEqual(manifest.schema_version, 1)
        self.assertEqual(manifest.project.name, "example")
        self.assertEqual(manifest.workflows[0].module_name, "host_valid_workflow")
        self.assertEqual(manifest.workflows[0].object_path, "workflow")
        with self.assertRaises(ValidationError):
            manifest.project.name = "changed"  # type: ignore[misc]

    def test_manifest_rejects_wrong_filename_invalid_toml_and_extra_fields(self) -> None:
        """Verify filename, TOML syntax, and unknown schema keys have stable errors."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wrong = root / "project.toml"
            wrong.write_text(self.manifest("workflow:workflow"), encoding="utf-8")
            with self.assertRaises(ProjectLoadError) as captured:
                load_project_manifest(wrong)
            self.assertEqual(
                captured.exception.diagnostics[0].code,
                "PROJECT_MANIFEST_FILENAME_INVALID",
            )

            standard = root / "autoagent.toml"
            standard.write_text("schema_version = [", encoding="utf-8")
            with self.assertRaises(ProjectLoadError) as captured:
                load_project_manifest(standard)
            self.assertEqual(
                captured.exception.diagnostics[0].code,
                "PROJECT_MANIFEST_INVALID_TOML",
            )

            standard.write_text(
                self.manifest("workflow:workflow") + "\nunknown = true\n",
                encoding="utf-8",
            )
            with self.assertRaises(ProjectLoadError) as captured:
                load_project_manifest(standard)
            self.assertEqual(
                captured.exception.diagnostics[0].code,
                "PROJECT_MANIFEST_INVALID",
            )

    def test_manifest_rejects_type_coercion_and_bad_or_duplicate_entrypoints(self) -> None:
        """Verify strict types and the module:object grammar are enforced."""

        cases = (
            self.manifest("workflow:workflow").replace(
                'version = "0.1.0"', "version = 1"
            ),
            self.manifest("not an entrypoint"),
            self.manifest("workflow:workflow", "workflow:workflow"),
        )
        for source in cases:
            with self.subTest(source=source):
                with self.project(source) as root:
                    with self.assertRaises(ProjectLoadError) as captured:
                        load_project_manifest(root)
                self.assertEqual(
                    captured.exception.diagnostics[0].code,
                    "PROJECT_MANIFEST_INVALID",
                )

    def test_environment_file_is_overlaid_by_process_values(self) -> None:
        """Verify .env is optional and real environment values take precedence."""

        with self.project(self.manifest("workflow:workflow")) as root:
            (root / ".env").write_text(
                """
                AUTOAGENT_MAX_OPERATOR_CONCURRENCY=4
                AUTOAGENT_TRACE_HOST='from-file'
                CUSTOM_SECRET=file-secret
                """,
                encoding="utf-8",
            )
            environment = load_project_environment(
                root,
                environ={
                    "AUTOAGENT_MAX_OPERATOR_CONCURRENCY": "9",
                    "PROCESS_ONLY": "present",
                },
            )
            settings = load_host_settings(
                root,
                environ={"AUTOAGENT_MAX_OPERATOR_CONCURRENCY": "9"},
            )

        self.assertEqual(environment["AUTOAGENT_MAX_OPERATOR_CONCURRENCY"], "9")
        self.assertEqual(environment["CUSTOM_SECRET"], "file-secret")
        self.assertEqual(environment["PROCESS_ONLY"], "present")
        self.assertEqual(settings.max_operator_concurrency, 9)
        self.assertEqual(settings.trace_host, "from-file")
        self.assertIsNone(settings.model_extra)

    def test_environment_file_can_be_disabled_completely(self) -> None:
        """Ignore even an explicit malformed env file when loading is disabled."""

        with self.project(self.manifest("workflow:workflow")) as root:
            (root / ".env").write_text("not an assignment\n", encoding="utf-8")
            environment = load_project_environment(
                root,
                env_file=".env",
                use_env_file=False,
                environ={"PROCESS_ONLY": "present"},
            )
            settings = load_host_settings(
                root,
                env_file=".env",
                use_env_file=False,
                environ={"AUTOAGENT_RUNTIME_EVENT_SINK": "none"},
            )

        self.assertEqual(environment, {"PROCESS_ONLY": "present"})
        self.assertEqual(settings.runtime_event_sink, "none")

    def test_project_environment_scope_restores_the_exact_process_state(self) -> None:
        """Apply an explicit snapshot temporarily and restore caller values."""

        with patch.dict(
            os.environ,
            {"ORIGINAL_VALUE": "preserved"},
            clear=True,
        ):
            with project_environment_scope({"PROJECT_VALUE": "visible"}):
                self.assertEqual(
                    dict(os.environ),
                    {"PROJECT_VALUE": "visible"},
                )
                os.environ["PROJECT_MUTATION"] = "temporary"
            self.assertEqual(
                dict(os.environ),
                {"ORIGINAL_VALUE": "preserved"},
            )

    def test_settings_apply_defaults_and_resolve_project_paths(self) -> None:
        """Verify default and configured relative paths resolve under the project."""

        with self.project(self.manifest("workflow:workflow")) as root:
            defaults = load_host_settings(root, environ={})
            configured = load_host_settings(
                root,
                environ={
                    "AUTOAGENT_SQLITE_PATH": "data/events.db",
                    "AUTOAGENT_TRACE_UI_DIRECTORY": "web/dist",
                    "AUTOAGENT_RUNTIME_EVENT_SINK": "none",
                },
            )

        self.assertEqual(
            defaults.sqlite_path,
            (root / ".autoagent" / "runtime.db").resolve(),
        )
        self.assertEqual(configured.sqlite_path, (root / "data/events.db").resolve())
        self.assertEqual(
            configured.trace_ui_directory,
            (root / "web/dist").resolve(),
        )
        self.assertEqual(configured.runtime_event_sink, "none")

    def test_settings_reject_invalid_environment_and_remain_strict(self) -> None:
        """Verify invalid numeric, enum, and direct model types cannot be coerced."""

        with self.project(self.manifest("workflow:workflow")) as root:
            with self.assertRaises(HostSettingsError) as captured:
                load_host_settings(
                    root,
                    environ={
                        "AUTOAGENT_MAX_OPERATOR_CONCURRENCY": "0",
                        "AUTOAGENT_RUNTIME_EVENT_SINK": "database",
                        "AUTOAGENT_TRACE_PORT": "70000",
                    },
                )
        fields = {item.field for item in captured.exception.diagnostics}
        self.assertIn("AUTOAGENT_MAX_OPERATOR_CONCURRENCY", fields)
        self.assertIn("AUTOAGENT_RUNTIME_EVENT_SINK", fields)
        self.assertIn("AUTOAGENT_TRACE_PORT", fields)

        with self.assertRaises(ValidationError):
            HostSettings(
                sqlite_path=Path("runtime.db"),
                max_operator_concurrency="4",  # type: ignore[arg-type]
            )
        with self.assertRaises(ValidationError):
            HostSettings(
                sqlite_path=Path("runtime.db"),
                runtime_event_sink="http",
                http_sink_url="http://example.test:99999/events",
                http_user_event_sink_url="https://example.test/user-events",
            )
        for field in (
            "http_sink_timeout_seconds",
            "trace_refresh_seconds",
        ):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                HostSettings(**{field: 10**1000})

    def test_http_settings_require_a_valid_absolute_url(self) -> None:
        """Verify the HTTP sink cannot be selected without a usable URL."""

        with self.project(self.manifest("workflow:workflow")) as root:
            for url in (
                None,
                "relative/path",
                "ftp://example.test/events",
                "http://example.test:99999/events",
                "https://user:password@example.test/events",
            ):
                environment = {"AUTOAGENT_RUNTIME_EVENT_SINK": "http"}
                if url is not None:
                    environment["AUTOAGENT_HTTP_SINK_URL"] = url
                with self.subTest(url=url):
                    with self.assertRaises(HostSettingsError):
                        load_host_settings(root, environ=environment)

            with self.assertRaises(HostSettingsError) as captured:
                load_host_settings(
                    root,
                    environ={
                        "AUTOAGENT_RUNTIME_EVENT_SINK": "http",
                        "AUTOAGENT_HTTP_SINK_URL": (
                            "https://events.example.test/v1/runtime-events"
                        ),
                    },
                )
            self.assertIn(
                "AUTOAGENT_HTTP_USER_EVENT_SINK_URL",
                {item.field for item in captured.exception.diagnostics},
            )

            settings = load_host_settings(
                root,
                environ={
                    "AUTOAGENT_RUNTIME_EVENT_SINK": "http",
                    "AUTOAGENT_HTTP_SINK_URL": "https://events.example.test/v1",
                    "AUTOAGENT_HTTP_USER_EVENT_SINK_URL": (
                        "https://events.example.test/v1/user-events"
                    ),
                    "AUTOAGENT_HTTP_SINK_TOKEN": " token ",
                },
            )

        self.assertEqual(settings.http_sink_url, "https://events.example.test/v1")
        self.assertEqual(
            settings.http_user_event_sink_url,
            "https://events.example.test/v1/user-events",
        )
        self.assertEqual(settings.http_sink_token, "token")
        self.assertNotIn("token", repr(settings))
        self.assertNotIn("token", str(settings))

        with self.assertRaises(ValidationError) as captured:
            HostSettings(
                runtime_event_sink="http",
                http_sink_url="https://user:password@example.test/events",
                http_user_event_sink_url="https://example.test/user-events",
            )
        self.assertNotIn("password", str(captured.exception))

    def test_environment_reports_missing_or_malformed_explicit_files(self) -> None:
        """Verify explicit .env failures are diagnosed instead of silently ignored."""

        with self.project(self.manifest("workflow:workflow")) as root:
            with self.assertRaises(HostSettingsError) as captured:
                load_project_environment(root, env_file="missing.env", environ={})
            self.assertEqual(
                captured.exception.diagnostics[0].code,
                "ENV_FILE_NOT_FOUND",
            )

            invalid = root / "broken.env"
            invalid.write_text("not an assignment\n", encoding="utf-8")
            with self.assertRaises(HostSettingsError) as captured:
                load_project_environment(root, env_file=invalid, environ={})
            self.assertEqual(
                captured.exception.diagnostics[0].code,
                "ENV_FILE_LINE_INVALID",
            )

    def test_loader_imports_exact_workflows_with_a_temporary_sys_path(self) -> None:
        """Verify explicit exports load while the project path is always restored."""

        module_name = "host_project_success_module"
        before = tuple(sys.path)
        with self.project(
            self.manifest(f"{module_name}:Exports.workflow"),
            modules={
                f"{module_name}.py": """
                    import sys
                    from pathlib import Path
                    from autoagent import Workflow

                    IMPORT_SAW_ROOT = str(Path(__file__).parent) in sys.path

                    class Exports:
                        workflow = Workflow("loaded")

                    hidden = Workflow("hidden")
                """,
            },
        ) as root:
            loaded = ProjectLoader().load(root)
            self.assertEqual(tuple(sys.path), before)

        self.assertEqual(loaded.workflow_by_id("loaded").id, "loaded")
        self.assertTrue(sys.modules[module_name].IMPORT_SAW_ROOT)
        with self.assertRaises(KeyError):
            loaded.workflow_by_id("hidden")
        self.assertEqual(tuple(sys.path), before)
        sys.modules.pop(module_name, None)

    def test_loader_applies_and_restores_one_environment_snapshot(self) -> None:
        """Expose the supplied environment only while Workflow modules import."""

        module_name = "host_project_environment_snapshot"
        with self.project(
            self.manifest(f"{module_name}:workflow"),
            modules={
                f"{module_name}.py": """
                    import os
                    from autoagent import Workflow

                    IMPORT_VALUE = os.environ.get("PROJECT_IMPORT_VALUE")
                    workflow = Workflow("environment-snapshot")
                """,
            },
        ) as root:
            with patch.dict(
                os.environ,
                {"ORIGINAL_VALUE": "preserved"},
                clear=True,
            ):
                loaded = ProjectLoader().load(
                    root,
                    environment={"PROJECT_IMPORT_VALUE": "visible"},
                )
                self.assertEqual(
                    dict(os.environ),
                    {"ORIGINAL_VALUE": "preserved"},
                )

        self.assertEqual(loaded.workflows[0].workflow.id, "environment-snapshot")
        self.assertEqual(sys.modules[module_name].IMPORT_VALUE, "visible")
        sys.modules.pop(module_name, None)

    def test_loader_restores_environment_after_an_import_failure(self) -> None:
        """Restore the process environment when project module import fails."""

        module_name = "host_project_environment_failure"
        with self.project(
            self.manifest(f"{module_name}:workflow"),
            modules={f"{module_name}.py": "raise RuntimeError('broken import')\n"},
        ) as root:
            with patch.dict(
                os.environ,
                {"ORIGINAL_VALUE": "preserved"},
                clear=True,
            ):
                with self.assertRaises(ProjectLoadError):
                    ProjectLoader().load(
                        root,
                        environment={"PROJECT_IMPORT_VALUE": "temporary"},
                    )
                self.assertEqual(
                    dict(os.environ),
                    {"ORIGINAL_VALUE": "preserved"},
                )
        sys.modules.pop(module_name, None)

    def test_loader_rejects_module_names_already_owned_by_another_project(self) -> None:
        """Reject ambiguous module aliases instead of silently reusing wrong code."""

        loader = ProjectLoader()
        with self.project(
            self.manifest("workflow:workflow"),
            modules={
                "workflow.py": "from autoagent import Workflow\nworkflow = Workflow('first')\n"
            },
        ) as first_root:
            first = loader.load(first_root)
        try:
            with self.project(
                self.manifest("workflow:workflow"),
                modules={
                    "workflow.py": "from autoagent import Workflow\nworkflow = Workflow('second')\n"
                },
            ) as second_root:
                with self.assertRaises(ProjectLoadError) as captured:
                    loader.load(second_root)
            self.assertEqual(first.workflows[0].workflow.id, "first")
            self.assertEqual(
                captured.exception.diagnostics[0].code,
                "WORKFLOW_MODULE_CONFLICT",
            )
        finally:
            sys.modules.pop("workflow", None)

    def test_loader_rejects_a_preloaded_module_without_a_file_origin(self) -> None:
        """Never resolve a project entrypoint from an unverifiable module alias."""

        module_name = "host_project_originless_alias"
        alias = ModuleType(module_name)
        alias.workflow = object()
        sys.modules[module_name] = alias
        try:
            with self.project(
                self.manifest(f"{module_name}:workflow"),
                modules={
                    f"{module_name}.py": (
                        "from autoagent import Workflow\n"
                        "workflow = Workflow('from-project')\n"
                    )
                },
            ) as root:
                with self.assertRaises(ProjectLoadError) as captured:
                    ProjectLoader().load(root)
            diagnostic = captured.exception.diagnostics[0]
            self.assertEqual(diagnostic.code, "WORKFLOW_MODULE_CONFLICT")
            self.assertIn(
                f"<unknown origin: {module_name}>",
                diagnostic.metadata["namespaces"][module_name],
            )
        finally:
            sys.modules.pop(module_name, None)

    def test_loader_restores_sys_path_and_reports_entrypoint_failures(self) -> None:
        """Verify import, attribute, and type failures are aggregated safely."""

        module_name = "host_project_broken_exports"
        before = tuple(sys.path)
        with self.project(
            self.manifest(
                "host_project_missing_module:workflow",
                f"{module_name}:missing",
                f"{module_name}:not_workflow",
            ),
            modules={f"{module_name}.py": "not_workflow = object()\n"},
        ) as root:
            with self.assertRaises(ProjectLoadError) as captured:
                ProjectLoader().load(root)

        self.assertEqual(tuple(sys.path), before)
        self.assertEqual(
            {item.code for item in captured.exception.diagnostics},
            {
                "WORKFLOW_MODULE_IMPORT_FAILED",
                "WORKFLOW_OBJECT_NOT_FOUND",
                "WORKFLOW_OBJECT_INVALID",
            },
        )
        sys.modules.pop(module_name, None)

    def test_loader_wraps_system_exit_from_user_module_import(self) -> None:
        """Keep a Workflow module sys.exit call inside structured Host diagnostics."""

        module_name = "host_project_system_exit"
        with self.project(
            self.manifest(f"{module_name}:workflow"),
            modules={f"{module_name}.py": "raise SystemExit(7)\n"},
        ) as root:
            with self.assertRaises(ProjectLoadError) as captured:
                ProjectLoader().load(root)
        diagnostic = captured.exception.diagnostics[0]
        self.assertEqual(diagnostic.code, "WORKFLOW_MODULE_IMPORT_FAILED")
        self.assertEqual(diagnostic.metadata["exception_type"], "SystemExit")
        sys.modules.pop(module_name, None)

    def test_loader_structures_user_failure_while_resolving_an_object(self) -> None:
        """Keep dynamic object lookup failures inside stable Host diagnostics."""

        module_name = "host_project_object_resolution_failure"
        with self.project(
            self.manifest(f"{module_name}:workflow"),
            modules={
                f"{module_name}.py": """
                    def __getattr__(name):
                        raise RuntimeError(f"cannot resolve {name}")
                """,
            },
        ) as root:
            with self.assertRaises(ProjectLoadError) as captured:
                ProjectLoader().load(root)

        diagnostic = captured.exception.diagnostics[0]
        self.assertEqual(diagnostic.code, "WORKFLOW_OBJECT_RESOLUTION_FAILED")
        self.assertEqual(diagnostic.metadata["exception_type"], "RuntimeError")
        self.assertEqual(diagnostic.entrypoint, f"{module_name}:workflow")
        sys.modules.pop(module_name, None)

    def test_loader_rejects_duplicate_workflow_ids(self) -> None:
        """Verify distinct entrypoints cannot export the same Workflow id."""

        first = "host_project_duplicate_first"
        second = "host_project_duplicate_second"
        with self.project(
            self.manifest(f"{first}:workflow", f"{second}:workflow"),
            modules={
                f"{first}.py": "from autoagent import Workflow\nworkflow = Workflow('same')\n",
                f"{second}.py": "from autoagent import Workflow\nworkflow = Workflow('same')\n",
            },
        ) as root:
            with self.assertRaises(ProjectLoadError) as captured:
                ProjectLoader().load(root)

        diagnostic = captured.exception.diagnostics[0]
        self.assertEqual(diagnostic.code, "WORKFLOW_ID_DUPLICATE")
        self.assertEqual(
            diagnostic.metadata["entrypoints"],
            [f"{first}:workflow", f"{second}:workflow"],
        )
        sys.modules.pop(first, None)
        sys.modules.pop(second, None)

    @staticmethod
    def manifest(*entrypoints: str) -> str:
        workflow_tables = "\n".join(
            f'[[workflows]]\nentrypoint = "{entrypoint}"\n'
            for entrypoint in entrypoints
        )
        return textwrap.dedent(
            f"""
            schema_version = 1

            [project]
            name = "example"
            version = "0.1.0"
            description = "Host test project"

            {workflow_tables}
            """
        )

    @staticmethod
    @contextmanager
    def project(
        manifest: str,
        *,
        modules: dict[str, str] | None = None,
    ) -> Iterator[Path]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "autoagent.toml").write_text(manifest, encoding="utf-8")
            for relative, source in (modules or {}).items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(textwrap.dedent(source), encoding="utf-8")
            yield root


if __name__ == "__main__":
    unittest.main()
