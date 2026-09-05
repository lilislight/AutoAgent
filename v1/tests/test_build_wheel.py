from __future__ import annotations

from unittest import TestCase
from unittest.mock import patch

from scripts import build_wheel


class BuildWheelTests(TestCase):
    def test_python_build_removes_stale_build_tree(self) -> None:
        with (
            patch.object(build_wheel.shutil, "rmtree") as rmtree,
            patch.object(build_wheel.subprocess, "run") as run,
        ):
            build_wheel._build_wheel()

        rmtree.assert_called_once_with(
            build_wheel.PYTHON_BUILD_ROOT,
            ignore_errors=True,
        )
        run.assert_called_once_with(
            [
                build_wheel.sys.executable,
                "-m",
                "build",
                "--wheel",
            ],
            cwd=build_wheel.REPOSITORY_ROOT,
            check=True,
        )

    def test_windows_resolves_npm_cmd(self) -> None:
        def which(command: str) -> str | None:
            return "C:\\Program Files\\nodejs\\npm.cmd" if command == "npm.cmd" else None

        with (
            patch.object(build_wheel.sys, "platform", "win32"),
            patch.object(build_wheel.shutil, "which", side_effect=which),
        ):
            executable = build_wheel._npm_executable()

        self.assertEqual("C:\\Program Files\\nodejs\\npm.cmd", executable)

    def test_posix_resolves_npm(self) -> None:
        with (
            patch.object(build_wheel.sys, "platform", "linux"),
            patch.object(
                build_wheel.shutil,
                "which",
                return_value="/usr/bin/npm",
            ) as which,
        ):
            executable = build_wheel._npm_executable()

        self.assertEqual("/usr/bin/npm", executable)
        which.assert_called_once_with("npm")

    def test_missing_npm_has_clear_error(self) -> None:
        with (
            patch.object(build_wheel.sys, "platform", "win32"),
            patch.object(build_wheel.shutil, "which", return_value=None),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "npm is required to build the tracing UI",
            ):
                build_wheel._npm_executable()
