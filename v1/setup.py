from __future__ import annotations

from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py


class AutoAgentBuildPy(build_py):
    """Reject Wheels missing generated UI or packaged authoring examples."""

    def run(self) -> None:
        package_root = Path(__file__).resolve().parent / "autoagent"
        example_root = package_root / "examples" / "authoring"
        required = (
            package_root / "core" / "server" / "ui" / "index.html",
            example_root / "auto-agent.toml",
        )
        missing = [str(path) for path in required if not path.is_file()]
        if missing:
            raise RuntimeError(
                "Generated Wheel resources are missing: "
                f"{', '.join(missing)}. Build release Wheels with "
                "`python scripts/build_wheel.py`."
            )

        expected_example_files = tuple(
            path.relative_to(example_root)
            for path in example_root.rglob("*")
            if path.is_file()
        )
        super().run()

        built_example_root = (
            Path(self.build_lib) / "autoagent" / "examples" / "authoring"
        )
        missing_from_build = [
            str(relative)
            for relative in expected_example_files
            if not (built_example_root / relative).is_file()
        ]
        if missing_from_build:
            raise RuntimeError(
                "Authoring example files are missing from the Wheel build: "
                f"{', '.join(missing_from_build)}."
            )


setup(cmdclass={"build_py": AutoAgentBuildPy})
