from __future__ import annotations

from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py


class AutoAgentBuildPy(build_py):
    """Reject Wheels that were built without the generated tracing UI."""

    def run(self) -> None:
        ui_index = (
            Path(__file__).resolve().parent
            / "autoagent"
            / "core"
            / "server"
            / "ui"
            / "index.html"
        )
        if not ui_index.is_file():
            raise RuntimeError(
                "The generated tracing UI is missing. "
                "Build release Wheels with "
                "`python scripts/build_wheel.py`."
            )
        super().run()


setup(cmdclass={"build_py": AutoAgentBuildPy})
