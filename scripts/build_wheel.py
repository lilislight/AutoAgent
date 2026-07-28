from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
UI_ROOT = REPOSITORY_ROOT / "ui"
UI_DIST = UI_ROOT / "dist"
UI_STAGING = REPOSITORY_ROOT / "autoagent" / "core" / "server" / "ui"
EXAMPLES_SOURCE = REPOSITORY_ROOT / "examples" / "authoring"
EXAMPLES_STAGING = REPOSITORY_ROOT / "autoagent" / "examples" / "authoring"


def _build_ui() -> None:
    subprocess.run(
        ["npm", "run", "build"],
        cwd=UI_ROOT,
        check=True,
    )
    if not (UI_DIST / "index.html").is_file():
        raise RuntimeError("UI build completed without producing dist/index.html.")


def _stage_ui() -> None:
    shutil.rmtree(UI_STAGING, ignore_errors=True)
    shutil.copytree(
        UI_DIST,
        UI_STAGING,
        ignore=shutil.ignore_patterns("*.map"),
    )


def _stage_examples() -> None:
    if not (EXAMPLES_SOURCE / "auto-agent.toml").is_file():
        raise RuntimeError(
            "Authoring examples are missing examples/authoring/auto-agent.toml."
        )
    shutil.rmtree(EXAMPLES_STAGING, ignore_errors=True)
    shutil.copytree(
        EXAMPLES_SOURCE,
        EXAMPLES_STAGING,
        ignore=shutil.ignore_patterns(
            "__pycache__",
            "*.pyc",
            ".autoagent",
        ),
    )


def _build_wheel() -> None:
    subprocess.run(
        [sys.executable, "-m", "build", "--wheel"],
        cwd=REPOSITORY_ROOT,
        check=True,
    )


def main() -> int:
    try:
        _build_ui()
        _stage_ui()
        _stage_examples()
        _build_wheel()
    finally:
        shutil.rmtree(UI_STAGING, ignore_errors=True)
        shutil.rmtree(EXAMPLES_STAGING, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
