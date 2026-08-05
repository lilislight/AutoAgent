#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys


_INVOCATION_PATTERN = re.compile(r"^INVOCATION ([0-9a-f-]+)$", re.MULTILINE)


def main() -> int:
    arguments = _parser().parse_args()
    workspace = arguments.workspace.resolve()
    real_cli = _resolve_real_cli(arguments.real_cli)
    evaluation_root = workspace / ".evaluation"
    if evaluation_root.exists():
        raise RuntimeError(
            f"Evaluation is already prepared: {evaluation_root}"
        )
    requirements = workspace / "REQUIREMENTS.md"
    input_file = workspace / arguments.input_file
    for required in (
        workspace / "auto-agent.toml",
        workspace / ".env.example",
        requirements,
        input_file,
    ):
        if not required.is_file():
            raise FileNotFoundError(required)
    incident_text = requirements.read_text(encoding="utf-8")
    if incident_text.count("<INVOCATION_ID>") != 1:
        raise RuntimeError(
            "REQUIREMENTS.md must contain exactly one <INVOCATION_ID>."
        )
    env_file = workspace / ".env"
    if not env_file.exists():
        shutil.copy2(workspace / ".env.example", env_file)

    setup_root = evaluation_root / "setup"
    setup_root.mkdir(parents=True)
    checks = (
        ("project-check", ["project", "check"]),
        (
            "workflow-check",
            ["workflow", "check", arguments.workflow_id],
        ),
        ("eval-check", ["eval", "check", arguments.workflow_id]),
    )
    for name, command in checks:
        _run_setup_command(real_cli, workspace, setup_root, name, command)
    invocation_output = _run_setup_command(
        real_cli,
        workspace,
        setup_root,
        "incident",
        [
            "invocation",
            "run",
            arguments.workflow_id,
            "--event-mode",
            arguments.event_mode,
            "--store",
            "database",
            "--input-file",
            arguments.input_file,
        ],
    )
    match = _INVOCATION_PATTERN.search(invocation_output)
    if match is None:
        raise RuntimeError("Incident command did not return an Invocation ID.")
    invocation_id = match.group(1)
    requirements.write_text(
        incident_text.replace("<INVOCATION_ID>", invocation_id),
        encoding="utf-8",
    )

    baseline = {
        "schema_version": 1,
        "source_invocation_id": invocation_id,
        "workflow_id": arguments.workflow_id,
        "event_mode": arguments.event_mode,
        "protected_hashes": _protected_hashes(workspace),
        "project_hashes": _project_hashes(workspace),
    }
    (evaluation_root / "baseline.json").write_text(
        json.dumps(baseline, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (evaluation_root / "config.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "real_cli": str(real_cli),
                "workspace": str(workspace),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    commands = evaluation_root / "commands"
    commands.mkdir()
    proxy = evaluation_root / "bin" / "autoagent"
    proxy.parent.mkdir()
    shutil.copy2(Path(__file__).with_name("cli_proxy.py"), proxy)
    proxy.chmod(0o755)

    print(f"WORKSPACE {workspace}")
    print(f"SOURCE_INVOCATION {invocation_id}")
    print(f"REAL_CLI {real_cli}")
    print(f'ACTIVATE export PATH="{proxy.parent}:$PATH"')
    print("PREPARE_RESULT ready")
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare one isolated AutoAgent Debug Skill evaluation."
    )
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--real-cli", type=Path)
    parser.add_argument("--workflow-id", default="fulfillment_review")
    parser.add_argument(
        "--input-file",
        default="inputs/high-value-order.json",
    )
    parser.add_argument(
        "--event-mode",
        choices=("standard", "full"),
        default="full",
    )
    return parser


def _resolve_real_cli(explicit: Path | None) -> Path:
    candidate = str(explicit) if explicit is not None else shutil.which("autoagent")
    if not candidate:
        raise RuntimeError("autoagent executable was not found.")
    resolved = Path(candidate).resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    return resolved


def _run_setup_command(
    real_cli: Path,
    workspace: Path,
    output_root: Path,
    name: str,
    arguments: list[str],
) -> str:
    completed = subprocess.run(
        [str(real_cli), *arguments],
        cwd=workspace,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    (output_root / f"{name}.stdout.txt").write_text(
        completed.stdout,
        encoding="utf-8",
    )
    (output_root / f"{name}.stderr.txt").write_text(
        completed.stderr,
        encoding="utf-8",
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"Setup command failed ({name}, exit={completed.returncode}):\n"
            f"{completed.stdout}{completed.stderr}"
        )
    return completed.stdout


def _protected_hashes(workspace: Path) -> dict[str, str]:
    paths = [workspace / "REQUIREMENTS.md", workspace / "auto-agent.toml"]
    paths.extend(sorted((workspace / "inputs").rglob("*")))
    paths.extend(sorted((workspace / "evals").rglob("*")))
    return _hash_files(workspace, paths)


def _project_hashes(workspace: Path) -> dict[str, str]:
    paths: list[Path] = []
    for suffix in ("*.py", "*.toml", "*.json", "*.md"):
        paths.extend(workspace.rglob(suffix))
    return _hash_files(workspace, paths)


def _hash_files(workspace: Path, paths: list[Path]) -> dict[str, str]:
    values: dict[str, str] = {}
    for path in sorted(set(paths)):
        if not path.is_file() or _excluded(path, workspace):
            continue
        values[str(path.relative_to(workspace))] = _sha256(path)
    return values


def _excluded(path: Path, workspace: Path) -> bool:
    relative = path.relative_to(workspace)
    return any(
        part in {".evaluation", ".git", ".venv", "__pycache__", "reports"}
        for part in relative.parts
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as value:
        while chunk := value.read(64 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(f"PREPARE_ERROR {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
