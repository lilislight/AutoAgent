#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any


_CANDIDATE_PATTERN = re.compile(r"^CANDIDATE ([0-9a-f-]+)$", re.MULTILINE)


def main() -> int:
    workspace = _parser().parse_args().workspace.resolve()
    evaluation_root = workspace / ".evaluation"
    baseline = _load_json(evaluation_root / "baseline.json")
    config = _load_json(evaluation_root / "config.json")
    records = [
        _load_json(path)
        for path in sorted((evaluation_root / "commands").glob("*.json"))
    ]
    checks: list[tuple[str, bool, str]] = []

    protected = baseline["protected_hashes"]
    current_protected = {
        path: _sha256(workspace / path)
        for path in protected
        if (workspace / path).is_file()
    }
    checks.append(
        (
            "protected_files_unchanged",
            current_protected == protected,
            _hash_difference(protected, current_protected),
        )
    )
    checks.append(
        (
            "workflow_uses_public_api",
            not _workflow_contains_forbidden_import(workspace),
            "workflow source must not import framework internals or database clients",
        )
    )
    source_id = baseline["source_invocation_id"]
    first_command = records[0] if records else None
    checks.append(
        (
            "report_first",
            _matches(first_command, "invocation", "report", source_id),
            "first audited AutoAgent command must report the source ID",
        )
    )
    report_index = _find(records, "invocation", "report", source_id)
    query_index = _find_bounded_query(records, source_id)
    rerun_index = _find(records, "invocation", "rerun", source_id)
    checks.append(
        (
            "bounded_evidence_query",
            query_index is not None,
            "expected a bounded source Node, Edge, or Operator Call query",
        )
    )
    checks.append(
        (
            "rerun_source_boundary",
            rerun_index is not None,
            "expected invocation rerun of the source ID",
        )
    )
    candidate_id = _candidate_id(evaluation_root, records, rerun_index)
    checks.append(
        (
            "candidate_created",
            candidate_id is not None and candidate_id != source_id,
            "Rerun must return a distinct candidate Invocation ID",
        )
    )
    compare_index = (
        None
        if candidate_id is None
        else _find(records, "invocation", "compare", source_id, candidate_id)
    )
    checks.append(
        (
            "comparison_uses_observed_ids",
            compare_index is not None,
            "expected comparison of source and Rerun candidate IDs",
        )
    )
    ordered = (
        None not in {report_index, query_index, rerun_index, compare_index}
        and report_index < query_index < rerun_index < compare_index
    )
    checks.append(
        (
            "debug_command_order",
            ordered,
            "expected Report -> Query -> Rerun -> Comparison order",
        )
    )
    for name, command in (
        ("project_check", ("project", "check")),
        ("workflow_check", ("workflow", "check", baseline["workflow_id"])),
        ("eval_check", ("eval", "check", baseline["workflow_id"])),
    ):
        checks.append(
            (
                name,
                _find(records, *command) is not None,
                f"expected {' '.join(command)}",
            )
        )
    eval_index = _find_after(
        records,
        compare_index,
        "eval",
        "run",
        baseline["workflow_id"],
    )
    eval_passed = eval_index is not None and "RESULT passed" in _stdout(
        evaluation_root,
        records[eval_index],
    )
    checks.append(
        (
            "evaluation_passed_after_comparison",
            eval_passed,
            "expected the registered Eval Suite to pass after Comparison",
        )
    )
    if candidate_id is not None:
        source_report = _report(
            Path(config["real_cli"]),
            workspace,
            source_id,
        )
        candidate_report = _report(
            Path(config["real_cli"]),
            workspace,
            candidate_id,
        )
        checks.extend(
            [
                (
                    "candidate_mode_matches_source",
                    _field(source_report, "EVENT_MODE")
                    == _field(candidate_report, "EVENT_MODE")
                    == baseline["event_mode"],
                    "source and candidate Event modes must match",
                ),
                (
                    "candidate_session_is_isolated",
                    _field(source_report, "SESSION")
                    != _field(candidate_report, "SESSION"),
                    "source and candidate Sessions must differ",
                ),
            ]
        )
    else:
        checks.extend(
            [
                ("candidate_mode_matches_source", False, "candidate unavailable"),
                ("candidate_session_is_isolated", False, "candidate unavailable"),
            ]
        )
    comparison_output = (
        ""
        if compare_index is None
        else _stdout(evaluation_root, records[compare_index])
    )
    checks.append(
        (
            "comparison_preserves_request_boundary",
            "INPUT_EQUAL true" in comparison_output
            and "ENTRY_NODE_EQUAL true" in comparison_output,
            "Comparison must confirm equal input and entry Node",
        )
    )

    changed = _changed_project_files(workspace, baseline["project_hashes"])
    for name, passed, detail in checks:
        print(f"CHECK {name} {'passed' if passed else 'failed'}")
        if not passed:
            print(f"  {detail}")
    print("CHANGED_FILES " + (", ".join(changed) if changed else "<none>"))
    print(f"SOURCE_INVOCATION {source_id}")
    print(f"CANDIDATE_INVOCATION {candidate_id or '<missing>'}")
    print(
        "PROCESS_VISIBILITY autoagent CLI calls are audited; non-CLI shell "
        "activity requires the Coding Agent transcript for verification"
    )
    passed = all(value for _, value, _ in checks)
    print(f"REVIEW_RESULT {'passed' if passed else 'failed'}")
    return 0 if passed else 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Review observable AutoAgent Debug Skill evaluation evidence."
    )
    parser.add_argument("--workspace", type=Path, required=True)
    return parser


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _command(record: dict[str, Any], group: str) -> list[str] | None:
    arguments = record["arguments"]
    try:
        index = arguments.index(group)
    except ValueError:
        return None
    return arguments[index:]


def _matches(record: dict[str, Any] | None, *prefix: str) -> bool:
    if record is None:
        return False
    command = _command(record, prefix[0])
    return command is not None and command[: len(prefix)] == list(prefix)


def _find(records: list[dict[str, Any]], *prefix: str) -> int | None:
    for index, record in enumerate(records):
        if _matches(record, *prefix):
            return index
    return None


def _find_after(
    records: list[dict[str, Any]],
    after: int | None,
    *prefix: str,
) -> int | None:
    if after is None:
        return None
    for index in range(after + 1, len(records)):
        if _matches(records[index], *prefix):
            return index
    return None


def _find_bounded_query(
    records: list[dict[str, Any]],
    source_id: str,
) -> int | None:
    for index, record in enumerate(records):
        command = _command(record, "invocation")
        if command is None or command[:3] != ["invocation", "query", source_id]:
            continue
        if len(command) < 4 or command[3] not in {
            "nodes",
            "node",
            "edges",
            "edge",
            "operator-calls",
            "operator-call",
        }:
            continue
        if "--limit" in command:
            position = command.index("--limit")
            if position + 1 >= len(command) or int(command[position + 1]) > 20:
                continue
        return index
    return None


def _candidate_id(
    evaluation_root: Path,
    records: list[dict[str, Any]],
    rerun_index: int | None,
) -> str | None:
    if rerun_index is None:
        return None
    match = _CANDIDATE_PATTERN.search(
        _stdout(evaluation_root, records[rerun_index])
    )
    return None if match is None else match.group(1)


def _stdout(evaluation_root: Path, record: dict[str, Any]) -> str:
    return (evaluation_root / record["stdout_file"]).read_text(
        encoding="utf-8"
    )


def _report(real_cli: Path, workspace: Path, invocation_id: str) -> str:
    completed = subprocess.run(
        [
            str(real_cli),
            "invocation",
            "report",
            invocation_id,
            "--source",
            "database",
        ],
        cwd=workspace,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        return completed.stdout + completed.stderr
    return completed.stdout


def _field(output: str, name: str) -> str | None:
    prefix = f"{name} "
    for line in output.splitlines():
        if line.startswith(prefix):
            return line[len(prefix) :]
    return None


def _workflow_contains_forbidden_import(workspace: Path) -> bool:
    forbidden = ("autoagent.core", "sqlite3", "sqlalchemy")
    return any(
        value in path.read_text(encoding="utf-8")
        for path in (workspace / "workflows").rglob("*.py")
        for value in forbidden
    )


def _hash_difference(
    expected: dict[str, str],
    actual: dict[str, str],
) -> str:
    changed = sorted(
        path
        for path in set(expected) | set(actual)
        if expected.get(path) != actual.get(path)
    )
    return "protected files changed: " + ", ".join(changed)


def _changed_project_files(
    workspace: Path,
    baseline: dict[str, str],
) -> list[str]:
    current: dict[str, str] = {}
    for path in workspace.rglob("*"):
        if not path.is_file() or any(
            part in {
                ".evaluation",
                ".git",
                ".venv",
                "__pycache__",
                "reports",
            }
            for part in path.relative_to(workspace).parts
        ):
            continue
        if path.suffix not in {".py", ".toml", ".json", ".md"}:
            continue
        current[str(path.relative_to(workspace))] = _sha256(path)
    return sorted(
        path
        for path in set(baseline) | set(current)
        if baseline.get(path) != current.get(path)
    )


def _sha256(path: Path) -> str:
    if not path.is_file():
        return "<missing>"
    digest = hashlib.sha256()
    with path.open("rb") as value:
        while chunk := value.read(64 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(f"REVIEW_ERROR {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
