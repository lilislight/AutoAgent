from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from autoagent.core.compiler import CompileResult
from autoagent.core.runtime import Invocation, JsonRuntimeSerializer, RuntimeEvent
from autoagent.evaluation.loader import LoadedEvaluation
from autoagent.evaluation.result import EvalResult
from autoagent.project.manifest import EvalSuiteLocator
from autoagent.project import ProjectDefinition, ProjectDiagnostic


def render_project_diagnostics(
    diagnostics: tuple[ProjectDiagnostic, ...] | list[ProjectDiagnostic],
) -> str:
    lines = ["PROJECT INVALID", ""]
    for item in diagnostics:
        lines.extend(
            _problem_lines(
                severity=item.severity,
                code=item.code,
                object_text=item.entrypoint or item.path,
                field=item.field,
                message=item.message,
                hint=item.hint,
            )
        )
    lines.append("RESULT failed")
    return "\n".join(lines)


def render_compile_result(result: CompileResult) -> str:
    lines = [
        f"WORKFLOW {result.workflow_id or '<unknown>'}",
        f"VERSION {result.workflow_version}",
    ]
    if result.workflow_ir is not None:
        workflow_ir = result.workflow_ir
        lines.extend(
            [
                f"NODES {len(workflow_ir.nodes)}",
                f"EDGES {len(workflow_ir.edges)}",
                f"ENTRIES {', '.join(workflow_ir.entry_node_ids)}",
                f"EXITS {', '.join(workflow_ir.exit_node_ids)}",
                f"LOOPS {len(workflow_ir.graph.loop_regions)}",
            ]
        )
    if result.diagnostics:
        lines.append("")
        for item in result.diagnostics:
            object_text = (
                f"{item.object_type}:{item.object_id}"
                if item.object_type and item.object_id
                else item.object_type
            )
            lines.extend(
                _problem_lines(
                    severity=item.severity,
                    code=item.code,
                    object_text=object_text,
                    field=item.field,
                    message=item.message,
                    hint=item.hint,
                )
            )
    lines.append(f"RESULT {'valid' if result.ok else 'failed'}")
    return "\n".join(lines)


def render_workflow_list(
    project: ProjectDefinition,
    results: dict[str, CompileResult],
) -> str:
    lines = [
        f"PROJECT {project.metadata.name}",
        f"VERSION {project.metadata.version}",
        f"WORKFLOWS {len(project.workflows)}",
        "",
    ]
    for loaded in project.workflows:
        workflow = loaded.workflow
        result = results[workflow.id]
        lines.extend(
            [
                f"WORKFLOW {workflow.id}",
                f"  VERSION {workflow.version if workflow.version is not None else 1}",
                f"  ENTRYPOINT {loaded.locator.entrypoint}",
                f"  NODES {len(workflow.nodes)}",
                f"  EDGES {len(workflow.edges)}",
                f"  STATUS {'valid' if result.ok else 'invalid'}",
            ]
        )
    lines.append("RESULT listed")
    return "\n".join(lines)


def render_evaluation_list(
    project: ProjectDefinition,
    locators: tuple[EvalSuiteLocator, ...],
) -> str:
    lines = [
        f"PROJECT {project.metadata.name}",
        f"VERSION {project.metadata.version}",
        f"EVAL_SUITES {len(locators)}",
    ]
    for locator in locators:
        lines.extend(
            [
                "",
                f"EVAL {locator.id}",
                f"  WORKFLOW {locator.workflow_id}",
                f"  ENTRYPOINT {locator.entrypoint}",
            ]
        )
    lines.extend(["", "RESULT listed"])
    return "\n".join(lines)


def render_evaluation_check(
    loaded: LoadedEvaluation,
    compile_result: CompileResult,
) -> str:
    lines = [
        f"EVAL {loaded.locator.id}",
        f"WORKFLOW {loaded.locator.workflow_id}",
        f"ENTRYPOINT {loaded.locator.entrypoint}",
        f"CASES {len(loaded.case_ids)}",
    ]
    lines.extend(f"  CASE {case_id}" for case_id in loaded.case_ids)
    lines.extend(["", render_compile_result(compile_result)])
    lines.append(f"EVAL_RESULT {'valid' if compile_result.ok else 'failed'}")
    return "\n".join(lines)


def render_evaluation_result(
    result: EvalResult,
    *,
    serializer: JsonRuntimeSerializer,
) -> str:
    lines = [
        f"EVAL {result.suite_id}",
        f"WORKFLOW {result.workflow_id}",
        f"WORKFLOW_REVISION {result.workflow_revision_id}",
        f"STATUS {result.status}",
        f"CASES {len(result.case_results)}",
    ]
    if result.error is not None:
        lines.extend(
            [
                f"ERROR {result.error.code}",
                f"MESSAGE {result.error.message}",
            ]
        )
    for case in result.case_results:
        lines.extend(
            [
                "",
                f"CASE {case.case_id}",
                f"  STATUS {case.status}",
                f"  SESSION {case.session_id or '<none>'}",
                f"  STEPS {len(case.step_results)}",
            ]
        )
        if case.error is not None:
            lines.extend(
                [
                    f"  ERROR {case.error.code}",
                    f"  MESSAGE {case.error.message}",
                ]
            )
        for step in case.step_results:
            lines.extend(
                [
                    f"  STEP {step.index} {step.action}",
                    f"    STATUS {step.status}",
                    f"    INVOCATION {step.invocation_id or '<none>'}",
                    "    THROUGH_SEQUENCE "
                    f"{step.through_sequence if step.through_sequence is not None else '<none>'}",
                ]
            )
            if step.error is not None:
                lines.extend(
                    [
                        f"    ERROR {step.error.code}",
                        f"    MESSAGE {step.error.message}",
                    ]
                )
            for evaluator in step.evaluator_results:
                evaluator_status = (
                    "error"
                    if evaluator.error is not None
                    else "passed"
                    if evaluator.passed is True
                    else "failed"
                    if evaluator.passed is False
                    else "completed"
                )
                lines.append(
                    f"    EVALUATOR {evaluator.key} {evaluator_status}"
                )
                if evaluator.score is not None:
                    lines.append(f"      SCORE {evaluator.score}")
                if evaluator.value is not None:
                    lines.extend(
                        [
                            "      VALUE",
                            _indent(_json_text(evaluator.value, serializer), 8),
                        ]
                    )
                if evaluator.comment:
                    lines.append(f"      COMMENT {evaluator.comment}")
                if evaluator.error is not None:
                    lines.extend(
                        [
                            f"      ERROR {evaluator.error.code}",
                            f"      MESSAGE {evaluator.error.message}",
                        ]
                    )
    lines.extend(["", f"RESULT {result.status}"])
    return "\n".join(lines)


def render_invocation(
    invocation: Invocation,
    *,
    events: list[RuntimeEvent],
    include_trace: bool,
    serializer: JsonRuntimeSerializer,
) -> str:
    duration_ms = max(0, invocation.updated_at_ms - invocation.created_at_ms)
    lines = [
        f"INVOCATION {invocation.id}",
        f"WORKFLOW {invocation.workflow_id}",
        f"STATE {invocation.state}",
        f"MODE {invocation.event_mode}",
        f"ENTRY {invocation.entry_node_id}",
        f"DURATION_MS {duration_ms}",
    ]
    if invocation.error is not None:
        lines.extend(
            [
                "",
                "FAILURE",
                f"CODE {invocation.error.code}",
                f"MESSAGE {invocation.error.message}",
            ]
        )
    if invocation.result is not None:
        lines.extend(
            ["", "OUTPUT", _json_text(invocation.result, serializer)]
        )

    if include_trace or invocation.state in {"failed", "interrupted", "cancelled"}:
        lines.extend(["", f"TRACE {len(events)}"])
        for event in events:
            elapsed = (
                f" elapsed_ns={event.elapsed_ns}"
                if event.elapsed_ns is not None
                else ""
            )
            status = f" status={event.status}" if event.status else ""
            lines.append(
                f"{event.sequence} {event.event_name} "
                f"{event.subject_type}:{event.subject_id}{status}{elapsed}"
            )
    lines.append(f"RESULT {invocation.state}")
    return "\n".join(lines)


def render_remote_invocation(
    invocation: dict[str, Any],
    *,
    events: list[dict[str, Any]],
    include_trace: bool,
) -> str:
    """Render the Server's JSON Invocation contract like a local run."""

    state = str(invocation["state"])
    duration_ms = max(
        0,
        int(invocation["updated_at_ms"]) - int(invocation["created_at_ms"]),
    )
    lines = [
        f"INVOCATION {invocation['id']}",
        f"WORKFLOW {invocation['workflow_id']}",
        f"STATE {state}",
        f"MODE {invocation['event_mode']}",
        f"ENTRY {invocation['entry_node_id']}",
        f"DURATION_MS {duration_ms}",
    ]
    error = invocation.get("error")
    if error is not None:
        lines.extend(
            [
                "",
                "FAILURE",
                f"CODE {error.get('code')}",
                f"MESSAGE {error.get('message')}",
            ]
        )
    result = invocation.get("result")
    if result is not None:
        lines.extend(
            [
                "",
                "OUTPUT",
                json.dumps(
                    result,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                ),
            ]
        )
    if include_trace or state in {"failed", "interrupted", "cancelled"}:
        lines.extend(["", f"TRACE {len(events)}"])
        for event in events:
            elapsed_ns = event.get("elapsed_ns")
            elapsed = (
                f" elapsed_ns={elapsed_ns}" if elapsed_ns is not None else ""
            )
            status_value = event.get("status")
            status = f" status={status_value}" if status_value else ""
            lines.append(
                f"{event['sequence']} {event['event_name']} "
                f"{event['subject_type']}:{event['subject_id']}{status}{elapsed}"
            )
    lines.append(f"RESULT {state}")
    return "\n".join(lines)


def render_submitted_invocation(submitted: dict[str, Any]) -> str:
    return "\n".join(
        (
            f"INVOCATION {submitted['invocation_id']}",
            f"WORKFLOW {submitted['workflow_id']}",
            f"WORKFLOW_REVISION {submitted['workflow_revision_id']}",
            f"SESSION {submitted['session_id']}",
            f"SESSION_KEY {submitted['session_key']}",
            f"STATE {submitted['state']}",
            "RESULT submitted",
        )
    )


def write_report(report: str, path: str | None) -> None:
    print(report)
    if path is not None:
        target = Path(path).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"{report}\n", encoding="utf-8")


def _problem_lines(
    *,
    severity: str,
    code: str,
    object_text: str | None,
    field: str | None,
    message: str,
    hint: str | None,
) -> list[str]:
    lines = [f"{severity.upper()} {code}"]
    if object_text:
        lines.append(f"OBJECT {object_text}")
    if field:
        lines.append(f"FIELD {field}")
    lines.append(f"MESSAGE {message}")
    if hint:
        lines.append(f"HINT {hint}")
    lines.append("")
    return lines


def _json_text(value: Any, serializer: JsonRuntimeSerializer) -> str:
    json_value = serializer.json_view(serializer.dumps_unchecked(value))
    return json.dumps(
        json_value,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )


def _indent(value: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(f"{prefix}{line}" for line in value.splitlines())
