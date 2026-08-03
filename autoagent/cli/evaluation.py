from __future__ import annotations

import asyncio
from typing import Any

from autoagent.cli.render import (
    render_evaluation_check,
    render_evaluation_list,
    render_evaluation_result,
    write_report,
)
from autoagent.evaluation.loader import EvaluationLoader
from autoagent.evaluation.runner import EvaluationRunner
from autoagent.project import ProjectCompiler, ProjectHost


def list_evaluations(project: Any, arguments: Any) -> int:
    locators = EvaluationLoader().list(project)
    write_report(
        render_evaluation_list(project, locators),
        arguments.report_file,
    )
    return 0


def check_evaluation(project: Any, arguments: Any) -> int:
    loaded = EvaluationLoader().load(project, arguments.suite_id)
    compile_result = ProjectCompiler().compile(
        project.workflow_by_id(loaded.locator.workflow_id)
    )
    write_report(
        render_evaluation_check(loaded, compile_result),
        arguments.report_file,
    )
    return 0 if compile_result.ok else 1


async def run_evaluation(
    project: Any,
    environment: dict[str, str],
    app_settings: Any,
    arguments: Any,
) -> int:
    loaded = EvaluationLoader().load(project, arguments.suite_id)
    selected_cases = tuple(arguments.cases or ())
    if len(set(selected_cases)) != len(selected_cases):
        raise ValueError("--case cannot select the same Eval Case twice.")

    host = ProjectHost(
        project,
        app_settings=app_settings,
        environment=environment,
        workflow_ids=(loaded.locator.workflow_id,),
    )
    async with host:
        operation = EvaluationRunner(host).run(
            loaded,
            case_ids=selected_cases or None,
            max_concurrency=arguments.max_concurrency,
        )
        result = (
            await operation
            if arguments.timeout_ms is None
            else await asyncio.wait_for(
                operation,
                timeout=_timeout_seconds(arguments.timeout_ms),
            )
        )
        write_report(
            render_evaluation_result(
                result,
                serializer=host.app.runtime_serializer,
            ),
            arguments.report_file,
        )

    if result.status == "error":
        return 2
    if result.status == "failed":
        return 1
    return 0


def _timeout_seconds(timeout_ms: int) -> float:
    if timeout_ms <= 0:
        raise ValueError("--timeout-ms must be positive.")
    return timeout_ms / 1000
