from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
import sys
from typing import Any, Sequence

from autoagent.core.server import AutoAgentServer
from autoagent.project import (
    ProjectCompiler,
    ProjectHost,
    ProjectLoadError,
    ProjectLoader,
    load_project_environment,
)
from autoagent.project.environment import _project_environment_scope
from autoagent.cli.render import (
    render_compile_result,
    render_invocation,
    render_project_diagnostics,
    render_remote_invocation,
    render_submitted_invocation,
    render_workflow_list,
    write_report,
)
from autoagent.cli.evaluation import (
    check_evaluation,
    list_evaluations,
    run_evaluation,
)
from autoagent.cli.debugging import (
    InvocationReportCliError,
    run_invocation_comparison,
    run_invocation_query,
    run_invocation_report,
    run_invocation_rerun,
)
from autoagent.cli.server_client import (
    AutoAgentServerClient,
    ServerClientError,
    resolve_server_url,
)
from autoagent.cli.settings import (
    add_runtime_arguments,
    app_settings_from_arguments,
    server_settings_from_arguments,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="autoagent",
        description="Check, run, and host an AutoAgent project.",
    )
    parser.add_argument(
        "--project",
        help=(
            "Project directory or explicit auto-agent.toml path; with --file, "
            "sets the standalone root for .env and relative outputs."
        ),
    )
    env_group = parser.add_mutually_exclusive_group()
    env_group.add_argument(
        "--env-file",
        help="Environment file; relative paths resolve from the project root.",
    )
    env_group.add_argument(
        "--no-env-file",
        action="store_true",
        help="Do not load the project-root .env file.",
    )
    parser.add_argument(
        "--log-level",
        choices=("debug", "info", "warning", "error"),
        default="warning",
    )
    parser.add_argument("--version", action="version", version="AutoAgent 0.1.0")

    commands = parser.add_subparsers(dest="group", required=True)
    project = commands.add_parser("project")
    project_commands = project.add_subparsers(dest="command", required=True)
    project_check = project_commands.add_parser("check")
    _add_check_arguments(project_check)

    workflow = commands.add_parser("workflow")
    workflow_commands = workflow.add_subparsers(dest="command", required=True)
    workflow_list = workflow_commands.add_parser("list")
    workflow_list.add_argument("--report-file")
    workflow_check = workflow_commands.add_parser("check")
    _add_workflow_source_arguments(workflow_check)
    _add_check_arguments(workflow_check)
    workflow_preview = workflow_commands.add_parser(
        "preview",
        help="Analyze and render an expanded Workflow execution graph.",
        description="Analyze and render an expanded Workflow execution graph.",
    )
    _add_workflow_source_arguments(workflow_preview)
    workflow_preview.add_argument(
        "--format",
        dest="preview_format",
        choices=("terminal", "mermaid", "json"),
        default="terminal",
        help="Output format (default: terminal).",
    )
    workflow_preview.add_argument(
        "-o",
        "--output",
        help=(
            "Write to this path instead of stdout; relative paths use the "
            "project root. Use '-' for stdout."
        ),
    )

    invocation = commands.add_parser("invocation")
    invocation_commands = invocation.add_subparsers(dest="command", required=True)
    invocation_run = invocation_commands.add_parser("run")
    _add_workflow_source_arguments(invocation_run)
    _add_server_connection_arguments(invocation_run, selectable=True)
    _add_json_input_arguments(
        invocation_run,
        file_flag="--input-file",
        json_flag="--input-json",
    )
    invocation_run.add_argument("--session")
    invocation_run.add_argument("--entry-node")
    invocation_run.add_argument(
        "--event-mode",
        choices=("minimal", "standard", "full"),
        default="standard",
    )
    _add_execution_arguments(invocation_run)

    invocation_submit = invocation_commands.add_parser("submit")
    invocation_submit.add_argument("workflow_id")
    _add_server_connection_arguments(invocation_submit, selectable=False)
    _add_json_input_arguments(
        invocation_submit,
        file_flag="--input-file",
        json_flag="--input-json",
    )
    invocation_submit.add_argument("--session")
    invocation_submit.add_argument("--entry-node")
    invocation_submit.add_argument(
        "--event-mode",
        choices=("minimal", "standard", "full"),
        default="standard",
    )
    invocation_submit.add_argument("--report-file")

    invocation_resume = invocation_commands.add_parser("resume")
    _add_workflow_source_arguments(invocation_resume)
    _add_server_connection_arguments(invocation_resume, selectable=True)
    invocation_resume.add_argument("--session", required=True)
    invocation_resume.add_argument("--wait-key", required=True)
    _add_json_input_arguments(
        invocation_resume,
        file_flag="--response-file",
        json_flag="--response-json",
    )
    _add_execution_arguments(invocation_resume)

    invocation_report = invocation_commands.add_parser(
        "report",
        help="Build a compact progressive-debugging index for one Invocation.",
    )
    invocation_report.add_argument("invocation_id")
    invocation_report.add_argument(
        "--source",
        choices=("auto", "server", "database"),
        default="auto",
        help="Prefer a matching Server, require Server, or require database.",
    )
    invocation_report.add_argument("--server-url")
    invocation_report.add_argument("--report-file")

    invocation_query = invocation_commands.add_parser(
        "query",
        help="Progressively query bounded evidence for one Invocation.",
    )
    invocation_query.add_argument("invocation_id")
    invocation_query.add_argument(
        "kind",
        choices=(
            "nodes",
            "node",
            "edges",
            "edge",
            "operator-calls",
            "operator-call",
            "runtime-events",
            "runtime-event",
            "user-events",
            "user-event",
            "runtime-state",
        ),
    )
    invocation_query.add_argument(
        "subject_id",
        nargs="?",
        help="Required by singular node, edge, Operator Call, or Event queries.",
    )
    invocation_query.add_argument("--cursor")
    invocation_query.add_argument("--through-sequence", type=int)
    invocation_query.add_argument("--limit", type=int, default=20)
    invocation_query.add_argument("--path")
    invocation_query.add_argument("--include-stream-deltas", action="store_true")
    invocation_query.add_argument(
        "--node-execution-id",
        help="Restrict operator-calls to one NodeExecution.",
    )
    invocation_query.add_argument(
        "--source",
        choices=("auto", "server", "database"),
        default="auto",
    )
    invocation_query.add_argument("--server-url")
    invocation_query.add_argument("--report-file")

    invocation_compare = invocation_commands.add_parser(
        "compare",
        help="Compare two observed Invocations without assigning a business verdict.",
    )
    invocation_compare.add_argument("baseline_invocation_id")
    invocation_compare.add_argument("candidate_invocation_id")
    invocation_compare.add_argument(
        "--source",
        choices=("auto", "server", "database"),
        default="auto",
    )
    invocation_compare.add_argument("--server-url")
    invocation_compare.add_argument("--report-file")

    invocation_rerun = invocation_commands.add_parser(
        "rerun",
        help="Run the current Workflow revision from an Invocation start boundary.",
    )
    invocation_rerun.add_argument("source_invocation_id")
    _add_server_connection_arguments(invocation_rerun, selectable=True)
    invocation_rerun.add_argument("--timeout-ms", type=int)
    invocation_rerun.add_argument("--report-file")
    add_runtime_arguments(invocation_rerun)

    evaluation = commands.add_parser(
        "eval",
        help="List, validate, and run project-owned Workflow Evaluations.",
    )
    evaluation_commands = evaluation.add_subparsers(
        dest="command",
        required=True,
    )
    evaluation_list = evaluation_commands.add_parser("list")
    evaluation_list.add_argument("--report-file")
    evaluation_check = evaluation_commands.add_parser("check")
    evaluation_check.add_argument("suite_id")
    evaluation_check.add_argument("--report-file")
    evaluation_run = evaluation_commands.add_parser("run")
    evaluation_run.add_argument("suite_id")
    evaluation_run.add_argument(
        "--case",
        dest="cases",
        action="append",
        help="Run one eval_* Case method; repeat to select several Cases.",
    )
    evaluation_run.add_argument(
        "--max-concurrency",
        type=int,
        default=1,
        help="Maximum number of isolated Cases to execute concurrently.",
    )
    evaluation_run.add_argument("--timeout-ms", type=int)
    evaluation_run.add_argument(
        "--verbose",
        action="store_true",
        help="Include values and comments for successful Evaluators.",
    )
    evaluation_run.add_argument("--report-file")
    add_runtime_arguments(evaluation_run)

    server = commands.add_parser("server")
    server.add_argument("--host")
    server.add_argument("--port", type=int)
    server.add_argument("--reload", action="store_true")
    server.add_argument("--read-only", action="store_true")
    server.add_argument("--secure-cookies", action="store_true")
    server.add_argument("--ui-directory")
    server.add_argument("--trace-cache-size", type=int)
    add_runtime_arguments(server)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, arguments.log_level.upper()),
        format="%(levelname)s %(name)s: %(message)s",
    )
    try:
        return _dispatch(arguments)
    except ProjectLoadError as exc:
        write_report(
            render_project_diagnostics(exc.diagnostics),
            getattr(arguments, "report_file", None),
        )
        return 2
    except InvocationReportCliError as exc:
        write_report(
            f"REPORT ERROR\nMESSAGE {exc}\n\nREPORT_RESULT failed",
            getattr(arguments, "report_file", None),
        )
        return 2
    except (KeyError, ValueError) as exc:
        write_report(
            f"CONFIGURATION ERROR\nMESSAGE {exc}\n\nRESULT failed",
            getattr(arguments, "report_file", None),
        )
        return 2
    except ServerClientError as exc:
        write_report(
            f"SERVER ERROR\nMESSAGE {exc}\n\nRESULT failed",
            getattr(arguments, "report_file", None),
        )
        return 2
    except TimeoutError:
        subject = (
            "Evaluation"
            if getattr(arguments, "group", None) == "eval"
            else "Invocation"
        )
        write_report(
            f"EXECUTION ERROR\nMESSAGE {subject} timed out.\n\nRESULT failed",
            getattr(arguments, "report_file", None),
        )
        return 1
    except KeyboardInterrupt:
        print("\nRESULT cancelled", file=sys.stderr)
        return 130


def _dispatch(arguments: argparse.Namespace) -> int:
    _validate_command_project_source(arguments)
    loader = ProjectLoader()
    project_root = loader._resolve_root(
        arguments.project,
        workflow_file=getattr(arguments, "workflow_file", None),
    )
    environment = load_project_environment(
        project_root,
        env_file=arguments.env_file,
        use_env_file=not arguments.no_env_file,
    )
    with _project_environment_scope(environment):
        project = _load_command_project(arguments, loader=loader)
        return _dispatch_project_command(project, environment, arguments)


def _dispatch_project_command(
    project: Any,
    environment: dict[str, str],
    arguments: argparse.Namespace,
) -> int:
    if arguments.group == "project" and arguments.command == "check":
        return _project_check(project, arguments)
    if arguments.group == "workflow" and arguments.command == "list":
        return _workflow_list(project, arguments)
    if arguments.group == "workflow" and arguments.command == "check":
        return _workflow_check(project, arguments)
    if arguments.group == "workflow" and arguments.command == "preview":
        return _workflow_preview(project, arguments)
    if arguments.group == "eval" and arguments.command == "list":
        return list_evaluations(project, arguments)
    if arguments.group == "eval" and arguments.command == "check":
        return check_evaluation(project, arguments)

    if arguments.group == "invocation":
        if arguments.command == "report":
            return asyncio.run(
                run_invocation_report(project, environment, arguments)
            )
        if arguments.command == "query":
            return asyncio.run(
                run_invocation_query(project, environment, arguments)
            )
        if arguments.command == "compare":
            return asyncio.run(
                run_invocation_comparison(project, environment, arguments)
            )
        if arguments.command == "rerun":
            if _uses_server(arguments):
                _validate_remote_runtime_arguments(arguments)
            app_settings = (
                None
                if _uses_server(arguments)
                else app_settings_from_arguments(arguments, environment)
            )
            return asyncio.run(
                run_invocation_rerun(
                    project,
                    environment,
                    app_settings,
                    arguments,
                )
            )
        if _uses_server(arguments):
            return asyncio.run(
                _remote_invocation_command(environment, arguments)
            )
        app_settings = app_settings_from_arguments(arguments, environment)
        return asyncio.run(
            _invocation_command(project, environment, app_settings, arguments)
        )
    if arguments.group == "eval" and arguments.command == "run":
        app_settings = app_settings_from_arguments(arguments, environment)
        return asyncio.run(
            run_evaluation(project, environment, app_settings, arguments)
        )
    if arguments.group == "server":
        app_settings = app_settings_from_arguments(arguments, environment)
        return _server(project, environment, app_settings, arguments)
    raise RuntimeError("Unknown CLI command.")


def _load_command_project(
    arguments: argparse.Namespace,
    *,
    loader: ProjectLoader | None = None,
) -> Any:
    loader = ProjectLoader() if loader is None else loader
    workflow_file = getattr(arguments, "workflow_file", None)
    if workflow_file is not None:
        project = loader.load_workflow_file(
            workflow_file,
            object_path=(
                getattr(arguments, "workflow_object", None) or "workflow"
            ),
            project_root=arguments.project,
        )
        arguments.workflow_id = project.workflows[0].workflow.id
        return project
    return loader.load(arguments.project)


def _validate_command_project_source(arguments: argparse.Namespace) -> None:
    workflow_file = getattr(arguments, "workflow_file", None)
    workflow_object = getattr(arguments, "workflow_object", None)
    workflow_id = getattr(arguments, "workflow_id", None)
    if workflow_file is not None:
        if _uses_server(arguments):
            raise ValueError("--file cannot be combined with Server execution.")
        if workflow_id is not None:
            raise ValueError("Pass either workflow_id or --file, not both.")
        return
    if workflow_object is not None:
        raise ValueError("--object requires --file.")
    if hasattr(arguments, "workflow_id") and workflow_id is None:
        raise ValueError("Pass a manifest workflow_id or --file.")


def _uses_server(arguments: argparse.Namespace) -> bool:
    return (
        arguments.group == "invocation"
        and (
            arguments.command == "submit"
            or bool(getattr(arguments, "server", False))
            or getattr(arguments, "server_url", None) is not None
        )
    )


def _project_check(project: Any, arguments: argparse.Namespace) -> int:
    compiler = ProjectCompiler()
    results = [
        compiler.compile(loaded.workflow) for loaded in project.workflows
    ]
    report = "\n\n".join(render_compile_result(result) for result in results)
    write_report(report, arguments.report_file)
    failed = any(not result.ok for result in results)
    warned = any(
        diagnostic.severity == "warning"
        for result in results
        for diagnostic in result.diagnostics
    )
    return 1 if failed or (arguments.warnings_as_errors and warned) else 0


def _workflow_list(project: Any, arguments: argparse.Namespace) -> int:
    compiler = ProjectCompiler()
    results = {
        loaded.workflow.id: compiler.compile(loaded.workflow)
        for loaded in project.workflows
    }
    write_report(
        render_workflow_list(project, results),
        arguments.report_file,
    )
    return 0


def _workflow_check(project: Any, arguments: argparse.Namespace) -> int:
    workflow = project.workflow_by_id(arguments.workflow_id)
    result = ProjectCompiler().compile(workflow)
    write_report(render_compile_result(result), arguments.report_file)
    warned = any(
        diagnostic.severity == "warning"
        for diagnostic in result.diagnostics
    )
    return 1 if not result.ok or (arguments.warnings_as_errors and warned) else 0


def _workflow_preview(project: Any, arguments: argparse.Namespace) -> int:
    workflow = project.workflow_by_id(arguments.workflow_id)
    preview = ProjectCompiler().preview(workflow)
    output = arguments.output
    if output is None or output == "-":
        print(preview.render(arguments.preview_format))
        return 0

    target = Path(output).expanduser()
    if not target.is_absolute():
        target = project.root / target
    try:
        path = preview.save(target, format=arguments.preview_format)
    except OSError as exc:
        raise ValueError(f"Could not write Workflow preview: {exc}") from exc
    status = (
        "invalid"
        if preview.error_count
        else "warning"
        if preview.warning_count
        else "valid"
    )
    print(
        "\n".join(
            (
                f"WORKFLOW {workflow.id}",
                f"FORMAT {arguments.preview_format}",
                f"OUTPUT {path}",
                f"STATUS {status}",
                "RESULT previewed",
            )
        )
    )
    return 0


async def _invocation_command(
    project: Any,
    environment: dict[str, str],
    app_settings: Any,
    arguments: argparse.Namespace,
) -> int:
    host = ProjectHost(
        project,
        app_settings=app_settings,
        environment=environment,
        workflow_ids=(arguments.workflow_id,),
    )
    async with host:
        workflow = host.workflow(arguments.workflow_id)
        timeout = (
            None
            if arguments.timeout_ms is None
            else arguments.timeout_ms / 1000
        )
        if arguments.command == "run":
            invocation_input = _read_json_argument(arguments)
            if invocation_input is not None and not isinstance(
                invocation_input,
                dict,
            ):
                raise ValueError(
                    "Invocation input must be a JSON object or null."
                )
            invocation = await _with_timeout(
                host.app.ainvoke(
                    workflow,
                    input=invocation_input,
                    session_id=arguments.session,
                    entry_node_id=arguments.entry_node,
                    event_mode=arguments.event_mode,
                ),
                timeout,
            )
        else:
            response_supplied = (
                arguments.response_file is not None
                or arguments.response_json is not None
            )
            resume_options: dict[str, Any] = {
                "session_id": arguments.session,
                "wait_key": arguments.wait_key,
            }
            if response_supplied:
                resume_options["output"] = _read_json_argument(arguments)
            invocation = await _with_timeout(
                host.app.aresume(workflow, **resume_options),
                timeout,
            )
        events = list(host.app.runtime_store.runtime_events.get(invocation.id, ()))
        write_report(
            render_invocation(
                invocation,
                events=events,
                include_trace=arguments.trace,
                serializer=host.app.runtime_serializer,
            ),
            arguments.report_file,
        )
        return 1 if invocation.state in {"failed", "interrupted", "cancelled"} else 0


async def _remote_invocation_command(
    environment: dict[str, str],
    arguments: argparse.Namespace,
) -> int:
    _validate_remote_runtime_arguments(arguments)
    server_url = resolve_server_url(
        environment,
        explicit_url=arguments.server_url,
    )
    access_token = environment.get("AUTOAGENT_SERVER_ACCESS_TOKEN") or None
    timeout = (
        None
        if getattr(arguments, "timeout_ms", None) is None
        else arguments.timeout_ms / 1_000
    )
    if timeout is not None and timeout <= 0:
        raise ValueError("--timeout-ms must be positive.")

    async with AutoAgentServerClient(
        server_url,
        access_token=access_token,
    ) as client:
        if arguments.command in {"run", "submit"}:
            invocation_input = _read_json_argument(arguments)
            if invocation_input is not None and not isinstance(
                invocation_input,
                dict,
            ):
                raise ValueError(
                    "Invocation input must be a JSON object or null."
                )
            submitted = await client.submit(
                arguments.workflow_id,
                input=invocation_input,
                session_key=arguments.session,
                entry_node_id=arguments.entry_node,
                event_mode=arguments.event_mode,
            )
            if arguments.command == "submit":
                write_report(
                    render_submitted_invocation(submitted),
                    arguments.report_file,
                )
                return 0
            invocation_id = str(submitted["invocation_id"])
        else:
            output_supplied = (
                arguments.response_file is not None
                or arguments.response_json is not None
            )
            resumed = await client.resume(
                arguments.workflow_id,
                session_key=arguments.session,
                wait_key=arguments.wait_key,
                output_supplied=output_supplied,
                output=(
                    _read_json_argument(arguments)
                    if output_supplied
                    else None
                ),
            )
            invocation_id = str(resumed["invocation_id"])

        detail = await client.wait_for_invocation(
            invocation_id,
            timeout=timeout,
        )
        include_trace = bool(arguments.trace)
        events = (
            await client.events(invocation_id)
            if include_trace
            or detail["state"] in {"failed", "interrupted", "cancelled"}
            else []
        )
        write_report(
            render_remote_invocation(
                detail,
                events=events,
                include_trace=include_trace,
            ),
            arguments.report_file,
        )
        return (
            1
            if detail["state"] in {"failed", "interrupted", "cancelled"}
            else 0
        )


def _server(
    project: Any,
    environment: dict[str, str],
    app_settings: Any,
    arguments: argparse.Namespace,
) -> int:
    host = ProjectHost(
        project,
        app_settings=app_settings,
        environment=environment,
    )
    server_settings = server_settings_from_arguments(arguments, environment)
    ui_directory = server_settings.ui_directory
    if ui_directory is not None and not ui_directory.is_absolute():
        ui_directory = project.root / ui_directory
    server = AutoAgentServer(
        host.app,
        execution_enabled=server_settings.execution_enabled,
        access_token=server_settings.access_token,
        secure_cookies=server_settings.secure_cookies,
        ui_directory=ui_directory,
        trace_cache_size=server_settings.trace_cache_size,
        shutdown_callback=host.close,
    )
    try:
        server.run(
            host=server_settings.host,
            port=server_settings.port,
            reload=arguments.reload,
        )
    finally:
        asyncio.run(host.close())
    return 0


async def _with_timeout(awaitable: Any, timeout: float | None) -> Any:
    if timeout is None:
        return await awaitable
    if timeout <= 0:
        raise ValueError("--timeout-ms must be positive.")
    return await asyncio.wait_for(awaitable, timeout=timeout)


def _read_json_argument(arguments: argparse.Namespace) -> Any:
    file_value = getattr(
        arguments,
        "input_file",
        getattr(arguments, "response_file", None),
    )
    json_value = getattr(
        arguments,
        "input_json",
        getattr(arguments, "response_json", None),
    )
    if json_value is not None:
        return json.loads(json_value)
    if file_value is None:
        return None
    if file_value == "-":
        return json.load(sys.stdin)
    with Path(file_value).expanduser().open(encoding="utf-8") as stream:
        return json.load(stream)


def _add_check_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--warnings-as-errors", action="store_true")
    parser.add_argument("--report-file")


def _add_workflow_source_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "workflow_id",
        nargs="?",
        help="Workflow id declared by the project manifest; omit with --file.",
    )
    parser.add_argument(
        "--file",
        dest="workflow_file",
        help="Standalone Python file exporting a Workflow object.",
    )
    parser.add_argument(
        "--object",
        dest="workflow_object",
        help="Exported object path inside --file (default: workflow).",
    )


def _add_server_connection_arguments(
    parser: argparse.ArgumentParser,
    *,
    selectable: bool,
) -> None:
    if selectable:
        parser.add_argument(
            "--server",
            action="store_true",
            help="Execute through the configured AutoAgent Server.",
        )
    parser.add_argument(
        "--server-url",
        help=(
            "Override AUTOAGENT_SERVER_URL or the configured Server host/port."
        ),
    )


def _validate_remote_runtime_arguments(arguments: argparse.Namespace) -> None:
    if getattr(arguments, "store", "auto") != "auto":
        raise ValueError("--store applies only to local execution.")
    for name, flag in (
        ("max_thread_workers", "--max-thread-workers"),
        ("max_parallel_units", "--max-parallel-units"),
        ("shutdown_timeout_ms", "--shutdown-timeout-ms"),
    ):
        if getattr(arguments, name, None) is not None:
            raise ValueError(f"{flag} applies only to local execution.")


def _add_json_input_arguments(
    parser: argparse.ArgumentParser,
    *,
    file_flag: str,
    json_flag: str,
) -> None:
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument(file_flag)
    inputs.add_argument(json_flag)


def _add_execution_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--timeout-ms", type=int)
    parser.add_argument("--trace", action="store_true")
    parser.add_argument("--report-file")
    add_runtime_arguments(parser)
