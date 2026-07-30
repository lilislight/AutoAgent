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
from autoagent.cli.render import (
    render_compile_result,
    render_invocation,
    render_project_diagnostics,
    render_workflow_list,
    write_report,
)
from autoagent.cli.settings import (
    add_runtime_arguments,
    app_settings_from_arguments,
    server_settings_from_arguments,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="autoagent",
        description="Check, run, and serve an AutoAgent project.",
    )
    parser.add_argument(
        "--project",
        help="Project directory or explicit auto-agent.toml path.",
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
    workflow_check.add_argument("workflow_id")
    _add_check_arguments(workflow_check)

    invocation = commands.add_parser("invocation")
    invocation_commands = invocation.add_subparsers(dest="command", required=True)
    invocation_run = invocation_commands.add_parser("run")
    invocation_run.add_argument("workflow_id")
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

    invocation_resume = invocation_commands.add_parser("resume")
    invocation_resume.add_argument("workflow_id")
    invocation_resume.add_argument("--session", required=True)
    invocation_resume.add_argument("--wait-key", required=True)
    _add_json_input_arguments(
        invocation_resume,
        file_flag="--response-file",
        json_flag="--response-json",
    )
    _add_execution_arguments(invocation_resume)

    serve = commands.add_parser("serve")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)
    serve.add_argument("--reload", action="store_true")
    serve.add_argument("--read-only", action="store_true")
    serve.add_argument("--secure-cookies", action="store_true")
    serve.add_argument("--ui-directory")
    serve.add_argument("--trace-cache-size", type=int)
    add_runtime_arguments(serve)
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
    except (KeyError, ValueError) as exc:
        write_report(
            f"CONFIGURATION ERROR\nMESSAGE {exc}\n\nRESULT failed",
            getattr(arguments, "report_file", None),
        )
        return 2
    except TimeoutError:
        write_report(
            "EXECUTION ERROR\nMESSAGE Invocation timed out.\n\nRESULT failed",
            getattr(arguments, "report_file", None),
        )
        return 1
    except KeyboardInterrupt:
        print("\nRESULT cancelled", file=sys.stderr)
        return 130


def _dispatch(arguments: argparse.Namespace) -> int:
    project = ProjectLoader().load(arguments.project)
    if arguments.group == "project" and arguments.command == "check":
        return _project_check(project, arguments)
    if arguments.group == "workflow" and arguments.command == "list":
        return _workflow_list(project, arguments)
    if arguments.group == "workflow" and arguments.command == "check":
        return _workflow_check(project, arguments)

    environment = load_project_environment(
        project.root,
        env_file=arguments.env_file,
        use_env_file=not arguments.no_env_file,
    )
    app_settings = app_settings_from_arguments(arguments, environment)
    if arguments.group == "invocation":
        return asyncio.run(
            _invocation_command(project, environment, app_settings, arguments)
        )
    if arguments.group == "serve":
        return _serve(project, environment, app_settings, arguments)
    raise RuntimeError("Unknown CLI command.")


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


def _serve(
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
