from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy.engine import make_url

from autoagent.cli.render import (
    render_invocation_comparison,
    render_invocation_query,
    render_invocation_report,
    render_invocation_rerun,
    write_report,
)
from autoagent.cli.server_client import (
    AutoAgentServerClient,
    ServerClientError,
    resolve_server_url,
)
from autoagent.core.app import AutoAgentSettings
from autoagent.debug import (
    DebugQueryService,
    InvocationComparison,
    InvocationReport,
    InvocationRerunResult,
    build_rerun_result,
)
from autoagent.project import ProjectHost


class InvocationReportCliError(RuntimeError):
    """The CLI could not resolve or query authoritative Invocation evidence."""


async def run_invocation_rerun(
    project: Any,
    environment: dict[str, str],
    app_settings: AutoAgentSettings | None,
    arguments: argparse.Namespace,
) -> int:
    """Execute an isolated Invocation from a Standard/Full start boundary."""

    source_id = UUID(arguments.source_invocation_id)
    timeout = (
        None if arguments.timeout_ms is None else arguments.timeout_ms / 1_000
    )
    if timeout is not None and timeout <= 0:
        raise ValueError("--timeout-ms must be positive.")
    if arguments.server or arguments.server_url is not None:
        server_url = resolve_server_url(environment, explicit_url=arguments.server_url)
        access_token = environment.get("AUTOAGENT_SERVER_ACCESS_TOKEN") or None
        async with AutoAgentServerClient(
            server_url,
            access_token=access_token,
        ) as client:
            source_report = InvocationReport.model_validate(
                await client.invocation_report(str(source_id))
            )
            _ensure_project_workflow(project, source_report)
            submitted = InvocationRerunResult.model_validate(
                await client.rerun(
                    source_report.workflow_id,
                    source_invocation_id=str(source_id),
                )
            )
            detail = await client.wait_for_invocation(
                submitted.candidate_invocation_id,
                timeout=timeout,
            )
            result = submitted.model_copy(update={"state": str(detail["state"])})
    else:
        if app_settings is None:
            raise RuntimeError("Local Rerun requires App settings.")
        host = ProjectHost(
            project,
            app_settings=app_settings,
            environment=environment,
        )
        async with host:
            seed = await host.app.runtime_store.aload_invocation_rerun_seed(
                source_id
            )
            if seed is None:
                raise InvocationReportCliError(
                    f"Configured Runtime has no Invocation '{source_id}'."
                )
            try:
                workflow = host.workflow(seed.workflow_id)
            except KeyError as exc:
                raise InvocationReportCliError(
                    f"Invocation belongs to Workflow '{seed.workflow_id}', "
                    "which is not declared by the current auto-agent.toml."
                ) from exc
            operation = host.app._arerun(
                workflow,
                source_invocation_id=source_id,
            )
            admitted, invocation = (
                await operation
                if timeout is None
                else await asyncio.wait_for(operation, timeout)
            )
            result = build_rerun_result(admitted, invocation)
    write_report(render_invocation_rerun(result), arguments.report_file)
    return 1 if result.state in {"failed", "interrupted", "cancelled"} else 0


async def run_invocation_comparison(
    project: Any,
    environment: dict[str, str],
    arguments: argparse.Namespace,
) -> int:
    """Compare two Invocations from one authoritative evidence source."""

    baseline_id = UUID(arguments.baseline_invocation_id)
    candidate_id = UUID(arguments.candidate_invocation_id)
    server_error: ServerClientError | None = None
    if arguments.source in {"auto", "server"}:
        server_url = resolve_server_url(environment, explicit_url=arguments.server_url)
        access_token = environment.get("AUTOAGENT_SERVER_ACCESS_TOKEN") or None
        try:
            async with AutoAgentServerClient(
                server_url,
                access_token=access_token,
                timeout=2.0,
            ) as client:
                comparison = InvocationComparison.model_validate(
                    await client.compare_invocations(
                        str(baseline_id),
                        str(candidate_id),
                    )
                )
        except ServerClientError as exc:
            if arguments.source == "server":
                raise
            server_error = exc
        else:
            _ensure_comparison_project_workflow(project, comparison)
            write_report(
                render_invocation_comparison(comparison),
                arguments.report_file,
            )
            return 0

    settings = AutoAgentSettings.from_env(env_file=None, environ=environment)
    if arguments.source in {"auto", "database"} and settings.database_url:
        store = settings.runtime_store(database_read_only=True)
        assert store.backend is not None
        _ensure_database_already_exists(store.backend.database_url)
        try:
            await store.ainitialize()
            comparison = await DebugQueryService(
                store,
                source="database",
            ).compare(baseline_id, candidate_id)
        except Exception as exc:
            if isinstance(exc, InvocationReportCliError):
                raise
            raise InvocationReportCliError(
                "Configured database could not compare the requested "
                f"Invocations: {exc}"
            ) from exc
        finally:
            await store.aclose()
        _ensure_comparison_project_workflow(project, comparison)
        write_report(
            render_invocation_comparison(comparison),
            arguments.report_file,
        )
        return 0

    if arguments.source == "database":
        raise InvocationReportCliError(
            "Invocation Comparison requires AUTOAGENT_DATABASE_URL when "
            "--source database is selected."
        )
    server_detail = f" Server probe failed: {server_error}." if server_error else ""
    raise InvocationReportCliError(
        "No authoritative evidence source is available for these Invocations."
        f"{server_detail} Start the matching AutoAgent Server or configure "
        "AUTOAGENT_DATABASE_URL for its durable database."
    )


async def run_invocation_report(
    project: Any,
    environment: dict[str, str],
    arguments: argparse.Namespace,
) -> int:
    """Resolve authoritative evidence and render one compact Report."""

    invocation_id = UUID(arguments.invocation_id)
    server_error: ServerClientError | None = None
    if arguments.source in {"auto", "server"}:
        server_url = resolve_server_url(
            environment,
            explicit_url=arguments.server_url,
        )
        access_token = environment.get("AUTOAGENT_SERVER_ACCESS_TOKEN") or None
        try:
            async with AutoAgentServerClient(
                server_url,
                access_token=access_token,
                timeout=2.0,
            ) as client:
                report = InvocationReport.model_validate(
                    await client.invocation_report(str(invocation_id))
                )
        except ServerClientError as exc:
            if arguments.source == "server":
                raise
            server_error = exc
        else:
            _ensure_project_workflow(project, report)
            write_report(render_invocation_report(report), arguments.report_file)
            return 0

    settings = AutoAgentSettings.from_env(env_file=None, environ=environment)
    if arguments.source in {"auto", "database"} and settings.database_url:
        store = settings.runtime_store(database_read_only=True)
        assert store.backend is not None
        _ensure_database_already_exists(store.backend.database_url)
        try:
            try:
                await store.ainitialize()
                try:
                    report = await DebugQueryService(
                        store,
                        source="database",
                    ).report(invocation_id)
                except KeyError as exc:
                    raise InvocationReportCliError(
                        f"Configured database has no Invocation '{invocation_id}'."
                    ) from exc
            except InvocationReportCliError:
                raise
            except Exception as exc:
                raise InvocationReportCliError(
                    "Configured database could not be queried as an AutoAgent "
                    f"Runtime database: {exc}"
                ) from exc
        finally:
            await store.aclose()
        _ensure_project_workflow(project, report)
        write_report(render_invocation_report(report), arguments.report_file)
        return 0

    if arguments.source == "database":
        raise InvocationReportCliError(
            "Invocation Report requires AUTOAGENT_DATABASE_URL when "
            "--source database is selected."
        )
    server_detail = (
        f" Server probe failed: {server_error}." if server_error else ""
    )
    raise InvocationReportCliError(
        "No authoritative evidence source is available for this Invocation."
        f"{server_detail} Start the matching AutoAgent Server or configure "
        "AUTOAGENT_DATABASE_URL for its durable database."
    )


async def run_invocation_query(
    project: Any,
    environment: dict[str, str],
    arguments: argparse.Namespace,
) -> int:
    """Resolve one source and query only the requested bounded evidence."""

    invocation_id = UUID(arguments.invocation_id)
    _validate_query_arguments(arguments)
    server_error: ServerClientError | None = None
    if arguments.source in {"auto", "server"}:
        server_url = resolve_server_url(
            environment,
            explicit_url=arguments.server_url,
        )
        access_token = environment.get("AUTOAGENT_SERVER_ACCESS_TOKEN") or None
        try:
            async with AutoAgentServerClient(
                server_url,
                access_token=access_token,
                timeout=2.0,
            ) as client:
                report = InvocationReport.model_validate(
                    await client.invocation_report(str(invocation_id))
                )
                _ensure_project_workflow(project, report)
                value = await _server_query(client, invocation_id, arguments)
        except ServerClientError as exc:
            if arguments.source == "server":
                raise
            server_error = exc
        else:
            write_report(
                render_invocation_query(
                    str(invocation_id),
                    arguments.kind,
                    value,
                ),
                arguments.report_file,
            )
            return 0

    settings = AutoAgentSettings.from_env(env_file=None, environ=environment)
    if arguments.source in {"auto", "database"} and settings.database_url:
        store = settings.runtime_store(database_read_only=True)
        assert store.backend is not None
        _ensure_database_already_exists(store.backend.database_url)
        try:
            try:
                await store.ainitialize()
                service = DebugQueryService(store, source="database")
                report = await service.report(invocation_id)
                _ensure_project_workflow(project, report)
                value = await _database_query(
                    service,
                    invocation_id,
                    arguments,
                )
            except (InvocationReportCliError, ValueError):
                raise
            except KeyError as exc:
                raise InvocationReportCliError(str(exc)) from exc
            except Exception as exc:
                raise InvocationReportCliError(
                    "Configured database could not answer the requested "
                    f"debug query: {exc}"
                ) from exc
        finally:
            await store.aclose()
        write_report(
            render_invocation_query(
                str(invocation_id),
                arguments.kind,
                value,
            ),
            arguments.report_file,
        )
        return 0

    if arguments.source == "database":
        raise InvocationReportCliError(
            "Invocation query requires AUTOAGENT_DATABASE_URL when "
            "--source database is selected."
        )
    server_detail = (
        f" Server probe failed: {server_error}." if server_error else ""
    )
    raise InvocationReportCliError(
        "No authoritative evidence source is available for this Invocation."
        f"{server_detail} Start the matching AutoAgent Server or configure "
        "AUTOAGENT_DATABASE_URL for its durable database."
    )


async def _server_query(
    client: AutoAgentServerClient,
    invocation_id: UUID,
    arguments: argparse.Namespace,
) -> dict[str, Any]:
    common = {
        "cursor": arguments.cursor,
        "through_sequence": arguments.through_sequence,
        "limit": arguments.limit,
    }
    kind = arguments.kind
    if kind == "nodes":
        return await client.debug_node_executions(str(invocation_id), **common)
    if kind == "node":
        return await client.debug_node_execution(
            str(invocation_id),
            arguments.subject_id,
            through_sequence=arguments.through_sequence,
        )
    if kind == "edges":
        return await client.debug_edge_evaluations(str(invocation_id), **common)
    if kind == "edge":
        return await client.debug_edge_evaluation(
            str(invocation_id),
            arguments.subject_id,
            through_sequence=arguments.through_sequence,
        )
    if kind == "operator-calls":
        return await client.debug_operator_calls(str(invocation_id), **common)
    if kind == "operator-call":
        return await client.debug_operator_call(
            str(invocation_id),
            arguments.subject_id,
            through_sequence=arguments.through_sequence,
        )
    if kind == "runtime-events":
        return await client.debug_runtime_events(str(invocation_id), **common)
    if kind == "runtime-event":
        return await client.debug_runtime_event(
            str(invocation_id),
            int(arguments.subject_id),
            through_sequence=arguments.through_sequence,
        )
    if kind == "user-events":
        return await client.debug_user_events(
            str(invocation_id),
            **common,
            include_stream_deltas=arguments.include_stream_deltas,
        )
    if kind == "user-event":
        return await client.debug_user_event(
            str(invocation_id),
            int(arguments.subject_id),
        )
    return await client.debug_runtime_state(
        str(invocation_id),
        through_sequence=arguments.through_sequence,
        path=arguments.path,
    )


async def _database_query(
    service: DebugQueryService,
    invocation_id: UUID,
    arguments: argparse.Namespace,
) -> dict[str, Any]:
    common = {
        "cursor": arguments.cursor,
        "through_sequence": arguments.through_sequence,
        "limit": arguments.limit,
    }
    kind = arguments.kind
    if kind == "nodes":
        return (await service.node_executions(invocation_id, **common)).model_dump(
            mode="json"
        )
    if kind == "node":
        return await service.node_execution(
            invocation_id,
            UUID(arguments.subject_id),
            through_sequence=arguments.through_sequence,
        )
    if kind == "edges":
        return (await service.edge_evaluations(invocation_id, **common)).model_dump(
            mode="json"
        )
    if kind == "edge":
        return await service.edge_evaluation(
            invocation_id,
            arguments.subject_id,
            through_sequence=arguments.through_sequence,
        )
    if kind == "operator-calls":
        return (await service.operator_calls(invocation_id, **common)).model_dump(
            mode="json"
        )
    if kind == "operator-call":
        return await service.operator_call(
            invocation_id,
            UUID(arguments.subject_id),
            through_sequence=arguments.through_sequence,
        )
    if kind == "runtime-events":
        return (await service.runtime_events(invocation_id, **common)).model_dump(
            mode="json"
        )
    if kind == "runtime-event":
        return await service.runtime_event(
            invocation_id,
            int(arguments.subject_id),
            through_sequence=arguments.through_sequence,
        )
    if kind == "user-events":
        return (
            await service.user_events(
                invocation_id,
                **common,
                include_stream_deltas=arguments.include_stream_deltas,
            )
        ).model_dump(mode="json")
    if kind == "user-event":
        return await service.user_event(
            invocation_id,
            int(arguments.subject_id),
        )
    return await service.runtime_state(
        invocation_id,
        through_sequence=arguments.through_sequence,
        path=arguments.path,
    )


def _validate_query_arguments(arguments: argparse.Namespace) -> None:
    singular = {
        "node",
        "edge",
        "operator-call",
        "runtime-event",
        "user-event",
    }
    if arguments.kind in singular and arguments.subject_id is None:
        raise ValueError(
            f"{arguments.kind} query requires its subject identifier."
        )
    if arguments.kind not in singular and arguments.subject_id is not None:
        raise ValueError(
            f"{arguments.kind} query does not accept a subject identifier."
        )
    if arguments.through_sequence is not None and arguments.through_sequence < 0:
        raise ValueError("--through-sequence cannot be negative.")
    if not 1 <= arguments.limit <= 100:
        raise ValueError("--limit must be between 1 and 100.")
    if arguments.path is not None and arguments.kind != "runtime-state":
        raise ValueError("--path is valid only for runtime-state.")
    if arguments.cursor is not None and arguments.kind in singular | {"runtime-state"}:
        raise ValueError("--cursor is valid only for collection queries.")


def _ensure_project_workflow(project: Any, report: InvocationReport) -> None:
    workflow_ids = {loaded.workflow.id for loaded in project.workflows}
    if report.workflow_id not in workflow_ids:
        raise InvocationReportCliError(
            f"Invocation belongs to Workflow '{report.workflow_id}', which is "
            "not declared by the current auto-agent.toml. Run the command from "
            "the owning project."
        )


def _ensure_comparison_project_workflow(
    project: Any,
    comparison: InvocationComparison,
) -> None:
    if comparison.workflow_id is None:
        return
    workflow_ids = {loaded.workflow.id for loaded in project.workflows}
    if comparison.workflow_id not in workflow_ids:
        raise InvocationReportCliError(
            f"Invocations belong to Workflow '{comparison.workflow_id}', which "
            "is not declared by the current auto-agent.toml. Run the command "
            "from the owning project."
        )


def _ensure_database_already_exists(database_url: str) -> None:
    """Keep a read-only debug command from creating a missing SQLite file."""

    url = make_url(database_url)
    if url.get_backend_name() != "sqlite":
        return
    database = url.database
    if database is None or database == ":memory:" or database.startswith("file:"):
        raise InvocationReportCliError(
            "Invocation Report requires a durable SQLite database file."
        )
    if not Path(database).expanduser().exists():
        raise InvocationReportCliError(
            f"Configured SQLite database does not exist: {database}"
        )
