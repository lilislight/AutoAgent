from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy.engine import make_url

from autoagent.cli.render import render_invocation_report, write_report
from autoagent.cli.server_client import (
    AutoAgentServerClient,
    ServerClientError,
    resolve_server_url,
)
from autoagent.core.app import AutoAgentSettings
from autoagent.debug import DebugQueryService, InvocationReport


class InvocationReportCliError(RuntimeError):
    """The CLI could not resolve or query authoritative Invocation evidence."""


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


def _ensure_project_workflow(project: Any, report: InvocationReport) -> None:
    workflow_ids = {loaded.workflow.id for loaded in project.workflows}
    if report.workflow_id not in workflow_ids:
        raise InvocationReportCliError(
            f"Invocation belongs to Workflow '{report.workflow_id}', which is "
            "not declared by the current auto-agent.toml. Run the command from "
            "the owning project."
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
