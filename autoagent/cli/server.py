"""Foreground Server adapter, with one ready report after successful startup."""

from pathlib import Path
import logging

from . import CliError
from .output import render


def launch(options, stdout, stderr):
    try:
        from autoagent.host.project import Project, import_scope
        from autoagent.server.serving import run_server
    except ModuleNotFoundError as error:
        raise CliError("DEPENDENCY_MISSING", "Install autoagent[server] to serve API and UI.", exit_code=3) from error
    from .execution import local_error
    try:
        project = Project.find(getattr(options, "project", "."))
        def ready(settings, workflow_count):
            address = settings.host
            address = "127.0.0.1" if address == "0.0.0.0" else "[::1]" if address == "::" else f"[{address}]" if ":" in address else address
            origin = f"http://{address}:{settings.port}"
            report = {"schema_version": 1, "command": "server", "ok": True,
                "source": {"kind": "project", "root": str(project.root), "name": project.name},
                "data": {"status": "ready", "api_url": origin + settings.api_prefix,
                    "ui_url": None if options.no_ui else origin + settings.ui_path,
                    "listening": f"{settings.host}:{settings.port}", "registered_workflows": workflow_count}}
            stdout.write(render(report, getattr(options, "format", "text")))
            stdout.flush()
        def stopping():
            logging.getLogger("autoagent.server").info("Stopping Server; saving Host state…")
        def stopped():
            logging.getLogger("autoagent.server").info("Server stopped.")
        with import_scope(Path.cwd()):
            run_server(project=getattr(options, "project", "."), host=options.host, port=options.port,
                ui=not options.no_ui, env_file=getattr(options, "env_file", None),
                use_env_file=not getattr(options, "no_env_file", False), on_ready=ready,
                on_stopping=stopping, on_stopped=stopped,
                access_log=options.access_log, debug=getattr(options, "debug", False))
    except Exception as error:
        mapped = local_error(error)
        if mapped is error:
            raise
        raise mapped from error
