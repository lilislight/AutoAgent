"""Run `python -m autoagent.server --project PATH` to serve API and UI."""

import argparse
from dataclasses import replace

import uvicorn
from uvicorn.config import STARTUP_FAILURE

from .app import create_app
from .serving import _Server


def main():
    parser = argparse.ArgumentParser(description="AutoAgent Local Server and Workflow UI")
    parser.add_argument("--project", default=".")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--no-ui", action="store_true", help="Serve only the API")
    args = parser.parse_args()
    app = create_app(project=args.project, ui=not args.no_ui)
    settings = app.state.autoagent_settings
    overrides = {key: value for key, value in {"host": args.host, "port": args.port}.items() if value is not None}
    settings = replace(settings, **overrides)
    server = _Server(uvicorn.Config(
        app, host=settings.host, port=settings.port,
        timeout_graceful_shutdown=settings.shutdown_timeout_seconds,
    ))
    try:
        server.run()
    except KeyboardInterrupt:
        pass
    if not server.started:
        raise SystemExit(STARTUP_FAILURE)


if __name__ == "__main__":
    main()
