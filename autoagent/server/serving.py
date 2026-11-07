"""Standalone transport shutdown, before ASGI lifespan teardown."""

import asyncio
import logging
from dataclasses import replace

import uvicorn


class _Server(uvicorn.Server):
    on_ready = None
    on_stopping = None
    on_stopped = None

    async def startup(self, sockets=None):
        await super().startup(sockets=sockets)
        callback = self.on_ready
        if self.started and callback:
            host = self.config.app.state.autoagent_host
            count, cursor = 0, None
            while True:
                page = await host.list_workflows(limit=1000, cursor=cursor)
                count += len(page.items)
                cursor = page.next_cursor
                if cursor is None:
                    break
            callback(self.config.app.state.autoagent_settings, count)

    async def shutdown(self, sockets=None):
        # Uvicorn drains HTTP responses before sending lifespan.shutdown.
        # Close our unbounded responses first, while Host is still available.
        hubs = (tuple(getattr(self.config.app.state, '_autoagent_trace_hubs', ()))
                + tuple(getattr(self.config.app.state, '_autoagent_user_event_hubs', ())))
        callback = self.on_stopping
        if self.started and callback:
            callback()
        await asyncio.gather(*(hub.close() for hub in hubs))
        await super().shutdown(sockets=sockets)
        callback = self.on_stopped
        if self.started and callback:
            callback()


class _StartupLogFilter(logging.Filter):
    def __init__(self, app):
        self.app = app

    def filter(self, record):
        # The CLI reports the original startup exception once. Uvicorn's
        # preformatted traceback is useful only when debug is requested.
        return getattr(self.app.state, "autoagent_startup_error", None) is None


def run_server(*, project=".", host=None, port=None, ui=True, env_file=None, use_env_file=True,
               on_ready=None, on_stopping=None, on_stopped=None, access_log=None, debug=False):
    """Own one foreground ASGI lifespan for both Python and CLI entry points."""
    from .app import create_app

    app = create_app(project=project, ui=ui, env_file=env_file, use_env_file=use_env_file)
    overrides = {key: value for key, value in {"host": host, "port": port, "access_log": access_log}.items() if value is not None}
    settings = replace(app.state.autoagent_settings, **overrides)
    app.state.autoagent_settings = settings
    server = _Server(uvicorn.Config(app, host=settings.host, port=settings.port,
        log_config=None, log_level=None, access_log=settings.access_log,
        timeout_graceful_shutdown=settings.shutdown_timeout_seconds))
    server.on_ready = on_ready
    server.on_stopping = on_stopping
    server.on_stopped = on_stopped
    startup_filter = _StartupLogFilter(app)
    logger = logging.getLogger("uvicorn.error")
    if not debug:
        logger.addFilter(startup_filter)
    try:
        try:
            server.run()
        except KeyboardInterrupt:
            pass
        except SystemExit as error:
            startup_error = getattr(app.state, "autoagent_startup_error", None)
            if startup_error is not None:
                raise startup_error
            cause = error.__context__
            raise RuntimeError(f"Cannot listen on {settings.host}:{settings.port}. Check whether the address is available or choose --port.") from cause
    finally:
        logger.removeFilter(startup_filter)
    if not server.started:
        startup_error = getattr(app.state, "autoagent_startup_error", None)
        if startup_error is not None:
            raise startup_error
        raise RuntimeError("Server did not finish startup.")
