from __future__ import annotations

from typing import Any

from autoagent import AutoAgentApp, AutoAgentSettings


def started_app(*args: Any, **kwargs: Any) -> AutoAgentApp:
    """Build and explicitly start an App for tests without durable recovery."""

    # Unit tests must not inherit a developer's repository-local ``.env``.
    # In particular, enabling AUTOAGENT_DATABASE_URL for a manual tracing run
    # used to turn every ordinary ``started_app()`` call into a durable App and
    # leave database connections/persistence workers behind in tests that only
    # need the in-memory RuntimeStore.
    kwargs.setdefault("settings", AutoAgentSettings())
    app = AutoAgentApp(*args, **kwargs)
    app.start()
    return app
