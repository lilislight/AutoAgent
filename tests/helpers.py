from __future__ import annotations

from typing import Any

from autoagent import AutoAgentApp


def started_app(*args: Any, **kwargs: Any) -> AutoAgentApp:
    """Build and explicitly start an App for tests without durable recovery."""

    app = AutoAgentApp(*args, **kwargs)
    app.start()
    return app
