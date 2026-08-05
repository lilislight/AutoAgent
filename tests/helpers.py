from __future__ import annotations

import inspect
from typing import Any, Callable, TypeVar

from autoagent import AutoAgentApp, AutoAgentSettings


F = TypeVar("F", bound=Callable[..., Any])


def dynamic_json_callable(handler: F) -> F:
    """Give a test-only lambda an explicit dynamic JSON callable contract."""

    signature = inspect.signature(handler)
    handler.__annotations__ = {
        **{name: Any for name in signature.parameters},
        "return": Any,
    }
    return handler


def isolated_app(*args: Any, **kwargs: Any) -> AutoAgentApp:
    """Build an App whose settings never inherit the developer environment."""

    # Unit tests must not inherit a developer's repository-local ``.env``.
    # In particular, enabling AUTOAGENT_DATABASE_URL for a manual tracing run
    # used to turn every ordinary ``started_app()`` call into a durable App and
    # leave database connections/persistence workers behind in tests that only
    # need the in-memory RuntimeStore.
    kwargs.setdefault("settings", AutoAgentSettings())
    return AutoAgentApp(*args, **kwargs)


def started_app(*args: Any, **kwargs: Any) -> AutoAgentApp:
    """Build and explicitly start an App for tests without durable recovery."""

    app = isolated_app(*args, **kwargs)
    app.start()
    return app
