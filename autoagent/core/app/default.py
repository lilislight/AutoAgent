from __future__ import annotations

from threading import Lock

from autoagent.core.app.app import AutoAgentApp


_default_app: AutoAgentApp | None = None
_default_app_lock = Lock()


def get_default_app() -> AutoAgentApp:
    """Return the process-wide default App used by module-level decorators.

    Creation is lazy so importing autoagent has no registry side effects. The
    same function can later be used by Workflow.invoke without requiring users
    to construct or retrieve the default App explicitly.
    """

    global _default_app
    if _default_app is None:
        with _default_app_lock:
            if _default_app is None:
                _default_app = AutoAgentApp()
    return _default_app
