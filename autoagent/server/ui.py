"""Static UI hosting with API configuration and safe SPA navigation."""

from __future__ import annotations

import asyncio
import json
from html import escape
from pathlib import Path

from fastapi import FastAPI
from starlette.exceptions import HTTPException
from starlette.responses import HTMLResponse
from starlette.staticfiles import StaticFiles

from .settings import ServerSettings


class WorkflowUI(StaticFiles):
    def __init__(self, directory: Path, settings: ServerSettings):
        super().__init__(directory=directory, html=True)
        self.settings = settings
        self.index_path = directory / "index.html"
        if "<!--AUTOAGENT_CONFIG-->" not in self.index_path.read_text(encoding="utf-8"):
            raise ValueError("UI index.html is missing the AutoAgent configuration marker.")

    async def get_response(self, path: str, scope):
        # StaticFiles validates paths (including symlinks) before SPA fallback.
        try:
            response = await super().get_response(path, scope)
        except HTTPException as exc:
            # Inspect the encoded segment so arbitrary Session IDs (including
            # encoded slashes) remain one route segment. Unknown asset paths
            # must still return 404 instead of the application's HTML.
            raw_path = scope.get("raw_path", b"").split(b"?", 1)[0]
            segment = (raw_path.rsplit(b"/agent/c/", 1)[-1] if b"/agent/c/" in raw_path
                else path[len("agent/c/"):].encode()).rstrip(b"/")
            agent_page = path in ("agent", "agent/") or (
                path.startswith("agent/c/") and bool(segment) and b"/" not in segment
            )
            if exc.status_code != 404 or not (path == "workflows" or path.startswith("workflows/") or agent_page):
                raise
            response = None
        if path in (".", "", "index.html", "workflows") or response is None:
            mounted_root = scope.get("root_path", "").rstrip("/")
            ui_path = self.settings.ui_path
            parent_root = mounted_root
            if ui_path != "/" and mounted_root.endswith(ui_path):
                parent_root = mounted_root[:-len(ui_path)]
            host = getattr(getattr(scope.get("app"), "state", None), "autoagent_host", None)
            standalone = (await host.agent_app.catalog())["standalone"] if host is not None else False
            config = json.dumps({
                "apiPrefix": parent_root + self.settings.api_prefix,
                "uiBase": mounted_root + "/",
                "pageSize": self.settings.page_size,
                "defaultView": "agents" if standalone else "workflows",
            }).replace("<", "\\u003c")
            injection = (
                f'<base href="{escape(mounted_root, quote=True)}/">'
                f'<script id="autoagent-config" type="application/json">{config}</script>'
            )
            # Vite rebuilds hashed assets. Never serve startup HTML pointing
            # at chunks that a later local build has already replaced.
            index = await asyncio.to_thread(self.index_path.read_text, encoding="utf-8")
            return HTMLResponse(
                index.replace("<!--AUTOAGENT_CONFIG-->", injection),
                headers={"Cache-Control": "no-store"},
            )
        return response


def mount_ui(app: FastAPI, *, settings: ServerSettings | None = None) -> None:
    """Mount UI separately from the APIRouter, after the application's API routes.

    The enclosing application owns its Host lifespan. This only serves files.
    API/UI prefixes in settings must match the actual include_router/mount paths.
    """
    settings = settings or ServerSettings()
    directory = settings.ui_directory or Path(__file__).with_name("static")
    if not (directory / "index.html").is_file():
        raise FileNotFoundError("AutoAgent UI is not built. Run `npm ci && npm run build` in ui/.")
    app.mount(settings.ui_path, WorkflowUI(directory, settings), name="autoagent_ui")
