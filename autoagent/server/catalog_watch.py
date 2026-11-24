"""Live definition catalog notifications, independent of execution streams."""

import json
import time

from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse

from .workflow_visibility import hidden_workflows


class CatalogWatch:
    def __init__(self):
        self.closed = False
        self.count = 0

    async def close(self):
        self.closed = True


def install_catalog_watch(router, host, settings, query):
    watch = CatalogWatch()

    def visible(head, request):
        excluded = hidden_workflows(settings) if request.query_params.get("ui") == "workflow" else ()
        return {**head, "workflows": {key: value for key, value in head["workflows"].items() if key not in excluded}}

    @router.get("/workflow-catalog/head")
    async def catalog_head(request: Request):
        return visible(await query(host.workflow_catalog_head(), settings), request)

    @router.get("/workflow-catalog/watch")
    async def catalog_watch(request: Request):
        if watch.closed or watch.count >= settings.trace_max_subscribers:
            raise HTTPException(503, detail="Workflow catalog subscription capacity is exhausted.")
        initial = await query(host.workflow_catalog_head(), settings)
        if watch.closed or watch.count >= settings.trace_max_subscribers:
            raise HTTPException(503, detail="Workflow catalog subscription capacity is exhausted.")
        watch.count += 1
        async def events():
            head, sent = initial, time.monotonic()
            try:
                yield "event: catalog\ndata: " + json.dumps(visible(head, request), separators=(",", ":")) + "\n\n"
                while not watch.closed and not await request.is_disconnected():
                    # Short cancellation boundaries let Server shutdown drain
                    # these streams without waiting for a full heartbeat.
                    latest = await host.wait_workflow_catalog(head["version"], timeout=1)
                    if latest is None:
                        return
                    if latest["version"] != head["version"]:
                        yield "event: catalog\ndata: " + json.dumps(visible(latest, request), separators=(",", ":")) + "\n\n"
                        sent = time.monotonic()
                    elif time.monotonic() - sent >= settings.trace_heartbeat_seconds:
                        yield ": heartbeat\n\n"
                        sent = time.monotonic()
                    head = latest
            finally:
                watch.count -= 1
        return StreamingResponse(events(), media_type="text/event-stream",
            headers={"Cache-Control": "no-cache, no-store", "X-Accel-Buffering": "no"})
    return watch
