"""Product visibility in Workflow UI requests, separate from SDK access."""

from fastapi import HTTPException, Request
from sqlalchemy import select

from autoagent.host.persistence.schema import meta


WORKFLOW_UI_HEADER = "X-AutoAgent-UI"


def hidden_workflows(settings):
    if settings.workflow_ui_show_agent_app:
        return ()
    from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID
    return (BUILTIN_AGENT_WORKFLOW_ID,)


class VisibilityStorage:
    def __init__(self, writer, queries):
        self.queries = queries

    async def hidden(self, excluded, *, workflows=(), revisions=(), invocations=(), session=None):
        if set(workflows).intersection(excluded):
            return True
        definitions, inv = (meta.tables[name] for name in ("workflow_revisions", "invocations"))
        async with self.queries.view() as (conn, _, _, _):
            if revisions and await conn.scalar(select(definitions.c.revision_id).where(
                    definitions.c.workflow_id.in_(excluded), definitions.c.revision_id.in_(revisions)).limit(1)):
                return True
            if invocations and await conn.scalar(select(inv.c.invocation_id).join(definitions).where(
                    definitions.c.workflow_id.in_(excluded), inv.c.invocation_id.in_(invocations)).limit(1)):
                return True
            if session:
                visible = select(inv.c.invocation_id).join(definitions).where(inv.c.session_id == session,
                    inv.c.status.is_not(None), definitions.c.workflow_id.not_in(excluded)).limit(1)
                hidden = select(inv.c.invocation_id).join(definitions).where(inv.c.session_id == session,
                    definitions.c.workflow_id.in_(excluded)).limit(1)
                return not await conn.scalar(visible) and bool(await conn.scalar(hidden))
        return False


class WorkflowVisibility:
    def __init__(self, host, settings):
        self.host, self.settings = host, settings

    def excluded(self, request):
        return list(hidden_workflows(self.settings)) if request.headers.get(WORKFLOW_UI_HEADER) == "workflow" else []

    async def guard(self, request: Request):
        excluded = self.excluded(request)
        if not excluded:
            return
        collection = request.url.path.rsplit("/", 1)[-1]
        if collection in {"workflow-catalog", "workflows", "execution-sessions", "executions",
                "session-invocations", "session-history", "session-workflows"} and request.method == "GET":
            return
        values = dict(request.query_params)
        values.update(request.path_params)
        if request.method in {"POST", "PUT", "PATCH"} and request.headers.get("content-type", "").startswith("application/json"):
            try:
                body = await request.json()
            except ValueError:
                return
            if isinstance(body, dict):
                values.update(body)
        options = {
            "workflows": [values[key] for key in ("workflow_id",) if isinstance(values.get(key), str)],
            "revisions": [values[key] for key in ("revision_id", "base_revision_id", "target_revision_id") if isinstance(values.get(key), str)],
            "invocations": [values[key] for key in ("invocation_id", "target_invocation_id", "root_invocation_id") if isinstance(values.get(key), str)],
            "session": values.get("session_id") if collection in {"session-information", "session-checkpoints"} else None,
        }
        if not any(options.values()):
            return
        hidden = await self.host._perform(lambda: self.host._persistence.extension_operation(
            VisibilityStorage, "hidden", excluded, **options))
        if hidden:
            raise HTTPException(404, detail={"code": "agent_app_hidden", "message": "Agent App is hidden in this view."})
