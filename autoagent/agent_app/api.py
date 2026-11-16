"""Agent application configuration and chat routes over reusable Host capabilities."""

from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from fastapi.routing import APIRoute
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, Field, model_validator

from autoagent.agent import AgentModelSelection
from autoagent.agent_app.models import ApplicationSessionSettings
from autoagent.agent_app.resource_config import AgentResourceConfig
from autoagent.agent import ToolPolicy
from autoagent.agent import ToolOperation
from autoagent.host.persistence import ConflictError


class ConfigureAgent(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    session_id: str = Field(min_length=1, max_length=512)
    expected_version: int = Field(ge=0)
    settings: ApplicationSessionSettings


class AgentMessage(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    session_id: str = Field(min_length=1, max_length=512)
    message: str = Field(min_length=1)
    workspace_id: str | None = Field(None, min_length=1, max_length=512)
    model: AgentModelSelection | None = None


class QueuedAgentMessage(AgentMessage):
    request_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    send_to: str | None = Field(None, min_length=1, max_length=512)
    initial_settings: ApplicationSessionSettings | None = None

    @model_validator(mode="after")
    def _steering_model(self):
        if self.send_to is not None and self.model is not None:
            raise ValueError("Messages to a running task cannot change its model.")
        return self


class QueueAction(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    session_id: str = Field(min_length=1, max_length=512)
    item_id: str = Field(min_length=1, max_length=64)
    action: str = Field(pattern="^(cancel|send_now|continue)$")
    invocation_id: str | None = Field(None, min_length=1, max_length=512)


class RenameConversation(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    title: str = Field(min_length=1, max_length=120)
    expected_version: int = Field(ge=0)


class ConfigureResource(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    configuration: AgentResourceConfig
    expected_version: int = Field(default=0, ge=0)


class CreateWorkspace(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    name: str = Field(min_length=1, max_length=128)
    path: str = Field(min_length=1, max_length=4096)


class PolicyPreview(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    policy: ToolPolicy
    tool_name: str | None = Field(default=None, min_length=1)
    tool_id: str | None = Field(default=None, min_length=1, max_length=512)
    workspace_id: str | None = Field(default=None, min_length=1, max_length=512)
    arguments: dict = Field(default_factory=dict)
    operations: list[ToolOperation] = Field(default_factory=list)
    context: dict = Field(default_factory=dict)


class ParsePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    format: Literal["json", "yaml"]
    text: str = Field(max_length=1_048_576)
    editor: bool = False


class AcceptMCPSchema(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    remote_name: str = Field(min_length=1, max_length=512)
    schema_version: str = Field(pattern="^[0-9a-f]{64}$")


class _AgentRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()
        async def handle(request):
            try:
                return await original(request)
            except RequestValidationError as error:
                # Invalid raw credentials must not be echoed in validation responses.
                errors = [dict(path=list(item['loc']), code=item['type'], message=item['msg'])
                    for item in error.errors()]
                raise HTTPException(422, detail=dict(code='invalid_input', message='Input validation failed.', errors=errors)) from None
        return handle




def agent_router(application, settings, query, *, user_events):
    host = application.host
    history, watch = user_events.history, user_events.watch
    from autoagent.server.app import _browse_cursor, _browse_encode
    router = APIRouter(route_class=_AgentRoute)

    async def agent_query(operation):
        try:
            return await query(operation, settings)
        except ConflictError as error:
            raise HTTPException(409, detail=str(error)) from error

    @router.get('/agent/catalog')
    async def catalog():
        return await query(application.catalog(), settings)

    @router.get('/agent/resource-configs')
    async def resource_configs(workspace_id: str | None = Query(None, min_length=1, max_length=512),
                               scope: Literal["tools", "mcp"] | None = None):
        return await agent_query(application.resource_configs(workspace_id, scope=scope))

    @router.get('/agent/providers')
    async def providers():
        return await agent_query(application.list_providers())

    @router.post('/agent/policies/preview')
    async def preview_policy(body: PolicyPreview):
        return await agent_query(application.preview_policy(body.policy, tool_name=body.tool_name,
            arguments=body.arguments, operations=body.operations, context=body.context,
            tool_id=body.tool_id, workspace_id=body.workspace_id))

    @router.post('/agent/policies/parse')
    async def parse_policy(body: ParsePolicy):
        return await agent_query(application.parse_policy(body.text, format=body.format, editor=body.editor))

    @router.post('/agent/mcp/test')
    async def test_mcp(body: ConfigureResource):
        return await agent_query(application.test_mcp(body.configuration))

    @router.post('/agent/mcp/{connection_id}/accept-schema', status_code=204)
    async def accept_mcp_schema(connection_id: str, body: AcceptMCPSchema):
        await agent_query(application.accept_mcp_schema(connection_id, body.remote_name, body.schema_version))

    @router.get('/agent/skills/document')
    async def skill_document(management_id: str = Query(min_length=1, max_length=4096),
                             workspace_id: str | None = Query(None, min_length=1, max_length=512)):
        return await agent_query(application.skill_document(management_id, workspace_id))

    @router.post('/agent/resource-configs', status_code=201)
    async def create_resource(body: ConfigureResource):
        return await agent_query(application.save_resource(body.configuration,
            expected_version=body.expected_version))

    @router.put('/agent/resource-configs/{resource_id}')
    async def update_resource(resource_id: str, body: ConfigureResource):
        return await agent_query(application.save_resource(body.configuration,
            resource_id=resource_id, expected_version=body.expected_version))

    @router.delete('/agent/resource-configs/{resource_id}')
    async def delete_resource(resource_id: str, expected_version: int = Query(ge=1)):
        await agent_query(application.save_resource(None,
            resource_id=resource_id, expected_version=expected_version))
        return {"deleted": True}

    @router.post('/agent/messages', status_code=202)
    async def send_message(body: AgentMessage):
        ref = await agent_query(application.start_conversation(body.session_id, body.message, body.workspace_id,
            unbound="workspace_id" in body.model_fields_set and body.workspace_id is None, model=body.model))
        return dict(accepted=True, invocation_id=ref.invocation_id, session_id=ref.session_id)

    @router.get('/agent/queue')
    async def message_queue(session_id: str = Query(min_length=1, max_length=512)):
        return await agent_query(application.queue_view(session_id))

    @router.post('/agent/queue', status_code=202)
    async def queue_message(body: QueuedAgentMessage):
        return await agent_query(application.queue_submit(body.session_id, body.message,
            body.request_id, body.workspace_id, send_to=body.send_to,
            unbound="workspace_id" in body.model_fields_set and body.workspace_id is None, initial_settings=body.initial_settings, model=body.model))

    @router.post('/agent/queue/actions')
    async def queue_action(body: QueueAction):
        return await agent_query(application.queue_action(body.session_id, body.item_id,
            body.action, body.invocation_id))

    @router.get('/agent/sessions')
    async def sessions(cursor: str | None = Query(None, max_length=4096),
                       workspace_id: str | None = Query(None, min_length=1, max_length=512), unbound: bool = False,
                       limit: int = Query(20, ge=1, le=100)):
        from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID
        scope = ["agent.sessions", BUILTIN_AGENT_WORKFLOW_ID]
        if unbound and workspace_id is not None:
            raise HTTPException(400, detail="Choose a Workspace or unbound conversations.")
        if workspace_id is not None or unbound:
            scope.append(workspace_id)
        position = _browse_cursor(cursor, scope)
        page = await agent_query(application.list_sessions(workspace_id=workspace_id, unbound=unbound, limit=limit, cursor=position))
        next_cursor = _browse_encode(page.next_cursor, scope)
        return dict(items=page.items, next_cursor=next_cursor)

    @router.patch('/agent/sessions/{session_id}/title')
    async def rename_conversation(session_id: str, body: RenameConversation):
        return await agent_query(application.rename_conversation(session_id, body.title, expected_version=body.expected_version))

    @router.get('/agent/invocations')
    async def invocations(session_id: str = Query(min_length=1, max_length=512),
                          cursor: str | None = Query(None, max_length=4096)):
        from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID
        await query(application.workspace_session(session_id), settings)
        scope = ["agent.invocations", session_id]
        position = _browse_cursor(cursor, scope)
        page = await query(host.list_session_invocations(session_id, workflow_ids=(BUILTIN_AGENT_WORKFLOW_ID,), limit=20, cursor=position), settings)
        next_cursor = _browse_encode(page.next_cursor, scope)
        return dict(items=page.items, next_cursor=next_cursor)

    @router.get('/agent/settings')
    async def get_settings(session_id: str = Query(min_length=1, max_length=512),
                           workspace_id: str | None = Query(None, min_length=1, max_length=512), unbound: bool = False):
        return await agent_query(application.conversation_settings(session_id, workspace_id, unbound=unbound))

    @router.put('/agent/settings')
    async def configure(body: ConfigureAgent):
        try:
            return await query(application.configure(body.session_id, body.settings,
                expected_version=body.expected_version), settings)
        except ConflictError as error:
            raise HTTPException(409, detail=str(error)) from error

    @router.get('/agent/resources')
    async def resources(revision_id: str = Query(min_length=1, max_length=4096),
                        session_id: str | None = Query(None, min_length=1, max_length=512),
                        workspace_id: str | None = Query(None, min_length=1, max_length=512), unbound: bool = False):
        return await agent_query(application.workspace_resources(revision_id, session_id=session_id,
            workspace_id=workspace_id, unbound=unbound))

    @router.get('/agent/workspaces')
    async def workspaces():
        return dict(items=await query(application.list_workspaces(), settings))

    @router.post('/agent/workspaces', status_code=201)
    async def create_workspace(body: CreateWorkspace):
        try:
            return await query(application.create_workspace(**body.model_dump()), settings)
        except OSError as error:
            raise HTTPException(400, detail=f"Cannot prepare Workspace directory: {error.strerror or str(error)}") from error

    @router.get('/agent/execution')
    async def execution(invocation_id: str = Query(min_length=1, max_length=4096)):
        return await agent_query(application.execution(invocation_id))

    @router.get('/agent/usage')
    async def usage(session_id: str = Query(min_length=1, max_length=512),
                    invocation_id: str | None = Query(None, min_length=1, max_length=4096)):
        return await agent_query(application.usage(session_id, invocation_id))



    @router.get('/agent/history')
    async def agent_history(invocation_id: str = Query(min_length=1, max_length=4096),
                            after: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
        await query(application.workspace_invocation(invocation_id), settings)
        return await history(invocation_id, after, limit)

    @router.get('/agent/events/watch')
    async def agent_watch(session_id: str = Query(min_length=1, max_length=512)):
        await query(application.workspace_session(session_id, allow_new=True), settings)
        return await watch(invocation_id=None, session_id=session_id)

    return router
