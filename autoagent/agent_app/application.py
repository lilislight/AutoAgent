"""Composition and lifecycle of the built-in Agent product."""

from contextlib import AsyncExitStack, asynccontextmanager

from .workflow import BUILTIN_AGENT_WORKFLOW_ID, builtin_agent_workflow, is_builtin_agent


class AgentApplication:
    """Own product registration and services without adopting user Workflows.

    Host enters the runtime scope after project resources have been declared,
    starts chat submissions once execution is ready, and stops submissions
    before shutting down execution.
    """

    def __init__(self, host):
        self.host = host
        from .persistence import AgentApplicationStorage
        self.storage = AgentApplicationStorage(lambda: host._persistence)
        self._credentials = None
        self._edit_code_permissions = True
        from .context import ApplicationContext
        self.context = ApplicationContext(host)
        from .providers import ApplicationProviders
        self.providers = ApplicationProviders(self)
        self._workflow = None
        self._entered = False
        self._runtime_active = False
        self._queue_started = False

    def _configure_startup(self):
        """Read product-only configuration when the product is actually used."""
        if self._credentials is not None:
            return
        from .credentials import ApplicationCredentials
        flag = self.host.environment.get("AUTOAGENT_AGENT_EDIT_CODE_PERMISSIONS", "true").strip().lower()
        if flag not in {"true", "false", "1", "0"}:
            raise ValueError("AUTOAGENT_AGENT_EDIT_CODE_PERMISSIONS must be true, false, 1 or 0.")
        credentials = ApplicationCredentials(self.host, self.host.environment.get("AUTOAGENT_APP_SECRETS_FILE"))
        self._edit_code_permissions = flag in {"true", "1"}
        self._credentials = credentials

    @property
    def credentials(self):
        self._configure_startup()
        return self._credentials

    @property
    def edit_code_permissions(self):
        self._configure_startup()
        return self._edit_code_permissions

    @staticmethod
    def project_workflows(workflows, environment):
        """Include the product according to startup configuration, reserving its ID."""
        workflows = tuple(workflows)
        if any(workflow.id == BUILTIN_AGENT_WORKFLOW_ID for workflow in workflows):
            raise ValueError(f"Workflow id {BUILTIN_AGENT_WORKFLOW_ID!r} is reserved for the Agent application.")
        enabled = environment.get("AUTOAGENT_BUILTIN_AGENT_ENABLED", "true").strip().lower()
        if enabled not in {"true", "false", "1", "0"}:
            raise ValueError("AUTOAGENT_BUILTIN_AGENT_ENABLED must be true, false, 1 or 0.")
        return (*workflows, builtin_agent_workflow()) if enabled in {"true", "1"} else workflows

    @staticmethod
    def validate_workflows(workflows):
        """Only the product implementation may occupy the reserved Workflow ID."""
        selected = None
        for workflow in workflows:
            if workflow.id != BUILTIN_AGENT_WORKFLOW_ID:
                continue
            if not is_builtin_agent(workflow):
                raise ValueError(f"Workflow id {BUILTIN_AGENT_WORKFLOW_ID!r} is reserved for the Agent application.")
            if selected is not None:
                raise ValueError(f"Duplicate Workflow id: {BUILTIN_AGENT_WORKFLOW_ID}")
            selected = workflow
        return selected

    @property
    def enabled(self):
        """Whether this Host registered the product's implementation."""
        return self._workflow is not None

    @property
    def running(self):
        """Whether runtime resources and chat submission processing have started."""
        return self._runtime_active and self._queue_started

    def owns(self, workflow):
        """Recognize product definitions and compiled IR, not arbitrary Agent builds."""
        return self.enabled and is_builtin_agent(workflow)

    def require_enabled(self):
        from autoagent.host import HostNotFoundError
        if not self.enabled:
            raise HostNotFoundError("The Agent application is disabled.")

    async def catalog(self):
        result = await self.context.workspace_catalog()
        choices = await self.providers.describe() if self.enabled else {'items': []}
        result['provider_options'] = [dict(id=row['alias'], name=row['configuration']['name'],
            models=row['configuration'].get('models', [])) for row in choices['items']
            if row['configuration'].get('enabled', True)]
        return result

    async def resource_configs(self, workspace_id=None):
        self.require_enabled()
        result = await self.context.shared_resources.describe(workspace_id)
        from .resource_config import ResourcePermissions
        return {**result, "policy_schema": ResourcePermissions.model_json_schema(),
            "edit_code_permissions": self.edit_code_permissions}

    async def save_resource(self, config, *, resource_id=None, expected_version=0):
        self.require_enabled()
        return await self.context.shared_resources.save(config, resource_id=resource_id, expected_version=expected_version)

    async def skill_document(self, management_id, workspace_id=None):
        self.require_enabled()
        return await self.context.shared_resources.skill_document(management_id, workspace_id)

    async def preview_policy(self, policy, *, tool_name, arguments, operations=()):
        """Evaluate an editor example without executing a Tool or issuing an approval."""
        self.require_enabled()
        import json
        from autoagent.agent import LLMToolCall, ToolContext
        call = LLMToolCall(name=tool_name, call_id="policy-preview", arguments=json.dumps(arguments))
        context = ToolContext(operations=operations).for_call(call, arguments)
        return dict(preview_only=True, decision=policy(call, context).model_dump(mode='json'))

    async def test_mcp(self, config):
        self.require_enabled()
        from .resource_config import MCPResourceConfig
        from autoagent.agent import MCPClient
        if not isinstance(config, MCPResourceConfig):
            raise ValueError("An MCP resource configuration is required.")
        async def test():
            try:
                transport = config.resolve_transport(self.credentials)
                async with MCPClient(transport, reconnect_policy=config.reconnect) as client:
                    tools = await client.list_tools()
                    return dict(connected=True, tools=[dict(name=tool.name, description=tool.description,
                        input_schema=tool.input_schema) for tool in tools])
            except Exception:
                return dict(connected=False, tools=[], error="Cannot connect to MCP Server; check configuration and credential references.")
        return await self.host._perform(lambda: self.context.approvals._run(test))

    async def list_providers(self):
        self.require_enabled()
        return await self.providers.describe()

    async def save_provider(self, config, *, provider_id=None, expected_version=0):
        self.require_enabled()
        return await self.providers.save(config, provider_id=provider_id, expected_version=expected_version)

    async def start_conversation(self, *args, **kwargs):
        self.require_enabled()
        return await self.context.start_conversation(*args, **kwargs)

    async def conversation_settings(self, *args, **kwargs):
        self.require_enabled()
        return await self.context.conversation_settings(*args, **kwargs)

    async def configure(self, session_id, settings, *, expected_version=None):
        self.require_enabled()
        await self.context.workspace_session(session_id, allow_new=True)
        return await self.context.configure(session_id, settings, expected_version=expected_version)

    async def rename_conversation(self, *args, **kwargs):
        self.require_enabled()
        return await self.context.rename_conversation(*args, **kwargs)

    async def workspace_session(self, *args, **kwargs):
        self.require_enabled()
        return await self.context.workspace_session(*args, **kwargs)

    async def workspace_invocation(self, *args, **kwargs):
        self.require_enabled()
        return await self.context.workspace_invocation(*args, **kwargs)

    async def workspace_resources(self, *args, **kwargs):
        self.require_enabled()
        return await self.context.workspace_resources(*args, **kwargs)

    async def list_workspaces(self):
        self.require_enabled()
        return await self.context.list_workspaces()

    async def create_workspace(self, **kwargs):
        self.require_enabled()
        return await self.context.create_workspace(**kwargs)

    async def queue_view(self, session_id):
        self.require_enabled()
        return await self.context.messages.view(session_id)

    async def queue_submit(self, *args, **kwargs):
        self.require_enabled()
        return await self.context.messages.submit(*args, **kwargs)

    async def queue_action(self, *args, **kwargs):
        self.require_enabled()
        return await self.context.messages.act(*args, **kwargs)

    async def get_settings(self, session_id):
        await self.workspace_session(session_id, allow_new=True)
        return await self.context.get_settings(session_id)

    async def get_snapshot(self, invocation_id):
        await self.workspace_invocation(invocation_id)
        return await self.context.get_snapshot(invocation_id)

    async def list_conversation_sessions(self, workspace_id, **kwargs):
        self.require_enabled()
        return await self.context.list_conversation_sessions(workspace_id, **kwargs)

    async def list_sessions(self, *, workspace_id=None, unbound=False, limit=20, cursor=None):
        import asyncio
        from autoagent.host import HostNotFoundError
        from autoagent.host.persistence.models import Page
        if not self.enabled:
            return Page(())
        page = await self.context.list_conversation_sessions(workspace_id, limit=limit, cursor=cursor) if workspace_id is not None or unbound else (
            await self.host.list_execution_sessions(workflow_ids=(BUILTIN_AGENT_WORKFLOW_ID,), limit=limit, cursor=cursor))
        async def describe(row):
            if row['workflow_count'] != 1:
                return None
            try:
                info = await self.context.workspace_session(row['session_id'])
            except HostNotFoundError:
                return None
            if info['parent_invocation_id'] is not None:
                return None
            current = await self.host.list_session_invocations(row['session_id'], limit=1, workflow_ids=(BUILTIN_AGENT_WORKFLOW_ID,))
            if not current.items:
                return None
            record = await self.context.get_settings(row['session_id'])
            return dict(session_id=row['session_id'], title=record.title or 'Chat', invocation_id=current.items[0]['invocation_id'],
                updated_at_us=row['updated_at_us'], invocation_count=row['invocation_count'],
                workspace_id=record.workspace.workspace_id if record.workspace else None, metadata_version=record.metadata_version)
        items = await asyncio.gather(*(describe(row) for row in page.items))
        return Page(tuple(item for item in items if item is not None), page.next_cursor)

    async def execution(self, invocation_id):
        import asyncio
        import base64
        import json
        family = await self.workspace_invocation(invocation_id)
        snapshot = await self.context.get_snapshot(invocation_id)
        root = family['root'] or family['current']
        controls, children = await asyncio.gather(self.host.get_execution_controls(root['invocation_id']),
            self.host.list_child_executions(invocation_id, limit=100, order='asc'))
        if controls['next_cursor']:
            controls['next_cursor'] = base64.urlsafe_b64encode(json.dumps([root['invocation_id'], controls['next_cursor']]).encode()).decode().rstrip('=')
        return dict(current=family['current'], root=root, ancestors=family['ancestors'], children=children.items,
            children_truncated=children.next_cursor is not None, controls=controls, snapshot=snapshot)

    async def usage(self, session_id, invocation_id=None):
        await self.workspace_session(session_id, allow_new=True)
        if invocation_id is not None:
            family = await self.workspace_invocation(invocation_id)
            if family['current']['session_id'] != session_id:
                raise ValueError('Execution belongs to another conversation.')
        session = await self.host.token_usage(session_id=session_id)
        invocation = await self.host.token_usage(invocation_id=invocation_id, include_descendants=True) if invocation_id else None
        return dict(session=session, invocation=invocation)

    @asynccontextmanager
    async def runtime_scope(self, setup):
        """Own product resource monitoring on the runtime resource loop."""
        if self._entered:
            raise RuntimeError("An Agent application can be entered only once.")
        self._entered = True
        self._workflow = self.validate_workflows(setup.workflows)
        try:
            if self.enabled:
                self._configure_startup()
                self.context.initialize_defaults(self.host.agent_context)
                async with AsyncExitStack() as stack:
                    stack.enter_context(self.host.agent_context.context_scope(self.owns, self.context))
                    await stack.enter_async_context(self.context.resource_scope())
                    await stack.enter_async_context(self.providers.lifespan())
                    await stack.enter_async_context(self.context.shared_resources.lifespan())
                    self._runtime_active = True
                    yield
            else:
                self._runtime_active = True
                yield
        finally:
            self._runtime_active = False

    async def start(self):
        """Start product submissions on the Host loop after runtime preparation."""
        if not self._runtime_active:
            raise RuntimeError("Agent application runtime resources are not running.")
        if not self.enabled:
            return
        if self._queue_started:
            raise RuntimeError("Agent application submissions have already started.")
        self._queue_started = True
        try:
            await self.context.messages.start()
        except BaseException:
            await self.close()
            raise

    async def close(self):
        """Stop product submissions before Host execution and persistence close."""
        if self._queue_started:
            try:
                await self.context.messages.close()
            finally:
                self._queue_started = False
