"""Composition and lifecycle of the built-in Agent product."""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager

from .workflow import BUILTIN_AGENT_WORKFLOW_ID, builtin_agent_workflow, is_builtin_agent


class AgentApplication:
    id = "agent"
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
        from .forks import ConversationForks
        self.forks = ConversationForks(self)
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
        credentials = ApplicationCredentials(self.host)
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

    def prepare_workflows(self, workflows):
        selected = self.validate_workflows(workflows)
        if selected is None:
            workflows = self.project_workflows(workflows, self.host.environment)
        else:
            # Product registries belong to this application instance. Reusing a
            # Workflow definition after shutdown must not reuse previous live registrations.
            from .registry import ApplicationResources
            handler = next(node.executable.handler for node in selected.nodes if node.id == "start")
            template = handler.__autoagent_resources__
            resources = ApplicationResources(self.context, tools=template.tools, mcp=template.mcp, skills=template.skills,
                namespace=template.namespace)
            fresh = builtin_agent_workflow(resources=resources, execution=getattr(handler, "__agent_app_execution__", None) or False,
                command_policy=handler.__agent_app_command_policy__)
            workflows = tuple(fresh if workflow is selected else workflow for workflow in workflows)
        self._workflow = self.validate_workflows(workflows)
        if self._workflow is not None:
            handler = next(node.executable.handler for node in self._workflow.nodes if node.id == "start")
            from .registry import ApplicationResources
            registry = handler.__autoagent_resources__
            if not isinstance(registry, ApplicationResources):
                registry = ApplicationResources(self.context, tools=registry.tools, mcp=registry.mcp, skills=registry.skills,
                    namespace=registry.namespace)
                handler.__autoagent_resources__ = registry
            self.context.resources = registry
        return workflows

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
        choices = await self.host.providers.describe()
        result['provider_options'] = [dict(id=row['id'], name=row['id'],
            models=[model['id'] for model in row['models'] if model['enabled']],
            model_details=[model for model in row['models'] if model['enabled']]) for row in choices['items'] if row['enabled']]
        result['providers'] = [row['id'] for row in result['provider_options']]
        return result

    async def resource_configs(self, workspace_id=None, *, scope=None):
        self.require_enabled()
        result = await self.context.shared_resources.describe(workspace_id, scope=scope)
        from autoagent.agent import ToolPolicy
        return {**result, "policy_schema": ToolPolicy.model_json_schema(),
            "edit_code_permissions": self.edit_code_permissions}

    async def save_resource(self, config, *, resource_id=None, expected_version=0):
        self.require_enabled()
        return await self.context.shared_resources.save(config, resource_id=resource_id, expected_version=expected_version)

    async def skill_document(self, management_id, workspace_id=None):
        self.require_enabled()
        return await self.context.shared_resources.skill_document(management_id, workspace_id)

    async def accept_mcp_schema(self, connection_id, remote_name, version):
        self.require_enabled()
        await self.context.shared_resources.accept_mcp_schema(connection_id, remote_name, version)

    async def preview_policy(self, policy, *, arguments, tool_name=None, operations=(), context=None,
                             tool_id=None, workspace_id=None):
        """Evaluate an editor example without executing a Tool or issuing an approval."""
        self.require_enabled()
        import json
        from autoagent.agent import LLMToolCall, LLMToolResult, ToolContext, ToolSet
        supplied = context or {}
        if set(supplied) - {"user_id", "session_id", "attributes"}:
            raise ValueError("Preview Context accepts user_id, session_id and attributes only.")
        for field in ("user_id", "session_id"):
            if supplied.get(field) is not None and not isinstance(supplied[field], str):
                raise ValueError(f"Preview Context {field} must be a string or null.")
        if "attributes" in supplied and not isinstance(supplied["attributes"], dict):
            raise ValueError("Preview Context attributes must be an object.")
        current = ToolContext(operations=operations, **supplied)
        registered, callbacks = (), False
        if tool_id is not None:
            from autoagent.core.operators import run_sync
            from autoagent.agent.workflow.builtins import tool_kind
            resources = self.context.shared_resources
            entry = await resources.describe(workspace_id, tool_id=tool_id,
                scope="mcp" if tool_id.startswith("mcp/") else None)
            if tool_kind(entry) != "tool":
                raise ValueError("This native Agent tool has no configurable permissions.")
            call = LLMToolCall(name=entry.name, call_id="policy-preview", arguments=json.dumps(arguments))
            tool = resources._executable(entry.value)
            prepared = await run_sync(ToolSet((tool,))._prepare, call, current)
            if isinstance(prepared, LLMToolResult):
                raise ValueError(prepared.structured_content["error"]["message"])
            current = prepared.context
            code, callbacks = resources.registered_policies(entry)
            configured = resources.configured_policies(resources.resource_identity(entry), entry.resource_id)
            registered = (*(("code", value) for value in code),
                *((source, value) for source, value in configured if source != "tool"))
        else:
            if tool_name is None:
                raise ValueError("Choose a Tool to test.")
            call = LLMToolCall(name=tool_name, call_id="policy-preview", arguments=json.dumps(arguments))
            current = current.for_call(call, arguments)
        from autoagent.agent.tools.operations import combine_decisions
        explanations = [(source, value.explain(call, current)) for source, value in (*registered, ("custom", policy))]
        decision = combine_decisions(value["decision"] for _, value in explanations)
        operations = []
        for rows in zip(*(value["operations"] for _, value in explanations), strict=True):
            matched_rules = []
            defaults = []
            for layer, ((source, _), operation) in enumerate(zip(explanations, rows, strict=True)):
                matched_rules.extend({**rule, "source": source, "layer": layer} for rule in operation["matched_rules"])
                if operation["default_used"]:
                    defaults.append(dict(source=source, layer=layer, decision=operation["decision"]["action"]))
            operations.append(dict(operation=rows[0]["operation"], matched_rules=matched_rules,
                decision=combine_decisions(operation["decision"] for operation in rows).model_dump(mode="json"),
                defaults=defaults, default_used=len(defaults) == len(rows)))
        return dict(preview_only=True, decision=decision.model_dump(mode="json"), operations=operations,
            includes_code_callbacks=not callbacks)

    async def parse_policy(self, text, *, format, editor=False):
        """Validate editor text using the SDK parser, without touching application settings."""
        self.require_enabled()
        from autoagent.agent import ToolPolicy
        if format not in {"json", "yaml"}:
            raise ValueError("Unsupported policy format.")
        policy = ToolPolicy.from_yaml(text) if format == "yaml" else ToolPolicy.from_json(text)
        if editor:
            import json
            import yaml
            value = policy.to_dict()
            for rule in value["rules"]:
                rule.pop("id")
                rule.pop("reason")
            return dict(policy=value, json=json.dumps(value, indent=2, ensure_ascii=False),
                yaml=yaml.safe_dump(value, sort_keys=False, allow_unicode=True))
        return dict(policy=policy.to_dict(), json=policy.to_json(), yaml=policy.to_yaml())

    async def test_mcp(self, config):
        self.require_enabled()
        from .resource_config import MCPResourceConfig
        from autoagent.agent import AgentResourceRegistry, MCPClient, MCPResourceSource, MCPToolRegistry
        if not isinstance(config, MCPResourceConfig):
            raise ValueError("An MCP resource configuration is required.")
        async def test():
            try:
                transport = config.resolve_transport(self.credentials)
                async with MCPClient(transport, reconnect_policy=config.reconnect) as client:
                    registry = MCPToolRegistry(client, prefix=config.prefix)
                    resources = AgentResourceRegistry(namespace="mcp-preview")
                    async with MCPResourceSource(resources, registry, reconnect_policy=config.reconnect):
                        return dict(connected=True, tools=[dict(name=tool["name"],
                            description=tool.get("description"), input_schema=tool["inputSchema"])
                            for tool in registry.metadata.values()])
            except Exception:
                return dict(connected=False, tools=[], error="Cannot connect to MCP Server; check configuration and credential references.")
        return await self.host._perform(lambda: self.context.approvals._run(test))

    async def list_providers(self):
        self.require_enabled()
        return await self.host.providers.describe()

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

    async def fork_conversation(self, session_id, new_session_id, invocation_id):
        self.require_enabled()
        return await self.forks.create(session_id, new_session_id, invocation_id)

    async def workspace_session(self, *args, **kwargs):
        self.require_enabled()
        return await self.context.workspace_session(*args, **kwargs)

    async def workspace_invocation(self, *args, **kwargs):
        self.require_enabled()
        return await self.context.workspace_invocation(*args, **kwargs)

    async def conversation_page(self, session_id, **options):
        await self.workspace_session(session_id)
        return await self.host._perform(lambda: self.storage.conversation("page", session_id, **options))

    async def conversation_index(self, session_id):
        await self.workspace_session(session_id)
        return await self.host._perform(lambda: self.storage.conversation("index", session_id))

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
        from autoagent.host.persistence.models import Page
        if not self.enabled:
            return Page(())
        return await self.context.list_conversation_summaries(workspace_id,
            all_workspaces=workspace_id is None and not unbound, limit=limit, cursor=cursor)

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
                await self.forks.target(session_id, invocation_id)
        session = await self.host.token_usage(session_id=session_id, include_descendants=True)
        invocation = await self.host.token_usage(invocation_id=invocation_id, include_descendants=True) if invocation_id else None
        return dict(session=session, invocation=invocation)

    async def list_subagents(self, session_id, *, limit, cursor):
        await self.workspace_session(session_id)
        return await self.host._perform(lambda: self.context.storage(
            "conversation_subagents", session_id, limit=limit, cursor=cursor))

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
                self.context.initialize_defaults(self.host.extensions["agent"].context)
                async with AsyncExitStack() as stack:
                    stack.enter_context(self.host.extensions["agent"].context.context_scope(self.owns, self.context))
                    await stack.enter_async_context(self.context.approvals.runtime_scope())
                    await stack.enter_async_context(self.context.resource_scope())
                    monitor = asyncio.create_task(self.context._watch_resources())
                    async def stop_monitor():
                        monitor.cancel()
                        await asyncio.gather(monitor, return_exceptions=True)
                    stack.push_async_callback(stop_monitor)
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
            await self._cancel_open_tasks("Agent application restarted; the previous task was stopped.")
            await self.context.messages.start()
        except BaseException:
            await self.close()
            raise

    async def _cancel_open_tasks(self, reason):
        cursor = None
        while True:
            page = await self.host.list_execution_sessions(workflow_ids=(BUILTIN_AGENT_WORKFLOW_ID,), statuses=("created", "running", "waiting", "settling"), limit=100, cursor=cursor)
            for row in page.items:
                latest = await self.host.list_session_invocations(row["session_id"], workflow_ids=(BUILTIN_AGENT_WORKFLOW_ID,), limit=1)
                if not latest.items:
                    continue
                invocation = latest.items[0]
                if invocation["parent_invocation_id"] is None and invocation["status"] not in {"completed", "failed", "cancelled"}:
                    await self.host.cancel(invocation["invocation_id"], reason=reason)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor

    async def close(self):
        """Stop product submissions before Host execution and persistence close."""
        if self._queue_started:
            try:
                await self.context.messages.close()
            finally:
                self._queue_started = False
