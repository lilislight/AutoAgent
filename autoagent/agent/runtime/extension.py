"""Agent SDK integration using the Host's product-neutral extension lifecycle."""

from contextlib import AsyncExitStack, asynccontextmanager
from autoagent.agent.context import agent_runtime_context
from .context import AgentRuntime


class AgentRuntimeExtension:
    id = 'agent'

    def __init__(self, services):
        self.services = services
        self.context = AgentRuntime(services)
        self._pending = {}

    @asynccontextmanager
    async def lifespan(self, setup):
        async with AsyncExitStack() as stack:
            stack.enter_context(agent_runtime_context(self.context))
            await stack.enter_async_context(self.context.approvals.runtime_scope())
            yield

    def resource_scope(self, setup):
        return self.context.resource_scope()

    async def prepare(self, workflows, setup):
        for context in self.context.contexts():
            await context.prepare_resources({key: ir for key, ir in workflows.items()
                if self.context.for_definition(ir) is context})
        self.context.install_operators(setup, workflows)

    async def before_event(self, event):
        if event.event_name == 'invocation.started':
            ir = self.services.runtime.workflow_irs.get(event.payload.workflow_revision_id)
            if ir is not None and self.context.options_for_workflow(ir) is not None:
                context = self.context.for_workflow(ir.workflow_revision_id)
                value = event.payload.input
                from collections.abc import Mapping
                binding = await context.activate(event.session_id, event.invocation_id, reason='start',
                    activation_id=event.id, workflow_revision=ir.workflow_revision_id,
                    model=value.get('model') if isinstance(value, Mapping) else None,
                    message=value.get('message') if isinstance(value, Mapping) else None)
                self._pending[event.id] = (context, binding)

    async def after_event(self, event):
        pending = self._pending.pop(event.id, None)
        if pending is not None:
            pending[0].adopt(pending[1])
        if event.event_name in {'node_occurrence.waiting', 'invocation.waiting'}:
            for context in self.context.contexts():
                await context.approvals.waiting(event.session_id)
        elif event.event_name == 'invocation.cancelled':
            await self.context.cancel_processes({event.invocation_id})
        elif event.event_name == 'child_invocation.planned':
            for unit in event.payload.units:
                self.context._agent_parents[unit.child_invocation_id] = event.invocation_id

    async def restored(self, graph):
        for checkpoint in graph.sessions:
            if checkpoint.state.invocation:
                for plan in checkpoint.state.invocation.child_plans.values():
                    for unit in plan.units:
                        self.context._agent_parents[unit.invocation_id] = checkpoint.state.invocation.id

    def forget(self, invocation_id):
        self.context.forget(invocation_id)

    async def project(self, result):
        context = self.context.for_workflow(result.ref.workflow_revision_id)
        return await context.approvals.project(result)

    async def respond(self, ref, wait_id, response):
        context = self.context.for_workflow(ref.workflow_revision_id)
        if not await self._agent_wait(ref, wait_id):
            return False
        await context.approvals.respond(ref, wait_id, response)
        return True

    async def action_view(self, ref, wait_id):
        context = self.context.for_workflow(ref.workflow_revision_id)
        if not await self._agent_wait(ref, wait_id):
            return None
        return await context.approvals.action_view(ref, wait_id)

    async def _agent_wait(self, ref, wait_id):
        from collections.abc import Mapping
        status = await self.services.runtime.app.astatus(ref)
        wait = next((item for item in status.waits if item.id == wait_id and item.kind == 'external'), None)
        return wait is not None and isinstance(wait.request, Mapping) and wait.request.get('kind') in {
            'agent.tool_approval', 'agent.ask_user'}

    def allows_recovery(self, revision, status, **options):
        return self.context.for_workflow(revision).allows_recovery(status, **options)

    async def recovered(self, ref, result):
        return await self.context.for_workflow(ref.workflow_revision_id).approvals.reconcile(ref, result)
