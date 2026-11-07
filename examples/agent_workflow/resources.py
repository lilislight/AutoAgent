"""Compose changing MCP/Skill catalogs inside an application's Host lifespan."""

from contextlib import AsyncExitStack, asynccontextmanager

from autoagent.agent import AgentApprovalResponse


@asynccontextmanager
async def synchronized_resources(setup, *, mcp_client=None, skill_roots=()):
    """Use inside the project lifespan; Provider registration remains there too.

    MCP clients must be created/entered on the Host Runtime loop. Sources own
    only their listeners and published bindings, and close before the client.
    Both handles expose refresh() and last_error for application controls.
    """
    async with AsyncExitStack() as stack:
        sources = {}
        if mcp_client is not None:
            await stack.enter_async_context(mcp_client)
            sources["mcp"] = await stack.enter_async_context(
                setup.agent_context.mcp_tools(mcp_client, prefix="mcp_", workflows={"agent"}))
        if skill_roots:
            sources["skills"] = await stack.enter_async_context(
                setup.agent_context.skill_directories(skill_roots, workflows={"agent"}))
        yield sources


async def approve_current_batch(host, execution, wait_id, *, approved):
    """Read the refreshed Ask subset and echo its revision when replying."""
    batch = await host.agent_context.get_approval(execution, wait_id)
    reply = AgentApprovalResponse(revision=batch.revision,
        decisions=tuple(request.respond(approved=approved) for request in batch.requests))
    await host.respond(execution, wait_id, reply.model_dump(mode="json"))
