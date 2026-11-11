"""Adapt Agent collaboration intent to durable Core Commands and waits."""

from autoagent.agent.tools import ToolBusinessError
from autoagent.agent.workflow.models import AgentInput
from autoagent.agent.workflow.subagents import AgentSignalMessage, project_observation
from autoagent.core.commands import (
    CommandInvoker, Spawn, Status, Cancel, CancelRequest, SendSignal, SendSignalRequest,
    SelectRequest, ChildRequestCase, SignalRequestCase, TimerRequestCase,
)


class AgentCollaboration:
    def __init__(self, service):
        self.service = service

    async def _context(self):
        commands = CommandInvoker.current()
        snapshot = await self.service.snapshot()
        options = self.service.workflow_options(snapshot)
        if not options.subagents.enabled:
            raise ToolBusinessError("Agent collaboration is disabled.", code="collaboration_disabled")
        return commands, snapshot, options

    @staticmethod
    def _neighbors(commands, config):
        return {handle.invocation_id: (relation, handle) for relation, handle in commands.visible_handles(
            include_parent=config.include_parent, include_siblings=config.include_siblings)}

    @staticmethod
    def _select_ids(visible, agent_ids):
        if agent_ids is None:
            return list(visible)
        if len(agent_ids) != len(set(agent_ids)):
            raise ToolBusinessError("Agent IDs must be unique.", code="invalid_arguments")
        if set(agent_ids) - set(visible):
            raise ToolBusinessError("Agent is unknown or outside the visible relationship scope.", code="agent_not_visible")
        return agent_ids

    @staticmethod
    def observation(raw, relation):
        return project_observation(raw, relation)

    @staticmethod
    def _duration(config, seconds):
        try:
            return config.wait_seconds(seconds)
        except ValueError as error:
            raise ToolBusinessError(str(error), code="invalid_arguments") from error

    async def get(self, agent_ids=None):
        commands, _, options = await self._context()
        visible = self._neighbors(commands, options.subagents)
        selected = self._select_ids(visible, agent_ids)
        agents, versions = [], {}
        for index, agent_id in enumerate(selected):
            relation, handle = visible[agent_id]
            raw = await commands.execute(Status(), handle, key="status:" + str(index))
            raw = raw.target_only()
            agents.append(self.observation(raw, relation))
            versions[agent_id] = raw.version
        return {"self_id": commands.identity()[0].invocation_id, "agents": agents,
            "_agent_versions": versions}

    async def _wait_request(self, seconds, agent_ids, wake_on_message):
        commands, _, options = await self._context()
        duration = self._duration(options.subagents, seconds)
        visible = self._neighbors(commands, options.subagents)
        selected = self._select_ids(visible, agent_ids or [])
        cases = {}
        # Message readiness has priority over already-known Agent state.
        if wake_on_message:
            for index, endpoint in enumerate((options.user_signal_endpoint, options.subagents.signal_endpoint)):
                cases["message:" + str(index)] = SignalRequestCase(endpoint=endpoint, consume=False)
        # The ordinary Dispatcher passes the last acknowledged observations;
        # models never need to copy Runtime version tokens.
        from autoagent.agent.tools import ToolContext
        versions = ToolContext.current().services.get("agent_observations", {})
        for index, agent_id in enumerate(selected):
            _, handle = visible[agent_id]
            raw = await commands.execute(Status(), handle, key="wait-status:" + str(index))
            raw = raw.target_only()
            after = versions.get(agent_id) if raw.status not in {"completed", "failed", "cancelled"} else None
            cases["agent:" + agent_id] = ChildRequestCase(handle=handle, after=after, scope="graph")
        cases["timer"] = TimerRequestCase(deadline_at_us=commands.clock_us() + int(duration * 1_000_000))
        return SelectRequest(cases=cases), tuple(selected), {agent_id: visible[agent_id][0] for agent_id in selected}

    async def wait(self, seconds, agent_ids=None, wake_on_message=True):
        request, selected, relations = await self._wait_request(seconds, agent_ids, wake_on_message)
        return {"_agent_wait": {"request": request.model_dump(mode="json"), "receipt": {},
            "agent_ids": list(selected), "relations": relations}}

    async def spawn(self, type, task, *, wait, seconds=None):
        commands, snapshot, options = await self._context()
        target = options.subagents.targets.get(type)
        if target is None:
            raise ToolBusinessError("Unknown Agent type; choose an advertised type.", code="unknown_agent_type")
        if wait:
            self._duration(options.subagents, seconds)
        workflow = commands.workflow(snapshot.bindings["target:" + type])
        if workflow.workflow_id != target.workflow_id:
            raise ToolBusinessError("Agent target Revision belongs to another Workflow.", code="agent_target_changed")
        # Fixed task input keeps the LLM-facing contract portable. The target
        # Workflow's entry contract still performs its own strict validation.
        # Child execution inherits the bound model and Workspace, not the spawning Tool's
        # temporary dependencies, approval identity or nested restrictions.
        from autoagent.agent.tools.context import tool_context
        with tool_context(None):
            handle = await commands.execute(Spawn(workflow.workflow_revision_id, target.entry_node_id),
                AgentInput(message=task, model={"provider": snapshot.settings.model.provider, "model": snapshot.settings.model.model}), key="spawn")
        raw = await commands.execute(Status(), handle, key="spawn-status")
        raw = raw.target_only()
        receipt = self.observation(raw, "child")
        if wait:
            request, selected, relations = await self._wait_request(seconds, [handle.invocation_id], True)
            return {"_agent_wait": {"request": request.model_dump(mode="json"),
                "receipt": {"agent_id": handle.invocation_id}, "agent_ids": list(selected), "relations": relations}}
        return {**receipt, "_agent_versions": {handle.invocation_id: raw.version}}

    async def send(self, target, message, reply_requested, reply_to, seconds):
        commands, _, options = await self._context()
        visible = self._neighbors(commands, options.subagents)
        self._select_ids(visible, [target])
        if seconds is not None:
            self._duration(options.subagents, seconds)
        _, handle = visible[target]
        target_workflow = commands.workflow(handle.workflow_revision_id)
        target_options = self.service.options_for_workflow(target_workflow)
        if target_options is None or not target_options.subagents.enabled:
            raise ToolBusinessError("Target does not expose Agent messaging.", code="agent_messaging_unavailable")
        payload = AgentSignalMessage(message=message, reply_requested=reply_requested,
            reply_to=reply_to, wait_timeout_seconds=seconds)
        receipt = await commands.execute(SendSignal(), SendSignalRequest(handle=handle,
            endpoint=target_options.subagents.signal_endpoint, payload=payload.model_dump(mode="json")), key="send")
        result = {"message_id": receipt.message_id, "accepted": True, "wake_reason": None}
        if seconds is not None:
            request, selected, relations = await self._wait_request(seconds, [], True)
            return {"_agent_wait": {"request": request.model_dump(mode="json"), "receipt": result, "agent_ids": []}}
        return result

    async def cancel(self, agent_id, reason=None):
        commands, _, options = await self._context()
        visible = self._neighbors(commands, options.subagents)
        self._select_ids(visible, [agent_id])
        relation, handle = visible[agent_id]
        if relation != "child":
            raise ToolBusinessError("Cancellation requires a directly owned Agent.", code="agent_control_denied")
        receipt = await commands.execute(Cancel(), CancelRequest(handle=handle, reason=reason), key="cancel")
        return {"agent_id": agent_id, "accepted": True, "disposition": receipt.disposition}
