"""The general Agent Workflow with optional native human-question branches."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator

from autoagent.core import (
    AggregationContext, ConditionContext, ContextOperation, ContextPatch, Edge, InputMappingContext,
    Map, Node, Operator, OutputBindingContext, Recovery, ResourcePolicy, Stream,
    UserEventContext, UserEventSpec, Wait, Workflow, workflow_hook,
)
from autoagent.core.commands import SelectRequest
from autoagent.core.operators.context import OperatorCallContext

from ..context import current_agent_context, plain, restore
from ..integration import LLM_CALL
from ..models import LLMRequest, LLMResponse, LLMToolResult
from . import context as preparation
from .completion import complete_agent_round, finish_agent_round
from .configuration import AgentWorkflowExtensions, AgentWorkflowPolicies
from .models import (
    AgentApprovalRequest, AgentApprovalResponse, AgentInput, AgentResult, ApprovedTools,
    AgentWorkflowOptions, CompactionChunk, CompleteRoundInput, ContextPlan,
    ContextPreparation, Conversation, PreparedRequest, RoundResult,
    ToolBranchInput, ToolBranchResult, ToolTask,
)
from .tools import branch_tools, execute_tool, prepare_tool_wait, route_approved_tools
from .questions import (
    ASK_USER_RESOURCE, AskUserRequest, AskUserResponse, QuestionResults, question_request,
)
from .subagents import WaitPreparationBatch, WaitPreparation, CompletedToolWait, CompletedToolWaitBatch, collect_tool_wait, AgentSignalMessage
from .history import HistoryRoute, TokenAnchor, PendingToolRound
from .projection import PROJECTION_VERSION
from .provenance import restore_response
from .runtime_context import AgentLLMStreamReducer, TIME_KEY, append_time
from .policy import ContextPolicy, ContextLimitExceeded, UsagePlusCharacters, over_context_limit, always_compact
from .events import AgentEventMessage
from ._events import map_agent_event
from ._event_streams import (
    branch_stream, BranchReducer, execute_tool_stream, execute_retryable_tool_stream, ToolExecutionReducer,
    prepare_wait_stream, WaitPreparationReducer,
)


def agent_workflow(*, options: AgentWorkflowOptions | None = None,
                   extensions: AgentWorkflowExtensions | None = None,
                   policies: AgentWorkflowPolicies | None = None,
                   context_policy: ContextPolicy | None = None,
                   resources=None, tool_context=None, tool_context_provider=None,
                   **settings) -> Workflow:
    """Build an Agent using one explicit, caller-owned resource registry.

    Resource declarations belong to this Workflow Revision. MCP clients are
    entered/closed on the owning Runtime loop; the registry owner controls refresh.

    ContextPolicy controls estimation, branch selection and ordinary Operator
    compaction. Configure a window to enable the default threshold strategy.
    include_ask_user opts into native question Wait branches and a local builtin
    Tool, without registering it outside this Workflow.
    user_events opts into versioned Agent presentation events. Progress is
    transient; committed messages, results and resolved interactions persist.
    inject_current_time appends a hidden server-local clock snapshot before
    each model call. Earlier snapshots remain until Context compaction.
    tool_context supplies fixed Tool identity/attributes; tool_context_provider
    retrieves fresh facts at preflight, approval review and execution boundaries.
    Both are application-owned runtime objects, excluded from Workflow State.
    """
    if options is not None and settings:
        raise ValueError("Pass AgentWorkflowOptions or keyword options, not both.")
    if options is not None and not isinstance(options, AgentWorkflowOptions):
        raise TypeError("options must be AgentWorkflowOptions.")
    if extensions is not None and not isinstance(extensions, AgentWorkflowExtensions):
        raise TypeError("extensions must be AgentWorkflowExtensions.")
    if policies is not None and not isinstance(policies, AgentWorkflowPolicies):
        raise TypeError("policies must be AgentWorkflowPolicies.")
    if context_policy is not None and not isinstance(context_policy, ContextPolicy):
        raise TypeError("context_policy must be ContextPolicy.")
    context_policy = context_policy or ContextPolicy()
    options = restore(AgentWorkflowOptions, options or AgentWorkflowOptions(**settings))
    extensions = extensions or AgentWorkflowExtensions()
    policies = policies or AgentWorkflowPolicies()
    from .resources import AgentResources
    if resources is not None and not isinstance(resources, AgentResources):
        raise TypeError("resources must be AgentResources.")
    resources = resources if resources is not None else AgentResources()
    from ..tools import ToolContext, ToolContextProvider
    if tool_context is not None and tool_context_provider is not None:
        raise ValueError("Pass tool_context or tool_context_provider, not both.")
    if tool_context is not None and not isinstance(tool_context, ToolContext):
        raise TypeError("tool_context must be ToolContext.")
    if tool_context_provider is not None and (not isinstance(tool_context_provider, ToolContextProvider)
            or not callable(tool_context_provider.get_context)):
        raise TypeError("tool_context_provider must implement get_context.")
    context_source = tool_context if tool_context is not None else tool_context_provider
    identity = {"runtime_binding": 1, "history_commits": 1, "final_inbox_check": 1, "agent_result": 2, "builtin_tool_results": 2,
        "tool_routing": 3, "history_projection": PROJECTION_VERSION, "options": options.model_dump(mode="json"),
        "resources": resources.manifest, "wait_collection": 1, "user_event_contract": 1,
        "workspace_context": 1, "skill_tools": 3, "file_tools": 3, "execution_tools": 3,
        "builtin_tools": {"ask_user": ASK_USER_RESOURCE.version} if options.include_ask_user else {},
        "context_policy": context_policy.identity(), "extensions": {
        name: {"id": operator.id, "version": operator.version}
        for name in ("complete_round", "finish")
        if (operator := getattr(extensions, name)) is not None}}
    if context_source is not None:
        from .resources import callable_identity
        if tool_context is not None:
            from ..tools.context import _plain
            identity["tool_context"] = dict(user_id=tool_context.user_id,
                session_id=tool_context.session_id, attributes=_plain(tool_context.attributes),
                allowed_tools=sorted(tool_context.allowed_tools) if tool_context.allowed_tools is not None else None)
        else:
            identity["tool_context"] = dict(provider=callable_identity(tool_context_provider.get_context),
                version=getattr(tool_context_provider, "version", None))
    version = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:16]
    hook = workflow_hook(version=version)

    def state(context):
        return context.invocation_context.get("agent", {})

    def conversation(context):
        return restore(Conversation, context.session_context.get("agent_conversation", {}))

    async def start(value: AgentInput) -> AgentInput:
        return value

    from ..runtime.extension import AgentRuntimeExtension
    start.__runtime_extension__ = AgentRuntimeExtension
    start.__autoagent_options__ = options
    start.__autoagent_resources__ = resources
    start.__autoagent_context_policy__ = context_policy
    start.__autoagent_tool_context_source__ = context_source

    @hook
    async def start_binding(context: OutputBindingContext) -> ContextPatch:
        value = restore(AgentInput, context.output)
        previous = conversation(context)
        reconcile = getattr(current_agent_context(), "reconcile_history", None)
        if previous.pending is not None and reconcile is not None:
            previous = await reconcile(previous, context.self_handle.invocation_id)
        previous = previous.finish_pending(interrupted=True)
        updated = previous.append(preparation.message("user", value.message))
        binding = await current_agent_context().export_binding(context.self_handle.invocation_id)
        operations = [ContextOperation.set("agent", {"rounds": 0})]
        if binding is not None:
            operations.append(ContextOperation.set("agent_runtime_binding", binding))
        return ContextPatch(session=(ContextOperation.set("agent_conversation", updated.model_dump(mode="json")),),
            invocation=tuple(operations))

    @hook
    def context_input(context: InputMappingContext) -> Conversation:
        return conversation(context)

    async def prepare(value: Conversation) -> ContextPlan:
        value = value.finish_pending()
        from autoagent.core.commands import CommandInvoker, ReceiveSignal, ReceiveSignalRequest
        commands = CommandInvoker.current()
        own, _ = commands.identity()
        inbox_limit = commands.workflow(own.workflow_revision_id).signal_limits.max_messages
        endpoints = [options.user_signal_endpoint]
        if options.subagents.enabled:
            endpoints.append(options.subagents.signal_endpoint)
        received, messages = [], []
        for index, endpoint in enumerate(endpoints):
            batch = await commands.execute(ReceiveSignal(), ReceiveSignalRequest(endpoint=endpoint,
                limit=inbox_limit), key="inbox:" + str(index))
            received.extend(batch.messages)
        for incoming in sorted(received, key=lambda item: item.accepted_sequence):
            payload = restore(AgentSignalMessage, incoming.payload)
            if options.user_events:
                messages.append(AgentEventMessage(message_id=incoming.message_id,
                    source="agent" if incoming.sender_id is not None else "user", sender_id=incoming.sender_id,
                    content=({"type": "text", "text": payload.message},),
                    reply_requested=payload.reply_requested, reply_to=payload.reply_to))
            header = {"message_id": incoming.message_id,
                "from": incoming.sender_id if incoming.sender_id is not None else "user",
                "reply_requested": payload.reply_requested, "reply_to": payload.reply_to,
                "wait_timeout_seconds": payload.wait_timeout_seconds}
            value = value.append(preparation.message("user", "Incoming message "
                + json.dumps(header, ensure_ascii=False) + "\n" + payload.message))
        plan = await preparation.prepare_agent_context(value, options, context_policy)
        return plan.model_copy(update={"received_messages": tuple(messages)})

    def bind_request(request, snapshot, overrides, *, role, prepared_snapshot_id):
        settings = preparation.request_settings(snapshot, overrides, role=role, require_model=False)
        values = request.model_dump(mode="json")
        # Keep the immutable execution model while rebinding routing for each call.
        if prepared_snapshot_id != snapshot.snapshot_id:
            values.update(settings)
        values["routing"] = {**values["routing"], **settings["routing"]}
        return restore(LLMRequest, values)

    @hook
    def prepared_binding(context: OutputBindingContext) -> ContextPatch:
        plan = restore(ContextPlan, context.output)
        changed = plan.preparation.conversation != conversation(context)
        operations = [ContextOperation.set("agent.branch", None)]
        if options.user_events:
            operations.append(ContextOperation.set("agent.compaction", {
                "id": OperatorCallContext.current().occurrence_id, "before_tokens": plan.estimate.input_tokens,
                "generation": plan.base_generation, "requested": plan.needs_compaction}))
        return ContextPatch(invocation=tuple(operations), session=(
            ContextOperation.set("agent_conversation", plan.preparation.conversation.model_dump(mode="json")),) if changed else ())

    @hook
    def compression_input(context: InputMappingContext) -> ContextPlan:
        return restore(ContextPlan, next(iter(context.incoming.values())))

    async def compact(value: ContextPlan) -> AsyncIterator[CompactionChunk]:
        yield CompactionChunk(type="started")
        result = await preparation.compact_agent_context(value, context_policy)
        yield CompactionChunk(type="completed", plan=result)

    @hook
    def build_input(context: InputMappingContext) -> ContextPlan:
        return restore(ContextPlan, next(iter(context.incoming.values())))

    async def build(value: ContextPlan) -> PreparedRequest:
        return await preparation.build_agent_request(value, context_policy)

    @hook
    async def build_binding(context: OutputBindingContext) -> ContextPatch:
        value = restore(PreparedRequest, context.output)
        previous = conversation(context)
        if previous.view.generation != value.base_generation or len(previous.items) != value.source_count:
            raise ValueError("Prepared Context no longer matches the Session history.")
        updated = Conversation(items=previous.items, view=value.view, origins=previous.origins,
            token_anchor=previous.token_anchor if previous.view == value.view else None,
            runtime_times=previous.runtime_times if previous.view == value.view else {})
        return ContextPatch(invocation=(ContextOperation.set("agent.prepared", value.model_dump(mode="json")),
            ContextOperation.set("agent.request_snapshot_id", value.request.routing["agent_snapshot_id"])),
            session=(ContextOperation.set("agent_conversation", updated.model_dump(mode="json")),))

    @hook
    async def llm_input(context: InputMappingContext) -> LLMRequest:
        if options.max_rounds is not None and state(context).get("rounds", 0) >= options.max_rounds:
            raise ValueError("Agent round limit reached before the next model call.")
        prepared = restore(PreparedRequest, next(iter(context.incoming.values())))
        # A clean pause can occur after Build Request. Rebind only per-call
        # settings here; history, compression and capability preparation remain
        # explicit Operators. The advertised binding remains the prepared one.
        snapshot = await current_agent_context().execution_snapshot(context.self_handle.invocation_id)
        request = bind_request(prepared.request, snapshot, options.llm_options, role="agent",
            prepared_snapshot_id=state(context)["request_snapshot_id"])
        if prepared.runtime_time is not None:
            request = request.model_copy(update={"input": request.input[:-1]})
        if state(context)["request_snapshot_id"] != snapshot.snapshot_id:
            # Re-render the frozen sources from original history. The old
            # request may have deliberately omitted another model's replay data.
            target = current_agent_context().history_route(snapshot, request.model)
            evaluation = ContextPreparation(conversation=conversation(context), fixed_input=prepared.fixed_input, tools=request.tools,
                settings=request.model_dump(mode="json", exclude={"input", "tools"}),
                fingerprint="resumed-request", snapshot_id=snapshot.snapshot_id, replay_target=target,
                inject_current_time=options.inject_current_time)
            request = request.model_copy(update={"input": evaluation.model_input})
            estimate = await context_policy.estimate_input(evaluation)
            limit = context_policy.budget(evaluation.settings).input_limit
            if limit is not None and estimate.input_tokens > limit:
                raise ContextLimitExceeded("Resumed request exceeds the configured Context window.")
        if options.inject_current_time:
            target = current_agent_context().history_route(snapshot, request.model)
            request = append_time(request, target)
        return request

    @hook
    async def llm_binding(context: OutputBindingContext) -> ContextPatch:
        response, route, response_snapshot_id = restore_response(restore(LLMResponse, context.output))
        metadata = dict(response.provider_metadata)
        runtime_time = metadata.pop(TIME_KEY, None)
        response = response.model_copy(update={"provider_metadata": metadata})
        prepared = restore(PreparedRequest, state(context)["prepared"])
        snapshot = await current_agent_context().execution_snapshot(context.self_handle.invocation_id)
        anchor = None
        if (response.usage is not None and response.usage.input_tokens is not None and prepared.request.model is not None
                and state(context)["request_snapshot_id"] == snapshot.snapshot_id
                and response_snapshot_id in (None, snapshot.snapshot_id)):
            anchor = TokenAnchor(fingerprint=prepared.fingerprint, input_tokens=response.usage.input_tokens,
                source_count=prepared.source_count, view_generation=prepared.view.generation).model_dump(mode="json")
        previous = conversation(context)
        if runtime_time is not None:
            previous = previous.model_copy(update={"runtime_times": {
                **previous.runtime_times, str(len(previous.items)): runtime_time,
            }})
        updated = previous.append(*response.output, origin=route)
        updated = Conversation(items=updated.items, view=updated.view, origins=updated.origins,
            pending=PendingToolRound(start=len(previous.items)) if response.tool_calls else None,
            token_anchor=restore(TokenAnchor, anchor) if anchor is not None else None,
            runtime_times=updated.runtime_times)
        operations = [ContextOperation.set("agent.response", response.model_dump(mode="json"))]
        if options.user_events:
            operations.append(ContextOperation.set("agent.response_id", OperatorCallContext.current().occurrence_id))
        return ContextPatch(session=(ContextOperation.set("agent_conversation", updated.model_dump(mode="json")),),
            invocation=tuple(operations))

    @hook
    def branch_input(context: InputMappingContext) -> ToolBranchInput:
        if options.max_rounds is not None and state(context).get("rounds", 0) + 1 >= options.max_rounds:
            raise ValueError("Agent round limit reached before dispatching Tools that cannot be consumed.")
        return ToolBranchInput(response=restore(LLMResponse, state(context)["response"]),
            prepared=restore(PreparedRequest, state(context)["prepared"]))

    async def branch(value: ToolBranchInput) -> ToolBranchResult:
        return await branch_tools(value)

    def result_operations(context, results):
        history = conversation(context)
        if history.pending is None:
            raise ValueError("Tool results require a recorded LLM Tool round.")
        expected = {call.call_id for call in restore(LLMResponse, state(context)["response"]).tool_calls}
        operations, seen = [], set()
        for result in results:
            if result.call_id not in expected or result.call_id in seen or result.call_id in history.pending.results:
                raise ValueError("Every Tool result must be recorded exactly once for the current round.")
            seen.add(result.call_id)
            operations.append(ContextOperation.set(("agent_conversation", "pending", "results", result.call_id),
                result.model_dump(mode="json")))
        return tuple(operations)

    @hook
    def results_binding(context: OutputBindingContext) -> ContextPatch:
        return ContextPatch(session=result_operations(context, (restore(LLMToolResult, item) for item in context.output)))

    @hook
    def questions_binding(context: OutputBindingContext) -> ContextPatch:
        return ContextPatch(session=result_operations(context, restore(QuestionResults, context.output).results))

    @hook
    def branch_binding(context: OutputBindingContext) -> ContextPatch:
        result = restore(ToolBranchResult, context.output)
        return ContextPatch(session=result_operations(context, result.immediate),
            invocation=(ContextOperation.set("agent.branch", plain(context.output)),))

    @hook
    def allow_input(context: InputMappingContext) -> list[ToolTask]:
        result = restore(ToolBranchResult, next(iter(context.incoming.values())))
        return [item.model_copy(update={"observed_versions": dict(state(context).get("observed_versions", {}))}) for item in result.allow]

    @hook
    async def approval_input(context: InputMappingContext) -> AgentApprovalRequest:
        result = restore(ToolBranchResult, next(iter(context.incoming.values())))
        snapshot = await current_agent_context().execution_snapshot(context.self_handle.invocation_id)
        return AgentApprovalRequest(request_id=result.ask[0].request_id, invocation_id=snapshot.invocation_id,
            snapshot_id=snapshot.snapshot_id,
            requests=tuple(item.approval_request for item in result.ask),
            bindings=tuple(item.binding for item in result.ask))

    @hook
    def approved_input(context: InputMappingContext) -> ApprovedTools:
        batch = restore(ToolBranchResult, state(context)["branch"])
        reply = restore(AgentApprovalResponse, next(iter(context.incoming.values())))
        expected = {item.approval_request.id for item in batch.ask}
        received = reply.resolutions or reply.decisions
        if len(received) != len(expected) or {value.request_id for value in received} != expected:
            raise ValueError("Approval replies must match the complete pending batch.")
        tasks = [item.model_copy(update={"observed_versions": dict(state(context).get("observed_versions", {}))}) for item in batch.ask]
        if reply.resolutions:
            resolutions = {value.request_id: value for value in reply.resolutions}
            return ApprovedTools(tasks=tuple(item.model_copy(update={
                "approval": resolutions[item.approval_request.id].approval,
                "observation": resolutions[item.approval_request.id].result,
            }) for item in tasks))
        approvals = {value.request_id: value for value in reply.decisions}
        return ApprovedTools(tasks=tuple(item.model_copy(update={
            "approval": approvals[item.approval_request.id],
        }) for item in tasks))

    @hook
    def complete_input(context: InputMappingContext) -> CompleteRoundInput:
        results = []
        batch = state(context).get("branch")
        if batch is not None:
            results.extend(restore(ToolBranchResult, batch).immediate)
        for edge in ("allow_complete", "approved_complete", "approved_rejected",
                     "questions_complete", "questions_complete_approved",
                     "wait_preparation_complete", "wait_preparation_complete_approved", "wait_complete", "wait_complete_approved"):
            incoming = context.incoming.get(edge, ())
            if edge == "approved_rejected":
                incoming = incoming.get("immediate", ()) if incoming else ()
            elif edge.startswith("wait_preparation_complete") and incoming:
                incoming = restore(WaitPreparationBatch, incoming).results
            elif edge.startswith(("questions_complete", "wait_complete")) and incoming:
                incoming = restore(QuestionResults, incoming).results
            for item in incoming:
                results.append(restore(LLMToolResult, item))
        return CompleteRoundInput(response=restore(LLMResponse, state(context)["response"]),
            results=tuple(results), previous_rounds=state(context).get("rounds", 0))

    async def complete(value: CompleteRoundInput) -> RoundResult:
        result = complete_agent_round(value)
        if not result.continue_loop:
            from autoagent.core.commands import CommandInvoker, HasSignal, HasSignalRequest
            endpoints = [options.user_signal_endpoint]
            if options.subagents.enabled:
                endpoints.append(options.subagents.signal_endpoint)
            pending = await CommandInvoker.current().execute(HasSignal(),
                HasSignalRequest(endpoints=endpoints), key="final-inbox")
            if pending.has_messages:
                result = result.model_copy(update={"continue_loop": True})
        return result

    @hook
    def complete_binding(context: OutputBindingContext) -> ContextPatch:
        result = restore(RoundResult, context.output)
        expected_ids = [call.call_id for call in result.response.tool_calls]
        observed_ids = [item.call_id for item in result.results]
        if (result.response != restore(LLMResponse, state(context)["response"])
                or result.rounds != state(context).get("rounds", 0) + 1
                or len(expected_ids) != len(set(expected_ids)) or observed_ids != expected_ids):
            raise ValueError("Round completion must preserve the response, ordered Tool results and round counter.")
        if not result.continue_loop and not result.response.tool_calls:
            complete_agent_round(CompleteRoundInput(response=result.response, results=result.results,
                previous_rounds=state(context).get("rounds", 0)))
        history = conversation(context)
        recorded = history.pending.results if history.pending is not None else {}
        if set(recorded) != set(expected_ids) or any(recorded[item.call_id] != item for item in result.results):
            raise ValueError("Round completion requires every Tool result to be committed by its producing Node.")
        versions = dict(state(context).get("observed_versions", {}))
        for item in result.results:
            versions.update(item.tool_metadata.get("agent_observed_versions", {}))
        return ContextPatch(invocation=(ContextOperation.set("agent.rounds", result.rounds),
                        ContextOperation.set("agent.observed_versions", versions)))

    async def finish(value: RoundResult) -> AgentResult:
        return finish_agent_round(value)

    @hook
    def finish_input(context: InputMappingContext) -> RoundResult:
        return restore(RoundResult, next(iter(context.incoming.values())))

    def test_field(field, positive=True):
        @hook
        def condition(context: ConditionContext) -> bool:
            return bool(context.output[field]) is positive
        return condition

    @hook
    def agent_events(context: UserEventContext) -> UserEventSpec | None:
        return map_agent_event(context)

    @hook
    def questions_input(context: InputMappingContext) -> list[AskUserRequest]:
        batch = restore(ToolBranchResult, next(iter(context.incoming.values())))
        return [question_request(task) for task in batch.questions]

    @hook
    def question_results(context: AggregationContext) -> QuestionResults:
        return QuestionResults(results=tuple(restore(AskUserResponse, response).tool_result(restore(AskUserRequest, request))
            for request, response in zip(context.inputs, context.outputs, strict=True)))

    @hook
    def wait_preparation_input(context: InputMappingContext) -> list[ToolTask]:
        batch = restore(ToolBranchResult, next(iter(context.incoming.values())))
        return [item.model_copy(update={"observed_versions": dict(state(context).get("observed_versions", {}))})
            for item in batch.waits]

    @hook
    def wait_preparation_results(context: AggregationContext) -> WaitPreparationBatch:
        outputs = [restore(WaitPreparation, output) for output in context.outputs]
        return WaitPreparationBatch(results=tuple(output.result for output in outputs if output.result is not None),
            waits=tuple(output.wait for output in outputs if output.wait is not None))

    @hook
    def waits_input(context: InputMappingContext) -> list["SelectRequest"]:
        return [item.request for item in restore(WaitPreparationBatch, next(iter(context.incoming.values()))).waits]

    @hook
    def collect_wait_input(context: InputMappingContext) -> list[CompletedToolWait]:
        return list(restore(CompletedToolWaitBatch, next(iter(context.incoming.values()))).waits)

    def wait_hooks(key):
        @hook
        def prepared(context: OutputBindingContext) -> ContextPatch:
            batch = restore(WaitPreparationBatch, context.output)
            return ContextPatch(session=result_operations(context, batch.results),
                invocation=(ContextOperation.set(("agent", key), plain(context.output)),))

        @hook
        def selected(context: AggregationContext) -> CompletedToolWaitBatch:
            from autoagent.core.commands import SelectResult
            batch = restore(WaitPreparationBatch, state(context)[key])
            return CompletedToolWaitBatch(waits=tuple(
                CompletedToolWait(wait=wait, selected_case=restore(SelectResult, output).case)
                for wait, output in zip(batch.waits, context.outputs, strict=True)))

        @hook
        def collected(context: AggregationContext) -> QuestionResults:
            batch = restore(WaitPreparationBatch, state(context)[key])
            return QuestionResults(results=(*batch.results, *(restore(LLMToolResult, value) for value in context.outputs)))

        @hook
        def bound(context: OutputBindingContext) -> ContextPatch:
            batch = restore(WaitPreparationBatch, state(context)[key])
            completed_ids = {item.call_id for item in batch.results}
            results = restore(QuestionResults, context.output).results
            return ContextPatch(session=result_operations(context, (item for item in results if item.call_id not in completed_ids)))

        return prepared, selected, collected, bound

    @hook
    def approved_routing_binding(context: OutputBindingContext) -> ContextPatch:
        result = restore(ToolBranchResult, context.output)
        return ContextPatch(session=result_operations(context, result.immediate))

    safe = Recovery(mode="replay_safe")
    def node(identity, handler, **kwargs):
        selected = getattr(extensions, identity, None)
        return Node(identity, selected or Operator(handler, id=options.workflow_id + "." + identity, version=version),
                    recovery_mode=Recovery() if selected is not None else safe, **kwargs)
    dispatch = Operator(execute_tool_stream if options.user_events else execute_tool,
        id=options.workflow_id + ".tool.dispatch", version="6" if options.user_events else "5")
    dispatch_stream = Stream(ToolExecutionReducer()) if options.user_events else None
    nodes = [
        node("start", start, output_binding=start_binding),
        node("prepare_context", prepare, input_mapping=context_input, output_binding=prepared_binding),
        Node("compact_context", Operator(compact, id=options.workflow_id + ".compact_context", version=version),
             stream=Stream(preparation.CompactionReducer()), input_mapping=compression_input),
        node("build_request", build, input_mapping=build_input, output_binding=build_binding),
        Node("llm_call", LLM_CALL, input_mapping=llm_input, output_binding=llm_binding,
             stream=Stream(AgentLLMStreamReducer()),
             resource=ResourcePolicy(max_executions_per_invocation=options.max_rounds)),
        node("tool_branch", branch_stream if options.user_events else branch,
             stream=Stream(BranchReducer()) if options.user_events else None,
             input_mapping=branch_input, output_binding=branch_binding),
        Node("tool_execute", dispatch, input_mapping=allow_input, output_binding=results_binding,
             map=Map(max_parallelism=options.max_parallelism), stream=dispatch_stream),
        Node("wait_approval", Wait(AgentApprovalRequest, AgentApprovalResponse), input_mapping=approval_input),
        node("route_approved", route_approved_tools, input_mapping=approved_input, output_binding=approved_routing_binding),
        Node("tool_execute_approved", dispatch, input_mapping=allow_input, output_binding=results_binding,
             map=Map(max_parallelism=options.max_parallelism), stream=dispatch_stream),
        node("complete_round", complete, input_mapping=complete_input, output_binding=complete_binding),
        node("finish", finish, input_mapping=finish_input),
    ]
    edges = [
        Edge("start", "prepare_context", id="start_prepare"),
        Edge("prepare_context", "compact_context", test_field("needs_compaction"), id="prepare_compact"),
        Edge("prepare_context", "build_request", test_field("needs_compaction", False), id="prepare_build"),
        Edge("compact_context", "build_request", id="compact_build"),
        Edge("build_request", "llm_call", id="build_llm"),
        Edge("llm_call", "tool_branch", _tool_calls_condition(hook, True), id="llm_tools"),
        Edge("llm_call", "complete_round", _tool_calls_condition(hook, False), id="llm_complete"),
        Edge("tool_branch", "tool_execute", test_field("allow"), id="branch_allow"),
        Edge("tool_branch", "wait_approval", test_field("ask"), id="branch_ask"),
        Edge("tool_branch", "complete_round", test_field("immediate"), id="branch_complete"),
        Edge("tool_execute", "complete_round", id="allow_complete"),
        Edge("wait_approval", "route_approved", id="approval_route"),
        Edge("route_approved", "tool_execute_approved", test_field("allow"), id="approval_execute"),
        Edge("route_approved", "complete_round", test_field("immediate"), id="approved_rejected"),
        Edge("tool_execute_approved", "complete_round", id="approved_complete"),
        Edge("complete_round", "prepare_context", test_field("continue_loop"), id="round_back"),
        Edge("complete_round", "finish", test_field("continue_loop", False), id="round_finish"),
    ]
    # Keep native waits independent when one batch contains approved and direct calls.
    for source, suffix in (("tool_branch", ""), ("route_approved", "_approved")):
        if options.include_ask_user:
            questions = "ask_user" + suffix
            nodes.append(Node(questions, Wait(AskUserRequest, AskUserResponse), input_mapping=questions_input,
                output_binding=questions_binding, map=Map(max_parallelism=options.max_parallelism, aggregate=question_results)))
            edges.extend([
                Edge(source, questions, test_field("questions"), id="branch_questions" + suffix),
                Edge(questions, "complete_round", id="questions_complete" + suffix),
            ])
        if options.subagents.enabled:
            from autoagent.core.commands import Select
            prepare_wait, wait_tools, collect = ("prepare_wait" + suffix, "wait_tools" + suffix, "collect_wait_results" + suffix)
            prepared, selected, collected, bound = wait_hooks("prepared_waits" + suffix)
            nodes.extend([
                Node(prepare_wait, Operator(prepare_wait_stream if options.user_events else prepare_tool_wait,
                    id=options.workflow_id + "." + prepare_wait, version="4" if options.user_events else "3"),
                    input_mapping=wait_preparation_input, output_binding=prepared,
                    map=Map(max_parallelism=options.max_parallelism, aggregate=wait_preparation_results),
                    stream=Stream(WaitPreparationReducer()) if options.user_events else None),
                Node(wait_tools, Select(), input_mapping=waits_input,
                    map=Map(max_parallelism=options.max_parallelism, aggregate=selected)),
                node(collect, collect_tool_wait, input_mapping=collect_wait_input,
                    output_binding=bound, map=Map(max_parallelism=options.max_parallelism, aggregate=collected)),
            ])
            edges.extend([
                Edge(source, prepare_wait, test_field("waits"), id="branch_wait" + suffix),
                Edge(prepare_wait, wait_tools, test_field("waits"), id="prepare_wait_enter" + suffix),
                Edge(prepare_wait, "complete_round", test_field("waits", False), id="wait_preparation_complete" + suffix),
                Edge(wait_tools, collect, id="wait_collect" + suffix),
                Edge(collect, "complete_round", id="wait_complete" + suffix),
            ])
    if not (isinstance(context_policy.estimate, UsagePlusCharacters)
            and context_policy.trigger in (over_context_limit, always_compact)):
        nodes[1].recovery_mode = Recovery()
    unavailable = set(policies.nodes) - {selected.id for selected in nodes}
    if unavailable:
        raise ValueError(f"Agent Node policies target disabled Nodes: {sorted(unavailable)!r}.")
    for selected in nodes:
        if options.user_events:
            selected.user_events = agent_events
        node_policy = policies.nodes.get(selected.id)
        if node_policy is None and selected.id in {
            "ask_user_approved", "prepare_wait_approved", "wait_tools_approved", "collect_wait_results_approved",
        }:
            node_policy = policies.nodes.get(selected.id.removesuffix("_approved"))
        if node_policy is not None:
            node_policy.apply(selected)
        if options.user_events and selected.id in {"tool_execute", "tool_execute_approved"} and selected.retry is not None:
            selected.executable = Operator(execute_retryable_tool_stream,
                id=options.workflow_id + ".tool.dispatch.retryable", version="1")
    from autoagent.core import SignalEndpoint
    endpoints = [SignalEndpoint(options.user_signal_endpoint, AgentSignalMessage)]
    if options.subagents.enabled:
        endpoints.append(SignalEndpoint(options.subagents.signal_endpoint, AgentSignalMessage))
    return Workflow(options.workflow_id, nodes=nodes, edges=edges, version=version,
        resource=policies.resource, signal_endpoints=endpoints)


def _tool_calls_condition(hook, positive):
    @hook
    def condition(context: ConditionContext) -> bool:
        return bool(restore(LLMResponse, context.output).tool_calls) is positive
    return condition
