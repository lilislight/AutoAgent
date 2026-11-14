"""Frozen request sources, policy evaluation, and fixed request assembly."""

import hashlib
import json

from autoagent.core import StreamContext

from ..context import current_agent_context, restore
from ..models import LLMMessage, LLMRequest, TextContent
from .compaction import validate_tool_history
from .history import Conversation, ContextView
from .projection import PROJECTION_VERSION
from .models import (
    AgentWorkflowOptions, CompactionChunk, CompactionState, ContextPlan,
    ContextPreparation, PreparedRequest, ToolBinding,
)
from .policy import (
    CompactionInput, CompactionResult, ContextEvaluation, ContextLimitExceeded,
    ContextPolicy, call_policy, over_context_limit, always_compact,
)


def definition_hash(definition):
    return hashlib.sha256(json.dumps(definition.model_dump(mode="json"), sort_keys=True,
        ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def request_settings(snapshot, overrides, *, role, require_model=True):
    settings = snapshot.settings.model
    values = dict(model=settings.model, streaming=settings.streaming,
        max_output_tokens=settings.max_output_tokens, temperature=settings.temperature, reasoning_effort=settings.reasoning_effort)
    if role == "compaction":
        values["reasoning_effort"] = None
    allowed = {"model", "streaming", "max_output_tokens", "temperature", "top_p", "output_options", "provider_options", "reasoning_effort"}
    if set(overrides) - allowed:
        raise ValueError("Agent LLM options can only override generation settings.")
    values.update(overrides)
    if require_model and not values["model"] and snapshot.bindings.get("provider") != "@capability":
        raise ValueError("Configure an Agent model before starting generation.")
    values["routing"] = dict(provider=settings.provider, agent_snapshot_id=snapshot.snapshot_id, agent_role=role,
        agent_managed_provider=snapshot.bindings.get("provider") != "@capability", reasoning_effort_bound=True)
    return values


def message(role, text):
    return LLMMessage(role=role, content=(TextContent(text=text),))


def request_fingerprint(fixed_input, tools, settings, snapshot, replay_target, *, inject_current_time=False):
    fields = {key: settings.get(key) for key in ("model", "provider_options", "output_options", "reasoning_effort")}
    value = dict(projection_version=PROJECTION_VERSION, fixed=[item.model_dump(mode="json") for item in fixed_input],
        tools=[item.model_dump(mode="json") for item in tools], settings=fields,
        provider=snapshot.settings.model.provider, provider_version=snapshot.bindings.get("provider"),
        replay_target=replay_target.model_dump(mode="json") if replay_target else None)
    value["inject_current_time"] = inject_current_time
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


async def prepare_agent_context(conversation: Conversation, options: AgentWorkflowOptions,
                                policy: ContextPolicy) -> ContextPlan:
    """Read live sources once, then estimate and choose one mutually exclusive branch."""
    from ..skills import SkillSet
    service = current_agent_context()
    snapshot = await service.snapshot()
    workspace = await service.prepare_workspace(snapshot)
    from .builtins import workflow_catalog
    from .subagents import BUILTIN_NAMES
    catalog, skills = workflow_catalog(service, snapshot, options)
    toolset = service.toolset(snapshot, catalog=catalog)
    from ..tools.context import tool_context
    with tool_context(toolset.context):
        advertised = toolset.definitions
    builtins = BUILTIN_NAMES & catalog.keys() if options.subagents.enabled else frozenset()
    definitions = tuple(item for item in advertised
        if item.name != "ask_user" and item.name not in builtins
        and snapshot.settings.access.rules.get(item.name, snapshot.settings.access.default) != "deny")
    definitions = (*definitions, *(entry.value.definition for name, entry in catalog.items() if name in builtins))
    if options.include_ask_user and "ask_user" in catalog:
        definitions = (*definitions, catalog["ask_user"].value.definition)
    elif "ask_user" in catalog:
        definitions = (*definitions, *(item for item in advertised if item.name == "ask_user"
            and snapshot.settings.access.rules.get(item.name, snapshot.settings.access.default) != "deny"))
    bindings = tuple(ToolBinding(name=item.name, resource_id=catalog[item.name].resource_id, version=catalog[item.name].version,
        definition_hash=definition_hash(item),
        kind=("ask_user" if options.include_ask_user and item.name == "ask_user" else
            "agent_builtin" if options.subagents.enabled and item.name in BUILTIN_NAMES else "tool")) for item in definitions)
    fixed = []
    if options.instructions.strip():
        fixed.append(message("system", options.instructions))
    if workspace is not None:
        fixed.append(message("user", json.dumps({"workspace": workspace.workspace.model_dump(mode="json"),
            "working_directory": workspace.working_directory,
            "instructions": [item.model_dump(mode="json") for item in workspace.instructions]}, ensure_ascii=False)))
    skill_catalog = SkillSet(skills).catalog_message()
    if skill_catalog is not None:
        fixed.append(skill_catalog)
    settings = request_settings(snapshot, options.llm_options, role="agent", require_model=False)
    target = service.history_route(snapshot, settings.get("model"))
    preparation = ContextPreparation(conversation=conversation, fixed_input=tuple(fixed), tools=definitions,
        bindings=bindings, settings=settings, generation_options=options.llm_options, snapshot_id=snapshot.snapshot_id,
        compaction_model=getattr(service, "compaction_model", None), replay_target=target,
        compaction_provider=getattr(service, "compaction_provider", None),
        compaction_reasoning_effort=getattr(service, "compaction_reasoning_effort", None),
        inject_current_time=options.inject_current_time,
        fingerprint=request_fingerprint(fixed, definitions, settings, snapshot, target,
            inject_current_time=options.inject_current_time))
    validate_tool_history(conversation.visible)
    estimate = await policy.estimate_input(preparation)
    budget = policy.budget(settings)
    evaluation = ContextEvaluation(preparation, estimate, budget)
    decision = policy.trigger(evaluation) if policy.trigger in (over_context_limit, always_compact) else await call_policy(policy.trigger, evaluation)
    if type(decision) is not bool:
        raise TypeError("Context trigger must return bool.")
    return ContextPlan(preparation=preparation, estimate=estimate, needs_compaction=decision,
                       base_generation=conversation.view.generation)


async def refresh_preparation(preparation: ContextPreparation) -> ContextPreparation:
    """Adopt an explicit resumed stage without changing the advertised resources."""
    snapshot = await current_agent_context().snapshot()
    if snapshot.snapshot_id == preparation.snapshot_id:
        return preparation
    settings = request_settings(snapshot, preparation.generation_options, role="agent", require_model=False)
    target = current_agent_context().history_route(snapshot, settings.get("model"))
    return preparation.model_copy(update={"settings": settings, "snapshot_id": snapshot.snapshot_id,
        "compaction_model": getattr(current_agent_context(), "compaction_model", None),
        "replay_target": target,
        "fingerprint": request_fingerprint(preparation.fixed_input, preparation.tools, settings, snapshot, target,
            inject_current_time=preparation.inject_current_time)})


async def compact_agent_context(plan: ContextPlan, policy: ContextPolicy):
    """Invoke any user method, without requiring an LLM Capability."""
    preparation = await refresh_preparation(plan.preparation)
    estimate_before = await policy.estimate_input(preparation)
    value = await call_policy(policy.compact, CompactionInput(preparation, policy,
        policy.budget(preparation.settings), estimate_before))
    if not isinstance(value, CompactionResult):
        raise TypeError("Context compactor must return CompactionResult.")
    if not isinstance(value.view, ContextView):
        raise TypeError("CompactionResult.view must be ContextView.")
    view = restore(ContextView, value.view)
    previous = preparation.conversation
    if view != previous.view and view.generation <= previous.view.generation:
        raise ValueError("A changed Context view must advance its generation.")
    conversation = Conversation(items=previous.items, view=view, origins=previous.origins,
        token_anchor=previous.token_anchor if view == previous.view else None,
        runtime_times=previous.runtime_times if view == previous.view else {})
    validate_tool_history(conversation.visible)
    updated = preparation.model_copy(update={"conversation": conversation})
    estimate = await policy.estimate_input(updated)
    return ContextPlan(preparation=updated, estimate=estimate,
        needs_compaction=False, base_generation=plan.base_generation)


async def build_agent_request(plan: ContextPlan, policy: ContextPolicy) -> PreparedRequest:
    """Assemble only the prepared sources; do not reread dynamic resource catalogs."""
    preparation = await refresh_preparation(plan.preparation)
    budget = policy.budget(preparation.settings)
    estimate = await policy.estimate_input(preparation)
    if budget.input_limit is not None and estimate.input_tokens > budget.input_limit:
        raise ContextLimitExceeded(f"Estimated input {estimate.input_tokens} exceeds available Context {budget.input_limit}.")
    validate_tool_history(preparation.conversation.visible)
    request = LLMRequest(input=preparation.model_input,
        tools=preparation.tools, **preparation.settings)
    runtime_time = None
    if preparation.inject_current_time:
        from .runtime_context import append_time, TIME_KEY
        request = append_time(request, preparation.replay_target)
        runtime_time = request.routing[TIME_KEY]
    return PreparedRequest(request=request, tools=preparation.bindings, view=preparation.conversation.view,
        fingerprint=preparation.fingerprint, source_count=len(preparation.conversation.items),
        base_generation=plan.base_generation, fixed_input=preparation.fixed_input, replay_target=preparation.replay_target,
        estimated_input_tokens=estimate.input_tokens, runtime_time=runtime_time)


class CompactionReducer:
    def initial(self, context: StreamContext) -> CompactionState:
        return CompactionState()

    def add(self, context: StreamContext, state: CompactionState, chunk: CompactionChunk) -> CompactionState:
        if state.plan is not None:
            raise ValueError("Compaction emitted data after its final result.")
        if chunk.type == "completed":
            if chunk.plan is None:
                raise ValueError("Completed compaction requires a Context plan.")
            return CompactionState(plan=chunk.plan)
        return state

    def finish(self, context: StreamContext, state: CompactionState) -> ContextPlan:
        if state.plan is None:
            raise ValueError("Compaction ended without a completed result.")
        return state.plan
