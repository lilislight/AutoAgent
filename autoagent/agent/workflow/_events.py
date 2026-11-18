"""Project-owned mappings from Agent boundaries to presentation snapshots."""

from autoagent.core import UserEventContext, UserEventSpec

from ..context import plain, restore
from ..models import LLMMessage, LLMReasoning, LLMResponse, LLMToolCall, LLMToolResult
from .events import (
    AgentCompactionEvent, AgentCompletedEvent, AgentErrorEvent, AgentEventMessage,
    AgentInteractionEvent, AgentInteractionResolution, AgentMediaBlock, AgentMessagesEvent,
    AgentReasoningBlock, AgentResponseDeltaEvent, AgentResponseEvent, AgentTextBlock,
    AgentToolBlock, AgentToolResultView, AgentToolSnapshot, AgentToolsEvent,
)


def tool_id(response_id, call_id):
    # Call IDs belong to one model response, not the whole Session.
    return response_id + ":" + call_id


def _content(part):
    return part.model_dump(mode="json", exclude={"native"})


def _tool_state(result):
    error = result.structured_content
    kind = error.get("error", {}).get("kind") if isinstance(error, dict) and isinstance(error.get("error"), dict) else None
    if kind in {"tool_access_denied", "tool_approval_denied"}:
        return "denied"
    return "failed" if result.is_error else "completed"


def _snapshot(response_id, call, *, state=None, result=None, progress=None):
    visible = None if result is None else result.structured_content
    if isinstance(visible, dict):
        # Explicit SDK presentation fields stay available to UI/audit without
        # entering the model's Tool result or exposing arbitrary Tool metadata.
        details = {}
        for namespace, keys in (("file", ("diff", "diff_truncated")),
                ("execution", ("wall_time_seconds", "total_output_bytes", "retained_start"))):
            metadata = result.tool_metadata.get(namespace, {})
            if isinstance(metadata, dict):
                details.update({key: metadata[key] for key in keys if key in metadata})
        visible = {**details, **visible}
    view = None if result is None else AgentToolResultView(content=tuple(_content(part) for part in result.content),
        structured_content=visible, is_error=result.is_error)
    return AgentToolSnapshot(tool_id=tool_id(response_id, call.call_id), response_id=response_id,
        call_id=call.call_id, name=call.name, arguments=call.arguments,
        state=state or _tool_state(result), result=view, progress=progress)


def _response(context):
    response = restore(LLMResponse, context.value)
    blocks = []
    for item in response.output:
        if isinstance(item, LLMMessage):
            for part in item.content:
                if part.type in {"text", "refusal"}:
                    if part.text.strip():
                        blocks.append(AgentTextBlock(type=part.type, text=part.text,
                            annotations=getattr(part, "annotations", ())))
                else:
                    blocks.append(AgentMediaBlock(content=_content(part)))
        elif isinstance(item, LLMReasoning):
            text = item.text if item.text and item.text.strip() else None
            summary = tuple(text for text in item.summary if text.strip())
            if text or summary:
                blocks.append(AgentReasoningBlock(text=text, summary=summary))
        elif isinstance(item, LLMToolCall):
            blocks.append(AgentToolBlock(tool_id=tool_id(context.occurrence_id, item.call_id)))
        # Opaque Provider Items and encrypted replay data are not UI content.
    return AgentResponseEvent(node_id=context.node_id, response_id=context.occurrence_id,
        model=response.model, finish_reason=response.finish_reason, blocks=tuple(blocks),
        usage=response.usage.model_dump(mode="json", exclude={"native"}) if response.usage else None)


def _request_view(request):
    """Only remove protocol fields, preserving user argument/content keys."""
    value = plain(request)
    for entry in value.get("requests", []):
        call = entry["call"]
        call.pop("native", None)
        call.pop("id", None)
    return value


def _interaction(context, response_id):
    value = context.value
    request = _request_view(value["request"])
    calls = ([entry["call"]["call_id"] for entry in request["requests"]]
        if context.node_id == "wait_approval" else [request["call_id"]])
    resolved = context.phase == "wait_resolved"
    return AgentInteractionEvent(node_id=context.node_id, interaction_id=value["wait_id"],
        type="approval" if context.node_id == "wait_approval" else "question",
        state="resolved" if resolved else "pending",
        tool_ids=tuple(tool_id(response_id, call_id) for call_id in calls), request=request,
        resolution=AgentInteractionResolution(outcome=value["outcome"], response=plain(value.get("response")),
            error=plain(value.get("error"))) if resolved else None)


def map_agent_event(context: UserEventContext) -> UserEventSpec | None:
    """Emit one typed snapshot; return None at non-presentation boundaries."""
    node, phase, value = context.node_id, context.phase, context.value
    state = context.invocation_context.get("agent", {})

    def emit(kind, payload, *, persist=True):
        return UserEventSpec(kind=kind, payload=payload.model_dump(mode="json"), persist=persist)

    if phase == "error":
        return emit("agent.error", AgentErrorEvent(node_id=node, error=plain(value["error"]),
            stage=value["stage"], handled=value["handled"]))

    if node == "start" and phase == "output":
        from .models import AgentInput
        return emit("agent.messages", AgentMessagesEvent(node_id=node, messages=(AgentEventMessage(
            message_id=context.occurrence_id, source="user", content=restore(AgentInput, value).display_content()),)))
    if node == "prepare_context" and phase == "output":
        messages = value.get("received_messages", ())
        return emit("agent.messages", AgentMessagesEvent(node_id=node,
            messages=tuple(restore(AgentEventMessage, item) for item in messages))) if messages else None

    if node == "compact_context" and phase == "chunk" and value["type"] == "started":
        compact = state["compaction"]
        return emit("agent.compaction", AgentCompactionEvent(node_id=node,
            compaction_id=compact["id"], state="running", before_tokens=compact["before_tokens"],
            view_generation=compact["generation"]), persist=False)
    if node == "build_request" and phase == "output" and state.get("compaction", {}).get("requested"):
        compact = state["compaction"]
        return emit("agent.compaction", AgentCompactionEvent(node_id=node,
            compaction_id=compact["id"], state="completed", before_tokens=compact["before_tokens"],
            after_tokens=value["estimated_input_tokens"], view_generation=value["view"]["generation"]))

    if node == "llm_call":
        if phase == "chunk" and value["type"] == "delta" and value["delta"]:
            return emit("agent.response.delta", AgentResponseDeltaEvent(node_id=node,
                response_id=context.occurrence_id, stream_id=context.operator_call_id,
                item_key=value["item_key"], content_index=value["content_index"], kind=value["kind"], delta=value["delta"]), persist=False)
        if phase == "output":
            return emit("agent.response", _response(context))
        return None

    response_id = state.get("response_id")
    if node in {"ask_user", "wait_approval"} and phase in {"waiting", "wait_resolved"}:
        return emit("agent.interaction", _interaction(context, response_id), persist=phase == "wait_resolved")

    if node == "finish" and phase == "output":
        return emit("agent.completed", AgentCompletedEvent(node_id=node, final_response_id=response_id, rounds=state["rounds"]))

    tools = []
    persist = phase == "output"
    if node == "tool_branch":
        if phase == "chunk":
            tools = [_snapshot(response_id, restore(LLMToolCall, item["call"]), state="queued")
                for category in ("allow", "ask", "questions", "waits") for item in value[category]]
        elif phase == "output":
            tools = _results(state, value["immediate"])
    elif node in {"tool_execute", "tool_execute_approved"}:
        if phase == "chunk":
            result = restore(LLMToolResult, value["result"]) if value["result"] is not None else None
            tools = [_snapshot(response_id, restore(LLMToolCall, value["call"]),
                state="running" if result is None else None, result=result)]
        elif phase == "output":
            tools = _results(state, value)
    elif node == "prepare_wait":
        if phase == "chunk" and value["preparation"]["wait"] is not None:
            tools = [_snapshot(response_id, restore(LLMToolCall, value["call"]), state="queued",
                progress=plain(value["preparation"]["wait"]["receipt"]))]
        elif phase == "output":
            tools = _results(state, value["results"])
    elif node == "wait_tools" and phase in {"waiting", "wait_resolved"}:
        wait = state["prepared_waits"]["waits"][context.unit_index]
        call = next(item for item in restore(LLMResponse, state["response"]).tool_calls if item.call_id == wait["call_id"])
        outcome = value.get("outcome")
        tools = [_snapshot(response_id, call, state="cancelled" if outcome == "cancelled" else
            "failed" if outcome == "timed_out" else "waiting" if phase == "waiting" else "running", progress={
                "wait_id": value["wait_id"], "deadline_at_us": value["deadline_at_us"], "outcome": outcome,
                "error": plain(value.get("error"))})]
        persist = False
    elif node == "collect_wait_results" and phase == "output":
        # prepare_wait has already recorded any immediate preparation failures.
        recorded = {item["call_id"] for item in state["prepared_waits"]["results"]}
        tools = _results(state, (item for item in value["results"] if item["call_id"] not in recorded))
    return emit("agent.tools", AgentToolsEvent(node_id=node, tools=tuple(tools)), persist=persist) if tools else None


def _results(state, values):
    calls = {item.call_id: item for item in restore(LLMResponse, state["response"]).tool_calls}
    return [_snapshot(state["response_id"], calls[result.call_id], result=result)
        for value in values for result in (restore(LLMToolResult, value),)]
