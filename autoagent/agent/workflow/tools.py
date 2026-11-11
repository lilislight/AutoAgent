"""Shared dynamic Tool dispatch for both Map branches."""

from ..context import current_agent_context, plain
from ..models import LLMToolResult
from ..tools import ToolApprovalRequired, ToolContext, ToolExecutionError, ToolOutputError
from ..tools.execution import error_result
from ..tools.context import tool_context
from .context import definition_hash
from .models import AuthorizationObservation, ToolBranchResult, ToolTask
from .builtins import workflow_catalog
from .resources import resource_permissions
from .subagents import BUILTIN_NAMES, WaitPreparation, DeferredToolWait


def authorization_binding(task):
    return {**task.binding.model_dump(mode="json"), "request_id": task.request_id,
        "operations": [dict(value) for value in task.operations]}


def binding_error(service, snapshot, task, *, catalog=None):
    if catalog is None:
        catalog, _ = workflow_catalog(service, snapshot)
    entry = catalog.get(task.call.name)
    if entry is None:
        return error_result(task.call, "tool_unavailable", "The Tool is no longer registered.")
    options = service.workflow_options(snapshot)
    kind = ("ask_user" if options.include_ask_user and task.call.name == "ask_user" else
        "agent_builtin" if options.subagents.enabled and task.call.name in BUILTIN_NAMES else "tool")
    if task.binding.kind != kind:
        return error_result(task.call, "tool_binding_changed", "The Tool execution kind changed.")
    if (entry.resource_id != task.binding.resource_id or entry.version != task.binding.version
            or definition_hash(entry.value.definition) != task.binding.definition_hash):
        return error_result(task.call, "tool_binding_changed", "The Tool binding changed after the model request.")
    return None


async def branch_tools(value):
    service = current_agent_context()
    snapshot = await service.snapshot()
    if snapshot.settings.workspace is not None:
        await service.prepare_workspace(snapshot)
    calls = value.response.tool_calls
    if len({call.call_id for call in calls}) != len(calls):
        raise ValueError("Model response contains duplicate Tool call IDs.")
    bindings = {item.name: item for item in value.prepared.tools}
    allow, ask, questions, waits, immediate, decisions = [], [], [], [], [], []
    for call in calls:
        if value.response.finish_reason == "length":
            immediate.append(error_result(call, "truncated_tool_call", "Model output was truncated; Tool arguments cannot be trusted."))
            continue
        if call.name not in bindings:
            immediate.append(error_result(call, "tool_not_advertised", "The Tool was not advertised in this model request."))
            continue
        task = ToolTask(request_id=value.prepared.request_id, call=call, binding=bindings[call.name])
        catalog, _ = workflow_catalog(service, snapshot)
        error = binding_error(service, snapshot, task, catalog=catalog)
        if error is not None:
            immediate.append(error)
            continue
        if task.binding.kind in {"ask_user", "agent_builtin"}:
            checked = catalog[call.name].value._prepare(call)
            if isinstance(checked, LLMToolResult):
                immediate.append(checked)
            elif task.binding.kind == "ask_user":
                questions.append(task)
            else:
                arguments, _ = checked
                waiting = (call.name in {"run_agent", "wait_for"}
                    or call.name == "send_message" and arguments["wait_timeout_seconds"] is not None)
                (waits if waiting else allow).append(task)
            continue
        tools = service.toolset(snapshot, binding=authorization_binding(task), catalog=catalog)
        with tool_context(tools.context):
            checked = await tools.apreflight(call)
        if checked.result is not None:
            immediate.append(checked.result)
            continue
        decision = checked.decision
        task = task.model_copy(update={"operations": checked.operations})
        decisions.append(AuthorizationObservation(call_id=call.call_id,
            approval_mode=snapshot.settings.approval_mode,
            policy_action=checked.policy_action, effective_action=decision.action))
        if decision.action == "deny":
            immediate.append(error_result(call, "tool_access_denied", decision.reason or "Tool access denied."))
        elif decision.action == "ask":
            ask.append(task.model_copy(update={"approval_request": decision.approval_request}))
        else:
            allow.append(task)
    return ToolBranchResult(allow=tuple(allow), ask=tuple(ask), questions=tuple(questions), waits=tuple(waits),
        immediate=tuple(immediate), decisions=tuple(decisions))


async def execute_tool(task: ToolTask) -> LLMToolResult:
    """Recheck bindings and permissions, returning classified Tool observations."""
    if task.binding.kind == "agent_builtin":
        result = await execute_builtin(task)
        if not result.is_error and "_agent_wait" in result.structured_content:
            raise ValueError("Waiting collaboration calls require the prepare_wait branch.")
        return result
    if task.observation is not None:
        return task.observation
    if task.approval is not None and not task.approval.approved:
        from ..tools.execution import approval_denied_result
        return approval_denied_result(task.call, task.approval)
    service = current_agent_context()
    snapshot = await service.snapshot()
    if snapshot.settings.workspace is not None:
        await service.prepare_workspace(snapshot)
    catalog, _ = workflow_catalog(service, snapshot)
    error = binding_error(service, snapshot, task, catalog=catalog)
    if error is not None:
        return error
    tools = service.toolset(snapshot, binding=authorization_binding(task), catalog=catalog,
        observations=task.observed_versions)
    permission_revision = service.resource_revision(snapshot)
    # Pin the checked executable. A final hook is the borrowing boundary:
    # changes during an async policy cannot silently replace that executable.
    from ..tools import ToolHooks
    class BindingChanged(Exception):
        def __init__(self, result):
            self.result = result
    async def verify_binding(call, context):
        if snapshot.settings.workspace is not None:
            await service.prepare_workspace(snapshot)
        error = binding_error(service, snapshot, task)
        if error is not None:
            raise BindingChanged(error)
        latest = await service.snapshot()
        if permission_revision != service.resource_revision(latest):
            raise BindingChanged(error_result(call, "tool_permissions_changed", "Tool permissions changed before execution; reevaluate this call."))
        current, _ = workflow_catalog(service, snapshot)
        if resource_permissions(current[call.name]) != plain(context.authorization.get("resource_permissions")):
            raise BindingChanged(error_result(call, "tool_permissions_changed", "Tool permissions changed before execution."))
    tools.hooks = ToolHooks(before=verify_binding)
    try:
        # A fresh Allow (including bypass) needs no previous approval token.
        from contextlib import AsyncExitStack
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(service.borrow_tool(catalog[task.call.name].resource_id))
            stack.enter_context(tool_context(tools.context))
            checked = await tools.apreflight(task.call)
            if checked.result is not None:
                return checked.result
            approval = task.approval if checked.decision.action == "ask" else None
            return await tools.ainvoke(task.call, approval=approval)
    except BindingChanged as error:
        return error.result
    except ToolApprovalRequired:
        return error_result(task.call, "tool_approval_required", "Tool authorization changed; this call was not executed and needs approval.")
    except ToolOutputError as error:
        return error_result(task.call, "tool_output_error", str(error))
    except ToolExecutionError as error:
        return error_result(task.call, "tool_execution_error", str(error))


async def execute_builtin(task: ToolTask) -> LLMToolResult:
    """Configured collaboration uses intrinsic access rules, independent of Tool approval modes."""
    service = current_agent_context()
    snapshot = await service.snapshot()
    catalog, _ = workflow_catalog(service, snapshot)
    error = binding_error(service, snapshot, task, catalog=catalog)
    if error is not None:
        return error
    context = ToolContext(session_id=snapshot.session_id,
        services={"agent_snapshot": snapshot, "agent_observations": task.observed_versions})
    try:
        with tool_context(context):
            result = await catalog[task.call.name].value.ainvoke(task.call, context=context)
    except ToolOutputError as error:
        return error_result(task.call, "tool_output_error", str(error))
    except ToolExecutionError as error:
        return error_result(task.call, "tool_execution_error", str(error))
    if not result.is_error:
        record = dict(result.structured_content)
        versions = record.pop("_agent_versions", {})
        result = result.model_copy(update={"structured_content": record,
            "tool_metadata": {**result.tool_metadata, "agent_observed_versions": versions}})
    return result


async def prepare_tool_wait(task: ToolTask) -> WaitPreparation:
    """Perform only the nonblocking prelude and produce a native Select request."""
    if task.binding.kind != "agent_builtin":
        raise ValueError("Only waiting collaboration calls enter prepare_wait.")
    result = await execute_builtin(task)
    if not result.is_error:
        record = result.structured_content
        if "_agent_wait" not in record:
            raise ValueError("Waiting collaboration call did not produce a Select request.")
        from ..context import restore
        return WaitPreparation(wait=restore(DeferredToolWait, {"call_id": task.call.call_id, **record["_agent_wait"]}))
    return WaitPreparation(result=result)
