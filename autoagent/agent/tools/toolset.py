"""A fixed Tool collection with name, argument and approval boundaries."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Collection, Iterable, Mapping
from contextlib import aclosing
from dataclasses import dataclass, replace
import hashlib
import inspect
import json
from types import MappingProxyType
from typing import TYPE_CHECKING

from autoagent.core.operators import run_sync

from ..models import LLMToolCall, LLMToolDefinition, LLMToolResult
from ..models._base import _check_finite
from .context import ToolContext, ToolHooks, _current_context, resolve_context
from .definition import get_tool_definition
from .execution import ExecutableTool, Tool, ToolBusinessError, ToolExecutionError, ToolOutputError, _unique_object, error_result
from .permissions import ToolApproval, ToolApprovalRequest, ToolApprovalRequired, ToolDecision

if TYPE_CHECKING:
    from autoagent.core import Operator


@dataclass
class _Prepared:
    tool: ExecutableTool
    arguments: dict
    record: dict
    context: ToolContext
    scopes: tuple[ToolSet, ...]
    policy_action: str | None = None
    policy_scopes: tuple[ToolSet, ...] | None = None


class _BoundTool:
    """Retain the owning ToolSet boundary when selecting a single executable."""

    def __init__(self, owner: ToolSet, name: str):
        self._owner, self._name = owner, name

    @property
    def definition(self) -> LLMToolDefinition:
        return self._owner._tools[self._name].definition

    def _check_name(self, call):
        call = LLMToolCall.model_validate(call)
        if call.name != self._name:
            raise ValueError(f"This bound Tool is named {self._name!r}.")
        return call

    def check(self, call, **kwargs) -> ToolDecision:
        return self._owner.check(self._check_name(call), **kwargs)

    async def acheck(self, call, **kwargs) -> ToolDecision:
        return await self._owner.acheck(self._check_name(call), **kwargs)

    def invoke(self, call, **kwargs) -> LLMToolResult:
        return self._owner.invoke(self._check_name(call), **kwargs)

    async def ainvoke(self, call, **kwargs) -> LLMToolResult:
        return await self._owner.ainvoke(self._check_name(call), **kwargs)

    async def astream(self, call, **kwargs):
        async with aclosing(self._owner._astream(self._check_name(call), **kwargs)) as stream:
            async for chunk in stream:
                yield chunk

    def as_operator(self, *, id: str | None = None, streaming: bool = False) -> Operator:
        if streaming:
            from autoagent.core import Operator
            from ..mcp.streaming import MCPToolStreamChunk

            async def stream(call: LLMToolCall):
                async with aclosing(self.astream(call)) as chunks:
                    async for chunk in chunks:
                        yield chunk

            stream.__annotations__["return"] = AsyncIterator[MCPToolStreamChunk]
            return Operator(stream, id=id or self._name)
        return self._owner.select(include=(self._name,)).as_operator(id=id or self._name)


class ToolSet:
    """Authorize every dispatch; policy returns allow, deny, or ask.

    check/acheck are side-effect-free preflight checks with respect to Tool
    execution. Policies should be repeatable: execution checks them again.
    Approval replies must come from an authenticated application, never an LLM.
    """

    def __init__(self, tools: Iterable[ExecutableTool | Callable[..., object]] = (), *,
                 context: ToolContext | None = None, hooks: ToolHooks | None = None,
                 policy: Callable | None = None, approval_mode="policy", replace_policies=False) -> None:
        if approval_mode not in ("policy", "always_ask", "never_ask", "bypass", "auto_approve"):
            raise ValueError("Unknown approval mode.")
        self.approval_mode = approval_mode
        self.context = context
        self.hooks = hooks or ToolHooks()
        self.policy = policy
        self.replace_policies = replace_policies
        if context is not None and not isinstance(context, ToolContext):
            raise TypeError("ToolSet context must be a ToolContext.")
        if not isinstance(self.hooks, ToolHooks):
            raise TypeError("ToolSet hooks must be ToolHooks.")
        if any(callback is not None and not callable(callback) for callback in
               (self.policy, self.hooks.before, self.hooks.after, self.hooks.on_error)):
            raise TypeError("Tool policies and hooks must be callable.")
        registered = {}
        definitions = []
        for candidate in tools:
            if isinstance(candidate, ExecutableTool):
                executable = candidate
            elif callable(candidate):
                get_tool_definition(candidate)
                executable = Tool(candidate)
            else:
                raise TypeError("ToolSet accepts decorated functions or ExecutableTool objects.")
            definition = LLMToolDefinition.model_validate(executable.definition)
            if definition.name in registered:
                raise ValueError(f"Duplicate Tool name: {definition.name!r}.")
            registered[definition.name] = executable
            definitions.append(definition)
        self._tools = MappingProxyType(registered)
        self._definitions = tuple(definitions)

    @property
    def definitions(self) -> tuple[LLMToolDefinition, ...]:
        return self.definitions_for()

    def definitions_for(self, context: ToolContext | None = None) -> tuple[LLMToolDefinition, ...]:
        """Filter names only; parameter policies are evaluated for actual calls."""
        current = resolve_context(LLMToolCall(name="advertisements", call_id="advertisements", arguments="{}"),
                                  context, self.context)
        definitions = []
        for value in self._definitions:
            if current.allowed_tools is not None and value.name not in current.allowed_tools:
                continue
            selected = self._tools[value.name]
            # The resolved context already includes call and inherited limits.
            # Follow only this name's owners instead of exporting every schema
            # in each owner again for every bound Tool.
            while isinstance(selected, _BoundTool):
                owner = selected._owner
                boundary = owner.context
                if boundary is not None and boundary.allowed_tools is not None and value.name not in boundary.allowed_tools:
                    break
                selected = owner._tools[value.name]
            else:
                definitions.append(LLMToolDefinition.model_validate(value.model_dump(mode="python")))
        return tuple(definitions)

    def __len__(self) -> int:
        return len(self._tools)

    def __getitem__(self, name: str) -> ExecutableTool:
        self._tools[name]
        return _BoundTool(self, name)

    def select(self, *, include: Collection[str] | None = None, exclude: Collection[str] = ()) -> ToolSet:
        """Select names while retaining policies, hooks and configured restrictions."""
        allowed = None if include is None else frozenset(include)
        denied = frozenset(exclude)
        if allowed is not None and (missing := allowed - self._tools.keys()):
            raise ValueError(f"Unknown included Tools: {sorted(missing)!r}.")
        return ToolSet((value for name, value in self._tools.items()
                        if (allowed is None or name in allowed) and name not in denied),
                       context=self.context, hooks=self.hooks, policy=self.policy,
                       approval_mode=self.approval_mode)

    @staticmethod
    def _operation_context(call, prepared):
        """Let discovered file targets use the same declarative policies as dispatch."""
        from .tool_policy import ToolPolicy
        scopes = prepared.policy_scopes if prepared.policy_scopes is not None else prepared.scopes
        policies = tuple(scope.policy for scope in scopes if isinstance(scope.policy, ToolPolicy))
        if policies:
            prepared.context = replace(prepared.context, services={**prepared.context.services,
                "tool_operation_policies": policies}).for_call(call, prepared.record)
        return prepared

    def _prepare(self, call, context) -> _Prepared | LLMToolResult:
        current = resolve_context(call, context, self.context)
        if call.name not in self._tools:
            return error_result(call, "unknown_tool", f"Unknown Tool: {call.name!r}.")
        if current.allowed_tools is not None and call.name not in current.allowed_tools:
            return error_result(call, "tool_access_denied", "Tool execution is not permitted.")
        selected = self._tools[call.name]
        if isinstance(selected, _BoundTool):
            prepared = selected._owner._prepare(call, current)
            if isinstance(prepared, _Prepared):
                prepared.scopes = (self, *prepared.scopes)
                if self.replace_policies:
                    prepared.policy_scopes = (self,)
                elif prepared.policy_scopes is not None:
                    prepared.policy_scopes = (self, *prepared.policy_scopes)
            return self._operation_context(call, prepared) if isinstance(prepared, _Prepared) else prepared
        if isinstance(selected, Tool):
            values = selected._prepare(call)
            if isinstance(values, LLMToolResult):
                return values
            arguments, record = values
        else:
            # External executables receive their original JSON after validation
            # against the advertised schema. No network schema retrieval occurs.
            from jsonschema.validators import validator_for
            from referencing import Registry
            from referencing.exceptions import NoSuchResource

            def no_remote(uri):
                raise NoSuchResource(ref=uri)

            schema = selected.definition.parameters
            validator_type = validator_for(schema)
            validator_type.check_schema(schema)
            validator = validator_type(schema, registry=Registry(retrieve=no_remote))
            try:
                record = json.loads(call.arguments, object_pairs_hook=_unique_object)
                _check_finite(record)
                if not isinstance(record, dict):
                    raise ValueError("Tool arguments must be a JSON object.")
                errors = list(validator.iter_errors(record))
                if errors:
                    raise ValueError("; ".join(error.message for error in errors))
            except (TypeError, ValueError) as error:
                return error_result(call, "invalid_arguments", str(error))
            except Exception as error:
                raise ToolExecutionError(
                    f"Tool {call.name!r} cannot validate its schema: {error}",
                    tool_name=call.name, call_id=call.call_id,
                ) from error
            arguments = record
        current = current.for_call(call, record)
        resolver = getattr(selected, "operation_resolver", None)
        if resolver is not None:
            try:
                operations = resolver(record, current)
                if inspect.isawaitable(operations):
                    if inspect.iscoroutine(operations):
                        operations.close()
                    raise TypeError("Tool operation resolution must be synchronous.")
                current = replace(current, operations=tuple(operations)).for_call(call, record)
                pinned = current.authorization.get("tool_binding", {}).get("operations", ())
                if pinned:
                    from .operations import ToolOperation
                    expected = tuple(ToolOperation.model_validate_json(json.dumps(_plain(value))) for value in pinned)
                    if expected != current.operations:
                        return error_result(call, "tool_target_changed", "Operation targets changed after preflight; request fresh authorization.")
                    current = replace(current, operations=expected).for_call(call, record)
            except ToolBusinessError as error:
                return error_result(call, error.code, str(error))
        return self._operation_context(call, _Prepared(selected, arguments, record, current, (self,)))

    @staticmethod
    def _sync_callback(callback, *arguments):
        value = callback(*arguments)
        if inspect.isawaitable(value):
            if inspect.iscoroutine(value):
                value.close()
            raise TypeError("Synchronous Tool dispatch requires synchronous policies/hooks.")
        return value

    @staticmethod
    async def _callback(callback, *arguments):
        if inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(getattr(callback, "__call__", None)):
            value = callback(*arguments)
        else:
            value = await run_sync(callback, *arguments)
        return await value if inspect.isawaitable(value) else value

    @staticmethod
    def _decision(value):
        if not isinstance(value, ToolDecision):
            raise TypeError("Tool policies must return ToolDecision.")
        value = ToolDecision.model_validate(value)
        if value.approval_request is not None:
            raise ValueError("Approval requests are bound by ToolSet, not by policies.")
        return value

    @staticmethod
    def _request(call, prepared, reason=None):
        from autoagent.core import OperatorCallContext

        runtime = OperatorCallContext.current()
        current = prepared.context
        identity = dict(call=call.model_dump(mode="json"), arguments=prepared.record,
                        user_id=current.user_id, session_id=current.session_id,
                        backend=dict(server_id=getattr(prepared.tool, "server_id", None),
                                     remote_name=getattr(prepared.tool, "remote_name", None)),
                        runtime=_plain(current.approval_scope) if current.approval_scope is not None else None if runtime is None else dict(
                            workflow_id=runtime.workflow_id, workflow_revision_id=runtime.workflow_revision_id,
                            session_id=runtime.session_id, invocation_id=runtime.invocation_id),
                        allowed_tools=sorted(current.allowed_tools) if current.allowed_tools is not None else None,
                        authorization=_plain(current.authorization),
                        operations=[value.model_dump(mode="json") for value in current.operations],
                        definitions=[scope._tools[call.name].definition.model_dump(mode="json")
                                     for scope in prepared.scopes])
        if current.attributes:
            identity["context_attributes"] = _plain(current.attributes)
        digest = hashlib.sha256(json.dumps(identity, ensure_ascii=False, sort_keys=True,
                                          allow_nan=False, separators=(",", ":")).encode()).hexdigest()
        return ToolApprovalRequest(id=digest, call=call, arguments=prepared.record,
                                   user_id=current.user_id, session_id=current.session_id, reason=reason,
                                   operations=tuple(value.model_dump(mode="json") for value in current.operations))

    def _bind(self, call, prepared, decisions):
        from ..context import apply_approval_mode
        denied = next((value for value in decisions if value.action == "deny"), None)
        if denied is not None:
            prepared.policy_action = "deny"
            return denied
        asks = [value for value in decisions if value.action == "ask"]
        prepared.policy_action = "ask" if asks else "allow"
        reason = "; ".join(value.reason for value in asks if value.reason) or None
        combined = apply_approval_mode(ToolDecision(action="ask" if asks else "allow", reason=reason),
                                       self.approval_mode)
        if combined.action != "ask":
            return combined
        # Include the raw proposal as well as normalized execution arguments:
        # changing the proposal, actor, session or advertised contract invalidates
        # its approval even if normalization happens to produce the same value.
        request = self._request(call, prepared, reason)
        return ToolDecision(action="ask", reason=reason, approval_request=request)

    def _check(self, call, prepared):
        decisions = [self._decision(self._sync_callback(scope.policy, call, prepared.context))
                     for scope in (prepared.policy_scopes if prepared.policy_scopes is not None else prepared.scopes) if scope.policy is not None]
        return self._bind(call, prepared, decisions)

    async def _acheck(self, call, prepared):
        decisions = []
        for scope in (prepared.policy_scopes if prepared.policy_scopes is not None else prepared.scopes):
            if scope.policy is not None:
                decisions.append(self._decision(await self._callback(scope.policy, call, prepared.context)))
        return self._bind(call, prepared, decisions)

    @staticmethod
    def _invalid_decision(result):
        return ToolDecision(action="deny", reason=result.structured_content["error"]["message"])

    def check(self, call: LLMToolCall, *, context: ToolContext | None = None) -> ToolDecision:
        """Validate and authorize without running Tools or execution hooks."""
        call = LLMToolCall.model_validate(call)
        prepared = self._prepare(call, context)
        if isinstance(prepared, LLMToolResult):
            return self._invalid_decision(prepared)
        token = _current_context.set(prepared.context)
        try:
            return self._check(call, prepared)
        finally:
            _current_context.reset(token)

    async def acheck(self, call: LLMToolCall, *, context: ToolContext | None = None) -> ToolDecision:
        """Async preflight; Workflow code may route ask to a Core Wait."""
        call = LLMToolCall.model_validate(call)
        prepared = await run_sync(self._prepare, call, context)
        if isinstance(prepared, LLMToolResult):
            return self._invalid_decision(prepared)
        token = _current_context.set(prepared.context)
        try:
            return await self._acheck(call, prepared)
        finally:
            _current_context.reset(token)

    async def apreflight(self, call: LLMToolCall, *, context: ToolContext | None = None):
        """Return the original argument/access error, or a bound policy decision."""
        from .permissions import ToolPreflight
        call = LLMToolCall.model_validate(call)
        prepared = await run_sync(self._prepare, call, context)
        if isinstance(prepared, LLMToolResult):
            return ToolPreflight(result=prepared)
        token = _current_context.set(prepared.context)
        try:
            decision = await self._acheck(call, prepared)
            return ToolPreflight(decision=decision, policy_action=prepared.policy_action,
                operations=tuple(value.model_dump(mode="json") for value in prepared.context.operations))
        finally:
            _current_context.reset(token)

    @classmethod
    def _authorize(cls, call, prepared, decision, approval):
        if approval is not None:
            approval = ToolApproval.model_validate(approval)
        if decision.action == "deny":
            return error_result(call, "tool_access_denied", decision.reason or "Tool execution is not permitted.")
        if approval is not None and approval.request_id != cls._request(call, prepared).id:
            return error_result(call, "tool_approval_mismatch", "Approval does not match the current Tool call.")
        if decision.action == "ask":
            if approval is None:
                raise ToolApprovalRequired(decision.approval_request)
        if approval is not None and not approval.approved:
            from .execution import approval_denied_result
            return approval_denied_result(call, approval)
        return None

    def invoke(self, call: LLMToolCall, *, context: ToolContext | None = None,
               approval: ToolApproval | None = None) -> LLMToolResult:
        """Execute synchronous Tools after a fresh permission/approval check."""
        call = LLMToolCall.model_validate(call)
        prepared = self._prepare(call, context)
        if isinstance(prepared, LLMToolResult):
            return prepared
        if not isinstance(prepared.tool, Tool) or prepared.tool.is_async:
            raise TypeError("Use ainvoke for async or remote Tools.")
        token = _current_context.set(prepared.context)
        try:
            denied = self._authorize(call, prepared, self._check(call, prepared), approval)
            if denied is not None:
                return denied
            for scope in prepared.scopes:
                self._sync_hook(scope.hooks.before, call, prepared.context)
            result = self._validate_result(call, prepared.tool._execute(call, prepared.arguments, prepared.context))
            for scope in reversed(prepared.scopes):
                self._sync_hook(scope.hooks.after, call, result, prepared.context)
            return result
        except ToolApprovalRequired:
            raise
        except Exception as error:
            for scope in reversed(prepared.scopes):
                try:
                    self._sync_hook(scope.hooks.on_error, call, error, prepared.context)
                except Exception as hook_error:
                    raise hook_error from error
            raise
        finally:
            _current_context.reset(token)

    async def ainvoke(self, call: LLMToolCall, *, context: ToolContext | None = None,
                      approval: ToolApproval | None = None) -> LLMToolResult:
        """Execute local/remote Tools; never wait internally for human input."""
        call = LLMToolCall.model_validate(call)
        prepared = await run_sync(self._prepare, call, context)
        if isinstance(prepared, LLMToolResult):
            return prepared
        token = _current_context.set(prepared.context)
        try:
            denied = self._authorize(call, prepared, await self._acheck(call, prepared), approval)
            if denied is not None:
                return denied
            for scope in prepared.scopes:
                await self._hook(scope.hooks.before, call, prepared.context)
            if isinstance(prepared.tool, Tool):
                value = await prepared.tool._aexecute(call, prepared.arguments, prepared.context)
            else:
                value = await prepared.tool.ainvoke(call)
            result = self._validate_result(call, value)
            for scope in reversed(prepared.scopes):
                await self._hook(scope.hooks.after, call, result, prepared.context)
            return result
        except (asyncio.CancelledError, ToolApprovalRequired):
            raise
        except Exception as error:
            for scope in reversed(prepared.scopes):
                try:
                    await self._hook(scope.hooks.on_error, call, error, prepared.context)
                except Exception as hook_error:
                    raise hook_error from error
            raise
        finally:
            _current_context.reset(token)

    async def _astream(self, call, *, context=None, approval=None):
        """Keep selected MCP progress streams behind the same execution boundary."""
        from ..mcp.tools import MCPTool
        from ..mcp.streaming import MCPToolStreamChunk

        call = LLMToolCall.model_validate(call)
        prepared = await run_sync(self._prepare, call, context)
        selected = self._tools[call.name]
        while isinstance(selected, _BoundTool):
            selected = selected._owner._tools[call.name]
        if not isinstance(selected, MCPTool):
            raise TypeError("Progress streaming requires an MCP Tool.")
        identity = dict(server_id=selected.server_id, tool_name=call.name, call_id=call.call_id)
        if isinstance(prepared, LLMToolResult):
            yield MCPToolStreamChunk(type="completed", result=prepared, **identity)
            return
        token = _current_context.set(prepared.context)
        try:
            denied = self._authorize(call, prepared, await self._acheck(call, prepared), approval)
            if denied is not None:
                yield MCPToolStreamChunk(type="completed", result=denied, **identity)
                return
            for scope in prepared.scopes:
                await self._hook(scope.hooks.before, call, prepared.context)
            async with aclosing(selected.astream(call)) as stream:
                async for chunk in stream:
                    if chunk.type == "completed":
                        result = self._validate_result(call, chunk.result)
                        for scope in reversed(prepared.scopes):
                            await self._hook(scope.hooks.after, call, result, prepared.context)
                    yield chunk
        except (asyncio.CancelledError, ToolApprovalRequired):
            raise
        except Exception as error:
            for scope in reversed(prepared.scopes):
                try:
                    await self._hook(scope.hooks.on_error, call, error, prepared.context)
                except Exception as hook_error:
                    raise hook_error from error
            raise
        finally:
            _current_context.reset(token)

    @classmethod
    def _sync_hook(cls, callback, *arguments):
        if callback is not None and cls._sync_callback(callback, *arguments) is not None:
            raise TypeError("Tool hooks must return None.")

    @classmethod
    async def _hook(cls, callback, *arguments):
        if callback is not None and await cls._callback(callback, *arguments) is not None:
            raise TypeError("Tool hooks must return None.")

    @staticmethod
    def _validate_result(call, value):
        try:
            result = LLMToolResult.model_validate(value)
            if result.call_id != call.call_id:
                raise ValueError("Tool result call_id does not match the incoming call.")
        except Exception as error:
            raise ToolOutputError(f"Tool {call.name!r} returned an invalid observation: {error}",
                                  tool_name=call.name, call_id=call.call_id) from error
        return result

    def as_operator(self, *, id: str = "tools") -> Operator:
        from ..integration.tools import tool_operator

        return tool_operator(self, id=id)


__all__ = ["ToolSet"]


def _plain(value):
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value
