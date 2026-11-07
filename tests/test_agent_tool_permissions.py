"""Tool permission decisions, exact approval binding and protected execution."""

import asyncio
from dataclasses import replace
from typing import Annotated
import unittest

from pydantic import AfterValidator, BaseModel, ConfigDict

from autoagent import AutoAgentApp, Node, Workflow, ValueContract
from autoagent.core import OperatorCallContext
from autoagent.core.operators.context import operator_call_context
from autoagent.agent import (
    LLMToolCall, LLMToolDefinition, LLMToolResult, Tool, ToolApproval,
    ToolApprovalRequired, ToolContext, ToolDecision, ToolHooks, ToolSet,
)


def call(*, name="publish", call_id="call-1", arguments='{"text":"report"}'):
    return LLMToolCall(name=name, call_id=call_id, arguments=arguments)


def ask(call, context):
    return ToolDecision(action="ask", reason="Review publication")


class Details(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tags: list[str]


class ToolPermissionTests(unittest.IsolatedAsyncioTestCase):
    def tools(self, *, policy=ask, context=None, hooks=None):
        effects = []
        def publish(text: str = "default") -> str:
            effects.append(text)
            return text
        return ToolSet((Tool(publish),), policy=policy, context=context, hooks=hooks), effects

    async def test_ask_preflight_and_missing_approval_have_no_side_effects(self):
        """Preflight and missing approval run neither Tool bodies nor execution hooks."""
        hooks = []
        tools, effects = self.tools(hooks=ToolHooks(before=lambda *args: hooks.append("before"),
                                                   on_error=lambda *args: hooks.append("error")))
        decision = await tools.acheck(call())
        self.assertEqual(decision.action, "ask")
        self.assertEqual(decision.approval_request.arguments, {"text": "report"})
        with self.assertRaises(ToolApprovalRequired) as caught:
            await tools.ainvoke(call())
        self.assertEqual(caught.exception.request.id, decision.approval_request.id)
        self.assertEqual(effects, [])
        self.assertEqual(hooks, [])

    async def test_approved_and_rejected_replies_are_explicit_execution_inputs(self):
        """Only a matching positive approval allows a policy requiring review."""
        tools, effects = self.tools()
        request = tools.check(call()).approval_request
        rejected = await tools.ainvoke(call(), approval=request.respond(approved=False))
        self.assertTrue(rejected.is_error)
        self.assertEqual(effects, [])
        approved = tools.invoke(call(), approval=request.respond(approved=True))
        self.assertFalse(approved.is_error)
        self.assertEqual(effects, ["report"])

    async def test_approval_cannot_authorize_changed_call_actor_or_session(self):
        """Arguments, Tool, call ID and trusted caller identity bind the approval."""
        tools, effects = self.tools()
        original = ToolContext(user_id="alice", session_id="session-a")
        approval = tools.check(call(), context=original).approval_request.respond(approved=True)
        cases = [
            (call(arguments='{"text":"changed"}'), original),
            (call(call_id="other"), original),
            (call(), replace(original, user_id="bob")),
            (call(), replace(original, session_id="session-b")),
        ]
        for proposal, context in cases:
            result = await tools.ainvoke(proposal, context=context, approval=approval)
            self.assertEqual(result.structured_content["error"]["kind"], "tool_approval_mismatch")
        self.assertTrue((await tools.ainvoke(call(name="missing"), approval=approval)).is_error)
        def archive(text: str) -> str:
            self.fail("Different Tool executed")
        combined = ToolSet((tools["publish"], Tool(archive)), policy=ask)
        result = await combined.ainvoke(call(name="archive"), context=original, approval=approval)
        self.assertEqual(result.structured_content["error"]["kind"], "tool_approval_mismatch")
        self.assertEqual(effects, [])

    async def test_current_denial_or_contract_change_overrides_old_approval(self):
        """Execution reevaluates policy and rejects approval for changed definitions."""
        state = ["ask"]
        tools, effects = self.tools(policy=lambda *args: ToolDecision(action=state[0]))
        approval = tools.check(call()).approval_request.respond(approved=True)
        state[0] = "deny"
        self.assertTrue((await tools.ainvoke(call(), approval=approval)).is_error)
        def publish(text: str, extra: int = 1) -> str:
            self.fail("Changed Tool executed")
        changed = ToolSet((Tool(publish),), policy=ask)
        self.assertEqual((await changed.ainvoke(call(), approval=approval)).structured_content["error"]["kind"],
                         "tool_approval_mismatch")
        self.assertEqual(effects, [])

    async def test_provided_approval_is_checked_even_when_policy_now_allows(self):
        """Relaxing current policy cannot validate an unrelated attached reply."""
        tools, effects = self.tools(policy=lambda *args: ToolDecision(action="allow"))
        result = await tools.ainvoke(call(), approval=ToolApproval(request_id="unrelated", approved=True))
        self.assertTrue(result.is_error)
        self.assertEqual(effects, [])

    async def test_names_and_invalid_arguments_are_rejected_before_policy(self):
        """Name restrictions and argument errors cannot enter policies or handlers."""
        calls = []
        tools, effects = self.tools(policy=lambda *args: calls.append("policy"),
                                    context=ToolContext(allowed_tools=frozenset()))
        self.assertEqual(tools.definitions, ())
        self.assertEqual((await tools.acheck(call())).action, "deny")
        self.assertTrue((await tools.ainvoke(call())).is_error)
        valid, _ = self.tools(policy=lambda *args: calls.append("policy"))
        for arguments in ('[]', '{', '{"text":2}', '{"text":"a","text":"b"}', '{"text":"a","approval":true}'):
            result = await valid.ainvoke(call(arguments=arguments))
            self.assertEqual(result.structured_content["error"]["kind"], "invalid_arguments")
            self.assertEqual((await valid.acheck(call(arguments=arguments))).action, "deny")
        self.assertEqual(calls, [])
        self.assertEqual(effects, [])

    async def test_policies_receive_normalized_defaults_and_deep_readonly_arguments(self):
        """Authorize the same validated values that reach the handler, exactly once."""
        validations = []
        def normalize(value):
            validations.append(value)
            return value.strip().lower()
        # Context injection is deliberately required and excluded from schema.
        def handler(text: Annotated[str, AfterValidator(normalize)], details: Details,
                    ctx: ToolContext, count: int = 2) -> str:
            self.assertEqual(ctx.arguments["text"], text)
            self.assertEqual(ctx.arguments["count"], count)
            return text
        def policy(incoming, context):
            self.assertEqual(dict(context.arguments), {"text": "report", "details": {"tags": ("one",)}, "count": 2})
            with self.assertRaises(TypeError):
                context.arguments["text"] = "other"
            with self.assertRaises(TypeError):
                context.arguments["details"]["tags"][0] = "other"
            return ToolDecision(action="allow")
        tools = ToolSet((Tool(handler, name="publish"),), policy=policy)
        result = await tools.ainvoke(call(arguments='{"text":" REPORT ","details":{"tags":["one"]}}'))
        self.assertEqual(result.content[0].text, "report")
        self.assertEqual(validations, [" REPORT "])
        with self.assertRaises(TypeError):
            ToolContext(arguments={"text": "forged"})

    async def test_policy_failures_and_invalid_returns_never_allow_execution(self):
        """Broken policies propagate errors instead of silently authorizing calls."""
        def broken(*args):
            raise ValueError("policy unavailable")
        for policy, expected in ((lambda *args: None, TypeError), (lambda *args: True, TypeError), (broken, ValueError)):
            tools, effects = self.tools(policy=policy)
            with self.assertRaises(expected):
                await tools.ainvoke(call())
            self.assertEqual(effects, [])

    async def test_sync_dispatch_rejects_async_policy_and_async_dispatch_supports_it(self):
        """Sync execution never skips an asynchronous permission decision."""
        async def policy(incoming, context):
            await asyncio.sleep(0)
            return ToolDecision(action="allow")
        tools, effects = self.tools(policy=policy)
        with self.assertRaises(TypeError):
            tools.invoke(call())
        with self.assertRaises(TypeError):
            tools.check(call())
        self.assertEqual(effects, [])
        self.assertFalse((await tools.ainvoke(call())).is_error)

    async def test_bound_views_selections_and_nested_sets_keep_all_policies(self):
        """Selecting or composing an executable cannot remove its original boundary."""
        tools, effects = self.tools(policy=lambda *args: ToolDecision(action="deny"))
        for executable in (tools["publish"], tools.select(include=("publish",)), ToolSet((tools["publish"],))):
            self.assertTrue((await executable.ainvoke(call())).is_error)
        self.assertEqual(effects, [])
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(Workflow("bound", nodes=[Node("tool", tools["publish"].as_operator())]), call())
            self.assertEqual(result.status, "completed")
            self.assertTrue(result.output["is_error"])
        finally:
            await app.aclose()
        restricted, _ = self.tools(context=ToolContext(allowed_tools=frozenset()))
        self.assertEqual(ToolSet((restricted["publish"],)).definitions, ())

    async def test_nested_approval_checks_every_scope_once_before_execution(self):
        """One composed approval binds all ToolSet policies and preserves hook order."""
        events = []
        inner, effects = self.tools(hooks=ToolHooks(before=lambda *args: events.append("inner")))
        outer = ToolSet((inner["publish"],), policy=ask,
                        hooks=ToolHooks(before=lambda *args: events.append("outer")))
        request = (await outer.acheck(call())).approval_request
        self.assertEqual(events, [])
        result = await outer.ainvoke(call(), approval=request.respond(approved=True))
        self.assertFalse(result.is_error)
        self.assertEqual(events, ["outer", "inner"])
        self.assertEqual(effects, ["report"])

    async def test_approval_is_bound_to_runtime_invocation_and_survives_model_restore(self):
        """Serializable approvals survive restore but cannot authorize another Invocation."""
        tools, effects = self.tools()
        runtime = OperatorCallContext(workflow_id="flow", session_id="s", invocation_id="one")
        with operator_call_context(runtime):
            decision = tools.check(call())
        contract = ValueContract.create(ToolDecision, location="permissions")
        restored = contract.restore(contract.to_record(decision))
        approval = restored.approval_request.respond(approved=True)
        with operator_call_context(replace(runtime, invocation_id="two")):
            self.assertTrue((await tools.ainvoke(call(), approval=approval)).is_error)
        with operator_call_context(runtime):
            self.assertFalse((await tools.ainvoke(call(), approval=approval)).is_error)
        self.assertEqual(effects, ["report"])

    async def test_remote_tool_arguments_are_validated_before_permission_and_rpc(self):
        """Remote adapters expose validated context without an unauthorized RPC."""
        effects = []
        class Remote:
            definition = LLMToolDefinition(name="publish", parameters={"type": "object", "properties": {"text": {"type": "string"}},
                                                                       "required": ["text"], "additionalProperties": False})
            async def ainvoke(self, incoming):
                effects.append(ToolContext.current().arguments["text"])
                return LLMToolResult(call_id=incoming.call_id)
        tools = ToolSet((Remote(),), policy=ask)
        proposal = call(arguments='{"text":2}')
        self.assertTrue((await tools.ainvoke(proposal)).is_error)
        request = (await tools.acheck(call())).approval_request
        self.assertEqual(effects, [])
        self.assertFalse((await tools.ainvoke(call(), approval=request.respond(approved=True))).is_error)
        self.assertEqual(effects, ["report"])

    async def test_skill_tool_uses_the_same_parameter_approval_boundary(self):
        """Skill resource access through ToolSet requires matching application approval."""
        from autoagent.agent import MemorySkillSource, Skill, SkillSet
        source = MemorySkillSource("report", {"SKILL.md": "---\nname: report\ndescription: Reports\n---\nInstructions"})
        tools = ToolSet(SkillSet((Skill(source),)).as_tools(), policy=ask)
        proposal = call(name="skill_load", arguments='{"name":"report"}')
        with self.assertRaises(ToolApprovalRequired):
            await tools["skill_load"].ainvoke(proposal)
        request = tools.check(proposal).approval_request
        result = await tools.ainvoke(proposal, approval=request.respond(approved=True))
        self.assertFalse(result.is_error)
        self.assertIn("Instructions", result.content[0].text)

    async def test_mcp_tool_requires_approval_before_any_network_preparation(self):
        """MCP execution and connection setup occur only after ToolSet authorization."""
        from types import SimpleNamespace
        import mcp_types
        from autoagent.agent import MCPTool

        effects = []
        class Client:
            generation = 1
            config = SimpleNamespace(server_id="test")
            async def aprepare(self):
                effects.append("connect")
            async def call_tool(self, name, arguments, **kwargs):
                if callback := kwargs.get("progress_callback"):
                    await callback(1, 2, "working")
                effects.append(arguments["text"])
                return mcp_types.CallToolResult(content=[])
        remote = MCPTool(Client(), mcp_types.Tool(name="publish", input_schema={
            "type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"],
        }))
        tools = ToolSet((remote,), policy=ask)
        request = (await tools.acheck(call())).approval_request
        with self.assertRaises(ToolApprovalRequired):
            await tools.ainvoke(call())
        self.assertEqual(effects, [])
        self.assertFalse((await tools.ainvoke(call(), approval=request.respond(approved=True))).is_error)
        self.assertEqual(effects, ["connect", "report"])
        effects.clear()
        bound = tools["publish"]
        with self.assertRaises(ToolApprovalRequired):
            async for _ in bound.astream(call()):
                self.fail("Unauthorized progress emitted")
        self.assertEqual(effects, [])
        chunks = [chunk async for chunk in bound.astream(call(), approval=request.respond(approved=True))]
        self.assertEqual([chunk.type for chunk in chunks], ["progress", "completed"])
        self.assertFalse(chunks[-1].result.is_error)
        self.assertEqual(effects, ["connect", "report"])
        self.assertEqual(bound.as_operator(streaming=True).contract.stream_chunk.annotation.__name__, "MCPToolStreamChunk")
        effects.clear()
        denied = ToolSet((remote,), policy=lambda *args: ToolDecision(action="deny"))
        chunks = [chunk async for chunk in denied["publish"].astream(call())]
        self.assertTrue(chunks[0].result.is_error)
        self.assertEqual(effects, [])
