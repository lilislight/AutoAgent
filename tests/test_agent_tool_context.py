"""Runtime Tool injection, policy ordering and concurrent context isolation."""

import asyncio
import json
import unittest
from unittest.mock import patch

from autoagent.agent import (
    Tool, ToolContext, ToolHooks, ToolSet, LLMToolCall, LLMToolDefinition, LLMToolResult,
    ToolDecision, ToolExecutionError, SkillDocument, SkillMetadata,
)


def call(name="lookup", *, call_id="1", arguments='{"value":2}'):
    return LLMToolCall(name=name, call_id=call_id, arguments=arguments)


class ToolContextTests(unittest.IsolatedAsyncioTestCase):
    def test_composed_advertisements_copy_each_visible_schema_once(self):
        """Composing bound Tools keeps schema copying linear in advertised names."""
        def lookup(value: int) -> int:
            return value

        names = [f'lookup_{index}' for index in range(12)]
        owner = ToolSet([Tool(lookup, name=name) for name in names])
        middle = ToolSet([owner[name] for name in names])
        outer = ToolSet([middle[name] for name in names])
        with patch.object(LLMToolDefinition, 'model_dump', autospec=True,
                          side_effect=LLMToolDefinition.model_dump) as copies:
            definitions = outer.definitions
        self.assertEqual([item.name for item in definitions], names)
        self.assertEqual(copies.call_count, len(names))
        definitions[0].parameters['properties']['value']['type'] = 'string'
        self.assertEqual(owner.definitions[0].parameters['properties']['value']['type'], 'integer')

    async def test_nested_advertisements_intersect_every_context_boundary(self):
        """Outer, owner, call and inherited restrictions all constrain bound Tools."""
        def lookup(value: int) -> int:
            return value

        names = ('a', 'b', 'c', 'd')
        owner = ToolSet([Tool(lookup, name=name) for name in names],
                        context=ToolContext(allowed_tools={'a', 'b', 'c'}))
        middle = ToolSet([owner[name] for name in names],
                         context=ToolContext(allowed_tools={'b', 'c', 'd'}))
        outer = ToolSet([middle[name] for name in names])
        self.assertEqual([item.name for item in outer.definitions], ['b', 'c'])
        self.assertEqual([item.name for item in outer.definitions_for(
            ToolContext(allowed_tools={'c', 'd'}))], ['c'])

        async def advertise(value: int) -> list[str]:
            return [item.name for item in outer.definitions_for(ToolContext(allowed_tools=set(names)))]

        wrapper = ToolSet([Tool(advertise)], context=ToolContext(allowed_tools={'advertise', 'b'}))
        result = await wrapper.ainvoke(call('advertise'))
        self.assertEqual(json.loads(result.content[0].text), ['b'])

    async def test_context_is_hidden_and_injected_for_sync_and_async_tools(self):
        """Runtime objects never appear in Tool schemas or model-controlled parameters."""
        def lookup(value: int, context: ToolContext) -> str:
            return f"{context.services['db']}:{context.call_id}:{value}"
        async def async_lookup(value: int, context: ToolContext) -> str:
            return lookup(value, context)
        for handler in (lookup, async_lookup):
            tool = Tool(handler, name="lookup")
            self.assertEqual(set(tool.definition.parameters["properties"]), {"value"})
            result = await tool.ainvoke(call(), context=ToolContext(services={"db":"connection"}))
            self.assertEqual(result.content[0].text, "connection:1:2")
            forged = await tool.ainvoke(call(arguments='{"value":2,"context":{}}'))
            self.assertTrue(forged.is_error)
        self.assertIsNone(ToolContext.current())

    async def test_concurrent_calls_keep_distinct_runtime_contexts(self):
        """A shared ToolSet cannot mix users or call identities across tasks."""
        async def lookup(value: int, ctx: ToolContext) -> str:
            await asyncio.sleep(0)
            self.assertIs(ToolContext.current(), ctx)
            return f"{ctx.user_id}:{ctx.call_id}"
        tools = ToolSet([Tool(lookup)])
        first, second = await asyncio.gather(
            tools.ainvoke(call(call_id="a"), context=ToolContext(user_id="alice")),
            tools.ainvoke(call(call_id="b"), context=ToolContext(user_id="bob")))
        self.assertEqual(first.content[0].text, "alice:a")
        self.assertEqual(second.content[0].text, "bob:b")
        self.assertIsNone(ToolContext.current())

    async def test_configured_restrictions_cannot_be_widened_per_call_or_selection(self):
        """Context replacement and ToolSet selection preserve configured restrictions."""
        calls = []
        def lookup(value: int) -> int:
            calls.append(value)
            return value
        tools = ToolSet([Tool(lookup)], context=ToolContext(allowed_tools=frozenset()))
        self.assertEqual(tools.definitions, ())
        self.assertEqual(tools.definitions_for(ToolContext(allowed_tools=frozenset({"lookup"}))), ())
        for selected in (tools, tools.select(include=["lookup"])):
            result = await selected.ainvoke(call(), context=ToolContext(allowed_tools=frozenset({"lookup"})))
            self.assertTrue(result.is_error)
        self.assertEqual(calls, [])

    async def test_hooks_order_and_policy_denial_precede_tool_side_effects(self):
        """Policies run before hooks and execution, including asynchronous decisions."""
        events = []
        def lookup(value: int) -> int:
            events.append("execute")
            return value
        async def policy(call, ctx):
            events.append("policy")
            if ctx.user_id != "alice":
                return ToolDecision(action="deny", reason="Denied")
            return ToolDecision(action="allow")
        def before(call, ctx):
            events.append("before")
        async def after(call, result, ctx):
            events.append("after")
        tools = ToolSet([Tool(lookup)], policy=policy, hooks=ToolHooks(before=before, after=after))
        result = await tools.ainvoke(call(), context=ToolContext(user_id="alice"))
        self.assertFalse(result.is_error)
        self.assertEqual(events, ["policy", "before", "execute", "after"])
        events.clear()
        self.assertTrue((await tools.ainvoke(call())).is_error)
        self.assertEqual(events, ["policy"])

    async def test_error_hook_and_cancellation_keep_failure_boundaries(self):
        """Unexpected execution invokes the error hook; external cancellation stays cancellation."""
        errors = []
        async def lookup(value: int) -> int:
            if value == 2:
                raise RuntimeError("failure")
            await asyncio.Future()
        def on_error(call, error, ctx):
            errors.append(type(error))
        tools = ToolSet([Tool(lookup)], hooks=ToolHooks(on_error=on_error))
        with self.assertRaises(ToolExecutionError):
            await tools.ainvoke(call())
        self.assertEqual(errors, [ToolExecutionError])
        task = asyncio.create_task(tools.ainvoke(call(arguments='{"value":3}')))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(errors), 1)
        self.assertIsNone(ToolContext.current())

    async def test_remote_executables_receive_context_without_changing_their_protocol(self):
        """Third-party executable adapters can read call context through the shared boundary."""
        from autoagent.agent import LLMToolDefinition
        class Remote:
            definition = LLMToolDefinition(name="lookup", parameters={"type":"object"})
            async def ainvoke(self, call):
                return LLMToolResult(call_id=call.call_id, structured_content={"user":ToolContext.current().user_id})
        result = await ToolSet([Remote()]).ainvoke(call(), context=ToolContext(user_id="alice"))
        self.assertEqual(result.structured_content, {"user":"alice"})

    async def test_skill_restrictions_intersect_permissions_and_require_pattern_bindings(self):
        """Skill declarations narrow capabilities and cannot infer shell permissions."""
        document = SkillDocument(metadata=SkillMetadata(name="example", description="Test", allowed_tools="lookup Bash(python:*)"),
                                 instructions="", revision="a"*64)
        context = ToolContext(allowed_tools=frozenset({"lookup", "read"}))
        with self.assertRaises(ValueError):
            document.restrict_tools(context)
        restricted = document.restrict_tools(context, bindings={"Bash(python:*)":["execute_python"]})
        self.assertEqual(restricted.allowed_tools, frozenset({"lookup"}))

    def test_multiple_or_defaulted_context_parameters_are_rejected(self):
        """Context injection has one unambiguous required parameter."""
        def duplicate(first: ToolContext, second: ToolContext) -> int:
            return 0
        def defaulted(ctx: ToolContext = None) -> int:
            return 0
        for handler in (duplicate, defaulted):
            with self.assertRaises(TypeError):
                Tool(handler)

    async def test_core_context_and_sync_hooks_keep_work_on_core_executor(self):
        """Context-enabled sync Tools keep Core worker and cancellation ownership."""
        import threading
        from unittest.mock import patch
        from autoagent import AutoAgentApp, Node, Workflow

        events = []
        def lookup(value: int, ctx: ToolContext) -> str:
            self.assertEqual(ctx.user_id, "alice")
            return threading.current_thread().name
        def before(call, ctx):
            events.append((ctx.call_id, threading.current_thread().name))
        tools = ToolSet([Tool(lookup)], context=ToolContext(user_id="alice"), hooks=ToolHooks(before=before))
        app = AutoAgentApp()
        try:
            with patch("asyncio.to_thread", side_effect=AssertionError("Unexpected executor")):
                result = await app.ainvoke(Workflow("context", nodes=[Node("tool", tools.as_operator())]), call())
            self.assertEqual(result.status, "completed")
            self.assertTrue(result.output["content"][0]["text"].startswith("autoagent-operator-"))
            self.assertEqual(events[0][0], "1")
            self.assertTrue(events[0][1].startswith("autoagent-operator-"))
        finally:
            await app.aclose()

    async def test_core_supports_async_hooks_for_sync_tools_on_owned_workers(self):
        """Async hooks run before synchronous Tools dispatched through the Core bridge."""
        import threading
        from autoagent import AutoAgentApp, Node, Workflow
        events = []
        def lookup(value: int) -> str:
            events.append("execute")
            return threading.current_thread().name
        async def before(call, ctx):
            events.append("before")
        tools = ToolSet([Tool(lookup)], hooks=ToolHooks(before=before))
        app = AutoAgentApp()
        try:
            with patch("asyncio.to_thread", side_effect=AssertionError("Unexpected executor")):
                result = await app.ainvoke(Workflow("async-hooks", nodes=[Node("tool", tools.as_operator())]), call())
            self.assertEqual(result.status, "completed", result.error)
            self.assertTrue(result.output["content"][0]["text"].startswith("autoagent-operator-"))
            self.assertEqual(events, ["before", "execute"])
        finally:
            await app.aclose()

    async def test_nested_explicit_context_cannot_bypass_parent_restriction(self):
        """A nested Tool cannot widen its parent's permissions by replacing context."""
        def forbidden(value: int) -> int:
            self.fail("Forbidden Tool executed")
        inner = Tool(forbidden)
        async def lookup(value: int) -> bool:
            result = await inner.ainvoke(call("forbidden"), context=ToolContext())
            return result.is_error
        tools = ToolSet([Tool(lookup)], context=ToolContext(allowed_tools=frozenset({"lookup"})))
        self.assertEqual((await tools.ainvoke(call())).content[0].text, "true")
