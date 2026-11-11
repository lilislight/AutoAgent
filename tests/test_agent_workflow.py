"""Complete Agent graph, Host settings and protected Tool effects."""

import asyncio
from contextlib import asynccontextmanager
import inspect
from pathlib import Path
import tempfile
import unittest

from autoagent.agent import (
    ContextPolicy, always_compact, AgentAccess, AgentApprovalRequest, AgentApprovalResponse, AgentInput,
    AgentModelSettings, AgentResourceRegistry, AgentHostSettings, AgentSessionSettings, LLM_CALL, LLMMessage, LLMSelection,
    LLMReasoning, LLMResponse, LLMStreamChunk, LLMToolCall, LLMToolResult,
    LLMUsage, MemorySkillSource, Skill, TextContent, Tool, ToolDecision, ToolSet, agent_workflow, llm_call_operator,
)
from autoagent.agent.context import restore
from autoagent.core import WorkflowCompiler
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.host.persistence import ConflictError
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


def settings(*, model="model-a", mode="policy", access=None, provider="offline", streaming=False):
    return AgentHostSettings(model=AgentModelSettings(provider=provider, model=model,
        streaming=streaming, max_output_tokens=512), approval_mode=mode,
        access=access or AgentAccess())


def answer(text="Done"):
    return LLMResponse(model="offline", finish_reason="stop", usage=LLMUsage(input_tokens=10, output_tokens=5, total_tokens=15),
        output=(LLMMessage(role="assistant", content=(TextContent(text=text),)),))


def calls(*names):
    return LLMResponse(model="offline", finish_reason="tool_calls",
        output=(LLMReasoning(text="Choose tools"), *(LLMToolCall(name=name,
            call_id=f"call-{index}", arguments='{"text":"hello"}') for index, name in enumerate(names))))


class ScriptedProvider:
    provider_id = "offline"

    def __init__(self, *steps):
        self.steps, self.requests, self.streams = iter(steps), [], 0

    async def ainvoke(self, request):
        self.requests.append(request)
        step = next(self.steps)
        value = step(request) if callable(step) else step
        return await value if inspect.isawaitable(value) else value

    async def astream(self, request):
        self.streams += 1
        yield LLMStreamChunk(type="completed", response=await self.ainvoke(request))


class AgentGraphTests(unittest.TestCase):
    def test_complete_graph_and_unique_back_edge(self):
        """The full eleven-Node, fifteen-Edge graph compiles with one Back Edge."""
        graph = WorkflowCompiler().compile_or_raise(agent_workflow())
        self.assertEqual(len(graph.nodes), 11)
        self.assertEqual(len(graph.edges), 15)
        self.assertEqual([edge for loop in graph.loop_regions for edge in loop.back_edge_ids], ["round_back"])

    def test_resource_versions_cannot_be_reused_after_removal(self):
        """Unregistering and re-registering cannot revive an old advertised binding."""
        registry = AgentResourceRegistry()
        def publish(text: str) -> str:
            return text
        registry.register_tool(Tool(publish), version="1")
        registry.unregister("tool", "publish")
        with self.assertRaises(ValueError):
            registry.register_tool(Tool(publish), version="1")


class AgentWorkflowContracts:
    async def database_url(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return "sqlite+aiosqlite:///" + str(Path(temporary.name) / "runtime.db")

    @asynccontextmanager
    async def host(self, provider, *, defaults=None, tools=(), policies=(), url=None, **options):
        @asynccontextmanager
        async def lifespan(setup):
            service = setup.agent_context
            service.resources.register_provider("offline", provider)
            service.set_defaults(defaults or settings())
            for tool in tools:
                service.resources.register_tool(tool, workflows={"agent"})
            for name, policy in policies:
                service.resources.register_policy(name, policy)
            yield
        async with AutoAgentHost(workflows=[agent_workflow(**options)], lifespan=lifespan,
            settings=HostSettings(database_url=url or await self.database_url()), environment={}) as host:
            yield host

    async def run_agent(self, host, *, session_id=None, session_context=None):
        return await host.join(await host.execute("agent", AgentInput(message="Help"),
            session_id=session_id, session_context=session_context))

    def publish(self, effects, *, policy=None):
        def publish(text: str) -> str:
            effects.append(text)
            return text
        tool = Tool(publish)
        return ToolSet((tool,), policy=policy)["publish"] if policy else tool

    async def test_final_answer_and_per_call_settings(self):
        """A final response finishes without executing Tools and retains streaming overrides."""
        provider = ScriptedProvider(answer())
        async with self.host(provider, llm_options={"streaming": True, "max_output_tokens": 77}) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(result.output["content"], [{"type": "text", "text": "Done"}])
            self.assertEqual(set(result.output), {"content"})
            response = restore(LLMResponse, host._runtime.repository.state(result.session_id).invocation.context["agent"]["response"])
            self.assertEqual(response.usage.total_tokens, 15)
            self.assertEqual(provider.streams, 1)
            self.assertEqual(provider.requests[0].max_output_tokens, 77)
            snapshot = await host.agent_context.get_snapshot(result.invocation_id)
            self.assertEqual(snapshot.settings.model.max_output_tokens, 512)

    async def test_custom_capability_and_llm_resolvers_remain_available(self):
        """An application resolver can select a model when no Session model is configured."""
        provider, requests = ScriptedProvider(answer()), []
        async def selection(request):
            self.assertIsNone(request.model)
            return LLMSelection(provider, "resolver-selected")
        @asynccontextmanager
        async def lifespan(setup):
            setup.agent_context.set_defaults(AgentHostSettings())
            operator = llm_call_operator(resolver=selection, id="application.model")
            setup.register_operator(operator, capability_id=LLM_CALL.id)
            async def resolver(capability, request):
                requests.append(request)
                return operator
            setup.register_capability_resolver(LLM_CALL.id, resolver)
            yield
        async with AutoAgentHost(workflows=[agent_workflow()], lifespan=lifespan,
            settings=HostSettings(database_url=await self.database_url()), environment={}) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(len(requests), 1)
            self.assertEqual(provider.requests[0].model, "resolver-selected")

    async def test_parallel_allow_approval_and_errors_join_in_call_order(self):
        """Allowed Tools finish while a batch waits; all observations join in call order."""
        effects = []
        def read(text: str) -> str:
            effects.append("read")
            return text
        def second(text: str) -> str:
            effects.append("second")
            return text
        response = calls("publish", "read", "second", "missing")
        provider = ScriptedProvider(response, answer())
        access = AgentAccess(rules={"publish": "ask", "second": "ask"})
        async with self.host(provider, defaults=settings(access=access),
            tools=(self.publish(effects), Tool(read), Tool(second))) as host:
            waiting = await self.run_agent(host)
            self.assertEqual(waiting.status, "waiting", waiting.error)
            self.assertEqual(effects, ["read"])
            request = restore(AgentApprovalRequest, waiting.waits[0].request)
            self.assertEqual(len(request.requests), 2)
            with self.assertRaises(ValueError):
                await host.respond(waiting.invocation_id, waiting.waits[0].id,
                    AgentApprovalResponse(decisions=()).model_dump(mode="json"))
            self.assertEqual((await host.get_execution(waiting.invocation_id)).status, "waiting")
            reply = AgentApprovalResponse(decisions=tuple(value.respond(approved=index == 0, reason="Keep this private" if index else None)
                for index, value in enumerate(request.requests)))
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply.model_dump(mode="json"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(effects, ["read", "hello"])
            results = [item for item in provider.requests[-1].input if isinstance(item, LLMToolResult)]
            self.assertEqual([item.call_id for item in results], [f"call-{i}" for i in range(4)])
            self.assertEqual([item.is_error for item in results], [False, False, True, True])
            self.assertEqual(results[2].structured_content["error"]["kind"], "tool_approval_denied")
            self.assertEqual(results[2].structured_content["error"]["reason"], "Keep this private")
            assistant_text = [part.text for item in provider.requests[-1].input
                if isinstance(item, LLMMessage) and item.role == "assistant"
                for part in item.content if isinstance(part, TextContent)]
            self.assertTrue(any(response.output[0].text in text for text in assistant_text))
            self.assertFalse(any(isinstance(item, LLMReasoning) for item in provider.requests[-1].input))
            self.assertIsInstance(response.output[0], LLMReasoning)

    async def test_four_modes_preserve_hard_deny(self):
        """Approval modes transform Ask only after combining nested Tool and Session rules."""
        for mode, expected in (("policy", "waiting"), ("always_ask", "waiting"),
                               ("never_ask", "completed"), ("bypass", "completed")):
            effects = []
            tool = self.publish(effects, policy=lambda *args: ToolDecision(action="ask"))
            provider = ScriptedProvider(calls("publish"), answer())
            async with self.host(provider, defaults=settings(mode=mode), tools=(tool,)) as host:
                result = await self.run_agent(host)
                self.assertEqual(result.status, expected, result.error)
                self.assertEqual(effects, ["hello"] if mode == "bypass" else [])
            effects = []
            provider = ScriptedProvider(calls("publish"), answer())
            denied = self.publish(effects, policy=lambda *args: ToolDecision(action="deny"))
            async with self.host(provider, defaults=settings(mode=mode), tools=(denied,)) as host:
                result = await self.run_agent(host)
                self.assertEqual(result.status, "completed", result.error)
                self.assertEqual(effects, [])

    async def test_settings_update_does_not_change_running_invocation(self):
        """Updates remain desired settings until the next Invocation in this Session."""
        effects = []
        provider = ScriptedProvider(calls("publish"), answer(), answer("Next turn"))
        async with self.host(provider, defaults=settings(access=AgentAccess(rules={"publish": "ask"})),
            tools=(self.publish(effects),)) as host:
            waiting = await self.run_agent(host, session_id="conversation")
            old = await host.agent_context.get_snapshot(waiting.invocation_id)
            record = await host.agent_context.configure("conversation", settings(model="model-b").session_defaults(), expected_version=1)
            self.assertEqual(record.version, 2)
            with self.assertRaises(ConflictError):
                await host.agent_context.configure("conversation", settings().session_defaults(), expected_version=0)
            request = restore(AgentApprovalRequest, waiting.waits[0].request)
            reply = AgentApprovalResponse(decisions=tuple(item.respond(approved=True) for item in request.requests))
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply.model_dump(mode="json"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual([request.model for request in provider.requests], ["model-a", "model-a"])
            self.assertEqual((await host.agent_context.get_snapshot(waiting.invocation_id)).snapshot_id, old.snapshot_id)
            next_turn = await self.run_agent(host, session_id="conversation")
            self.assertEqual(next_turn.status, "completed", next_turn.error)
            self.assertEqual(provider.requests[-1].model, "model-b")
            self.assertEqual(sum(isinstance(item, LLMMessage) and item.role == "user"
                for item in provider.requests[-1].input), 2)

    async def test_removal_or_replacement_after_advertisement_prevents_effects(self):
        """A Tool seen by the model cannot execute a removed or replacement implementation."""
        for replace in (False, True):
            effects = []
            provider = ScriptedProvider(calls("publish"), answer())
            async with self.host(provider, defaults=settings(access=AgentAccess(rules={"publish": "ask"})),
                tools=(self.publish(effects),)) as host:
                waiting = await self.run_agent(host)
                if replace:
                    host.agent_context.resources.register_tool(self.publish(effects), version="2", replace=True, workflows={"agent"})
                else:
                    host.agent_context.resources.unregister("tool", "publish")
                async with asyncio.timeout(10):
                    while True:
                        result = await host.join(waiting.invocation_id)
                        if result.status == "completed":
                            break
                        await asyncio.sleep(.02)
                self.assertEqual(result.status, "completed", result.error)
                self.assertEqual(effects, [])
                observed = next(item for item in provider.requests[-1].input if isinstance(item, LLMToolResult))
                self.assertEqual(observed.structured_content["error"]["kind"],
                    "tool_binding_changed" if replace else "tool_unavailable")

    async def test_restart_keeps_snapshot_but_new_turn_adopts_persisted_settings(self):
        """Restoring a pending Wait retains its stage and accepts its original exact approval."""
        url, effects = await self.database_url(), []
        async with self.host(ScriptedProvider(calls("publish")), url=url,
            defaults=settings(access=AgentAccess(rules={"publish": "ask"})), tools=(self.publish(effects),)) as host:
            waiting = await self.run_agent(host)
            original = await host.agent_context.get_snapshot(waiting.invocation_id)
            await host.agent_context.configure(waiting.session_id, settings(model="model-b").session_defaults())
            # Reopening an Invocation cannot create another settings stage
            # or replace its original model binding.
            staged = await host.agent_context.activate(waiting.session_id, waiting.invocation_id,
                reason="resume", activation_id="unaccepted-resume")
            self.assertEqual(staged["model"]["model"], original.settings.model.model)
        provider = ScriptedProvider(answer(), answer())
        async with self.host(provider, url=url, defaults=settings(access=AgentAccess(rules={"publish": "ask"})), tools=(self.publish(effects),)) as host:
            await host.agent_context.configure(waiting.session_id, settings(model="model-b").session_defaults())
            self.assertEqual((await host.agent_context.get_settings(waiting.session_id)).settings.model.model, "model-b")
            self.assertEqual((await host.agent_context.get_snapshot(waiting.invocation_id)).snapshot_id, original.snapshot_id)
            request = restore(AgentApprovalRequest, waiting.waits[0].request)
            reply = AgentApprovalResponse(decisions=tuple(item.respond(approved=True) for item in request.requests))
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply.model_dump(mode="json"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(effects, ["hello"])
            self.assertEqual(provider.requests[-1].model, "model-a")
            result = await self.run_agent(host, session_id=waiting.session_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(provider.requests[-1].model, "model-b")

    async def test_explicit_pause_resume_preserves_model_and_uses_live_approval(self):
        """Manual resume keeps the Invocation model while applying the latest approval mode."""
        effects = []
        provider = ScriptedProvider(calls("publish"), answer())
        async with self.host(provider, defaults=settings(access=AgentAccess(rules={"publish": "ask"})),
            tools=(self.publish(effects),)) as host:
            waiting = await self.run_agent(host)
            await host.pause(waiting.invocation_id)
            await host.agent_context.configure(waiting.session_id, settings(model="model-b", mode="bypass").session_defaults())
            resumed = await host.recover(waiting.invocation_id)
            self.assertEqual((await host.agent_context.get_snapshot(waiting.invocation_id)).epoch, 1)
            if resumed.waits:
                request = restore(AgentApprovalRequest, resumed.waits[0].request)
                reply = AgentApprovalResponse(decisions=tuple(item.respond(approved=True) for item in request.requests))
                await host.respond(waiting.invocation_id, resumed.waits[0].id, reply.model_dump(mode="json"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(effects, ["hello"])
            self.assertEqual(provider.requests[-1].model, "model-a")

    async def test_compression_node_is_observable_and_preserves_original_conversation(self):
        """Compression changes the request projection while preserving canonical history for later turns."""
        provider = ScriptedProvider(answer("Summary of earlier turn"), answer(), answer())
        old = LLMMessage(role="user", content=(TextContent(text="Old goal " * 30),))
        previous = LLMMessage(role="assistant", content=(TextContent(text="Old answer"),))
        async with self.host(provider, context_policy=ContextPolicy(context_window=4096, trigger=always_compact)) as host:
            result = await self.run_agent(host, session_context={"agent_conversation": {
                "items": [old.model_dump(mode="json"), previous.model_dump(mode="json")]}})
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual([item.routing["agent_role"] for item in provider.requests], ["compaction", "agent"])
            self.assertNotIn(old, provider.requests[-1].input)
            self.assertTrue(any("Summary of earlier turn" in str(item) for item in provider.requests[-1].input))
            # Raising the threshold changes only the projection policy, not the
            # original durable Conversation written by Complete Round.
            await host._persistence.flush()
            events = await host._persistence.read_recovery_bundle(result.invocation_id, full_history=True)
            self.assertIn("Old goal", str(events))

    async def test_dynamic_tools_and_skills_refresh_on_next_request(self):
        """The next request sees newly registered Tools and enabled Skill metadata."""
        effects = []
        provider = ScriptedProvider(calls("publish"), answer())
        async with self.host(provider, defaults=settings(access=AgentAccess(rules={"publish": "ask"})),
            tools=(self.publish(effects),)) as host:
            waiting = await self.run_agent(host)
            def lookup(text: str) -> str:
                return text
            host.agent_context.resources.register_tool(Tool(lookup), workflows={"agent"})
            skill = Skill.from_source(MemorySkillSource("research", {"SKILL.md":
                "---\nname: research\ndescription: Research helper\n---\nRead sources carefully.\n"}))
            host.agent_context.resources.register_skill(skill, workflows={"agent"})
            host.agent_context.resources.set_enabled("skill", "research", False)
            self.assertEqual(host.agent_context.resources.entries("skill"), ())
            host.agent_context.resources.set_enabled("skill", "research", True)
            request = restore(AgentApprovalRequest, waiting.waits[0].request)
            reply = AgentApprovalResponse(decisions=tuple(item.respond(approved=True) for item in request.requests))
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply.model_dump(mode="json"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertNotIn("lookup", [item.name for item in provider.requests[0].tools])
            self.assertIn("lookup", [item.name for item in provider.requests[-1].tools])
            self.assertIn("Research helper", str(provider.requests[-1].input))

    async def test_binding_change_during_async_policy_cannot_execute(self):
        """A replacement while an async permission check yields is rejected before effects."""
        effects, checked = [], [0]
        provider = ScriptedProvider(calls("publish"), answer())
        holder = {}
        async def policy(call, context):
            checked[0] += 1
            if checked[0] == 2:
                holder["host"].agent_context.resources.register_tool(self.publish(effects), version="2", replace=True, workflows={"agent"})
            return ToolDecision(action="allow")
        async with self.host(provider, defaults=settings(access=AgentAccess(policy="guard")),
            tools=(self.publish(effects),), policies=(("guard", policy),)) as host:
            holder["host"] = host
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(effects, [])
            observed = next(item for item in provider.requests[-1].input if isinstance(item, LLMToolResult))
            self.assertEqual(observed.structured_content["error"]["kind"], "tool_binding_changed")

    async def test_invalid_arguments_and_round_limit_do_not_run_tools(self):
        """Argument errors become observations and an exhausted round limit prevents side effects."""
        effects = []
        malformed = LLMResponse(model="offline", finish_reason="tool_calls", output=(
            LLMToolCall(name="publish", call_id="bad", arguments='{"text":1}'),))
        provider = ScriptedProvider(malformed, answer())
        async with self.host(provider, tools=(self.publish(effects),)) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            observed = next(item for item in provider.requests[-1].input if isinstance(item, LLMToolResult))
            self.assertEqual(observed.structured_content["error"]["kind"], "invalid_arguments")
        async with self.host(ScriptedProvider(calls("publish")), tools=(self.publish(effects),), max_rounds=1) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "failed")
            self.assertIn("round limit", result.error.message)
        self.assertEqual(effects, [])

    async def test_empty_or_incomplete_final_response_does_not_finish(self):
        """Empty, reasoning-only and truncated outputs cannot report a completed Agent turn."""
        responses = (LLMResponse(model="offline", finish_reason="stop", output=()),
            LLMResponse(model="offline", finish_reason="stop", output=(LLMReasoning(text="thinking"),)),
            answer().model_copy(update={"finish_reason": "length"}))
        for response in responses:
            async with self.host(ScriptedProvider(response)) as host:
                result = await self.run_agent(host)
                self.assertEqual(result.status, "failed")
                self.assertIn("response", result.error.message)

    async def test_approval_is_bound_to_one_model_request(self):
        """Repeated provider call IDs in later rounds cannot reuse an earlier approval reply."""
        effects = []
        provider = ScriptedProvider(calls("publish"), calls("publish"), answer())
        async with self.host(provider, defaults=settings(access=AgentAccess(rules={"publish": "ask"})),
            tools=(self.publish(effects),)) as host:
            first = await self.run_agent(host)
            request = restore(AgentApprovalRequest, first.waits[0].request)
            reply = AgentApprovalResponse(decisions=tuple(item.respond(approved=True) for item in request.requests))
            await host.respond(first.invocation_id, first.waits[0].id, reply.model_dump(mode="json"))
            second = await host.join(first.invocation_id)
            self.assertEqual(second.status, "waiting", second.error)
            current = restore(AgentApprovalRequest, second.waits[0].request)
            self.assertNotEqual(current.requests[0].id, request.requests[0].id)
            with self.assertRaises(ValueError):
                await host.respond(first.invocation_id, second.waits[0].id, reply.model_dump(mode="json"))
            denied = AgentApprovalResponse(decisions=tuple(item.respond(approved=False) for item in current.requests))
            await host.respond(first.invocation_id, second.waits[0].id, denied.model_dump(mode="json"))
            self.assertEqual((await host.join(first.invocation_id)).status, "completed")
            self.assertEqual(effects, ["hello"])


class SQLiteAgentWorkflowTests(AgentWorkflowContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLAgentWorkflowTests(AgentWorkflowContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
