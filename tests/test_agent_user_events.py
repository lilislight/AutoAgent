"""Agent presentation contracts, live progress and committed history snapshots."""

import asyncio
from contextlib import asynccontextmanager
import json
import unittest
from unittest.mock import patch

from autoagent.agent import (
    AGENT_EVENT_PAYLOADS, AgentAccess, AgentApprovalRequest, AgentApprovalResponse,
    AgentInput, AgentNodePolicy, AgentSignalMessage, AgentWorkflowPolicies, ContextPolicy, Conversation,
    LLMHistoryCompactor, LLMMessage, LLMNativeData, LLMProviderItem, LLMReasoning,
    LLMResponse, LLMStreamChunk, LLMToolCall, SubagentsConfig,
    SubagentTarget, TextContent, Tool, ToolDecision, ToolSet, always_compact, agent_workflow,
    CompactionResult,
)
from autoagent.agent.context import restore
from autoagent.core import RetryPolicy, WorkflowCompiler
from autoagent.host import AutoAgentHost, HostSettings
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import AgentWorkflowContracts, ScriptedProvider, answer, calls, settings
from tests.test_agent_ask_user import ask_call, reply, tool_response
from tests.test_agent_subagents import RoutedProvider, call, response


async def through(stream, kind, *, state=None):
    events = []
    async with asyncio.timeout(10):
        async for event in stream:
            events.append(event)
            if event.kind == kind and (state is None or event.payload.get("state") == state):
                return events
    raise AssertionError("Expected User Event did not arrive.")


def display_history(text):
    return LLMMessage(role="user", content=(TextContent(text=text),))


class AgentEventBuildTests(unittest.TestCase):
    def test_explicit_switch_preserves_graph_and_disables_all_mappers(self):
        """Events are opt-in and add no Nodes or Edges to the Agent graph."""
        off = WorkflowCompiler().compile_or_raise(agent_workflow())
        on = WorkflowCompiler().compile_or_raise(agent_workflow(user_events=True))
        self.assertEqual(len(off.nodes), len(on.nodes))
        self.assertEqual(len(off.edges), len(on.edges))
        self.assertTrue(all(node.user_events is None for node in off.nodes))
        self.assertTrue(all(node.user_events is not None for node in on.nodes))
        self.assertNotEqual(off.workflow_revision_id, on.workflow_revision_id)
        with self.assertRaises(ValueError):
            agent_workflow(user_events="true")


class AgentEventContracts:
    database_url = AgentWorkflowContracts.database_url
    run_agent = AgentWorkflowContracts.run_agent

    @asynccontextmanager
    async def host(self, provider, *, defaults=None, tools=(), **options):
        options.setdefault("user_events", True)
        @asynccontextmanager
        async def lifespan(setup):
            setup.agent_context.resources.register_provider("offline", provider)
            setup.agent_context.set_defaults(defaults or settings())
            yield
        async with AutoAgentHost(workflows=[agent_workflow(tools=tools, **options)], lifespan=lifespan,
            settings=HostSettings(database_url=await self.database_url()), environment={}) as host:
            yield host

    async def history(self, host, invocation_id):
        records = (await host.get_user_events(invocation_id, limit=1000)).items
        self.assertEqual([item["sequence"] for item in records], list(range(1, len(records) + 1)))
        for item in records:
            self.validate_event(item)
            self.assertTrue(item["persist"])
        return records

    def validate_event(self, record):
        payload = AGENT_EVENT_PAYLOADS[record["kind"]].model_validate_json(json.dumps(record["payload"]))
        self.assertEqual(payload.v, 1)

    async def test_disabled_events_leave_history_empty(self):
        """An uninstrumented Agent still completes without storing presentation content."""
        async with self.host(ScriptedProvider(answer()), user_events=False) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(await self.history(host, result.invocation_id), ())

    async def test_stream_deltas_are_live_and_final_response_is_self_contained(self):
        """Reload needs one full response and never exposes native or encrypted replay data."""
        native = LLMNativeData(provider_id="offline", protocol="openai.responses", payload={"secret": "hidden"})
        terminal = LLMResponse(model="offline", finish_reason="stop", output=(
            LLMReasoning(text="Plan", native=native, encrypted_content="ciphertext"),
            LLMMessage(role="assistant", content=(TextContent(text="", native=native), TextContent(text="Done", native=native))),
            LLMProviderItem(provider_type="opaque", native=native)))
        class Streaming(ScriptedProvider):
            async def astream(self, request):
                yield LLMStreamChunk(type="delta", item_key="reasoning", kind="reasoning", delta="Plan")
                yield LLMStreamChunk(type="delta", item_key="text", content_index=0, kind="text", delta="Do")
                yield LLMStreamChunk(type="delta", item_key="text", content_index=0, kind="text", delta="ne")
                yield LLMStreamChunk(type="completed", response=await self.ainvoke(request))
        async with self.host(Streaming(terminal), llm_options={"streaming": True}) as host:
            async with host.subscribe_user_events() as stream:
                result = await self.run_agent(host)
                self.assertEqual(result.status, "completed", result.error)
                live = await through(stream, "agent.completed")
            for event in live:
                self.validate_event(event.to_record())
            deltas = [event for event in live if event.kind == "agent.response.delta"]
            self.assertEqual([event.payload["delta"] for event in deltas], ["Plan", "Do", "ne"])
            self.assertTrue(all(not event.persist and event.sequence is None for event in deltas))
            records = await self.history(host, result.invocation_id)
            self.assertEqual([record["kind"] for record in records], ["agent.messages", "agent.response", "agent.completed"])
            payload = records[1]["payload"]
            self.assertEqual([block["type"] for block in payload["blocks"]], ["reasoning", "text"])
            self.assertEqual(payload["blocks"][1]["text"], "Done")
            self.assertEqual(records[-1]["payload"]["final_response_id"], payload["response_id"])
            self.assertEqual(records[-1]["payload"]["rounds"], 1)
            self.assertNotIn("hidden", json.dumps(records))
            self.assertNotIn("ciphertext", json.dumps(records))

    async def test_parallel_tools_commit_full_snapshots_while_approval_is_pending(self):
        """Allowed, denied, malformed and approved calls each have one final Tool snapshot."""
        effects = []
        def read(text: str) -> dict[str, str]:
            effects.append(text)
            return {"native": "business data", "value": text}
        def save(text: str) -> str:
            effects.append("saved:" + text)
            return text
        def denied(text: str) -> str:
            raise AssertionError("Denied Tool executed")
        request = LLMResponse(model="offline", finish_reason="tool_calls", output=(
            LLMToolCall(call_id="read", name="read", arguments='{"text":"ok"}'),
            LLMToolCall(call_id="save", name="save", arguments='{"text":"note"}'),
            LLMToolCall(call_id="deny", name="denied", arguments='{"text":"no"}'),
            LLMToolCall(call_id="bad", name="read", arguments='{"text":'),
        ))
        provider = ScriptedProvider(request, answer())
        denied_tool = ToolSet((Tool(denied),), policy=lambda *args: ToolDecision(action="deny"))["denied"]
        async with self.host(provider, defaults=settings(access=AgentAccess(rules={"save": "ask"})),
                             tools=(Tool(read), Tool(save), denied_tool)) as host:
            async with host.subscribe_user_events() as stream:
                waiting = await self.run_agent(host)
                self.assertEqual(waiting.status, "waiting", waiting.error)
                first = await self.history(host, waiting.invocation_id)
                tools = [tool for event in first if event["kind"] == "agent.tools" for tool in event["payload"]["tools"]]
                self.assertEqual({item["call_id"] for item in tools}, {"read", "deny", "bad"})
                self.assertEqual(next(item for item in tools if item["call_id"] == "deny")["state"], "denied")
                self.assertEqual(effects, ["ok"])
                approval = restore(AgentApprovalRequest, waiting.waits[0].request)
                await host.respond(waiting.invocation_id, waiting.waits[0].id,
                    AgentApprovalResponse(decisions=tuple(item.respond(approved=False) for item in approval.requests)).model_dump(mode="json"))
                done = await host.join(waiting.invocation_id)
                self.assertEqual(done.status, "completed", done.error)
                live = await through(stream, "agent.completed")
            self.assertTrue(any(e.kind == "agent.tools" and any(t["state"] == "running" for t in e.payload["tools"]) for e in live))
            records = await self.history(host, done.invocation_id)
            tools = [tool for event in records if event["kind"] == "agent.tools" for tool in event["payload"]["tools"]]
            self.assertEqual(len(tools), 4)
            self.assertEqual(next(t for t in tools if t["call_id"] == "save")["state"], "denied")
            self.assertEqual(next(t for t in tools if t["call_id"] == "read")["result"]["structured_content"]["native"], "business data")
            interactions = [event for event in records if event["kind"] == "agent.interaction"]
            self.assertEqual(len(interactions), 1)
            self.assertEqual(interactions[0]["payload"]["resolution"]["outcome"], "answered")
            references = next(event for event in records if event["kind"] == "agent.response")["payload"]["blocks"]
            self.assertEqual({block["tool_id"] for block in references}, {item["tool_id"] for item in tools})
            self.assertTrue(all("arguments" not in block for block in references))

    async def test_mapped_questions_resolve_individually_without_duplicate_tool_cards(self):
        """Each resolved Wait saves its original question and answer before Map aggregation."""
        provider = ScriptedProvider(tool_response(ask_call("a"), ask_call("b")), answer())
        async with self.host(provider, include_ask_user=True, max_parallelism=1) as host:
            waiting = await self.run_agent(host)
            self.assertEqual(waiting.status, "waiting", waiting.error)
            waits = {wait.request["call_id"]: wait.id for wait in waiting.waits}
            await host.respond(waiting.invocation_id, waits["b"], reply(text="Second"))
            await host.join(waiting.invocation_id)
            partial = await self.history(host, waiting.invocation_id)
            interaction = next(event["payload"] for event in partial if event["kind"] == "agent.interaction")
            self.assertEqual(interaction["interaction_id"], waits["b"])
            self.assertEqual(interaction["request"]["questions"][0]["text"], "What should I do?")
            self.assertEqual(interaction["resolution"]["response"]["answers"][0]["text"], "Second")
            await host.respond(waiting.invocation_id, waits["a"], reply(text="First"))
            done = await host.join(waiting.invocation_id)
            self.assertEqual(done.status, "completed", done.error)
            records = await self.history(host, done.invocation_id)
            self.assertEqual(sum(event["kind"] == "agent.interaction" for event in records), 2)
            self.assertFalse(any(event["kind"] == "agent.tools" for event in records))

    async def test_cancelled_question_is_a_resolved_interaction(self):
        """Cancellation closes the interaction without inventing an answer or final response."""
        async with self.host(ScriptedProvider(tool_response(ask_call())), include_ask_user=True) as host:
            waiting = await self.run_agent(host)
            await host.cancel(waiting.invocation_id)
            records = await self.history(host, waiting.invocation_id)
            interaction = next(event["payload"] for event in records if event["kind"] == "agent.interaction")
            self.assertEqual(interaction["resolution"]["outcome"], "cancelled")
            self.assertIsNone(interaction["resolution"]["response"])
            self.assertFalse(any(event["kind"] == "agent.completed" for event in records))

    async def test_compaction_only_persists_after_the_new_view_is_committed(self):
        """Live compression progress becomes a durable statistic at Build Request commit."""
        policy = ContextPolicy(context_window=4096, compact_at_tokens=500, target_tokens=300,
            compact=LLMHistoryCompactor(max_output_tokens=128))
        provider = ScriptedProvider(answer("Short summary"), answer())
        async with self.host(provider, context_policy=policy) as host:
            async with host.subscribe_user_events() as stream:
                execution = await host.execute("agent", AgentInput(message="Now"), session_context={
                    "agent_conversation": Conversation(items=(display_history("Original " * 400),)).model_dump(mode="json")})
                done = await host.join(execution)
                self.assertEqual(done.status, "completed", done.error)
                live = await through(stream, "agent.completed")
            compactions = [event for event in live if event.kind == "agent.compaction"]
            self.assertEqual([event.payload["state"] for event in compactions], ["running", "completed"])
            self.assertEqual([event.persist for event in compactions], [False, True])
            self.assertEqual(compactions[0].payload["compaction_id"], compactions[1].payload["compaction_id"])
            final = compactions[1].payload
            self.assertEqual(final["node_id"], "build_request")
            self.assertLess(final["after_tokens"], final["before_tokens"])
            records = await self.history(host, done.invocation_id)
            self.assertEqual(sum(event["kind"] == "agent.compaction" for event in records), 1)

    async def test_noop_compaction_still_closes_the_live_progress(self):
        """A custom compactor keeping the existing view still emits a committed completion."""
        def keep(context):
            return CompactionResult(view=context.conversation.view)
        async with self.host(ScriptedProvider(answer()), context_policy=ContextPolicy(trigger=always_compact, compact=keep)) as host:
            async with host.subscribe_user_events() as stream:
                result = await self.run_agent(host)
                self.assertEqual(result.status, "completed", result.error)
                events = await through(stream, "agent.completed")
            compactions = [event for event in events if event.kind == "agent.compaction"]
            self.assertEqual([event.payload["state"] for event in compactions], ["running", "completed"])
            self.assertEqual(compactions[-1].payload["view_generation"], 0)

    async def test_pending_question_restarts_without_republishing_the_prompt(self):
        """Host state restores controls while the eventual saved interaction remains self-contained."""
        url = await self.database_url()
        workflow = agent_workflow(user_events=True, include_ask_user=True)
        provider = ScriptedProvider(tool_response(ask_call()), answer())
        @asynccontextmanager
        async def lifespan(setup):
            setup.agent_context.resources.register_provider("offline", provider)
            setup.agent_context.set_defaults(settings())
            yield
        options = dict(workflows=[workflow], lifespan=lifespan, settings=HostSettings(database_url=url), environment={})
        async with AutoAgentHost(**options) as host:
            waiting = await self.run_agent(host)
            wait_id = waiting.waits[0].id
            before = await self.history(host, waiting.invocation_id)
            self.assertFalse(any(event["kind"] == "agent.interaction" for event in before))
        async with AutoAgentHost(**options) as host:
            async with host.subscribe_user_events() as stream:
                current = await host.get_execution(waiting.invocation_id)
                self.assertEqual(current.waits[0].id, wait_id)
                await host.respond(waiting.invocation_id, wait_id, reply())
                result = await host.join(waiting.invocation_id)
                self.assertEqual(result.status, "completed", result.error)
                events = await through(stream, "agent.completed")
            self.assertFalse(any(event.kind == "agent.interaction" and event.payload["state"] == "pending" for event in events))
            records = await self.history(host, result.invocation_id)
            interactions = [event for event in records if event["kind"] == "agent.interaction"]
            self.assertEqual(len(interactions), 1)
            self.assertEqual(interactions[0]["payload"]["interaction_id"], wait_id)
            self.assertEqual(interactions[0]["payload"]["resolution"]["response"]["answers"][0]["text"], "Build an agent")

    async def test_inbox_message_is_saved_once_when_prepare_context_consumes_it(self):
        """Signal identity and reply metadata survive as one committed presentation message."""
        async with self.host(ScriptedProvider(tool_response(ask_call()), answer()), include_ask_user=True) as host:
            waiting = await self.run_agent(host)
            receipt = await host.signal(waiting.invocation_id, "agent.user",
                AgentSignalMessage(message="Use SQLite", reply_requested=True).model_dump(mode="json"), source_id="ui", sequence=1)
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply())
            done = await host.join(waiting.invocation_id)
            self.assertEqual(done.status, "completed", done.error)
            records = await self.history(host, done.invocation_id)
            received = [message for event in records if event["kind"] == "agent.messages" and event["payload"]["node_id"] == "prepare_context"
                for message in event["payload"]["messages"]]
            self.assertEqual(len(received), 1)
            self.assertEqual(received[0]["message_id"], receipt.message_id)
            self.assertEqual(received[0]["content"][0]["text"], "Use SQLite")
            self.assertTrue(received[0]["reply_requested"])

    async def test_final_node_error_is_saved_without_a_completed_event(self):
        """A provider failure exposes the final Node error through the generic Mapper."""
        def fail(request):
            raise RuntimeError("Provider is unavailable")
        async with self.host(ScriptedProvider(fail)) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "failed")
            records = await self.history(host, result.invocation_id)
            self.assertEqual([record["kind"] for record in records], ["agent.messages", "agent.error"])
            self.assertEqual(records[-1]["payload"]["node_id"], "llm_call")
            self.assertFalse(records[-1]["payload"]["handled"])

    async def test_partial_stream_failure_is_final_and_progress_stays_transient(self):
        """Emitted streams keep Core's no-automatic-retry boundary and save one final error."""
        class Retrying(ScriptedProvider):
            attempt = 0
            async def astream(self, request):
                self.attempt += 1
                yield LLMStreamChunk(type="delta", item_key="text", kind="text", delta=str(self.attempt))
                if self.attempt == 1:
                    raise RuntimeError("retry this attempt")
                yield LLMStreamChunk(type="completed", response=await self.ainvoke(request))
        policy = AgentWorkflowPolicies(nodes={"llm_call": AgentNodePolicy(retry=RetryPolicy(max_attempts=1))})
        provider = Retrying(answer())
        async with self.host(provider, policies=policy, llm_options={"streaming": True}) as host:
            async with host.subscribe_user_events() as stream:
                done = await self.run_agent(host)
                self.assertEqual(done.status, "failed", done.error)
                live = await through(stream, "agent.error")
            deltas = [event.payload for event in live if event.kind == "agent.response.delta"]
            self.assertEqual(provider.attempt, 1)
            self.assertEqual(len({event["stream_id"] for event in deltas}), 1)
            self.assertEqual(len({event["response_id"] for event in deltas}), 1)
            records = await self.history(host, done.invocation_id)
            self.assertEqual(sum(event["kind"] == "agent.response" for event in records), 0)
            self.assertEqual(sum(event["kind"] == "agent.error" for event in records), 1)

    async def test_retry_before_the_first_chunk_only_saves_the_successful_response(self):
        """A failed unpublished attempt is retried without a false final error notification."""
        class Retrying(ScriptedProvider):
            attempt = 0
            async def astream(self, request):
                self.attempt += 1
                if self.attempt == 1:
                    raise RuntimeError("retry before output")
                yield LLMStreamChunk(type="delta", item_key="text", kind="text", delta="Done")
                yield LLMStreamChunk(type="completed", response=await self.ainvoke(request))
        policy = AgentWorkflowPolicies(nodes={"llm_call": AgentNodePolicy(retry=RetryPolicy(max_attempts=1))})
        provider = Retrying(answer())
        async with self.host(provider, policies=policy, llm_options={"streaming": True}) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(provider.attempt, 2)
            records = await self.history(host, result.invocation_id)
            self.assertEqual(sum(event["kind"] == "agent.response" for event in records), 1)
            self.assertFalse(any(event["kind"] == "agent.error" for event in records))

    async def test_tool_progress_wrapper_preserves_ordinary_retry(self):
        """Presentation progress cannot suppress a retry that ordinary dispatch would allow."""
        from autoagent.agent.workflow import _event_streams
        original = _event_streams.execute_tool
        attempts = []
        async def flaky(task):
            attempts.append(task.call.call_id)
            if len(attempts) == 1:
                raise RuntimeError("Retry dispatch before invoking Tool")
            return await original(task)
        def read(text: str) -> str:
            return text
        policy = AgentWorkflowPolicies(nodes={"tool_execute": AgentNodePolicy(retry=RetryPolicy(max_attempts=1))})
        with patch.object(_event_streams, "execute_tool", flaky):
            async with self.host(ScriptedProvider(calls("read"), answer()), tools=(Tool(read),), policies=policy) as host:
                result = await self.run_agent(host)
                self.assertEqual(result.status, "completed", result.error)
                self.assertEqual(len(attempts), 2)
                records = await self.history(host, result.invocation_id)
                self.assertEqual(sum(event["kind"] == "agent.tools" for event in records), 1)
                self.assertFalse(any(event["kind"] == "agent.error" for event in records))

    async def test_subagent_wait_keeps_progress_and_final_result_on_the_wait_branch(self):
        """Creation receipts are live while collected Child results form one durable Tool card."""
        provider = RoutedProvider([response(call("run_agent", {"type": "worker", "task": "child task", "wait_timeout_seconds": 2})), answer()])
        config = SubagentsConfig(enabled=True, targets={"worker": SubagentTarget(workflow_id="worker")})
        @asynccontextmanager
        async def lifespan(setup):
            setup.agent_context.resources.register_provider("offline", provider)
            setup.agent_context.set_defaults(settings())
            yield
        async with AutoAgentHost(workflows=[agent_workflow(user_events=True, subagents=config),
            agent_workflow(workflow_id="worker", user_events=True)], lifespan=lifespan,
            settings=HostSettings(database_url=await self.database_url()), environment={}) as host:
            async with host.subscribe_user_events() as stream:
                execution = await host.execute("agent", AgentInput(message="root task"))
                async with asyncio.timeout(10):
                    while True:
                        result = await host.join(execution)
                        if result.status != "waiting":
                            break
                        await asyncio.sleep(.01)
                self.assertEqual(result.status, "completed", result.error)
                live = []
                while not any(event.kind == "agent.completed" and event.invocation_id == execution.invocation_id for event in live):
                    live.extend(await through(stream, "agent.completed"))
            root_events = [event for event in live if event.invocation_id == execution.invocation_id]
            self.assertTrue(any(event.kind == "agent.tools" and event.payload["node_id"] == "prepare_wait" for event in root_events))
            records = await self.history(host, execution.invocation_id)
            tools = [tool for event in records if event["kind"] == "agent.tools" for tool in event["payload"]["tools"]]
            self.assertEqual(len(tools), 1)
            self.assertEqual(tools[0]["name"], "run_agent")
            self.assertEqual(tools[0]["state"], "completed")
            self.assertEqual(tools[0]["result"]["structured_content"]["agents"][0]["status"], "completed")


class SQLiteAgentEventTests(AgentEventContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLAgentEventTests(AgentEventContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
