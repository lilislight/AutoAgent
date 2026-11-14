"""Hidden clock snapshots preserve request prefixes across calls and restarts."""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from autoagent.agent import (
    AgentInput, AgentWorkflowOptions, ContextPolicy, Conversation, HistoryRoute,
    LLMHistoryCompactor, LLMMessage, LLMRequest, LLMToolCall, LLMToolResult,
    LiteLLMProvider, LiteLLMProviderConfig, OpenAIProvider, OpenAIProviderConfig,
    TextContent, TokenAnchor, UsagePlusCharacters, agent_runtime_context,
    agent_workflow, always_compact, build_agent_request, prepare_agent_context,
    project_history,
)
from autoagent.agent.context import restore
from autoagent.agent.providers import ChatCompletionsAdapter, ResponsesAdapter
from autoagent.agent.workflow.compaction import DEFAULT_COMPACTION_PROMPT
from autoagent.agent.workflow.context import compact_agent_context, message
from autoagent.agent.workflow.runtime_context import TIME_KEY, server_time, time_characters, time_message
from autoagent.agent_app import builtin_agent_workflow
from autoagent.core import InputMappingContext, WorkflowCompiler
from autoagent.host import AutoAgentHost, HostSettings
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_context_policy import Service, preparation
from tests.test_agent_workflow import ScriptedProvider, answer, settings


FIRST = "2026-11-12T16:30:45+08:00"
SECOND = "2026-11-12T16:31:12+08:00"


class RuntimeTimeTests(unittest.TestCase):
    def test_local_clock_has_second_precision_and_offset(self):
        """Clock records use the server-local offset rather than the browser's zone."""
        local = datetime(2026, 11, 12, 16, 30, 45, 123, tzinfo=timezone(timedelta(hours=8)))
        with patch("autoagent.agent.workflow.runtime_context.datetime") as clock:
            clock.now.return_value.astimezone.return_value = local
            self.assertEqual(server_time(), FIRST)

    def test_sdk_defaults_off_and_product_enables_clock(self):
        """Only the built-in product opts in unless an SDK caller explicitly enables it."""
        compiler = WorkflowCompiler()
        for workflow, enabled in ((agent_workflow(), False), (agent_workflow(inject_current_time=True), True),
                                  (builtin_agent_workflow(), True)):
            graph = compiler.compile_or_raise(workflow)
            options = graph.node("start").executable.handler.__autoagent_options__
            self.assertEqual(options.inject_current_time, enabled)

    def test_history_replays_hidden_times_in_place_and_round_trips(self):
        """Clock records survive JSON restore without entering user conversation Items."""
        history = Conversation(items=(message("user", "goal"), message("assistant", "answer")),
            runtime_times={"1": FIRST})
        restored = restore(Conversation, history.model_dump(mode="json"))
        self.assertEqual(restored, history)
        self.assertEqual(restored.visible, history.items)
        projected = project_history(restored, None)
        self.assertEqual(projected, (history.items[0], time_message(FIRST), history.items[1]))
        self.assertEqual(project_history(restored, None, start=1), (history.items[1],))
        self.assertEqual(restored.append(message("user", "next")).runtime_times, {"1": FIRST})

    def test_compaction_view_discards_clock_without_losing_business_dates(self):
        """A replacement drops automatic clock records while task dates remain ordinary history."""
        history = Conversation(items=(message("user", "Deadline: 2026-12-01"), message("assistant", "noted")),
            runtime_times={"1": FIRST})
        compacted = history.replace(0, 2, (message("user", "Deadline: 2026-12-01"),))
        self.assertEqual(compacted.runtime_times, {})
        self.assertNotIn(FIRST, str(project_history(compacted, None)))
        self.assertIn("2026-12-01", str(project_history(compacted, None)))

    def test_role_selection_is_conservative_and_overridable(self):
        """Official OpenAI preserves system position while arbitrary bridges use user carriers."""
        cases = (
            (OpenAIProvider(), "system"),
            (OpenAIProvider(OpenAIProviderConfig(base_url="http://localhost:1234/v1")), "user"),
            (LiteLLMProvider(LiteLLMProviderConfig(model_prefix="openai")), "system"),
            (LiteLLMProvider(LiteLLMProviderConfig(model_prefix="openai", base_url="http://localhost:1234/v1")), "user"),
            (LiteLLMProvider(LiteLLMProviderConfig(model_prefix="anthropic")), "user"),
        )
        for provider, role in cases:
            self.assertEqual(provider.context_message_role("test"), role)
        route = HistoryRoute(provider="p", version="1", model="m", provider_id="p", protocol="openai.responses",
            context_role="system")
        self.assertEqual(time_message(FIRST, route).role, "system")
        self.assertTrue(route.accepts(route.model_copy(update={"context_role": "user"})))

    def test_protocol_encoders_keep_the_trailing_system_message(self):
        """Native Chat and Responses encode the timestamp at its original input position."""
        clock = time_message(FIRST).model_copy(update={"role": "system"})
        request = LLMRequest(model="test", input=(message("system", "fixed"), message("user", "goal"), clock),
            routing={TIME_KEY: FIRST})
        for adapter, field in ((ChatCompletionsAdapter(), "messages"), (ResponsesAdapter(), "input")):
            encoded = adapter.encode_request(request, provider_id="openai")
            self.assertEqual(encoded[field][-1]["role"], "system")
            self.assertIn(FIRST, str(encoded[field][-1]))
            self.assertNotIn(TIME_KEY, str(encoded))

    def test_estimator_reserves_new_clock_without_double_counting_old_clock(self):
        """An actual Usage anchor already includes older snapshots; only the new one is reserved."""
        history = Conversation(items=(message("user", "goal"),), runtime_times={"1": FIRST},
            token_anchor=TokenAnchor(fingerprint="fixed-1", input_tokens=100, source_count=1, view_generation=0))
        value = preparation(history).model_copy(update={"inject_current_time": True})
        import math
        estimated = UsagePlusCharacters()(value)
        self.assertTrue(estimated.anchored)
        self.assertEqual(estimated.input_tokens, 100 + math.ceil(time_characters(value) / 4))

    def test_prompt_preserves_agent_handles_and_omits_only_runtime_clocks(self):
        """The handoff prompt retains coordination state and task dates while excluding clock noise."""
        for text in ("CONTEXT CHECKPOINT COMPACTION", "handoff summary", "task/agent ID", "parent-child",
                     "not yet collected", "output cursors", "unknown outcomes", "runtime clock records",
                     "deadlines", "Do not continue the conversation"):
            self.assertIn(text, DEFAULT_COMPACTION_PROMPT)


class RuntimeTimePreparationTests(unittest.IsolatedAsyncioTestCase):
    async def test_resume_after_build_refreshes_only_the_last_clock(self):
        """A pause after request assembly cannot send its stale clock or append it twice."""
        service = Service(ScriptedProvider())
        service.execution_snapshot = AsyncMock(return_value=service.state)
        history = Conversation(items=(message("user", "goal"),))
        options = AgentWorkflowOptions(inject_current_time=True)
        policy = ContextPolicy()
        workflow = agent_workflow(options=options,  )
        llm_input = next(node for node in workflow.nodes if node.id == "llm_call").input_mapping
        with agent_runtime_context(service), patch(
                "autoagent.agent.workflow.runtime_context.server_time", side_effect=[FIRST, SECOND]):
            plan = await prepare_agent_context(history, options, policy)
            built = await build_agent_request(plan, policy)
            context = InputMappingContext(invocation_input={}, incoming={"build_llm": built},
                invocation_context={"agent": {"request_snapshot_id": service.state.snapshot_id}},
                session_context={"agent_conversation": history.model_dump(mode="json")},
                self_handle=SimpleNamespace(invocation_id="invocation"))
            request = await llm_input(context)
        self.assertEqual(request.input[:-1], built.request.input[:-1])
        self.assertEqual(request.input[-1], time_message(SECOND))
        self.assertEqual(len(request.input), len(built.request.input))
        self.assertNotIn(FIRST, str(request.input))

    async def test_clock_is_read_after_compaction_and_not_in_summary_material(self):
        """Compression receives no clock records and request assembly reads a fresh clock afterward."""
        provider = ScriptedProvider(answer("Checkpoint: keep deadline 2026-12-01"))
        policy = ContextPolicy(context_window=4096, trigger=always_compact, target_tokens=500,
            compact=LLMHistoryCompactor(max_output_tokens=128))
        history = Conversation(items=(message("user", "Deadline 2026-12-01 " * 100),
            message("assistant", "old progress"), message("user", "continue")), runtime_times={"1": FIRST})
        with agent_runtime_context(Service(provider)), patch(
                "autoagent.agent.workflow.runtime_context.server_time", return_value=SECOND) as clock:
            plan = await prepare_agent_context(history, AgentWorkflowOptions(inject_current_time=True), policy)
            clock.assert_not_called()
            compacted = await compact_agent_context(plan, policy)
            clock.assert_not_called()
            built = await build_agent_request(compacted, policy)
            clock.assert_called_once()
        self.assertNotIn(FIRST, str(provider.requests[0].input))
        self.assertIn("2026-12-01", str(provider.requests[0].input))
        self.assertEqual(compacted.preparation.conversation.runtime_times, {})
        self.assertEqual(built.request.input[-1], time_message(SECOND))
        self.assertEqual(built.runtime_time, SECOND)


class RuntimeTimeHostContracts:
    async def database_url(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return "sqlite+aiosqlite:///" + str(Path(temporary.name) / "runtime.db")

    @asynccontextmanager
    async def host(self, provider, *, url=None, **options):
        @asynccontextmanager
        async def lifespan(setup):
            setup.providers.register("offline", provider)
            setup.extensions["agent"].context.set_defaults(settings())
            if "agent" in setup.applications:
                setup.applications["agent"].context.set_defaults(settings())
            yield
        async with AutoAgentHost(workflows=[agent_workflow(  user_events=True,
                **options)], lifespan=lifespan, environment={},
                settings=HostSettings(database_url=url or await self.database_url())) as host:
            yield host

    async def test_tool_loop_preserves_request_prefix_and_hides_clock_from_ui(self):
        """The next Tool round extends the exact prior request and commits only its actual sent time."""
        tool_call = answer().model_copy(update={"finish_reason": "tool_calls", "output": (
            LLMToolCall(call_id="call-1", name="missing", arguments="{}"),)})
        provider = ScriptedProvider(tool_call, answer())
        with patch("autoagent.agent.workflow.runtime_context.server_time",
                side_effect=["build-1", FIRST, "build-2", SECOND]):
            async with self.host(provider, inject_current_time=True) as host:
                result = await host.join(await host.execute("agent", AgentInput(message="goal")))
                self.assertEqual(result.status, "completed", result.error)
                first, second = provider.requests
                self.assertEqual(second.input[:len(first.input)], first.input)
                self.assertEqual(first.input[-1], time_message(FIRST))
                self.assertEqual(second.input[-1], time_message(SECOND))
                self.assertIsInstance(second.input[-2], LLMToolResult)
                history = restore(Conversation, host._runtime.repository.state(result.session_id).session.context["agent_conversation"])
                self.assertEqual(list(history.runtime_times.values()), [FIRST, SECOND])
                self.assertNotIn(FIRST, str(history.items))
                self.assertEqual(len(history.runtime_times), 2)
                events = (await host._persistence.user_events(result.invocation_id)).items
                self.assertNotIn("Current server time", json.dumps(events))
                self.assertNotIn(TIME_KEY, json.dumps(events))

    async def test_hidden_time_survives_host_restart(self):
        """A subsequent Invocation replays the prior hidden snapshot from durable Session Context."""
        url = await self.database_url()
        provider = ScriptedProvider(answer("first"), answer("second"))
        with patch("autoagent.agent.workflow.runtime_context.server_time", return_value=FIRST):
            async with self.host(provider, url=url, inject_current_time=True) as host:
                first = await host.join(await host.execute("agent", AgentInput(message="first")))
                self.assertEqual(first.status, "completed", first.error)
        with patch("autoagent.agent.workflow.runtime_context.server_time", return_value=SECOND):
            async with self.host(provider, url=url, inject_current_time=True) as host:
                second = await host.join(await host.execute("agent", AgentInput(message="second"), session_id=first.session_id))
                self.assertEqual(second.status, "completed", second.error)
        self.assertEqual(provider.requests[1].input[:len(provider.requests[0].input)], provider.requests[0].input)
        self.assertEqual(provider.requests[1].input[-1], time_message(SECOND))

    async def test_opt_out_does_not_read_clock(self):
        """SDK callers keep their original request layout when the option is absent."""
        provider = ScriptedProvider(answer())
        with patch("autoagent.agent.workflow.runtime_context.server_time") as clock:
            async with self.host(provider) as host:
                result = await host.join(await host.execute("agent", AgentInput(message="goal")))
                self.assertEqual(result.status, "completed", result.error)
        clock.assert_not_called()
        self.assertNotIn(TIME_KEY, provider.requests[0].routing)


class SQLiteRuntimeTimeHostTests(RuntimeTimeHostContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLRuntimeTimeHostTests(RuntimeTimeHostContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
