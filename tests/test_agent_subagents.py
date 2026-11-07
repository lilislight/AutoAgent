"""Built-in collaboration, automatic inbox ingestion and durable bounded waits."""

from contextlib import asynccontextmanager
import asyncio
import inspect
import json
from pathlib import Path
import tempfile
import threading
import unittest

from autoagent.agent import (
    AgentAccess, AgentApprovalRequest, AgentApprovalResponse, AgentInput, AgentSignalMessage, LLMMessage, LLMResponse, LLMToolCall,
    LLMToolResult, SubagentsConfig, SubagentTarget, Tool, agent_workflow,
    LLMStreamChunk,
)
from autoagent.core import WorkflowCompiler
from autoagent.agent.context import restore
from autoagent.host import AutoAgentHost, HostSettings
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import answer, settings


def call(name, arguments, id="call"):
    return LLMToolCall(name=name, arguments=json.dumps(arguments), call_id=id)


def response(*calls):
    return LLMResponse(model="offline", finish_reason="tool_calls", output=calls)


def results(request):
    return [item for item in request.input if isinstance(item, LLMToolResult)]


def incoming(request):
    messages = []
    for item in request.input:
        if isinstance(item, LLMMessage):
            for part in item.content:
                text = getattr(part, "text", "")
                if text.startswith("Incoming message "):
                    header, body = text.split("\n", 1)
                    messages.append((json.loads(header[len("Incoming message "):]), body))
    return messages


class RoutedProvider:
    provider_id = "offline"

    def __init__(self, root, child=None):
        self.root, self.child = iter(root), child or (lambda request: answer("Child complete"))
        self.requests = []

    async def ainvoke(self, request):
        self.requests.append(request)
        initial = next(item for item in request.input if isinstance(item, LLMMessage) and item.role == "user")
        is_child = initial.content[0].text.startswith("child")
        step = self.child if is_child else next(self.root)
        value = step(request) if callable(step) else step
        return await value if inspect.isawaitable(value) else value

    async def astream(self, request):
        yield LLMStreamChunk(type="completed", response=await self.ainvoke(request))


class SubagentConfigurationTests(unittest.TestCase):
    def test_distinct_endpoints_share_the_same_message_contract(self):
        """Every Agent has a user Endpoint; enabled collaboration adds a distinct identical contract."""
        root = WorkflowCompiler().compile_or_raise(agent_workflow())
        enabled = WorkflowCompiler().compile_or_raise(agent_workflow(user_signal_endpoint="user.inbox",
            subagents=SubagentsConfig(enabled=True, signal_endpoint="team.inbox"), include_ask_user=True))
        self.assertEqual([name for name, _ in root.signal_endpoints], ["agent.user"])
        contracts = dict(enabled.signal_endpoints)
        self.assertEqual(set(contracts), {"user.inbox", "team.inbox"})
        self.assertIs(contracts["user.inbox"].annotation, AgentSignalMessage)
        self.assertIs(contracts["team.inbox"].annotation, AgentSignalMessage)
        self.assertEqual((len(enabled.nodes), len(enabled.edges)), (15, 22))
        self.assertIn("prepare_wait", {node.id for node in enabled.nodes})
        self.assertNotIn("wait_tools_approved", {node.id for node in enabled.nodes})
        self.assertEqual(enabled.node("prepare_wait").recovery_mode.mode, "never")
        self.assertEqual([(edge.source, edge.target) for edge in enabled.edges if edge.target == "wait_tools"],
            [("prepare_wait", "wait_tools")])
        self.assertEqual([edge.target for edge in enabled.edges if edge.source == "tool_execute_approved"],
            ["complete_round"])
        with self.assertRaises(ValueError):
            agent_workflow(user_signal_endpoint="same", subagents=SubagentsConfig(signal_endpoint="same"))
        with self.assertRaises(ValueError):
            SubagentsConfig(default_wait_seconds=20, max_wait_seconds=10)


class SubagentContracts:
    async def database_url(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return "sqlite+aiosqlite:///" + str(Path(directory.name) / "runtime.db")

    @asynccontextmanager
    async def host(self, provider, *, url=None, root_config=None, child_config=None, defaults=None, tools=()):
        @asynccontextmanager
        async def lifespan(setup):
            setup.agent_context.resources.register_provider("offline", provider)
            setup.agent_context.set_defaults(defaults or settings())
            for tool in tools:
                setup.agent_context.resources.register_tool(tool, workflows={"agent", "worker"})
            yield
        config = root_config or SubagentsConfig(enabled=True, targets={"worker": SubagentTarget(workflow_id="worker")})
        child = child_config or SubagentsConfig(enabled=True)
        async with AutoAgentHost(workflows=[agent_workflow(subagents=config),
            agent_workflow(workflow_id="worker", subagents=child)], lifespan=lifespan,
            settings=HostSettings(database_url=url or await self.database_url()), environment={}) as host:
            yield host

    async def run_agent(self, host, *, session_id=None):
        return await host.join(await host.execute("agent", AgentInput(message="root task"), session_id=session_id))

    async def settled(self, host, invocation_id):
        async def finish():
            while True:
                result = await host.join(invocation_id)
                if result.status != "waiting" or any(wait.kind == "external" for wait in result.waits):
                    return result
                await asyncio.sleep(0.01)
        return await asyncio.wait_for(finish(), 60)

    async def test_spawn_query_and_model_settings_inherit_to_child(self):
        """Spawned Agent input and inherited Session settings work without a separate Root execution."""
        child_ids = []
        def query(request):
            item = results(request)[-1]
            self.assertFalse(item.is_error, item)
            child_ids.append(item.structured_content["agent_id"])
            return response(call("get_agents", {}))
        def done(request):
            value = results(request)[-1].structured_content
            self.assertEqual(value["agents"][0]["agent_id"], child_ids[0])
            self.assertEqual(value["agents"][0]["relation"], "child")
            return answer()
        def child(request):
            self.assertEqual(request.model, "chosen-model")
            return answer("child result")
        provider = RoutedProvider([response(call("spawn_agent", {"type": "worker", "task": "child task"})), query, done], child)
        async with self.host(provider) as host:
            await host.agent_context.configure("session", settings(model="chosen-model"))
            result = await self.run_agent(host, session_id="session")
            self.assertEqual(result.status, "completed", result.error)
            snapshot = await host.agent_context.get_snapshot(child_ids[0])
            self.assertEqual(snapshot.settings.model.model, "chosen-model")

    async def test_run_agent_joins_result_through_native_select(self):
        """run_agent delegates creation and returns the completed Child observation through a bounded Select."""
        def done(request):
            value = results(request)[-1]
            self.assertFalse(value.is_error, value)
            self.assertEqual(value.structured_content["wake_reason"], "agent")
            self.assertEqual(value.structured_content["agents"][0]["status"], "completed")
            return answer()
        provider = RoutedProvider([response(call("run_agent", {"type": "worker", "task": "child task", "wait_timeout_seconds": 2})), done])
        async with self.host(provider) as host:
            result = await self.run_agent(host)
            result = await self.settled(host, result.invocation_id)
            self.assertEqual(result.status, "completed", result.error)

    async def test_wait_for_collects_all_already_completed_targets_in_one_model_turn(self):
        """One Select wake collects every completed target instead of returning only its winning case."""
        from autoagent.core.commands import CommandInvoker, Status
        selected = []
        async def wait(request):
            selected.extend(item.structured_content["agent_id"] for item in results(request))
            commands = CommandInvoker.current()
            handles = dict((handle.invocation_id, handle) for _, handle in commands.visible_handles())
            async with asyncio.timeout(15):
                for poll in range(1000):
                    states = [await commands.execute(Status(), handles[id], key=f"gate:{poll}:{index}")
                        for index, id in enumerate(selected)]
                    if all(item.status == "completed" for item in states):
                        break
                    await asyncio.sleep(0.01)
            return response(call("wait_for", {"agent_ids": selected, "timeout_seconds": 5}))
        def done(request):
            observed = results(request)[-1]
            self.assertEqual(observed.structured_content["wake_reason"], "agent")
            agents = observed.structured_content["agents"]
            self.assertEqual([agent["agent_id"] for agent in agents], selected)
            self.assertEqual([agent["status"] for agent in agents], ["completed"] * 3)
            self.assertEqual(set(observed.tool_metadata["agent_observed_versions"]), set(selected))
            return answer()
        provider = RoutedProvider([response(*(call("spawn_agent", {"type": "worker", "task": "child " + str(index)},
            id="spawn-" + str(index)) for index in range(3))), wait, done])
        async with self.host(provider) as host:
            result = await self.run_agent(host)
            result = await self.settled(host, result.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(len(provider.requests), 6)

    async def test_user_message_wakes_without_consumption_and_enters_latest_context_once(self):
        """User messages wake Select and are consumed only by the next context preparation."""
        def done(request):
            self.assertEqual([body for _, body in incoming(request)], ["new user requirement"])
            self.assertEqual(incoming(request)[0][0]["from"], "user")
            self.assertEqual(results(request)[-1].structured_content["wake_reason"], "message")
            return answer()
        provider = RoutedProvider([response(call("wait_for", {"timeout_seconds": 10})), done])
        async with self.host(provider) as host:
            waiting = await self.run_agent(host)
            self.assertEqual(waiting.status, "waiting", waiting.error)
            await host.signal(waiting.invocation_id, "agent.user", AgentSignalMessage(message="new user requirement").model_dump(mode="json"),
                source_id="user", sequence=1)
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)

    async def test_parent_child_message_reply_metadata_and_optional_wait(self):
        """Goal changes and replies use the recipient's configured Endpoint with trustworthy sender identity."""
        sent = {}
        def send(request):
            value = results(request)[-1]
            self.assertFalse(value.is_error, value)
            sent["child"] = value.structured_content["agent_id"]
            return response(call("send_message", {"target": sent["child"], "message": "Use offline data and confirm.",
                "reply_requested": True, "wait_timeout_seconds": 5}, "guidance"))
        def child(request):
            if any(item.call_id == "reply" for item in results(request)):
                return answer("Child complete")
            messages = incoming(request)
            if messages:
                header, body = messages[-1]
                self.assertTrue(header["reply_requested"])
                self.assertEqual(body, "Use offline data and confirm.")
                sent["original"] = header["message_id"]
                return response(call("send_message", {"target": header["from"], "message": "Offline plan confirmed.",
                    "reply_to": header["message_id"]}, "reply"))
            return response(call("wait_for", {"timeout_seconds": 5}, "child-wait"))
        def done(request):
            self.assertEqual(results(request)[-1].structured_content["wake_reason"], "message")
            header, body = incoming(request)[-1]
            self.assertEqual(header["from"], sent["child"])
            self.assertEqual(header["reply_to"], sent["original"])
            self.assertEqual(body, "Offline plan confirmed.")
            return answer()
        provider = RoutedProvider([response(call("spawn_agent", {"type": "worker", "task": "child task"})), send, done], child)
        async with self.host(provider, child_config=SubagentsConfig(enabled=True, signal_endpoint="worker.mailbox")) as host:
            result = await self.run_agent(host)
            result = await self.settled(host, result.invocation_id)
            self.assertEqual(result.status, "completed", result.error)

    async def test_timer_only_wait_is_normal_result_and_limits_validate_before_effect(self):
        """Finite sleep completes normally while invalid wait durations fail before spawning a Child."""
        def sleep(request):
            value = results(request)[-1]
            self.assertTrue(value.is_error)
            self.assertEqual(value.structured_content["error"]["kind"], "invalid_arguments")
            return response(call("wait_for", {"timeout_seconds": 0.01, "wake_on_message": False}))
        def done(request):
            value = results(request)[-1]
            self.assertFalse(value.is_error)
            self.assertEqual(value.structured_content["wake_reason"], "timer")
            return answer()
        provider = RoutedProvider([response(call("run_agent", {"type": "worker", "task": "child task", "wait_timeout_seconds": 121})), sleep, done])
        async with self.host(provider) as host:
            result = await self.run_agent(host)
            while result.status == "waiting":
                result = await host.join(result.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(sum(any(isinstance(item, LLMMessage) and item.role == "user" and item.content[0].text.startswith("child")
                for item in request.input) for request in provider.requests), 0)

    async def test_sibling_visibility_and_direct_child_cancellation(self):
        """Configured siblings are visible, filters cannot expand scope and only direct children can be cancelled."""
        child_ids, relations = [], {}

        def child(request):
            initial = next(item for item in request.input if isinstance(item, LLMMessage) and item.role == "user")
            name = initial.content[0].text
            items = results(request)
            if not incoming(request):
                return response(call("wait_for", {"timeout_seconds": 20}, "start"))
            if not any(item.call_id == "inspect" for item in items):
                return response(call("get_agents", {}, "inspect"))
            value = next(item.structured_content for item in items if item.call_id == "inspect")
            if not any(item.call_id == "denied" for item in items):
                self.assertEqual({item["relation"] for item in value["agents"]}, {"parent", "sibling"})
                relations[name] = value
                parent = next(item["agent_id"] for item in value["agents"] if item["relation"] == "parent")
                return response(call("cancel_agent", {"agent_id": parent}, "denied"))
            denied = next(item for item in items if item.call_id == "denied")
            self.assertTrue(denied.is_error)
            self.assertEqual(denied.structured_content["error"]["kind"], "agent_control_denied")
            if not any(item.call_id == "report" for item in items):
                parent = next(item["agent_id"] for item in value["agents"] if item["relation"] == "parent")
                return response(call("send_message", {"target": parent, "message": name + " inspected"}, "report"))
            return response(call("wait_for", {"timeout_seconds": 20}, "idle"))

        def query(request):
            for item in results(request):
                self.assertFalse(item.is_error, item)
                child_ids.append(item.structured_content["agent_id"])
            return response(*(call("send_message", {"target": identifier, "message": "Inspect your neighbors."}, "start-" + str(index))
                for index, identifier in enumerate(child_ids)),
                call("get_agents", {"agent_ids": [child_ids[1]]}, "filter"),
                call("get_agents", {"agent_ids": ["not-visible"]}, "unknown"))

        def poll(request):
            items = {item.call_id: item for item in results(request)}
            self.assertEqual([item["agent_id"] for item in items["filter"].structured_content["agents"]], [child_ids[1]])
            self.assertTrue(items["unknown"].is_error)
            if "cancel-0" in items:
                self.assertTrue(all(items["cancel-" + str(index)].structured_content["accepted"] for index in range(2)))
                return answer()
            if len(incoming(request)) == 2:
                return response(*(call("cancel_agent", {"agent_id": identifier, "reason": "test complete"}, "cancel-" + str(index))
                    for index, identifier in enumerate(child_ids)))
            return response(call("wait_for", {"timeout_seconds": 1}, "poll-" + str(len(items))))

        provider = RoutedProvider([response(call("spawn_agent", {"type": "worker", "task": "child one"}, "one"),
            call("spawn_agent", {"type": "worker", "task": "child two"}, "two")), query, *([poll] * 8)], child)
        async with self.host(provider, child_config=SubagentsConfig(enabled=True, include_siblings=True)) as host:
            result = await self.run_agent(host)
            result = await self.settled(host, result.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(len(relations), 2)

    async def test_configured_collaboration_bypasses_ordinary_tool_approval(self):
        """Enabled builtins remain advertised and execute without ordinary Tool access or approval modes."""
        def done(request):
            item = results(request)[-1]
            self.assertFalse(item.is_error, item)
            self.assertEqual(item.structured_content["wake_reason"], "timer")
            self.assertEqual([item.call_id for item in results(request)], ["get", "wait"])
            return answer()
        for mode in ("always_ask", "never_ask"):
            with self.subTest(mode=mode):
                provider = RoutedProvider([response(call("get_agents", {}, "get"), call("wait_for", {
                    "timeout_seconds": 0.01, "wake_on_message": False}, "wait")), done])
                async with self.host(provider, defaults=settings(mode=mode,
                    access=AgentAccess(default="deny", allowed_tools=()))) as host:
                    result = await self.run_agent(host)
                    result = await self.settled(host, result.invocation_id)
                    self.assertEqual(result.status, "completed", result.error)

    async def test_waits_register_and_wake_before_slow_tool_and_unrelated_approval_finish(self):
        """Independent Select Map wakes while a slow ordinary Tool and approval remain unfinished, then joins all results once."""
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        async def slow_tool(text: str) -> str:
            started.set()
            try:
                while not release.is_set():
                    await asyncio.sleep(0.01)
                return text
            finally:
                finished.set()
        def guarded_tool() -> str:
            self.fail("Rejected Tool must not execute")
        expected = ["slow", "wait-a", "bad-agent", "wait-b", "approval"]
        def done(request):
            items = results(request)
            self.assertEqual([item.call_id for item in items], expected)
            self.assertFalse(items[0].is_error)
            self.assertTrue(items[2].is_error)
            self.assertTrue(items[4].is_error)
            self.assertEqual([items[index].structured_content["wake_reason"] for index in (1, 3)], ["message", "message"])
            self.assertEqual([body for _, body in incoming(request)], ["wake both waits"])
            return answer()
        provider = RoutedProvider([response(call("slow_tool", {"text": "ok"}, "slow"),
            call("wait_for", {"timeout_seconds": 30}, "wait-a"),
            call("run_agent", {"type": "missing", "task": "invalid target"}, "bad-agent"),
            call("wait_for", {"timeout_seconds": 30}, "wait-b"),
            call("guarded_tool", {}, "approval")), done])
        async with self.host(provider, tools=(Tool(slow_tool), Tool(guarded_tool)),
            defaults=settings(access=AgentAccess(rules={"guarded_tool": "ask"}))) as host:
            ref = await host.execute("agent", AgentInput(message="root task"))
            try:
                async with asyncio.timeout(20):
                    while True:
                        status = await host._runtime.app.astatus(ref)
                        if len(status.waits) == 3 and started.is_set():
                            break
                        self.assertNotIn(status.status, ("completed", "failed", "cancelled"), status.error)
                        await asyncio.sleep(0.01)
                self.assertFalse(finished.is_set())
                self.assertEqual(sum(wait.kind == "any" for wait in status.waits), 2)
                approval = next(wait for wait in status.waits if wait.kind == "external")
                await host.signal(ref, "agent.user", AgentSignalMessage(message="wake both waits").model_dump(mode="json"),
                    source_id="user", sequence=1)
                async with asyncio.timeout(20):
                    while True:
                        status = await host._runtime.app.astatus(ref)
                        if not any(wait.kind == "any" for wait in status.waits):
                            break
                        await asyncio.sleep(0.01)
                self.assertFalse(finished.is_set())
                self.assertEqual(len(provider.requests), 1)
                request = restore(AgentApprovalRequest, approval.request)
                await host.respond(ref, approval.id, AgentApprovalResponse(decisions=(
                    request.requests[0].respond(approved=False),)).model_dump(mode="json"))
            finally:
                release.set()
            result = await self.settled(host, ref.invocation_id)
            self.assertEqual(result.status, "completed", result.error)

    async def test_restart_preserves_wait_deadline_and_single_message_delivery(self):
        """Restoring a native wait preserves its absolute deadline and automatically ingests a user message once."""
        url = await self.database_url()
        provider = RoutedProvider([response(call("wait_for", {"timeout_seconds": 30}))])
        async with self.host(provider, url=url) as host:
            waiting = await self.run_agent(host)
            condition = waiting.waits[0].condition
            invocation_id, wait_id = waiting.invocation_id, waiting.waits[0].id
        def done(request):
            self.assertEqual([body for _, body in incoming(request)], ["after restart"])
            return answer()
        resumed = RoutedProvider([done])
        async with self.host(resumed, url=url) as host:
            recovered = await host.recover(invocation_id)
            self.assertEqual(recovered.status, "waiting", recovered.error)
            self.assertEqual(recovered.waits[0].id, wait_id)
            self.assertEqual(recovered.waits[0].condition, condition)
            await host.signal(invocation_id, "agent.user", AgentSignalMessage(message="after restart").model_dump(mode="json"),
                source_id="user", sequence=1)
            result = await self.settled(host, invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(len(resumed.requests), 1)


class SQLiteSubagentTests(SubagentContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLSubagentTests(SubagentContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
