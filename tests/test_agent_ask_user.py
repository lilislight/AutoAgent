"""Workflow-local human questions, request-aware Resume and durable Map waits."""

import json
import unittest

from autoagent.agent import (
    AgentAccess, AgentApprovalRequest, AgentApprovalResponse, AgentNodePolicy, SubagentsConfig,
    AgentWorkflowOptions, AgentWorkflowPolicies, AskUserArguments, AskUserRequest,
    AskUserResponse, LLMResponse, LLMToolCall, LLMToolResult, Tool, ToolPolicy, agent_workflow,
)
from autoagent.agent.context import restore
from autoagent.agent.workflow.questions import ASK_USER_TOOL
from autoagent.core import WorkflowCompiler
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import AgentWorkflowContracts, ScriptedProvider, answer, settings


def question(identifier="goal"):
    return {"id": identifier, "type": "text", "text": "What should I do?"}


def choice(identifier="database", *, multiple=False, custom=True):
    return {"id": identifier, "type": "multiple_choice" if multiple else "single_choice",
        "text": "Which database?", "allow_custom_answer": custom,
        "options": [{"id": "sqlite", "label": "SQLite"}, {"id": "pg", "label": "PostgreSQL"}]}


def ask_call(call_id="ask-1", questions=None, *, arguments=None):
    return LLMToolCall(call_id=call_id, name="ask_user",
        arguments=arguments if arguments is not None else json.dumps({"questions": questions or [question()]}))


def tool_response(*calls):
    return LLMResponse(model="offline", finish_reason="tool_calls", output=calls)


def reply(identifier="goal", *, text="Build an agent", selected=(), skipped=False):
    return {"answers": [{"question_id": identifier, "status": "skipped" if skipped else "answered",
        "selected_option_ids": list(selected), "text": None if skipped else text}]}


class AskUserModelTests(unittest.TestCase):
    def test_native_switches_preserve_graph_and_revision_identity(self):
        """Question and collaboration switches share one original graph and Revision."""
        def compile_workflow(ask_user, subagents):
            return WorkflowCompiler().compile_or_raise(agent_workflow(options=AgentWorkflowOptions(
                include_ask_user=ask_user, subagents=SubagentsConfig(enabled=subagents))))
        reference = compile_workflow(True, True)
        self.assertEqual((len(reference.nodes), len(reference.edges)), (15, 22))
        for ask_user, subagents in ((False, False), (True, False), (False, True)):
            compiled = compile_workflow(ask_user, subagents)
            self.assertEqual(compiled.workflow_revision_id, reference.workflow_revision_id)
            self.assertEqual([node.id for node in compiled.nodes], [node.id for node in reference.nodes])
            self.assertEqual([(edge.id, edge.source, edge.target) for edge in compiled.edges],
                [(edge.id, edge.source, edge.target) for edge in reference.edges])
        self.assertEqual([edge for loop in reference.loop_regions for edge in loop.back_edge_ids], ["round_back"])
        self.assertIsNotNone(reference.node("ask_user").map.aggregate)
        agent_workflow(policies=AgentWorkflowPolicies(nodes={"ask_user": AgentNodePolicy()}))
        with self.assertRaises(ValueError):
            agent_workflow(include_ask_user="true")

    def test_invalid_questions_fail_during_tool_preflight(self):
        """Empty, duplicate and inconsistent question definitions never reach a Wait."""
        invalid = [[], [question(), question()], [choice() | {"options": []}],
            [choice() | {"options": [{"id": "x", "label": "A"}, {"id": "x", "label": "B"}]}],
            [question() | {"allow_custom_answer": False}], [question() | {"text": " "}],
            [question() | {"type": "password"}], [question() | {"id": ""}]]
        for questions in invalid:
            with self.subTest(questions=questions):
                result = ASK_USER_TOOL._prepare(ask_call(arguments=json.dumps({"questions": questions})))
                self.assertIsInstance(result, LLMToolResult)
                self.assertEqual(result.structured_content["error"]["kind"], "invalid_arguments")
        with self.assertRaises(ValueError):
            restore(AskUserArguments, {"questions": [question()], "call_id": "spoofed"})
        defaults = restore(AskUserArguments, {"questions": [choice() | {"allow_custom_answer": None}]})
        self.assertIs(defaults.questions[0].allow_custom_answer, True)

    def test_answer_validation_and_server_resolved_labels(self):
        """Request-aware validation rejects forged selections and resolves labels from the Request."""
        request = restore(AskUserRequest, {"request_id": "r", "call_id": "c",
            "questions": [question(), choice(), choice("features", multiple=True)]})
        response = restore(AskUserResponse, {"answers": [reply()["answers"][0],
            reply("database", text="Also consider offline use", selected=("sqlite",))["answers"][0],
            reply("features", text=None, selected=("sqlite", "pg"))["answers"][0]]})
        result = response.tool_result(request)
        self.assertFalse(result.is_error)
        self.assertEqual(result.call_id, "c")
        self.assertEqual(result.structured_content["answers"][1]["selected_options"][0]["label"], "SQLite")
        for invalid in [reply(), {"answers": []}, {"answers": [reply()["answers"][0]] * 3},
                        {"answers": [item.model_dump(mode="json") | {"selected_options": []} for item in response.answers]}]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                restore(AskUserResponse, invalid).validate_request(request)


class AskUserContracts:
    host = AgentWorkflowContracts.host
    database_url = AgentWorkflowContracts.database_url
    run_agent = AgentWorkflowContracts.run_agent

    async def test_disabled_tool_is_not_advertised_or_dispatched(self):
        """A hallucinated builtin name has no effect when its build option is disabled."""
        provider = ScriptedProvider(tool_response(ask_call()), answer())
        async with self.host(provider) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            self.assertNotIn("ask_user", [tool.name for tool in provider.requests[0].tools])
            observed = next(item for item in provider.requests[-1].input if isinstance(item, LLMToolResult))
            self.assertEqual(observed.structured_content["error"]["kind"], "tool_not_advertised")

    async def test_reopen_with_both_native_switches_off_uses_same_revision(self):
        """New turns use the current build switches after reopening the same database."""
        url = await self.database_url()
        get_agents = LLMToolCall(name="get_agents", call_id="inspect", arguments="{}")
        first = ScriptedProvider(tool_response(ask_call(), get_agents), answer())
        async with self.host(first, url=url, include_ask_user=True,
                subagents=SubagentsConfig(enabled=True)) as host:
            waiting = await self.run_agent(host, session_id="chat")
            self.assertEqual([wait.request["kind"] for wait in waiting.waits], ["agent.ask_user"])
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply())
            self.assertEqual((await host.join(waiting.invocation_id)).status, "completed")
            revision = host._runtime.workflow_ids["agent"]
        second = ScriptedProvider(tool_response(ask_call(), get_agents), answer())
        async with self.host(second, url=url) as host:
            self.assertEqual(host._runtime.workflow_ids["agent"], revision)
            result = await self.run_agent(host, session_id="chat")
            self.assertEqual(result.status, "completed", result.error)
            self.assertFalse(second.requests[0].tools)
            self.assertEqual([item.structured_content["error"]["kind"]
                for item in second.requests[-1].input[-2:]], ["tool_not_advertised"] * 2)

    async def test_multiple_calls_wait_independently_and_join_in_call_order(self):
        """Map=1 exposes every question while ordinary Tools finish and replies may arrive out of order."""
        effects = []
        def read(text: str) -> str:
            effects.append(text)
            return text
        provider = ScriptedProvider(tool_response(ask_call("a"),
            LLMToolCall(call_id="read", name="read", arguments='{"text":"ok"}'), ask_call("b")), answer())
        async with self.host(provider, include_ask_user=True, max_parallelism=1, tools=(Tool(read),)) as host:
            waiting = await self.run_agent(host)
            self.assertEqual(waiting.status, "waiting", waiting.error)
            self.assertEqual(effects, ["ok"])
            self.assertEqual(len(waiting.waits), 2)
            self.assertIn("ask_user", [tool.name for tool in provider.requests[0].tools])
            self.assertFalse(self.resources.has("tool", "ask_user"))
            waits = {wait.request["call_id"]: wait.id for wait in waiting.waits}
            await host.respond(waiting.invocation_id, waits["b"], reply(text="second"))
            partial = await host.join(waiting.invocation_id)
            self.assertEqual([wait.id for wait in partial.waits], [waits["a"]])
            self.assertEqual(len(provider.requests), 1)
            await host.respond(waiting.invocation_id, waits["a"], reply(text="first"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            observed = [item for item in provider.requests[-1].input if isinstance(item, LLMToolResult)]
            self.assertEqual([item.call_id for item in observed], ["a", "read", "b"])
            self.assertEqual(observed[0].structured_content["answers"][0]["text"], "first")

    async def test_invalid_answers_do_not_consume_pending_wait(self):
        """Malformed or request-mismatched replies are rejected before native Resume admission."""
        provider = ScriptedProvider(tool_response(ask_call(questions=[choice(custom=False)])), answer())
        async with self.host(provider, include_ask_user=True) as host:
            waiting = await self.run_agent(host)
            self.assertEqual(waiting.status, "waiting", waiting.error)
            wait_id = waiting.waits[0].id
            invalid = [reply(), reply("database", selected=("missing",), text=None),
                reply("database", selected=("sqlite", "pg"), text=None), reply("database", text="custom"),
                reply("database", text=None), reply("database", selected=("sqlite", "sqlite"), text=None),
                {"answers": [reply("database")["answers"][0]] * 2},
                reply("database", text=" "), reply("database", skipped=True) | {"forged": True}]
            for value in invalid:
                with self.subTest(value=value), self.assertRaises(ValueError):
                    await host.respond(waiting.invocation_id, wait_id, value)
                self.assertEqual([wait.id for wait in (await host.join(waiting.invocation_id)).waits], [wait_id])
            await host.respond(waiting.invocation_id, wait_id, reply("database", text=None, selected=("pg",)))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)

    async def test_one_call_batches_questions_and_supports_skip_and_custom_text(self):
        """A single Wait requires every answer and reports skipped questions without a Tool error."""
        questions = [question(), choice(), choice("features", multiple=True)]
        provider = ScriptedProvider(tool_response(ask_call(questions=questions)), answer())
        async with self.host(provider, include_ask_user=True) as host:
            waiting = await self.run_agent(host)
            self.assertEqual(len(waiting.waits), 1, waiting.error)
            wait = waiting.waits[0]
            with self.assertRaises(ValueError):
                await host.respond(waiting.invocation_id, wait.id, reply())
            await host.respond(waiting.invocation_id, wait.id, {"answers": [
                reply("features", selected=("sqlite", "pg"), text=None)["answers"][0],
                reply("goal", skipped=True)["answers"][0],
                reply("database", text="MySQL")["answers"][0]]})
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            results = [item for item in provider.requests[-1].input if isinstance(item, LLMToolResult)]
            self.assertEqual(len(results), 1)
            self.assertFalse(results[0].is_error)
            answers = results[0].structured_content["answers"]
            self.assertEqual([value["question_id"] for value in answers], ["goal", "database", "features"])
            self.assertEqual(answers[0]["status"], "skipped")
            self.assertEqual(answers[1]["text"], "MySQL")

    async def test_invalid_tool_arguments_return_error_without_wait(self):
        """Question validation errors become Tool observations and can be corrected in the next round."""
        provider = ScriptedProvider(tool_response(ask_call(arguments='{"questions":[]}')), answer())
        async with self.host(provider, include_ask_user=True) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            observed = next(item for item in provider.requests[-1].input if isinstance(item, LLMToolResult))
            self.assertEqual(observed.structured_content["error"]["kind"], "invalid_arguments")

    async def test_question_requires_answer_even_with_bypass(self):
        """Bypass does not invent user input or consume the native question Wait."""
        provider = ScriptedProvider(tool_response(ask_call()), answer())
        async with self.host(provider, include_ask_user=True,
            defaults=settings(mode="bypass", access=AgentAccess(rules={"ask_user": "ask"}))) as host:
            waiting = await self.run_agent(host)
            self.assertEqual(waiting.waits[0].request["kind"], "agent.ask_user", waiting.error)
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply())
            self.assertEqual((await host.join(waiting.invocation_id)).status, "completed")

    async def test_enabled_question_is_independent_of_tool_permissions(self):
        """A native question enters its own Wait despite deny rules and Always Ask."""
        provider = ScriptedProvider(tool_response(ask_call()), answer())
        async with self.host(provider, include_ask_user=True,
            defaults=settings(mode="always_ask", access=AgentAccess(rules={"ask_user": "deny"}, allowed_tools=()))) as host:
            waiting = await self.run_agent(host)
            self.assertEqual(waiting.status, "waiting", waiting.error)
            self.assertIn("ask_user", [tool.name for tool in provider.requests[0].tools])
            self.assertEqual([wait.request["kind"] for wait in waiting.waits], ["agent.ask_user"])
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply())
            self.assertEqual((await host.join(waiting.invocation_id)).status, "completed")

    async def test_question_and_unrelated_approval_wait_are_visible_together(self):
        """Direct human input is not blocked behind an unrelated Tool's approval branch."""
        def write(text: str) -> str:
            return text
        provider = ScriptedProvider(tool_response(ask_call(),
            LLMToolCall(call_id="write", name="write", arguments='{"text":"ok"}')), answer())
        async with self.host(provider, include_ask_user=True, tools=(Tool(write),),
            defaults=settings(access=AgentAccess(rules={"write": "ask"}))) as host:
            waiting = await self.run_agent(host)
            self.assertEqual({wait.request["kind"] for wait in waiting.waits}, {"agent.ask_user", "agent.tool_approval"}, waiting.error)
            for wait in waiting.waits:
                if wait.request["kind"] == "agent.ask_user":
                    await host.respond(waiting.invocation_id, wait.id, reply())
                else:
                    request = restore(AgentApprovalRequest, wait.request)
                    await host.respond(waiting.invocation_id, wait.id,
                        AgentApprovalResponse(decisions=(request.requests[0].respond(approved=True),)).model_dump(mode="json"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)

    async def test_question_ignores_policy_ask_but_other_tool_still_needs_approval(self):
        """A policy Ask applies only to the ordinary Tool in a mixed batch."""
        effects = []
        def write(text: str) -> str:
            effects.append(text)
            return text
        provider = ScriptedProvider(tool_response(ask_call(),
            LLMToolCall(call_id="write", name="write", arguments='{"text":"ok"}')), answer())
        policy = ToolPolicy(rules=[
            dict(when=dict(field="args.questions[0].id", op="equals", value="goal"), decision="ask"),
            dict(when=dict(field="context.tool_name", op="equals", value="write"), decision="ask"),
        ])
        async with self.host(provider, include_ask_user=True, tools=(Tool(write),),
            defaults=settings(access=AgentAccess(policy="approval")), policies=(("approval", policy),)) as host:
            waiting = await self.run_agent(host)
            approval = next(wait for wait in waiting.waits if wait.request["kind"] == "agent.tool_approval")
            batch = restore(AgentApprovalRequest, approval.request)
            self.assertEqual([request.call.name for request in batch.requests], ["write"])
            self.assertEqual({wait.request["kind"] for wait in waiting.waits}, {"agent.ask_user", "agent.tool_approval"})
            self.assertTrue(all(not request.operations for request in batch.requests))
            await host.respond(waiting.invocation_id, approval.id,
                AgentApprovalResponse(decisions=tuple(item.respond(approved=True) for item in batch.requests)).model_dump(mode="json"))
            waiting = await host.join(waiting.invocation_id)
            self.assertEqual(waiting.status, "waiting", waiting.error)
            self.assertEqual(effects, ["ok"])
            self.assertEqual(len(provider.requests), 1)
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply())
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            observed = [item for item in provider.requests[-1].input if isinstance(item, LLMToolResult)]
            self.assertEqual([item.call_id for item in observed], ["ask-1", "write"])

    async def test_successive_rounds_issue_new_waits_and_keep_previous_answers(self):
        """The single loop Back Edge revisits question branches without reusing prior Waits or results."""
        provider = ScriptedProvider(tool_response(ask_call("first")), tool_response(ask_call("second")), answer())
        async with self.host(provider, include_ask_user=True) as host:
            first = await self.run_agent(host)
            await host.respond(first.invocation_id, first.waits[0].id, reply(text="one"))
            second = await host.join(first.invocation_id)
            self.assertEqual(second.status, "waiting", second.error)
            self.assertNotEqual(first.waits[0].id, second.waits[0].id)
            self.assertEqual(second.waits[0].request["call_id"], "second")
            await host.respond(first.invocation_id, second.waits[0].id, reply(text="two"))
            result = await host.join(first.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(len(provider.requests), 3)
            observed = [item for item in provider.requests[-1].input if isinstance(item, LLMToolResult)]
            self.assertEqual([item.structured_content["answers"][0]["text"] for item in observed], ["one", "two"])

    async def test_partial_answers_survive_restart_without_reasking(self):
        """Restart preserves original question identities and only unanswered Map units remain pending."""
        url = await self.database_url()
        async with self.host(ScriptedProvider(tool_response(ask_call("a"), ask_call("b"))),
            url=url, include_ask_user=True) as host:
            waiting = await self.run_agent(host)
            self.assertEqual(waiting.status, "waiting", waiting.error)
            waits = {wait.request["call_id"]: wait for wait in waiting.waits}
            await host.respond(waiting.invocation_id, waits["b"].id, reply(text="already answered"))
            self.assertEqual(len((await host.join(waiting.invocation_id)).waits), 1)
        provider = ScriptedProvider(answer())
        async with self.host(provider, url=url, include_ask_user=True) as host:
            result = await host.recover(waiting.invocation_id)
            self.assertEqual([wait.id for wait in result.waits], [waits["a"].id], result.error)
            self.assertEqual(result.waits[0].request, waits["a"].request)
            self.assertFalse(provider.requests)
            await host.respond(waiting.invocation_id, waits["a"].id, reply(text="new answer"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            results = [item for item in provider.requests[-1].input if isinstance(item, LLMToolResult)]
            self.assertEqual([item.call_id for item in results], ["a", "b"])
            self.assertEqual(results[1].structured_content["answers"][0]["text"], "already answered")

    async def test_approval_mode_change_does_not_consume_native_question(self):
        """Resuming after an approval-mode change keeps the original question pending."""
        provider = ScriptedProvider(tool_response(ask_call()), answer())
        async with self.host(provider, include_ask_user=True,
            defaults=settings(access=AgentAccess(rules={"ask_user": "ask"}))) as host:
            waiting = await self.run_agent(host)
            self.assertEqual(waiting.waits[0].request["kind"], "agent.ask_user")
            await host.pause(waiting.invocation_id)
            await host.extensions["agent"].context.configure(waiting.session_id, settings(mode="bypass").session_defaults())
            result = await host.recover(waiting.invocation_id)
            self.assertEqual(result.status, "waiting", result.error)
            self.assertEqual(result.waits[0].request["kind"], "agent.ask_user")
            self.assertEqual(result.waits[0].id, waiting.waits[0].id)
            self.assertEqual(len(provider.requests), 1)
            await host.respond(waiting.invocation_id, result.waits[0].id, reply())
            self.assertEqual((await host.join(waiting.invocation_id)).status, "completed")

    async def test_cancellation_does_not_invent_an_answer(self):
        """Cancelling an unanswered question cancels the Invocation without another model call."""
        provider = ScriptedProvider(tool_response(ask_call()))
        async with self.host(provider, include_ask_user=True) as host:
            waiting = await self.run_agent(host)
            await host.cancel(waiting.invocation_id)
            self.assertEqual((await host.join(waiting.invocation_id)).status, "cancelled")
            self.assertEqual(len(provider.requests), 1)

    async def test_registered_name_cannot_replace_enabled_builtin(self):
        """An explicit name collision fails safely rather than dispatching a different executable."""
        def external() -> str:
            return "not a question"
        provider = ScriptedProvider(answer())
        async with self.host(provider, include_ask_user=True, tools=(Tool(external, name="ask_user"),)) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "failed")
            self.assertIn("collides", str(result.error))
            self.assertFalse(provider.requests)


class SQLiteAskUserTests(AskUserContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLAskUserTests(AskUserContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
