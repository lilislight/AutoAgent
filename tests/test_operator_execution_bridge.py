"""Owned blocking work nested inside asynchronous Core Operators."""

import asyncio
from contextvars import ContextVar
import threading
import unittest
from unittest.mock import patch

from typing_extensions import TypedDict

from autoagent import Node, Workflow, run_sync
from autoagent.core.app import AutoAgentApp
from autoagent.agent import LLMToolCall, LLMToolResult, ToolSet, tool


class Value(TypedDict):
    value: str


async def wait_set(event):
    async with asyncio.timeout(2):
        while not event.is_set():
            await asyncio.sleep(0.005)


class OperatorExecutionBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_standalone_execution_preserves_context_and_keyword_arguments(self):
        """Outside Core the helper uses caller-owned workers and copies context."""
        marker = ContextVar("test_marker", default="missing")
        token = marker.set("standalone")
        try:
            result = await run_sync(lambda *, number: (number, marker.get(), threading.get_ident()), number=3)
        finally:
            marker.reset(token)
        self.assertEqual(result[:2], (3, "standalone"))
        self.assertNotEqual(result[2], threading.get_ident())

    async def test_nested_tool_uses_core_workers_and_releases_context(self):
        """A synchronous Tool inside an async Operator avoids the default executor."""
        @tool
        def identify() -> str:
            return threading.current_thread().name
        tools = ToolSet((identify,))
        async def embedded(call: LLMToolCall) -> LLMToolResult:
            return await tools.ainvoke(call)
        app = AutoAgentApp(max_operator_concurrency=1)
        try:
            with patch("asyncio.to_thread", side_effect=AssertionError("Default executor used")):
                result = await app.ainvoke(Workflow("nested", nodes=[Node("call", embedded)]),
                                          LLMToolCall(call_id="1", name="identify", arguments="{}"))
            self.assertEqual(result.status, "completed", result.error)
            self.assertTrue(result.output["content"][0]["text"].startswith("autoagent-operator-"))
        finally:
            await app.aclose()

    async def test_cancelled_nested_work_retains_capacity_until_physical_completion(self):
        """Cancelling an async owner cannot admit another Call while its sync Tool runs."""
        entered, release, second_entered = threading.Event(), threading.Event(), threading.Event()
        @tool
        def block() -> str:
            entered.set()
            release.wait(3)
            return "finished"
        tools = ToolSet((block,))
        async def embedded(call: LLMToolCall) -> LLMToolResult:
            return await tools.ainvoke(call)
        async def second() -> Value:
            second_entered.set()
            return {"value": "second"}
        app = AutoAgentApp(max_operator_concurrency=1)
        try:
            first = await app.asubmit_invoke(Workflow("first", nodes=[Node("call", embedded)]),
                                            LLMToolCall(call_id="1", name="block", arguments="{}"))
            await wait_set(entered)
            await app.acancel(first.ref)
            self.assertEqual((await app.ajoin(first.ref)).status, "cancelled")
            following = await app.asubmit_invoke(Workflow("second", nodes=[Node("call", second)]), None)
            await asyncio.sleep(.03)
            self.assertFalse(second_entered.is_set())
            release.set()
            result = await asyncio.wait_for(app.ajoin(following.ref), 2)
            self.assertEqual(result.status, "completed", result.error)
        finally:
            release.set()
            await app.aclose()

    async def test_detached_task_cannot_dispatch_after_operator_scope_ends(self):
        """Inherited contexts expire instead of submitting unowned late worker jobs."""
        gate, finished = threading.Event(), threading.Event()
        errors, effects = [], []
        async def owner() -> None:
            async def detached():
                while not gate.is_set():
                    await asyncio.sleep(.005)
                try:
                    await run_sync(effects.append, "unexpected")
                except Exception as error:
                    errors.append(error)
                finally:
                    finished.set()
            asyncio.create_task(detached())
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(Workflow("scope", nodes=[Node("owner", owner)]), None)
            self.assertEqual(result.status, "completed", result.error)
            gate.set()
            await wait_set(finished)
            self.assertEqual(effects, [])
            self.assertIsInstance(errors[0], RuntimeError)
            self.assertIn("scope has ended", str(errors[0]))
        finally:
            gate.set()
            await app.aclose()

    async def test_work_submitted_before_owner_returns_still_retains_capacity(self):
        """Already submitted nested work remains tracked even when its owner finishes."""
        entered, release, finished, next_entered = [threading.Event() for _ in range(4)]
        def blocking():
            entered.set()
            release.wait(3)
        async def owner() -> None:
            async def detached():
                try:
                    await run_sync(blocking)
                finally:
                    finished.set()
            asyncio.create_task(detached())
            while not entered.is_set():
                await asyncio.sleep(.005)
        async def following() -> None:
            next_entered.set()
        app = AutoAgentApp(max_operator_concurrency=1)
        try:
            result = await app.ainvoke(Workflow("owner", nodes=[Node("call", owner)]), None)
            self.assertEqual(result.status, "completed", result.error)
            next_call = await app.asubmit_invoke(Workflow("following", nodes=[Node("call", following)]), None)
            await asyncio.sleep(.03)
            self.assertFalse(next_entered.is_set())
            release.set()
            await wait_set(finished)
            self.assertEqual((await asyncio.wait_for(app.ajoin(next_call.ref), 2)).status, "completed")
        finally:
            release.set()
            await app.aclose()
