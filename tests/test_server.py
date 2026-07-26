from __future__ import annotations

import asyncio
import unittest
from uuid import uuid4

from fastapi import HTTPException

from autoagent import AutoAgentApp, SystemCommand, Workflow
from autoagent.core.server import AutoAgentServer
from autoagent.core.server.app import (
    InvocationResumeRequest,
    InvocationSubmitRequest,
)


class AutoAgentServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = AutoAgentApp()
        self.workflow = Workflow(id="server_wait")
        self.workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        self.app.register_workflow(self.workflow)
        self.server = AutoAgentServer(self.app)
        self.submit = next(
            route.endpoint
            for route in self.server.api.routes
            if getattr(route, "name", "") == "submit_invocation"
        )
        self.resume = next(
            route.endpoint
            for route in self.server.api.routes
            if getattr(route, "name", "") == "resume_invocation"
        )

    async def asyncTearDown(self) -> None:
        if self.server._invocation_tasks:
            await asyncio.gather(
                *tuple(self.server._invocation_tasks.values()),
                return_exceptions=True,
            )
        await self.app.aclose()

    async def test_waiting_session_rejects_submit_before_new_admission(self) -> None:
        first = await self.submit(
            self.workflow.id,
            InvocationSubmitRequest(
                session_key="same",
                input={"wait_key": "approval"},
            ),
        )
        await self._wait_for_state(first.invocation_id, "waiting")
        invocation_count = len(self.app.runtime_store.invocations)

        with self.assertRaises(HTTPException) as captured:
            await self.submit(
                self.workflow.id,
                InvocationSubmitRequest(
                    session_key="same",
                    input={"wait_key": "other"},
                ),
            )

        self.assertEqual(409, captured.exception.status_code)
        self.assertEqual(
            invocation_count,
            len(self.app.runtime_store.invocations),
        )

    async def test_submit_response_session_key_can_resume_wait(self) -> None:
        submitted = await self.submit(
            self.workflow.id,
            InvocationSubmitRequest(
                input={"wait_key": "approval"},
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "waiting")

        resumed = await self.resume(
            self.workflow.id,
            InvocationResumeRequest(
                session_key=submitted.session_key,
                wait_key="approval",
                output={"approved": True},
            ),
        )

        self.assertEqual(submitted.session_id, resumed.session_id)
        self.assertEqual(submitted.session_key, resumed.session_key)
        self.assertEqual(submitted.invocation_id, resumed.invocation_id)
        self.assertEqual("completed", resumed.state)

    async def test_submit_selects_event_mode_per_invocation(self) -> None:
        submitted = await self.submit(
            self.workflow.id,
            InvocationSubmitRequest(
                input={"wait_key": "minimal"},
                session_key="minimal",
                event_mode="minimal",
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "waiting")

        invocation = self.app.runtime_store.invocations[submitted.invocation_id]
        events = await self.app.runtime_store.alist_runtime_events(
            invocation_id=invocation.id,
        )
        self.assertEqual("minimal", invocation.event_mode)
        self.assertEqual([], list(events))

    async def test_background_failure_is_retrieved_and_retained(self) -> None:
        invocation_id = uuid4()

        async def fail() -> None:
            raise RuntimeError("background failed")

        task = asyncio.create_task(fail())
        self.server._invocation_tasks[invocation_id] = task
        task.add_done_callback(
            lambda completed: self.server._finish_invocation_task(
                invocation_id,
                completed,
            )
        )
        with self.assertRaisesRegex(RuntimeError, "background failed"):
            await task
        await asyncio.sleep(0)

        self.assertNotIn(invocation_id, self.server._invocation_tasks)
        self.assertIsInstance(
            self.server._invocation_failures[invocation_id],
            RuntimeError,
        )

    async def _wait_for_state(
        self,
        invocation_id,
        state: str,
    ) -> None:
        for _ in range(1_000):
            invocation = self.app.runtime_store.invocations[invocation_id]
            if invocation.state == state:
                return
            await asyncio.sleep(0.001)
        self.fail(f"Invocation did not reach state {state}.")
