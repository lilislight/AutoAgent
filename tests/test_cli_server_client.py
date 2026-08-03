from __future__ import annotations

import unittest

import httpx

from autoagent import AutoAgentApp, AutoAgentSettings, SystemCommand, Workflow
from autoagent.cli.server_client import (
    AutoAgentServerClient,
    ServerClientError,
    resolve_server_url,
)
from autoagent.core.server import AutoAgentServer


def echo(value: str) -> str:
    return value


class ServerUrlTests(unittest.TestCase):
    def test_default_url_uses_localhost_and_default_port(self) -> None:
        self.assertEqual(
            "http://127.0.0.1:8765",
            resolve_server_url({}),
        )

    def test_url_uses_environment_without_requiring_cli_address(self) -> None:
        self.assertEqual(
            "https://autoagent.example",
            resolve_server_url(
                {
                    "AUTOAGENT_SERVER_URL": "https://autoagent.example/",
                    "AUTOAGENT_SERVER_PORT": "9000",
                }
            ),
        )
        self.assertEqual(
            "http://127.0.0.1:9000",
            resolve_server_url(
                {
                    "AUTOAGENT_SERVER_HOST": "0.0.0.0",
                    "AUTOAGENT_SERVER_PORT": "9000",
                }
            ),
        )

    def test_explicit_url_overrides_environment(self) -> None:
        self.assertEqual(
            "http://other.example:8080",
            resolve_server_url(
                {"AUTOAGENT_SERVER_URL": "http://from-env.example"},
                explicit_url="http://other.example:8080/",
            ),
        )

    def test_invalid_url_and_port_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "absolute http"):
            resolve_server_url(
                {},
                explicit_url="autoagent.example:8765",
            )
        with self.assertRaisesRegex(ValueError, "between 1 and 65535"):
            resolve_server_url({"AUTOAGENT_SERVER_PORT": "0"})
        with self.assertRaisesRegex(ValueError, "only scheme"):
            resolve_server_url(
                {},
                explicit_url="https://autoagent.example/api",
            )


class AutoAgentServerClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_client_submits_and_waits_through_real_server_api(self) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="remote_echo")
        workflow.add_node(echo, node_id="echo")
        app.register_workflow(workflow)
        await app.astart()
        server = AutoAgentServer(app)
        transport = httpx.ASGITransport(app=server.api)

        try:
            async with AutoAgentServerClient(
                "http://autoagent.test",
                transport=transport,
            ) as client:
                submitted = await client.submit(
                    "remote_echo",
                    input={"value": "hello"},
                    session_key="remote-session",
                    entry_node_id=None,
                    event_mode="standard",
                )
                detail = await client.wait_for_invocation(
                    str(submitted["invocation_id"]),
                    timeout=2,
                )
                events = await client.events(str(submitted["invocation_id"]))
        finally:
            await app.aclose()

        self.assertEqual("remote_echo", submitted["workflow_id"])
        self.assertEqual("remote-session", submitted["session_key"])
        self.assertEqual("completed", detail["state"])
        self.assertEqual({"output": "hello"}, detail["result"])
        self.assertGreater(len(events), 0)

    async def test_client_rejects_missing_access_token(self) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        await app.astart()
        server = AutoAgentServer(app, access_token="secret")
        transport = httpx.ASGITransport(app=server.api)

        try:
            with self.assertRaisesRegex(
                ServerClientError,
                "AUTOAGENT_SERVER_ACCESS_TOKEN",
            ):
                async with AutoAgentServerClient(
                    "http://autoagent.test",
                    transport=transport,
                ):
                    pass
        finally:
            await app.aclose()

    async def test_client_resumes_waiting_invocation(self) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="remote_wait")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        app.register_workflow(workflow)
        await app.astart()
        server = AutoAgentServer(app)
        transport = httpx.ASGITransport(app=server.api)

        try:
            async with AutoAgentServerClient(
                "http://autoagent.test",
                transport=transport,
            ) as client:
                submitted = await client.submit(
                    "remote_wait",
                    input={"wait_key": "approval"},
                    session_key="resume-session",
                    entry_node_id=None,
                    event_mode="standard",
                )
                waiting = await client.wait_for_invocation(
                    str(submitted["invocation_id"]),
                    timeout=2,
                )
                resumed = await client.resume(
                    "remote_wait",
                    session_key="resume-session",
                    wait_key="approval",
                    output_supplied=True,
                    output={"approved": True},
                )
                completed = await client.wait_for_invocation(
                    str(resumed["invocation_id"]),
                    timeout=2,
                )
        finally:
            await app.aclose()

        self.assertEqual("waiting", waiting["state"])
        self.assertEqual(submitted["invocation_id"], resumed["invocation_id"])
        self.assertEqual("completed", completed["state"])

    async def test_client_uses_bearer_access_token(self) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        await app.astart()
        server = AutoAgentServer(app, access_token="secret")
        transport = httpx.ASGITransport(app=server.api)

        try:
            async with AutoAgentServerClient(
                "http://autoagent.test",
                access_token="secret",
                transport=transport,
            ) as client:
                health = await client.health()
        finally:
            await app.aclose()

        self.assertTrue(health["authenticated"])


if __name__ == "__main__":
    unittest.main()
