"""Product registration and lifecycle remain separate from user Agent Workflows."""

from contextlib import asynccontextmanager
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import TypedDict
import unittest
from unittest.mock import AsyncMock, patch

from autoagent import Node, Operator, Workflow
from autoagent.agent import AgentInput, agent_workflow
from autoagent.agent_app import AgentApplication, BUILTIN_AGENT_WORKFLOW_ID, builtin_agent_workflow
from autoagent.host import AutoAgentHost, HostSettings
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, settings


class EchoInput(TypedDict):
    text: str


def echo(value: EchoInput) -> EchoInput:
    return value


class ApplicationBoundaryTests(unittest.TestCase):
    def test_foundations_do_not_import_product_implementation_or_metadata(self):
        """Core, SDK and Host persistence stay independent of product APIs and Session metadata."""
        import ast
        from autoagent.agent.context import AgentSettingsRecord
        from autoagent.host.runtime import HostSetup
        root = Path(__file__).resolve().parents[1] / 'autoagent'
        for directory in ('core', 'agent', 'host/persistence'):
            for path in (root / directory).rglob('*.py'):
                for node in ast.walk(ast.parse(path.read_text())):
                    imports = [alias.name for alias in node.names] if isinstance(node, ast.Import) else [node.module or ''] if isinstance(node, ast.ImportFrom) else []
                    self.assertFalse(any(name.startswith('autoagent.agent_app') for name in imports), str(path))
        self.assertNotIn('title', AgentSettingsRecord.model_fields)
        self.assertNotIn('metadata_version', AgentSettingsRecord.model_fields)
        self.assertNotIn('agent_app', HostSetup.__dataclass_fields__)

    def test_importing_agent_sdk_does_not_import_the_product(self):
        """Importing the generic SDK does not load or re-export the Agent application."""
        result = subprocess.run([sys.executable, "-c",
            "import sys; import autoagent.agent as sdk; "
            "assert 'autoagent.agent_app' not in sys.modules; "
            "assert not hasattr(sdk, 'builtin_agent_workflow'); "
            "assert not hasattr(sdk, 'AgentApplication')"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_registration_requires_the_product_implementation(self):
        """The reserved ID and an ordinary Agent build cannot substitute for product registration."""
        self.assertIsNone(AgentApplication.validate_workflows([agent_workflow(workflow_id="custom")]))
        with self.assertRaisesRegex(ValueError, "reserved"):
            AgentApplication.validate_workflows([agent_workflow(workflow_id=BUILTIN_AGENT_WORKFLOW_ID)])
        product = builtin_agent_workflow(execution=False)
        self.assertIs(AgentApplication.validate_workflows([product]), product)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            AgentApplication.validate_workflows([product, product])


class ApplicationLifecycleContracts:
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.url = await self.database_url()

    async def database_url(self):
        return "sqlite+aiosqlite:///" + str(self.root / "runtime.db")

    def host(self, workflows=(), *, lifespan=None, read_only=False):
        return AutoAgentHost(root=self.root, workflows=workflows, lifespan=lifespan,
            settings=HostSettings(database_url=self.url), environment={}, read_only=read_only)

    async def test_user_workflows_run_without_product_services(self):
        """Custom Agent and ordinary Workflows execute without product queue or resource monitoring."""
        private = agent_workflow(workflow_id="private", files=False, execution=False)
        ordinary = Workflow("ordinary", nodes=[Node("echo", Operator(echo))])
        provider = ScriptedProvider(answer("Private answer"))

        @asynccontextmanager
        async def resources(setup):
            setup.agent_context.resources.register_provider("offline", provider)
            setup.agent_context.set_defaults(settings())
            yield

        async with self.host([private, ordinary], lifespan=resources) as host:
            self.assertFalse(host.agent_app.enabled)
            self.assertFalse(host.agent_app.running)
            self.assertFalse(host.agent_app.owns(private))
            self.assertIsNone(host.agent_app.context.messages.task)
            self.assertIsNone(host.agent_app.context.shared_resources.monitor)
            private_result = await host.join(await host.execute("private", AgentInput(message="Hello")))
            self.assertEqual(private_result.status, "completed", private_result.error)
            ordinary_result = await host.join(await host.execute("ordinary", {"text": "Hello"}))
            self.assertEqual(ordinary_result.output, {"text": "Hello"})
            self.assertEqual(ordinary_result.status, "completed", ordinary_result.error)

    async def test_product_owns_only_its_workflow_and_closes_its_services(self):
        """The product owns its definitions and IR, and stops both queue and monitor on Host exit."""
        product = builtin_agent_workflow(execution=False)
        private = agent_workflow(workflow_id="private", files=False, execution=False)
        async with self.host([product, private]) as host:
            application = host.agent_app
            self.assertIs(host._runtime.setup.applications["agent"], application)
            self.assertTrue(application.enabled)
            self.assertTrue(application.running)
            self.assertTrue(application.owns(product))
            self.assertTrue(application.owns(host._runtime.workflow_irs[
                host._runtime.workflow_ids[BUILTIN_AGENT_WORKFLOW_ID]]))
            self.assertFalse(application.owns(private))
            self.assertFalse(application.owns(host._runtime.workflow_irs[
                host._runtime.workflow_ids["private"]]))
            queue = host.agent_app.context.messages.task
            monitor = host.agent_app.context.shared_resources.monitor
            self.assertFalse(queue.done())
            self.assertFalse(monitor.done())
            with self.assertRaisesRegex(RuntimeError, "already started"):
                await application.start()
        self.assertFalse(application.running)
        self.assertTrue(queue.done())
        self.assertTrue(monitor.done())
        await application.close()

    async def test_project_lifespan_can_register_the_product(self):
        """Product identity is resolved after project lifespan has supplied its final Workflows."""
        @asynccontextmanager
        async def resources(setup):
            self.assertIsInstance(setup.applications["agent"], AgentApplication)
            setup.register_workflow(builtin_agent_workflow(execution=False))
            yield

        async with self.host(lifespan=resources) as host:
            self.assertTrue(host.agent_app.running)
            self.assertIn(BUILTIN_AGENT_WORKFLOW_ID, host._runtime.workflow_ids)

    async def test_reserved_id_fails_before_database_open(self):
        """A programmatic user Workflow cannot occupy the product ID or open its database."""
        host = self.host([agent_workflow(workflow_id=BUILTIN_AGENT_WORKFLOW_ID)])
        with patch("autoagent.host.host.DatabaseSink.open", new_callable=AsyncMock) as opened:
            with self.assertRaisesRegex(ValueError, "reserved"):
                async with host:
                    self.fail("An ordinary Agent build cannot become the product.")
            opened.assert_not_called()
        self.assertFalse(host.agent_app.running)

    async def test_late_reserved_id_failure_releases_project_resources(self):
        """An invalid lifespan registration unwinds resources and releases the database writer."""
        released = []

        @asynccontextmanager
        async def resources(setup):
            try:
                setup.register_workflow(agent_workflow(workflow_id=BUILTIN_AGENT_WORKFLOW_ID))
                yield
            finally:
                released.append(True)

        host = self.host(lifespan=resources)
        with self.assertRaisesRegex(ValueError, "reserved"):
            async with host:
                self.fail("The invalid product registration must fail.")
        self.assertEqual(released, [True])
        self.assertFalse(host.agent_app.running)
        self.assertIsNone(host.agent_app.context.messages.task)
        self.assertIsNone(host.agent_app.context.shared_resources.monitor)
        async with self.host() as reopened:
            self.assertFalse(reopened.agent_app.enabled)

    async def test_partial_submission_startup_closes_queue_and_runtime_resources(self):
        """A failure after queue creation closes every product service and releases the writer."""
        host = self.host([builtin_agent_workflow(execution=False)])
        start = host.agent_app.context.messages.start

        async def fail():
            await start()
            raise ValueError("submission startup failed")

        with self.assertNoLogs("autoagent.host.residency", level="ERROR"):
            with patch.object(host.agent_app.context.messages, "start", side_effect=fail):
                with self.assertRaisesRegex(ValueError, "submission startup failed"):
                    async with host:
                        self.fail("Queue startup must fail.")
        self.assertFalse(host.agent_app.running)
        self.assertTrue(host.agent_app.context.messages.task.done())
        self.assertTrue(host.agent_app.context.shared_resources.monitor.done())
        self.assertTrue(host._runtime.residency.task.done())
        async with self.host([builtin_agent_workflow(execution=False)]) as reopened:
            self.assertTrue(reopened.agent_app.running)

    async def test_read_only_host_does_not_start_product_services(self):
        """Opening existing history read-only never starts product resources or submissions."""
        product = builtin_agent_workflow(execution=False)
        async with self.host([product]):
            pass
        async with self.host([product], read_only=True) as host:
            self.assertFalse(host.agent_app.enabled)
            self.assertFalse(host.agent_app.running)
            self.assertIsNone(host.agent_app.context.messages.task)
            self.assertIsNone(host.agent_app.context.shared_resources.monitor)


class SQLiteApplicationLifecycleTests(ApplicationLifecycleContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL test database is not configured")
class PostgreSQLApplicationLifecycleTests(ApplicationLifecycleContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
