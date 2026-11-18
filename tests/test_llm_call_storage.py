"""Durable model-call metadata, optional storage and actual Core/Host correlation."""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
import unittest

from autoagent import InputMappingContext, Node, Workflow
from autoagent.core.app import AutoAgentApp
from autoagent.agent import (
    LLM_CALL, LLMCallRecord, LLMCallContext, LLMRequest, LLMResponse, LLMSelection,
    OpenAIResponsesProvider, llm_call_operator, llm_recording_context, runtime_llm_recorder,
)
from autoagent.core import OperatorCallContext
try:
    import sqlalchemy
except ModuleNotFoundError as error:
    raise unittest.SkipTest("Install autoagent[persistence] for database recording tests") from error

from autoagent.host import AutoAgentHost, HostSettings
from autoagent.host.persistence import DatabaseSink, ConflictError
from autoagent.host.persistence.llm_calls import llm_calls
from autoagent.host.persistence.schema import meta
from tests import test_agent_providers as providers
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


async def read_rows(url, statement):
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            return [dict(row) for row in (await connection.execute(statement)).mappings()]
    finally:
        await engine.dispose()


async def has_table(url, name):
    from sqlalchemy import inspect
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            return await connection.run_sync(lambda sync: inspect(sync).has_table(name))
    finally:
        await engine.dispose()


def running(**fields):
    values = dict(llm_call_id="model-call-1", kind="generation", provider_id="test", protocol="openai.responses",
                  requested_model="model", status="running", started_at_us=1)
    return LLMCallRecord(**(values | fields))


def completed(record, **fields):
    values = record.model_dump(mode="python") | dict(status="succeeded", completed_at_us=2, duration_ns=10,
                                                    usage_reported=True, input_tokens=3, output_tokens=2, total_tokens=5)
    return LLMCallRecord(**(values | fields))


class LLMCallStorageContracts:
    provider = providers.ProviderSDKTests.provider

    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "runtime.sqlite"
        self.url = await self.database_url()
        self.sink = await DatabaseSink.open(self.url)
        self.addAsyncCleanup(self.sink.close)

    async def database_url(self):
        return "sqlite+aiosqlite:///" + str(self.path)

    async def rows(self):
        return await read_rows(self.url, sqlalchemy.select(llm_calls).order_by(llm_calls.c.started_at_us, llm_calls.c.llm_call_id))

    async def test_records_are_durable_idempotent_and_terminal_rows_cannot_regress(self):
        """Duplicate writes are safe and late start messages cannot erase terminal measurements."""
        start = running(workflow_id="example", workflow_revision_id="revision-1", workflow_version="v1")
        end = completed(start)
        await self.sink.record_llm_call(start)
        self.assertEqual((await self.rows())[0]["status"], "running")
        await self.sink.record_llm_call(end)
        await self.sink.record_llm_call(end)
        await self.sink.record_llm_call(start)
        self.assertEqual(len(await self.rows()), 1)
        self.assertEqual((await self.rows())[0]["total_tokens"], 5)
        with self.assertRaises(ConflictError):
            await self.sink.record_llm_call(completed(start, total_tokens=6))
        with self.assertRaises(ConflictError):
            await self.sink.record_llm_call(running(workflow_revision_id="other"))

    async def test_optional_table_and_rows_survive_database_reopen(self):
        """Optional metrics storage does not alter or invalidate the canonical Runtime format."""
        await self.sink.record_llm_call(completed(running()))
        await self.sink.close()
        reopened = await DatabaseSink.open(self.url)
        self.addAsyncCleanup(reopened.close)
        await reopened.record_llm_call(completed(running()))
        self.assertEqual((await self.rows())[0]["status"], "succeeded")

    async def test_database_is_not_extended_until_recording_is_used(self):
        """Opening persistence alone does not create the optional model-call table."""
        self.assertFalse(await has_table(self.url, "llm_calls"))
        provider, _, _ = self.provider()
        await provider.ainvoke(providers.request())
        self.assertFalse(await has_table(self.url, "llm_calls"))

    async def test_provider_writes_both_phases_and_standalone_workflow_metadata(self):
        """Explicit recorder injection persists reported usage and supplied standalone associations."""
        provider, _, _ = self.provider(recorder=self.sink)
        with llm_recording_context(LLMCallContext(workflow_id="manual", workflow_revision_id="revision-1", workflow_version="v1")):
            await provider.ainvoke(providers.request())
        row = (await self.rows())[0]
        self.assertEqual(row["workflow_id"], "manual")
        self.assertEqual(row["workflow_revision_id"], "revision-1")
        self.assertEqual(row["workflow_version"], "v1")
        self.assertEqual(row["total_tokens"], 5)
        self.assertEqual(row["status"], "succeeded")

    async def test_two_concurrent_calls_persist_distinct_associations_and_usage(self):
        """Concurrent calls sharing a sink cannot overwrite each other's identity or tokens."""
        provider, _, _ = self.provider(recorder=self.sink)
        async def invoke(name):
            with llm_recording_context(LLMCallContext(workflow_id=name, workflow_revision_id=name + "-revision")):
                await provider.ainvoke(providers.request())
        await asyncio.gather(invoke("first"), invoke("second"))
        rows = await self.rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["workflow_id"] for row in rows}, {"first", "second"})
        self.assertTrue(all(row["total_tokens"] == 5 for row in rows))


@unittest.skipUnless(providers.AsyncOpenAI is not None, "Install autoagent[agent] for SDK transport tests")
class LLMCallStorageTests(LLMCallStorageContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL and providers.AsyncOpenAI is not None, "PostgreSQL URL and Agent SDK required")
class PostgreSQLLLMCallStorageTests(LLMCallStorageContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()


@unittest.skipUnless(providers.AsyncOpenAI is not None, "Install autoagent[agent] for SDK transport tests")
class RuntimeRecordingIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def database_url(self, path):
        return "sqlite+aiosqlite:///" + str(path)

    async def test_llm_capability_records_resolved_models_and_per_call_streaming(self):
        """Host stores actual selections while routing hints stay out of intercepted HTTP requests."""
        import httpx
        from openai import AsyncOpenAI

        captured = []
        async def handle(req):
            body = json.loads(req.content)
            captured.append(body)
            response = providers.response_body(model=body["model"])
            if body.get("stream"):
                return httpx.Response(200, content=providers.sse([
                    {"type": "response.completed", "response": response},
                ]), headers={"content-type": "text/event-stream"})
            return httpx.Response(200, json=response)
        @asynccontextmanager
        async def resources(setup):
            sdk = AsyncOpenAI(api_key="test", max_retries=0,
                              http_client=httpx.AsyncClient(transport=httpx.MockTransport(handle)))
            sdk._platform = "Linux"
            try:
                async with OpenAIResponsesProvider(client=sdk) as provider:
                    setup.register_operator(llm_call_operator(resolver=lambda value: LLMSelection(
                        provider, value.model or value.routing["selected_model"])), capability_id=LLM_CALL.id)
                    yield
            finally:
                await sdk.close()
        def model_input(ctx: InputMappingContext) -> LLMRequest:
            return providers.request(**dict(ctx.invocation_input))
        workflow = Workflow("selected", nodes=[Node("llm", LLM_CALL, input_mapping=model_input)])
        with tempfile.TemporaryDirectory() as directory:
            url = await self.database_url(Path(directory) / "selected.sqlite")
            async with AutoAgentHost(workflows=[workflow], settings=HostSettings(database_url=url),
                                     lifespan=resources, root=directory, environment={}) as host:
                for payload in ({"model": "fixed", "streaming": False, "max_output_tokens": 128},
                                {"model": None, "streaming": True, "max_output_tokens": 512,
                                 "routing": {"selected_model": "automatic"}}):
                    result = await host.join(await host.execute(workflow.id, payload))
                    self.assertEqual(result.status, "completed", result.error)
                rows = await read_rows(url, sqlalchemy.select(llm_calls).order_by(llm_calls.c.started_at_us))
                self.assertEqual([(row["requested_model"], row["streaming"], row["status"]) for row in rows],
                                 [("fixed", False, "succeeded"), ("automatic", True, "succeeded")])
                self.assertEqual({row["node_id"] for row in rows}, {"llm"})
                self.assertTrue(all(row["operator_call_id"] and row["operator_definition_id"] for row in rows))
        self.assertEqual([body["max_output_tokens"] for body in captured], [128, 512])
        self.assertTrue(all("routing" not in body and "streaming" not in body for body in captured))

    async def test_core_workflow_and_revision_are_automatic_and_override_manual_labels(self):
        """A real Core invocation records its actual Workflow revision and Operator Call identity."""
        from tests.test_agent_recording import Recorder
        recorder = Recorder()
        holder = {}
        @asynccontextmanager
        async def resources():
            import httpx
            from openai import AsyncOpenAI
            sdk = AsyncOpenAI(api_key="test", max_retries=0, http_client=httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=providers.response_body()))))
            sdk._platform = "Linux"
            async with OpenAIResponsesProvider(client=sdk) as provider:
                holder["provider"] = provider
                with runtime_llm_recorder(recorder):
                    try:
                        yield
                    finally:
                        await sdk.close()
        async def generate(value: LLMRequest) -> LLMResponse:
            with llm_recording_context(LLMCallContext(workflow_id="wrong", workflow_revision_id="wrong")):
                return await holder["provider"].ainvoke(value)
        workflow = Workflow("actual", version="release-1", nodes=[Node("model", generate)])
        app = AutoAgentApp(runtime_lifespan=resources)
        try:
            await app.aprepare()
            result = await app.ainvoke(workflow, providers.request())
            self.assertEqual(result.status, "completed", result.error)
            row = recorder.records[-1]
            self.assertEqual(row.workflow_id, "actual")
            self.assertEqual(row.workflow_version, "release-1")
            self.assertEqual(row.workflow_revision_id, result.ref.workflow_revision_id)
            self.assertEqual(row.session_id, result.ref.session_id)
            self.assertEqual(row.invocation_id, result.ref.invocation_id)
            self.assertEqual(row.node_id, "model")
            self.assertIsNotNone(row.operator_call_id)
            self.assertIsNotNone(row.occurrence_id)
            self.assertEqual(row.unit_index, 0)
            self.assertEqual(row.attempt, 1)
        finally:
            await app.aclose()

    async def test_sync_core_workers_receive_call_identity_and_context_is_reset(self):
        """Core worker dispatch preserves execution identities without leaking them to callers."""
        seen = []
        def inspect_context() -> None:
            context = OperatorCallContext.current()
            seen.append(context)
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(Workflow("sync-context", nodes=[Node("inspect", inspect_context)]), None)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(seen[0].workflow_id, "sync-context")
            self.assertEqual(seen[0].operator_id, "inspect_context")
            self.assertIsNotNone(seen[0].operator_call_id)
            self.assertIsNone(OperatorCallContext.current())
        finally:
            await app.aclose()

    async def test_host_automatically_persists_calls_without_provider_database_configuration(self):
        """Host binds its database on the Runtime loop and records actual Workflow and definition IDs."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "host.sqlite"
            url = await self.database_url(path)
            holder = {}
            @asynccontextmanager
            async def resources(setup):
                import httpx
                from openai import AsyncOpenAI
                sdk = AsyncOpenAI(api_key="test", max_retries=0, http_client=httpx.AsyncClient(transport=httpx.MockTransport(
                    lambda request: httpx.Response(200, json=providers.response_body()))))
                sdk._platform = "Linux"
                async with OpenAIResponsesProvider(client=sdk) as provider:
                    holder["provider"] = provider
                    try:
                        yield
                    finally:
                        await sdk.close()
            async def generate(value: LLMRequest) -> LLMResponse:
                return await holder["provider"].ainvoke(value)
            workflow = Workflow("host-model", version="release-2", nodes=[Node("model", generate)])
            settings = HostSettings(database_url=url)
            async with AutoAgentHost(workflows=[workflow], settings=settings, lifespan=resources, root=directory) as host:
                result = await host.join(await host.execute("host-model", providers.request()))
                self.assertEqual(result.status, "completed", result.error)
                row = (await read_rows(url, sqlalchemy.select(llm_calls)))[0]
                calls = await host.list_operator_calls(result.invocation_id)
                call = next(item for item in calls.items if item['call_id']==row['operator_call_id'])
                self.assertEqual(row["workflow_id"], "host-model")
                self.assertEqual(row["workflow_version"], "release-2")
                self.assertEqual(row["workflow_revision_id"], result.ref.workflow_revision_id)
                self.assertEqual(row["operator_definition_id"], call["operator_definition_id"])
                self.assertEqual(row["status"], "succeeded")
                self.assertEqual(row["total_tokens"], 5)
            async with await DatabaseSink.open(url) as sink:
                await sink.record_llm_call(completed(running(llm_call_id="standalone", workflow_id="other")))
                await sink.purge_root_graph(result.ref.invocation_id)
                remaining = await read_rows(url, sqlalchemy.select(llm_calls.c.llm_call_id))
                self.assertEqual(remaining, [{"llm_call_id": "standalone"}])


@unittest.skipUnless(POSTGRES_TEST_URL and providers.AsyncOpenAI is not None, "PostgreSQL URL and Agent SDK required")
class PostgreSQLRuntimeRecordingTests(RuntimeRecordingIntegrationTests):
    async def database_url(self, path):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
