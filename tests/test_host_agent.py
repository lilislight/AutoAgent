"""Host Agent environment profiles, resource ownership and complete request defaults."""

import asyncio
from contextlib import asynccontextmanager
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from autoagent import Node, Workflow
from autoagent.agent import EMBEDDING, LLM_CALL, llm_call_operator
from autoagent.host import AgentSettings, AutoAgentHost, HostSettings, embedding_request, llm_request
from tests.test_agent_capabilities import DemoEmbedding, DemoLLM, request
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class AgentSettingsTests(unittest.TestCase):
    def test_unconfigured_profiles_and_explicit_requests_need_no_credentials(self):
        """An empty route stays disabled; explicit model requests work independently of Host defaults."""
        self.assertIsNone(AgentSettings.from_environment({}))
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(llm_request(model="chosen", input=request().input).model, "chosen")
            self.assertEqual(embedding_request(model="vectors", input="hello").model, "vectors")
            with self.assertRaisesRegex(ValueError, "AUTOAGENT_LLM_MODEL"):
                llm_request(input=request().input)

    def test_routes_validate_transport_and_never_put_credentials_in_requests(self):
        """Provider and backend are independent, while request defaults contain no connection secrets."""
        env = dict(AUTOAGENT_LLM_PROVIDER="deepseek", AUTOAGENT_LLM_BACKEND="litellm",
                   AUTOAGENT_LLM_MODEL="demo", AUTOAGENT_LLM_API_KEY="private-value",
                   AUTOAGENT_LLM_MAX_OUTPUT_TOKENS="123", AUTOAGENT_LLM_STREAMING="true")
        settings = AgentSettings.from_environment(env)
        self.assertTrue(settings.streaming)
        self.assertNotIn("private-value", repr(settings))
        with patch.dict(os.environ, env, clear=True):
            value = llm_request(input=request().input, options={"thinking": {"type": "disabled"}})
            self.assertEqual(value.model, "demo")
            self.assertEqual(value.max_output_tokens, 123)
            self.assertEqual(value.provider_options.provider_id, "deepseek")
            self.assertNotIn("private-value", value.model_dump_json())
            self.assertEqual(llm_request(model="override", input=request().input, max_output_tokens=7).max_output_tokens, 7)
        for field, value in (("BACKEND", "direct"), ("TIMEOUT_SECONDS", "nan"), ("STREAMING", "maybe"),
                             ("MAX_OUTPUT_TOKENS", "0"), ("PROTOCOL", "unknown")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                AgentSettings.from_environment(dict(env, **{"AUTOAGENT_LLM_" + field: value}))

    def test_provider_factories_choose_vendor_and_protocol_without_loading_sdks(self):
        """Factory selection keeps vendor adaptations, OpenAI-compatible connections and embeddings explicit."""
        from autoagent.agent import DeepSeekProvider, LiteLLMProvider, OpenAIResponsesProvider, OpenAIEmbeddingProvider
        for changes, expected in (({"provider": "deepseek"}, DeepSeekProvider),
                                  ({"provider": "deepseek", "backend": "litellm"}, LiteLLMProvider),
                                  ({"protocol": "openai.responses"}, OpenAIResponsesProvider)):
            provider = AgentSettings(model="demo", api_key="test", **changes).create_provider()
            self.assertIsInstance(provider, expected)
        self.assertIsInstance(AgentSettings(model="embedding", api_key="test").create_provider(embedding=True), OpenAIEmbeddingProvider)
        with self.assertRaisesRegex(ValueError, "AUTOAGENT_LLM_API_KEY"):
            AgentSettings(model="demo").create_provider()
        with self.assertRaisesRegex(ValueError, "BASE_URL"):
            AgentSettings(provider="unknown", model="demo", api_key="test").create_provider()

    def test_env_example_documents_every_agent_profile_option(self):
        """Trace both actual environment parsers and compare their consumed names with the root example."""
        names = set()
        class Environment(dict):
            def get(self, key, default=None):
                names.add(key)
                return super().get(key, default)
        for prefix, embedding in (("AUTOAGENT_LLM_", False), ("AUTOAGENT_EMBEDDING_", True)):
            AgentSettings.from_environment(Environment({prefix + "MODEL": "demo"}), embedding=embedding)
        documented = {line.split("=", 1)[0] for line in (Path(__file__).resolve().parents[1] / ".env.example").read_text().splitlines()
                      if line.startswith(("AUTOAGENT_LLM_", "AUTOAGENT_EMBEDDING_"))}
        self.assertEqual(names, documented)


class HostAgentDefaultsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.url = "sqlite+aiosqlite:///" + str(Path(self.temp.name) / "runtime.db")

    async def asyncTearDown(self):
        self.temp.cleanup()

    def host(self, **kwargs):
        return AutoAgentHost(settings=HostSettings(database_url=self.url), **kwargs)

    async def test_environment_defaults_own_both_providers_on_the_runtime_loop(self):
        """Host enters, uses and closes generation and embedding clients on one owned Runtime loop."""
        from autoagent.agent import EmbeddingRequest
        llm, embedding = DemoLLM(), DemoEmbedding()
        loops = []
        @asynccontextmanager
        async def resource(provider):
            loops.append(asyncio.get_running_loop())
            try:
                yield provider
            finally:
                loops.append(asyncio.get_running_loop())
                await provider.aclose()
        def factory(settings, *, embedding=False):
            return resource(providers[embedding])
        providers = {False: llm, True: embedding}
        env = {"AUTOAGENT_LLM_MODEL": "test-model", "AUTOAGENT_EMBEDDING_MODEL": "embedding"}
        workflows = [Workflow("llm", nodes=[Node("call", LLM_CALL)]), Workflow("embed", nodes=[Node("call", EMBEDDING)])]
        with patch.object(AgentSettings, "create_provider", factory):
            async with self.host(workflows=workflows, environment=env) as host:
                generated = await host.join(await host.execute("llm", request()))
                embedded = await host.join(await host.execute("embed", EmbeddingRequest(model="embedding", input="one")))
                self.assertEqual(generated.status, "completed", generated.error)
                self.assertEqual(embedded.status, "completed", embedded.error)
                self.assertEqual(generated.output["model"], "test-model")
                self.assertEqual(host._runtime.setup.capability_resolvers, {})
        self.assertTrue(llm.closed)
        self.assertTrue(embedding.closed)
        self.assertEqual(len(set(loops)), 1)
        self.assertIsNot(loops[0], asyncio.get_running_loop())

    async def test_custom_registration_takes_precedence_and_unused_routes_stay_disabled(self):
        """Custom Operators require no default key and the unconfigured embedding route creates no client."""
        provider = DemoLLM()
        @asynccontextmanager
        async def lifespan(setup):
            setup.register_operator(llm_call_operator(provider), capability_id=LLM_CALL.id, default=True)
            yield
        with patch.object(AgentSettings, "create_provider", side_effect=AssertionError("Default should not be constructed")):
            async with self.host(workflows=[Workflow("llm", nodes=[Node("call", LLM_CALL)])], lifespan=lifespan,
                environment={"AUTOAGENT_LLM_MODEL": "configured"}) as host:
                result = await host.join(await host.execute("llm", request()))
                self.assertEqual(result.status, "completed", result.error)
        self.assertFalse(provider.closed)  # The custom lifespan owns its Provider.

    async def test_partial_setup_failure_closes_resources_and_releases_store(self):
        """If the second default fails, the first client closes and the database writer can reopen."""
        provider = DemoLLM()
        @asynccontextmanager
        async def first():
            try:
                yield provider
            finally:
                await provider.aclose()
        def factory(settings, *, embedding=False):
            if embedding:
                raise ValueError("embedding setup failed")
            return first()
        workflow = Workflow("llm", nodes=[Node("call", LLM_CALL)])
        with patch.object(AgentSettings, "create_provider", factory):
            with self.assertRaisesRegex(ValueError, "embedding setup failed"):
                async with self.host(workflows=[workflow], environment={"AUTOAGENT_LLM_MODEL": "llm", "AUTOAGENT_EMBEDDING_MODEL": "embedding"}):
                    self.fail("Startup should fail")
        self.assertTrue(provider.closed)
        async with self.host(workflows=[workflow], environment={}):
            pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL is required")
class PostgreSQLHostAgentDefaultsTests(HostAgentDefaultsTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.database = PostgreSQLDatabase()
        self.url = await self.database.create()

    async def asyncTearDown(self):
        try:
            await self.database.close()
        finally:
            await super().asyncTearDown()
