"""Per-call generation settings and the application-supplied LLM routing hook."""

import asyncio
from contextlib import aclosing
import unittest

from autoagent import Edge, InputMappingContext, Node, ValueContract, Workflow
from autoagent.core.app import AutoAgentApp
from autoagent.agent import (
    LLM_CALL, LLMRequest, LLMSelection, llm_call_operator,
)
from tests.test_agent_capabilities import DemoLLM, request


def explicit_input(ctx: InputMappingContext) -> LLMRequest:
    return request(**dict(ctx.invocation_input))


def final_input(ctx: InputMappingContext) -> LLMRequest:
    return request(model="final", max_output_tokens=1024, temperature=0.4, streaming=False)


class LLMCallTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_operator_isolates_models_limits_and_streaming_across_nodes(self):
        """Concurrent Workflows and sequential Nodes share one Provider without sharing call settings."""
        provider = DemoLLM()
        workflows = [Workflow("single", nodes=[Node("call", LLM_CALL, input_mapping=explicit_input)]),
                     Workflow("multiple", nodes=[Node("first", LLM_CALL, input_mapping=explicit_input),
                                                 Node("last", LLM_CALL, input_mapping=final_input)],
                              edges=[Edge("first", "last")])]
        payloads = [{"model": "small", "max_output_tokens": 128, "temperature": 0.2, "streaming": False},
                    {"model": "large", "max_output_tokens": 2048, "temperature": 0.7, "streaming": True}]
        async with aclosing(AutoAgentApp()) as app:
            app.register_operator(llm_call_operator(provider), capability_id=LLM_CALL.id)
            results = await asyncio.gather(*(app.ainvoke(workflow, payload)
                                             for workflow, payload in zip(workflows, payloads)))
        for result in results:
            self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(sorted((value.model, mode, value.max_output_tokens, value.temperature)
                                for mode, value in provider.calls),
                         [("final", "invoke", 1024, 0.4), ("large", "stream", 2048, 0.7),
                          ("small", "invoke", 128, 0.2)])
        self.assertFalse(provider.closed)
        self.assertEqual(provider.streams_closed, 1)

    async def test_sync_and_async_hooks_receive_hints_and_return_runtime_selections(self):
        """The routing interface accepts either hook style while preserving original durable requests."""
        for streaming in (False, True):
            with self.subTest(streaming=streaming):
                provider = DemoLLM()
                original = request(model=None, max_output_tokens=333, streaming=streaming,
                                   routing={"difficulty": "hard", "labels": ["review"]})
                contract = ValueContract.create(LLMRequest, location="routing input")
                self.assertEqual(contract.restore(contract.to_record(original)), original)
                seen = []
                def select(value):
                    seen.append(value)
                    return LLMSelection(provider=provider, model="selected-model")
                async def select_async(value):
                    await asyncio.sleep(0)
                    return select(value)
                async with aclosing(AutoAgentApp()) as app:
                    app.register_operator(llm_call_operator(resolver=select_async if streaming else select),
                                          capability_id=LLM_CALL.id)
                    result = await app.ainvoke(Workflow("route", nodes=[Node("call", LLM_CALL)]), original)
                self.assertEqual(result.status, "completed", result.error)
                self.assertEqual(seen, [original])
                actual = provider.calls[0][1]
                self.assertEqual(actual.model, "selected-model")
                self.assertEqual(actual.max_output_tokens, 333)
                self.assertEqual(actual.streaming, streaming)
                self.assertEqual(actual.routing, original.routing)
                self.assertIsNone(original.model)
                self.assertFalse(provider.closed)

    async def test_routing_hook_can_select_different_provider_instances(self):
        """An application hook may choose borrowed Providers per call through one registered Operator."""
        providers = {name: DemoLLM() for name in ("local", "remote")}
        def select(value):
            return LLMSelection(provider=providers[value.routing["target"]], model=value.model)
        async with aclosing(AutoAgentApp()) as app:
            app.register_operator(llm_call_operator(resolver=select), capability_id=LLM_CALL.id)
            workflow = Workflow("route", nodes=[Node("call", LLM_CALL)])
            for target in providers:
                result = await app.ainvoke(workflow, request(model=target, routing={"target": target}))
                self.assertEqual(result.status, "completed", result.error)
        for target, provider in providers.items():
            self.assertEqual(provider.calls[0][1].model, target)
            self.assertFalse(provider.closed)

    async def test_invalid_routes_fail_before_any_provider_call(self):
        """Missing models, replaced explicit models and invalid hook results never execute a Provider."""
        provider = DemoLLM()
        cases = [
            (llm_call_operator(provider), request(model=None), "Set a model"),
            (llm_call_operator(resolver=lambda value: None), request(model=None), "LLMSelection"),
            (llm_call_operator(resolver=lambda value: LLMSelection(provider, "other")),
             request(model="fixed"), "explicit model"),
        ]
        for operator, value, message in cases:
            async with aclosing(AutoAgentApp()) as app:
                app.register_operator(operator, capability_id=LLM_CALL.id)
                result = await app.ainvoke(Workflow("invalid", nodes=[Node("call", LLM_CALL)]), value)
                self.assertEqual(result.status, "failed")
                self.assertIn(message, result.error.message)
        self.assertEqual(provider.calls, [])

    def test_configuration_and_selections_reject_invalid_routes(self):
        """Registration requires exactly one Provider or hook and a selection requires a usable model."""
        provider = DemoLLM()
        for factory in (lambda: llm_call_operator(),
                        lambda: llm_call_operator(provider, resolver=lambda value: None),
                        lambda: LLMSelection(provider, " "), lambda: LLMSelection(provider, None)):
            with self.assertRaises(ValueError):
                factory()
        with self.assertRaises(TypeError):
            LLMSelection(object(), "model")
