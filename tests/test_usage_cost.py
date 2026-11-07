"""Offline rate cards, exact stored costs, shared scope aggregation and SQL count."""

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from sqlalchemy import event, select

from autoagent import UsagePolicy, ResourceBudgetExceeded, UsageAccountingError
from autoagent.agent import LocalTokenPricing, TokenPrice, TokenUsageQuery
from autoagent.agent.usage import TokenUsageCheck
from autoagent.core.money import usd_units, units_usd
from autoagent.host.persistence import DatabaseSink
from autoagent.host.persistence.llm_calls import llm_calls
from tests.test_llm_call_storage import running, completed, read_rows
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class CostCalculatorTests(unittest.TestCase):
    def price(self, **fields):
        return LocalTokenPricing(overrides={"test/model": TokenPrice(**fields)}, use_litellm_catalog=False)

    def test_decimal_costs_split_cache_and_reasoning_without_double_counting(self):
        """Cache and reasoning replace their portions of ordinary input/output charges."""
        pricing = self.price(input_per_million_usd="1", output_per_million_usd="3",
                             cached_input_per_million_usd="0.2", cache_creation_per_million_usd="2")
        record = completed(running(), input_tokens=100, output_tokens=2, total_tokens=102,
                           cache_read_tokens=60, billing_details={"usage": {"cache_creation_input_tokens": 10}})
        self.assertEqual(pricing.calculate(record).units, usd_units("0.000068"))
        exclusive = completed(running(), input_tokens=30, output_tokens=2, total_tokens=32,
                              cache_read_tokens=60, billing_details={"input_excludes_cache": True,
                                                                  "usage": {"cache_creation_input_tokens": 10}})
        self.assertEqual(pricing.calculate(exclusive).units, pricing.calculate(record).units)
        reasoning = self.price(input_per_million_usd="1", output_per_million_usd="3", reasoning_per_million_usd="10")
        result = reasoning.calculate(completed(running(), output_tokens=5, reasoning_tokens=2))
        self.assertEqual(result.units, usd_units("0.000032"))

    def test_missing_rates_or_billing_details_remain_unknown_and_explicit_zero_is_zero(self):
        """Unknown pricing is not free; explicit local zero overrides can be free without usage."""
        self.assertIsNone(LocalTokenPricing(use_litellm_catalog=False).calculate(completed(running())).units)
        zero = self.price(input_per_million_usd="0", output_per_million_usd="0")
        self.assertEqual(zero.calculate(completed(running(), input_tokens=None, output_tokens=None, total_tokens=None)).units, 0)
        cache = self.price(input_per_million_usd="1", output_per_million_usd="3", cached_input_per_million_usd="0.2")
        self.assertEqual(cache.calculate(completed(running())).metadata["reason"], "cache_read_usage_missing")
        plain = self.price(input_per_million_usd="1", output_per_million_usd="3")
        self.assertIsNone(plain.calculate(completed(running(), billing_details={"unsupported_modalities": True})).units)
        self.assertIsNone(plain.calculate(completed(running(), billing_details={"billable_tools": True})).units)

    def test_fixed_point_rounding_and_policy_validation_are_exact(self):
        """Round computed charges once to pico-USD and reject ambiguous budget representations."""
        pricing = self.price(input_per_million_usd="0.0000001", output_per_million_usd="0.0000001")
        self.assertEqual(pricing.calculate(completed(running())).units, 1)
        self.assertEqual(units_usd(usd_units("0.000000000001")), "0.000000000001")
        for value in (0.1, "NaN", "-1", "0.0000000000001"):
            with self.assertRaises((ValueError, TypeError)):
                UsagePolicy(max_cost_usd=value)

    def test_tiers_and_new_catalog_instances_use_versioned_local_data(self):
        """Local price versions stay pinned while new calculators see a replaced catalog."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "prices.json"
            entry = {"litellm_provider": "test", "input_cost_per_token": "0.000001", "output_cost_per_token": "0.000003",
                     "input_cost_per_token_priority": "0.000002", "output_cost_per_token_priority": "0.000004",
                     "input_cost_per_token_above_32k_tokens": "0.000005", "output_cost_per_token_above_32k_tokens": "0.000006"}
            path.write_text(json.dumps({"model": entry}))
            old = LocalTokenPricing(catalog_path=path)
            ordinary = old.calculate(completed(running()))
            priority = old.calculate(completed(running(), billing_details={"service_tier": "priority"}))
            self.assertEqual(priority.units, usd_units("0.000014"))
            long = old.calculate(completed(running(), input_tokens=32_001))
            self.assertEqual(long.units, usd_units("0.160017"))
            entry["input_cost_per_token"] = "0.000010"
            path.write_text(json.dumps({"model": entry}))
            new = LocalTokenPricing(catalog_path=path).calculate(completed(running()))
            self.assertNotEqual(new.metadata["price_version"], ordinary.metadata["price_version"])
            self.assertEqual(old.calculate(completed(running())).units, ordinary.units)

    @unittest.skipUnless(importlib.util.find_spec("litellm"), "LiteLLM catalog not installed")
    def test_bundled_catalog_is_populated_and_does_not_import_sdk_or_fetch_prices(self):
        """Reading the installed rate catalog neither imports LiteLLM nor issues HTTP."""
        before = sys.modules.get("litellm")
        with patch("httpx.get", side_effect=AssertionError("Unexpected HTTP")):
            pricing = LocalTokenPricing()
            info = pricing.catalog_info
            self.assertGreater(info["model_count"], 100)
            result = pricing.calculate(completed(running(provider_id="openai", requested_model="gpt-4o-mini"), cache_read_tokens=0))
        self.assertIs(sys.modules.get("litellm"), before)
        self.assertIsNotNone(result.units)
        self.assertEqual(result.metadata["source"], "litellm_local_catalog")


class CostStorageContracts:
    async def asyncSetUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.url = await self.database_url(Path(temporary.name) / "cost.sqlite")
        self.pricing = LocalTokenPricing(overrides={"test/model": TokenPrice("1", "3")}, use_litellm_catalog=False)
        self.sink = await DatabaseSink.open(self.url, pricing=self.pricing)
        self.addAsyncCleanup(self.sink.close)

    async def database_url(self, path):
        return "sqlite+aiosqlite:///" + str(path)

    async def test_five_scope_admission_executes_one_aggregate_and_one_insert(self):
        """Warm admission uses exactly two business SQL statements for all five scopes."""
        await self.sink.record_llm_call(completed(running(llm_call_id="warm")))
        statements = []
        def capture(conn, cursor, statement, parameters, context, executemany):
            first = statement.split(None, 1)[0].upper()
            if first not in ("BEGIN", "COMMIT", "ROLLBACK"):
                statements.append(first)
        async def install():
            event.listen(self.sink._writer.storage.engine.sync_engine, "before_cursor_execute", capture)
        await self.sink._writer.run(install)
        policy = UsagePolicy(max_total_tokens=100, max_cost_usd="1")
        filters = [dict(invocation_id="i", node_id="n"), dict(invocation_id="i"), dict(session_id="s"), dict(workflow_id="w"), {}]
        checks = tuple(TokenUsageCheck(str(index), policy, TokenUsageQuery(**value)) for index, value in enumerate(filters))
        record = running(llm_call_id="probe", invocation_id="i", node_id="n", session_id="s", workflow_id="w")
        await self.sink.admit_llm_call(record, checks)
        self.assertEqual(statements, ["SELECT", "INSERT"])
        statements.clear()
        with self.assertRaises(UsageAccountingError):
            await self.sink.admit_llm_call(record, checks)
        self.assertEqual(statements, ["SELECT"])

    async def test_cost_is_calculated_on_write_and_aggregates_in_same_scopes_and_time_range(self):
        """The call row carries frozen cost and shared queries aggregate it with tokens."""
        record = completed(running(llm_call_id="priced", session_id="s", invocation_id="i", node_id="n", workflow_id="w", started_at_us=1_000_000))
        await self.sink.record_llm_call(record)
        rows = await read_rows(self.url, select(llm_calls))
        self.assertEqual(rows[0]["cost_units"], usd_units("0.000009"))
        self.assertEqual(rows[0]["cost_metadata"]["source"], "configured")
        for query in ({}, dict(session_id="s"), dict(invocation_id="i", node_id="n"), dict(workflow_id="w"),
                      dict(start=datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc), end=datetime(1970, 1, 1, 0, 0, 2, tzinfo=timezone.utc))):
            summary = await self.sink.token_usage(**query)
            self.assertEqual((summary.total_tokens, summary.cost_usd, summary.missing_cost_calls), (5, "0.000009", 0))
        check = TokenUsageCheck("project", UsagePolicy(max_cost_usd="0.000009"), TokenUsageQuery())
        with self.assertRaises(ResourceBudgetExceeded):
            await self.sink.admit_llm_call(running(llm_call_id="blocked"), (check,))

    async def test_price_updates_do_not_reprice_replayed_terminal_calls(self):
        """Reopening with new prices cannot change an already settled cost."""
        original = completed(running(llm_call_id="one"))
        await self.sink.record_llm_call(original)
        before = (await read_rows(self.url, select(llm_calls)))[0]["cost_metadata"]
        await self.sink.close()
        new_prices = LocalTokenPricing(overrides={"test/model": TokenPrice("10", "30")}, use_litellm_catalog=False)
        self.sink = await DatabaseSink.open(self.url, pricing=new_prices)
        self.addAsyncCleanup(self.sink.close)
        await self.sink.record_llm_call(original)
        await self.sink.record_llm_call(completed(running(llm_call_id="two")))
        summary = await self.sink.token_usage()
        self.assertEqual(summary.cost_usd, "0.000099")
        rows = await read_rows(self.url, select(llm_calls).where(llm_calls.c.llm_call_id == "one"))
        self.assertEqual(rows[0]["cost_metadata"], before)

    async def test_missing_price_blocks_cost_limits_without_breaking_token_only_usage(self):
        """Unknown costs remain unknown and obey the same explicit unknown policy as tokens."""
        await self.sink.record_llm_call(completed(running(requested_model="unpriced")))
        summary = await self.sink.token_usage()
        self.assertIsNone(summary.cost_usd)
        self.assertEqual((summary.total_tokens, summary.missing_cost_calls), (5, 1))
        with self.assertRaises(UsageAccountingError):
            await self.sink.admit_llm_call(running(llm_call_id="blocked"),
                (TokenUsageCheck("cost", UsagePolicy(max_cost_usd="1"), TokenUsageQuery()),))
        await self.sink.admit_llm_call(running(llm_call_id="token-only"),
            (TokenUsageCheck("tokens", UsagePolicy(max_total_tokens=10), TokenUsageQuery()),))

    async def test_calculator_failure_retains_tokens_and_persists_unknown_cost(self):
        """A broken price calculation cannot discard settled tokens or record free usage."""
        with patch.object(self.pricing, "calculate", side_effect=ValueError("broken price data")):
            await self.sink.record_llm_call(completed(running()))
        summary = await self.sink.token_usage()
        self.assertEqual((summary.total_tokens, summary.cost_usd, summary.missing_cost_calls), (5, None, 1))
        row = (await read_rows(self.url, select(llm_calls)))[0]
        self.assertEqual(row["status"], "succeeded")
        self.assertEqual(row["cost_metadata"]["reason"], "calculation_failed")

    async def test_one_aggregate_keeps_overlapping_scope_costs_independent(self):
        """Conditional sums count each scope once despite overlapping Invocation/Session windows."""
        identities = [dict(invocation_id="i", node_id="n", session_id="s", workflow_id="w"),
                      dict(invocation_id="i", node_id="other", session_id="s", workflow_id="w"),
                      dict(invocation_id="other", node_id="n", session_id="s", workflow_id="w"),
                      dict(session_id="other", workflow_id="w"), dict(workflow_id="other")]
        for index, identity in enumerate(identities):
            await self.sink.record_llm_call(completed(running(llm_call_id=f"call-{index}", **identity)))
        filters = [dict(invocation_id="i", node_id="n"), dict(invocation_id="i"), dict(session_id="s"), dict(workflow_id="w"), {}]
        for selected in range(5):
            checks = tuple(TokenUsageCheck(str(index), UsagePolicy(max_cost_usd=units_usd((selected + 1) * 9_000_000)
                                                                   if index == selected else "1"), TokenUsageQuery(**query))
                           for index, query in enumerate(filters))
            with self.assertRaises(ResourceBudgetExceeded) as caught:
                await self.sink.admit_llm_call(running(llm_call_id="blocked"), checks)
            self.assertEqual(caught.exception.scope, str(selected))


class SQLiteCostStorageTests(CostStorageContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLCostStorageTests(CostStorageContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, path):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
