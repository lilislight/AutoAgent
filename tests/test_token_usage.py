"""Recorded token aggregation and transactional settled-usage admission."""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from autoagent import UsagePolicy, ResourceBudgetExceeded, UsageAccountingError
from autoagent.agent import TimeRange, UsageWindow, TokenUsageQuery
from autoagent.agent.usage import TokenUsageCheck
from autoagent.host.persistence import DatabaseSink
from tests.test_llm_call_storage import running, completed, has_table
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase

UTC = timezone.utc


class UsageTimeTests(unittest.TestCase):
    def test_calendar_boundaries_follow_local_days_and_dst(self):
        """Local days use midnight boundaries, not a fixed twenty-four hours."""
        day = TimeRange.day(at=datetime(2026, 10, 26, tzinfo=UTC), timezone="Asia/Hong_Kong")
        self.assertEqual(day.start.astimezone(UTC), datetime(2026, 10, 25, 16, tzinfo=UTC))
        spring = TimeRange.day(at=datetime(2026, 3, 8, 12, tzinfo=UTC), timezone="America/New_York")
        self.assertEqual(spring.end.astimezone(UTC) - spring.start.astimezone(UTC), timedelta(hours=23))
        autumn = TimeRange.day(at=datetime(2026, 11, 1, 12, tzinfo=UTC), timezone="America/New_York")
        self.assertEqual(autumn.end.astimezone(UTC) - autumn.start.astimezone(UTC), timedelta(hours=25))
        month = TimeRange.month(at=datetime(2026, 12, 20, tzinfo=UTC))
        self.assertEqual(month.end, datetime(2027, 1, 1, tzinfo=UTC))

    def test_invalid_queries_are_rejected_and_rolling_windows_use_elapsed_time(self):
        """Reject ambiguous times, backwards ranges and unscoped Node identities."""
        for build in (lambda: TimeRange(datetime(2026, 1, 1)),
                      lambda: TimeRange(datetime(2026, 2, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
                      lambda: TokenUsageQuery(node_id="a"), lambda: TokenUsageQuery(include_descendants=True),
                      lambda: TokenUsageQuery(invocation_id="i", include_descendants=1),
                      lambda: UsagePolicy(max_total_tokens=True)):
            with self.assertRaises((TypeError, ValueError)):
                build()
        at = TimeRange.day(at=datetime(2026, 3, 8, 12, tzinfo=UTC), timezone="America/New_York").end
        rolling = TimeRange.last_24_hours(at=at)
        self.assertEqual(rolling.end - rolling.start, timedelta(hours=24))


class TokenUsageContracts:
    async def asyncSetUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.url = await self.database_url(Path(temporary.name) / "usage.sqlite")
        self.sink = await DatabaseSink.open(self.url)
        self.addAsyncCleanup(self.sink.close)

    async def database_url(self, path):
        return "sqlite+aiosqlite:///" + str(path)

    def check(self, limit=5, **filters):
        return TokenUsageCheck("test", UsagePolicy(max_total_tokens=limit), TokenUsageQuery(**filters))

    async def test_five_scopes_and_half_open_range_use_call_start_time(self):
        """Scopes include all revisions and ranges partition by start rather than completion."""
        for index, (workflow, revision, session, invocation, node) in enumerate([
            ("research", "r1", "s1", "i1", "a"), ("research", "r1", "s1", "i1", "b"),
            ("research", "r2", "s1", "i2", "a"), ("other", "r3", "s2", "i3", "a"),
        ]):
            start = running(llm_call_id=str(index), workflow_id=workflow, workflow_revision_id=revision,
                            session_id=session, invocation_id=invocation, node_id=node, started_at_us=(index + 1) * 1_000_000)
            await self.sink.record_llm_call(completed(start, completed_at_us=100_000_000))
        for filters, expected in [({"invocation_id": "i1", "node_id": "a"}, 5),
                                  ({"invocation_id": "i1"}, 10), ({"session_id": "s1"}, 15),
                                  ({"workflow_id": "research"}, 15), ({}, 20),
                                  ({"workflow_revision_id": "r1"}, 10)]:
            self.assertEqual((await self.sink.token_usage(**filters)).total_tokens, expected)
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        first = await self.sink.token_usage(start=epoch + timedelta(seconds=1), end=epoch + timedelta(seconds=3))
        second = await self.sink.token_usage(start=epoch + timedelta(seconds=3), end=epoch + timedelta(seconds=5))
        self.assertEqual((first.total_tokens, second.total_tokens), (10, 10))

    async def test_missing_partial_and_pending_usage_are_not_zero_measurements(self):
        """Expose missing measurements per metric without adding reasoning/cache twice."""
        start = running(llm_call_id="reported")
        await self.sink.record_llm_call(completed(start, reasoning_tokens=1, cache_read_tokens=2))
        await self.sink.record_llm_call(completed(running(llm_call_id="partial"), total_tokens=None))
        await self.sink.record_llm_call(completed(running(llm_call_id="unknown"), usage_reported=False,
                                                input_tokens=None, output_tokens=None, total_tokens=None))
        await self.sink.record_llm_call(running(llm_call_id="pending"))
        summary = await self.sink.token_usage()
        self.assertEqual(summary.total_tokens, 5)
        self.assertEqual((summary.input_tokens, summary.output_tokens, summary.reasoning_tokens, summary.cache_read_tokens), (6, 4, 1, 2))
        self.assertEqual((summary.call_count, summary.pending_calls, summary.missing_usage_calls), (4, 1, 1))
        self.assertEqual(summary.missing_tokens["total_tokens"], 2)
        self.assertEqual(summary.unsettled_calls, 0)
        self.assertEqual((await self.sink.token_usage(requested_model="absent")).total_tokens, 0)

    async def test_descendant_query_includes_own_calls_without_runtime_metadata(self):
        """Standalone recorded calls remain queryable before any Invocation metadata exists."""
        await self.sink.record_llm_call(completed(running(invocation_id="standalone")))
        summary = await self.sink.token_usage(invocation_id="standalone", include_descendants=True)
        self.assertEqual((summary.call_count, summary.total_tokens), (1, 5))
        self.assertEqual((await self.sink.token_usage(invocation_id="absent", include_descendants=True)).call_count, 0)

    async def test_summary_read_does_not_create_optional_tables_and_supports_read_only(self):
        """Empty queries and read-only hosts work without initializing call storage."""
        self.assertEqual((await self.sink.token_usage()).call_count, 0)
        self.assertFalse(await has_table(self.url, "llm_calls"))
        await self.sink.close()
        read_only = await DatabaseSink.open(self.url, read_only=True)
        self.addAsyncCleanup(read_only.close)
        self.assertEqual((await read_only.token_usage()).total_tokens, 0)

    async def test_admission_is_idempotent_and_blocks_at_settled_limit(self):
        """One settled call exhausts its allowance and cannot be admitted twice."""
        record = running(session_id="s")
        await self.sink.admit_llm_call(record, (self.check(session_id="s"),))
        await self.sink.record_llm_call(completed(record))
        await self.sink.record_llm_call(completed(record))
        self.assertEqual((await self.sink.token_usage()).total_tokens, 5)
        with self.assertRaises(ResourceBudgetExceeded):
            await self.sink.admit_llm_call(running(llm_call_id="next", session_id="s"), (self.check(session_id="s"),))
        with self.assertRaises(UsageAccountingError):
            await self.sink.admit_llm_call(record, ())
        self.assertEqual((await self.sink.token_usage()).call_count, 1)

    async def test_inflight_calls_can_exceed_limit_but_reopened_unsettled_calls_block(self):
        """Settled limits allow concurrent calls; unknown restart liabilities fail closed."""
        checks = (self.check(),)
        one, two = running(llm_call_id="one"), running(llm_call_id="two")
        await asyncio.gather(self.sink.admit_llm_call(one, checks), self.sink.admit_llm_call(two, checks))
        await self.sink.record_llm_call(completed(one))
        await self.sink.record_llm_call(completed(two))
        self.assertEqual((await self.sink.token_usage()).total_tokens, 10)
        with self.assertRaises(ResourceBudgetExceeded):
            await self.sink.admit_llm_call(running(llm_call_id="three"), checks)
        await self.sink.record_llm_call(running(llm_call_id="orphan", session_id="unknown"))
        await self.sink.close()
        self.sink = await DatabaseSink.open(self.url)
        self.addAsyncCleanup(self.sink.close)
        self.assertEqual((await self.sink.token_usage(session_id="unknown")).unsettled_calls, 1)
        with self.assertRaises(UsageAccountingError):
            await self.sink.admit_llm_call(running(llm_call_id="four", session_id="unknown"), (self.check(100, session_id="unknown"),))

    async def test_unknown_usage_can_be_explicitly_allowed_and_zero_limit_denies_first_call(self):
        """Unknown values are never silently zero and zero allowances deny immediately."""
        record = completed(running(), usage_reported=False, input_tokens=None, output_tokens=None, total_tokens=None)
        await self.sink.record_llm_call(record)
        with self.assertRaises(UsageAccountingError):
            await self.sink.admit_llm_call(running(llm_call_id="blocked"), (self.check(),))
        permissive = TokenUsageCheck("test", UsagePolicy(max_total_tokens=10, on_unknown="allow"), TokenUsageQuery())
        await self.sink.admit_llm_call(running(llm_call_id="allowed"), (permissive,))
        with self.assertRaises(ResourceBudgetExceeded):
            await self.sink.admit_llm_call(running(llm_call_id="zero"), (self.check(0),))


class SQLiteTokenUsageTests(TokenUsageContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLTokenUsageTests(TokenUsageContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, path):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
