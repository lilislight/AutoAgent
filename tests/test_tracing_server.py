from __future__ import annotations

import asyncio
from collections.abc import Iterator
import importlib.util
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import threading
import time
import unittest

from typing_extensions import TypedDict

from autoagent import AutoAgentApp, Node, Stream, StreamContext, Workflow
from autoagent.core.runtime import RuntimeEvent
from autoagent.hosting import RuntimeEventStoreError, SQLiteRuntimeStore
from autoagent.tracing import (
    create_tracing_app,
    tracing_record,
    tracing_state_record,
)
from autoagent.tracing.server import (
    _decode_trace_cursor,
    _encode_trace_cursor,
    _iter_trace_stream,
)


_SERVER_AVAILABLE = all(
    importlib.util.find_spec(package) is not None
    for package in ("fastapi", "httpx")
)
if _SERVER_AVAILABLE:
    import httpx
else:  # pragma: no cover - exercised by minimal dependency installs
    httpx = None  # type: ignore[assignment]


class Value(TypedDict):
    value: int


class PrecisionValue(TypedDict):
    values: list[int]
    enabled: bool


class Chunk(TypedDict):
    value: int


class Total(TypedDict):
    total: int


def identity(value: Value) -> Value:
    return value


def preserve_precision(value: PrecisionValue) -> PrecisionValue:
    return value


def chunks(value: Value) -> Iterator[Chunk]:
    for index in range(value["value"]):
        yield {"value": index}


class SumReducer:
    def initial(self, _context: StreamContext) -> Total:
        return {"total": 0}

    def add(
        self,
        _context: StreamContext,
        state: Total,
        chunk: Chunk,
    ) -> Total:
        return {"total": state["total"] + chunk["value"]}

    def finish(self, _context: StreamContext, state: Total) -> Total:
        return state


class _Collector:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    async def append(self, event: RuntimeEvent) -> None:
        self.events.append(event)


def _capture_events() -> tuple[RuntimeEvent, ...]:
    collector = _Collector()
    app = AutoAgentApp(runtime_event_sink=collector)
    try:
        result = app.invoke(
            Workflow("captured-trace", nodes=[Node("work", identity)]),
            {"value": 1},
            session_id="captured-trace-session",
        )
        assert result.status == "completed"
    finally:
        app.close()
    return tuple(collector.events)


def _sse_events(body: str) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for block in body.split("\n\n"):
        if not block or block.startswith(":"):
            continue
        fields: dict[str, str] = {}
        for line in block.splitlines():
            name, value = line.split(": ", 1)
            fields[name] = value
        events.append(
            {
                "id": fields.get("id"),
                "event": fields["event"],
                "data": json.loads(fields["data"]),
            }
        )
    return events


@unittest.skipUnless(_SERVER_AVAILABLE, "Tracing Server extras are not installed.")
class TracingServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_user_events_page_and_sse_are_independent_from_runtime_trace(
        self,
    ) -> None:
        """Persist stream chunks and expose resumable User Event queries and SSE."""

        assert httpx is not None
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            core = AutoAgentApp(
                runtime_event_sink=store,
                user_event_sink=store,
            )
            try:
                result = await core.ainvoke(
                    Workflow(
                        "user-event-trace",
                        nodes=[
                            Node(
                                "stream",
                                chunks,
                                stream=Stream(SumReducer()),
                            )
                        ],
                    ),
                    {"value": 3},
                    session_id="user-event-trace-session",
                )
                app = create_tracing_app(store)
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://tracing.test",
                ) as client:
                    first = await client.get(
                        "/api/v1/invocations/user-events",
                        params={
                            "invocation_id": result.invocation_id,
                            "limit": 2,
                        },
                    )
                    self.assertEqual(first.status_code, 200)
                    page = first.json()
                    self.assertEqual(
                        [item["payload"] for item in page["items"]],
                        [{"value": 0}, {"value": 1}],
                    )
                    self.assertTrue(page["has_more"])
                    second = await client.get(
                        "/api/v1/invocations/user-events",
                        params={
                            "invocation_id": result.invocation_id,
                            "cursor": page["next_cursor"],
                        },
                    )
                    self.assertEqual(
                        [item["payload"] for item in second.json()["items"]],
                        [{"value": 2}],
                    )
                    stream = await client.get(
                        "/api/v1/invocations/user-events/stream",
                        params={
                            "invocation_id": result.invocation_id,
                            "cursor": page["next_cursor"],
                        },
                    )
                frames = _sse_events(stream.text)
                self.assertEqual(
                    [frame["event"] for frame in frames],
                    ["user_event", "stream_end"],
                )
                self.assertEqual(frames[0]["data"]["payload"], {"value": 2})
                self.assertEqual(frames[-1]["data"]["status"], "completed")
            finally:
                await core.aclose()
                await store.aclose()

    async def test_heartbeat_rejects_nonfinite_and_oversized_values(self) -> None:
        """Validate heartbeat numbers before the ASGI application is created."""

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            try:
                for heartbeat in (
                    True,
                    0,
                    float("nan"),
                    float("inf"),
                    10**1000,
                ):
                    with self.subTest(heartbeat=heartbeat), self.assertRaises(
                        ValueError
                    ):
                        create_tracing_app(
                            store,
                            heartbeat_seconds=heartbeat,
                        )
            finally:
                store.close()

    async def test_deep_json_integers_are_safe_for_javascript(self) -> None:
        """Stringify only unsafe deep integers while preserving safe values and bools."""

        safe = (1 << 53) - 1
        unsafe = 1 << 53
        record = {
            "occurred_at_ns": 123,
            "attributes": {
                "safe": safe,
                "unsafe": unsafe,
                "negative": -unsafe,
                "enabled": True,
                "nested": [False, {"value": unsafe + 1}],
            },
            "metrics": (safe, unsafe),
        }

        converted = tracing_record(record)

        self.assertEqual(converted["occurred_at_ns"], "123")
        attributes = converted["attributes"]
        self.assertEqual(attributes["safe"], safe)
        self.assertEqual(attributes["unsafe"], str(unsafe))
        self.assertEqual(attributes["negative"], str(-unsafe))
        self.assertIs(attributes["enabled"], True)
        self.assertIs(attributes["nested"][0], False)
        self.assertEqual(attributes["nested"][1]["value"], str(unsafe + 1))
        self.assertEqual(converted["metrics"], [safe, str(unsafe)])
        self.assertEqual(record["attributes"]["unsafe"], unsafe)

    async def test_state_timestamp_conversion_preserves_user_owned_keys(self) -> None:
        """Stringify Runtime clocks without rewriting similarly named user data."""

        record = {
            "session": {
                "created_at_ns": 10,
                "updated_at_ns": 11,
                "context": {"created_at_ns": 12, "flag_at_ns": True},
            },
            "invocation": {
                "created_at_ns": 20,
                "started_at_ns": 21,
                "completed_at_ns": 22,
                "output": {"created_at_ns": 23, "flag_at_ns": True},
                "scheduler": {
                    "occurrences": {
                        "one": {
                            "started_at_ns": 30,
                            "completed_at_ns": 31,
                            "output": {"created_at_ns": 32},
                        }
                    }
                },
            },
        }
        converted = tracing_state_record(record)
        self.assertEqual(converted["session"]["created_at_ns"], "10")
        self.assertEqual(converted["session"]["context"]["created_at_ns"], 12)
        self.assertIs(converted["session"]["context"]["flag_at_ns"], True)
        self.assertEqual(converted["invocation"]["created_at_ns"], "20")
        self.assertEqual(converted["invocation"]["output"]["created_at_ns"], 23)
        occurrence = converted["invocation"]["scheduler"]["occurrences"]["one"]
        self.assertEqual(occurrence["started_at_ns"], "30")
        self.assertEqual(occurrence["output"]["created_at_ns"], 32)

    async def test_state_api_preserves_deep_integer_precision(self) -> None:
        """Expose unsafe user integers as strings through the real State API."""

        assert httpx is not None
        safe = (1 << 53) - 1
        unsafe = 1 << 53
        value: PrecisionValue = {
            "values": [safe, unsafe, -unsafe],
            "enabled": True,
        }
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            core = AutoAgentApp(runtime_event_sink=store)
            try:
                result = await core.ainvoke(
                    Workflow(
                        "precision-state",
                        nodes=[Node("work", preserve_precision)],
                    ),
                    value,
                    session_id="precision-state-session",
                )
                app = create_tracing_app(store)
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://tracing.test",
                ) as client:
                    response = await client.get(
                        "/api/v1/invocations/state",
                        params={"invocation_id": result.invocation_id},
                    )

                self.assertEqual(response.status_code, 200)
                invocation = response.json()["state"]["invocation"]
                expected = {
                    "values": [safe, str(unsafe), str(-unsafe)],
                    "enabled": True,
                }
                self.assertEqual(invocation["input"], expected)
                self.assertEqual(invocation["output"], expected)
                operator_call = next(
                    iter(invocation["scheduler"]["operator_calls"].values())
                )
                self.assertEqual(operator_call["input"], expected)
                self.assertIs(operator_call["input"]["enabled"], True)
            finally:
                await core.aclose()
                await store.aclose()

    async def test_generated_cursor_is_bounded_and_round_trips_long_identity(self) -> None:
        """Keep server-generated cursors valid for unrestricted Invocation ids."""

        invocation_id = "会" * 1_000
        cursor = _encode_trace_cursor(invocation_id, 42)
        self.assertLessEqual(len(cursor), 512)
        self.assertEqual(_decode_trace_cursor(cursor, invocation_id), 42)

    async def test_queries_use_stable_pages_and_read_only_routes(self) -> None:
        """Expose stable pages without Core control routes."""

        assert httpx is not None
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            core = AutoAgentApp(runtime_event_sink=store)
            try:
                revisions: list[str] = []
                for index in range(3):
                    workflow = Workflow(
                        f"traced/{index}",
                        nodes=[Node("work", identity)],
                    )
                    compiled = core.register_workflow(workflow)
                    revisions.append(compiled.workflow_revision_id)
                    store.save_workflow(
                        core.workflow_definition_snapshot(
                            compiled.workflow_revision_id
                        )
                    )
                result = core.invoke(
                    revisions[-1],
                    {"value": 7},
                    session_id="traced/session 会",
                )
                ui = Path(directory) / "ui"
                ui.mkdir()
                (ui / "index.html").write_text("tracing ui", encoding="utf-8")
                (ui / ".env").write_text("SECRET=exposed", encoding="utf-8")
                (ui / "source.py").write_text("secret = 1", encoding="utf-8")
                (ui / "assets").mkdir()
                (ui / "assets" / "app.js").write_text(
                    "console.log('public')",
                    encoding="utf-8",
                )
                (ui / "assets" / "app-deadbeef.js").write_text(
                    "console.log('hashed')",
                    encoding="utf-8",
                )
                app = create_tracing_app(
                    store,
                    ui_directory=ui,
                    allowed_hosts=("tracing.test",),
                )
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://tracing.test",
                ) as client:
                    health = await client.get("/api/v1/health")
                    self.assertEqual(
                        health.json(),
                        {"status": "ok", "api_version": 1},
                    )
                    self.assertEqual(health.headers["cache-control"], "no-store")
                    self.assertEqual(
                        health.headers["x-content-type-options"], "nosniff"
                    )
                    self.assertIn(
                        "frame-ancestors 'none'",
                        health.headers["content-security-policy"],
                    )

                    first = await client.get("/api/v1/workflows?limit=2")
                    first_page = first.json()
                    self.assertEqual(
                        set(first_page),
                        {"items", "next_cursor", "has_more"},
                    )
                    self.assertEqual(len(first_page["items"]), 2)
                    self.assertTrue(first_page["has_more"])
                    second = await client.get(
                        "/api/v1/workflows",
                        params={"limit": 2, "cursor": first_page["next_cursor"]},
                    )
                    self.assertEqual(len(second.json()["items"]), 1)
                    self.assertFalse(second.json()["has_more"])

                    workflow = await client.get(
                        "/api/v1/workflows/detail",
                        params={"revision_id": revisions[-1]},
                    )
                    self.assertEqual(
                        workflow.json()["workflow_revision_id"],
                        revisions[-1],
                    )
                    sessions = await client.get(
                        "/api/v1/workflows/sessions",
                        params={"revision_id": revisions[-1]},
                    )
                    self.assertEqual(
                        sessions.json()["items"][0]["session_id"],
                        "traced/session 会",
                    )
                    invocations = await client.get(
                        "/api/v1/sessions/invocations",
                        params={"session_id": "traced/session 会"},
                    )
                    self.assertEqual(
                        invocations.json()["items"][0]["invocation_id"],
                        result.invocation_id,
                    )
                    missing = await client.get(
                        "/api/v1/sessions/invocations",
                        params={"session_id": "missing"},
                    )
                    self.assertEqual(missing.status_code, 404)
                    self.assertEqual(
                        missing.json()["detail"]["code"], "not_found"
                    )
                    bad_cursor = await client.get(
                        "/api/v1/workflows", params={"cursor": "a"}
                    )
                    self.assertEqual(bad_cursor.status_code, 400)
                    non_object_cursor = await client.get(
                        "/api/v1/workflows", params={"cursor": "W10"}
                    )
                    self.assertEqual(non_object_cursor.status_code, 400)
                    root = await client.get("/")
                    self.assertEqual(root.text, "tracing ui")
                    self.assertEqual(root.headers["cache-control"], "no-cache")
                    asset = await client.get("/assets/app.js")
                    self.assertEqual(asset.status_code, 200)
                    self.assertEqual(asset.headers["cache-control"], "no-cache")
                    hashed_asset = await client.get(
                        "/assets/app-deadbeef.js"
                    )
                    self.assertEqual(
                        hashed_asset.headers["cache-control"],
                        "public, max-age=31536000, immutable",
                    )
                    secret = await client.get("/.env")
                    self.assertEqual(secret.status_code, 404)
                    self.assertNotIn("SECRET=exposed", secret.text)
                    source = await client.get("/source.py")
                    self.assertEqual(source.status_code, 404)
                    client_route = await client.get("/workflows/example")
                    self.assertEqual(client_route.text, "tracing ui")
                    openapi_route = await client.get("/openapi.json")
                    self.assertEqual(openapi_route.status_code, 404)

                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://attacker.test",
                ) as untrusted_client:
                    rejected = await untrusted_client.get("/api/v1/health")
                self.assertEqual(rejected.status_code, 400)
                self.assertEqual(
                    rejected.json()["detail"]["code"], "invalid_host"
                )

                api_routes = [
                    route
                    for route in app.routes
                    if getattr(route, "path", "").startswith("/api/v1")
                ]
                self.assertTrue(api_routes)
                self.assertTrue(
                    all(route.methods <= {"GET", "HEAD"} for route in api_routes)
                )
                openapi = app.openapi()
                schemas = openapi["components"]["schemas"]
                for name in (
                    "ChildSessionSummaryResponse",
                    "InvocationSummaryResponse",
                    "SessionSummaryResponse",
                    "TraceEventResponse",
                    "WorkflowDefinitionResponse",
                    "WorkflowSummaryResponse",
                ):
                    self.assertIn(name, schemas)
                    self.assertFalse(schemas[name]["additionalProperties"])
                workflow_items = schemas["WorkflowPageResponse"]["properties"][
                    "items"
                ]["items"]
                self.assertTrue(
                    workflow_items["$ref"].endswith(
                        "/WorkflowSummaryResponse"
                    )
                )
                detail_schema = openapi["paths"][
                    "/api/v1/invocations/detail"
                ]["get"]["responses"]["200"]["content"]["application/json"][
                    "schema"
                ]
                self.assertTrue(
                    detail_schema["$ref"].endswith(
                        "/InvocationSummaryResponse"
                    )
                )
            finally:
                core.close()
                store.close()

    async def test_trace_cursor_and_historical_state_are_bounded(self) -> None:
        """Page safe Trace and bound historical State to one Invocation."""

        assert httpx is not None
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            core = AutoAgentApp(runtime_event_sink=store)
            try:
                workflow = Workflow(
                    "state-trace", nodes=[Node("work", identity)]
                )
                result = core.invoke(
                    workflow,
                    {"value": 3},
                    session_id="state-trace-session",
                )
                other = core.invoke(
                    workflow,
                    {"value": 4},
                    session_id="other-trace-session",
                )
                app = create_tracing_app(store)
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://tracing.test",
                ) as client:
                    detail = (
                        await client.get(
                            "/api/v1/invocations/detail",
                            params={"invocation_id": result.invocation_id},
                        )
                    ).json()
                    first = await client.get(
                        "/api/v1/invocations/trace",
                        params={"invocation_id": result.invocation_id, "limit": 2},
                    )
                    first_page = first.json()
                    self.assertEqual(
                        set(first_page),
                        {
                            "items",
                            "next_cursor",
                            "resume_cursor",
                            "resume_sequence",
                            "has_more",
                            "has_earlier",
                        },
                    )
                    self.assertTrue(first_page["has_more"])
                    self.assertNotIn("operations", json.dumps(first_page))
                    second = await client.get(
                        "/api/v1/invocations/trace",
                        params={
                            "invocation_id": result.invocation_id,
                            "limit": 2,
                            "cursor": first_page["next_cursor"],
                        },
                    )
                    first_ids = {item["id"] for item in first_page["items"]}
                    second_ids = {item["id"] for item in second.json()["items"]}
                    self.assertTrue(first_ids.isdisjoint(second_ids))

                    tail = await client.get(
                        "/api/v1/invocations/trace",
                        params={"invocation_id": result.invocation_id, "tail_limit": 3},
                    )
                    tail_page = tail.json()
                    tail_sequences = [
                        item["trace_sequence"] for item in tail_page["items"]
                    ]
                    self.assertEqual(len(tail_sequences), 3)
                    self.assertEqual(tail_sequences, sorted(tail_sequences))
                    self.assertTrue(tail_page["has_earlier"])
                    self.assertFalse(tail_page["has_more"])
                    self.assertIsNone(tail_page["next_cursor"])
                    self.assertEqual(
                        tail_page["resume_sequence"], tail_sequences[-1]
                    )
                    self.assertIsNotNone(tail_page["resume_cursor"])

                    earlier = await client.get(
                        "/api/v1/invocations/trace",
                        params={
                            "invocation_id": result.invocation_id,
                            "before_sequence": tail_sequences[0],
                            "limit": 2,
                        },
                    )
                    self.assertEqual(earlier.status_code, 200)
                    earlier_page = earlier.json()
                    self.assertTrue(
                        all(
                            item["trace_sequence"] < tail_sequences[0]
                            for item in earlier_page["items"]
                        )
                    )
                    invalid_history = await client.get(
                        "/api/v1/invocations/trace",
                        params={
                            "invocation_id": result.invocation_id,
                            "before_sequence": tail_sequences[0],
                            "tail_limit": 2,
                        },
                    )
                    self.assertEqual(invalid_history.status_code, 400)

                    wrong_invocation = await client.get(
                        "/api/v1/invocations/trace",
                        params={
                            "invocation_id": other.invocation_id,
                            "cursor": first_page["next_cursor"],
                        },
                    )
                    self.assertEqual(wrong_invocation.status_code, 400)

                    historical = await client.get(
                        "/api/v1/invocations/state",
                        params={
                            "invocation_id": result.invocation_id,
                            "through_sequence": detail["first_event_sequence"]
                        },
                    )
                    historical_record = historical.json()
                    self.assertEqual(
                        historical_record["through_sequence"],
                        detail["first_event_sequence"],
                    )
                    self.assertEqual(
                        historical_record["state"]["invocation"]["status"],
                        "created",
                    )
                    by_trace = await client.get(
                        "/api/v1/invocations/state",
                        params={
                            "invocation_id": result.invocation_id,
                            "through_trace_sequence": first_page["items"][-1][
                                "trace_sequence"
                            ],
                        },
                    )
                    self.assertEqual(by_trace.status_code, 200)
                    self.assertGreaterEqual(
                        by_trace.json()["through_sequence"],
                        detail["first_event_sequence"],
                    )
                    ambiguous = await client.get(
                        "/api/v1/invocations/state",
                        params={
                            "invocation_id": result.invocation_id,
                            "through_sequence": detail["first_event_sequence"],
                            "through_trace_sequence": first_page["items"][-1][
                                "trace_sequence"
                            ],
                        },
                    )
                    self.assertEqual(ambiguous.status_code, 400)
                    latest = await client.get(
                        "/api/v1/invocations/state",
                        params={"invocation_id": result.invocation_id},
                    )
                    self.assertEqual(
                        latest.json()["state"]["invocation"]["status"],
                        "completed",
                    )
                    outside = await client.get(
                        "/api/v1/invocations/state",
                        params={
                            "invocation_id": result.invocation_id,
                            "through_sequence": detail["last_event_sequence"] + 1
                        },
                    )
                    self.assertEqual(outside.status_code, 400)
                    future = await client.get(
                        "/api/v1/invocations/trace",
                        params={
                            "invocation_id": result.invocation_id,
                            "after_sequence": 10**100,
                        },
                    )
                    self.assertEqual(future.status_code, 400)
                    future_stream = await client.get(
                        "/api/v1/invocations/stream",
                        params={
                            "invocation_id": result.invocation_id,
                            "after_sequence": 10**100,
                        },
                    )
                    self.assertEqual(future_stream.status_code, 400)
            finally:
                core.close()
                store.close()

    async def test_historical_state_rejects_a_corrupt_invocation_range(self) -> None:
        """Reject a forged historical range without mislabeling another Invocation."""

        assert httpx is not None
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            store = SQLiteRuntimeStore(path)
            core = AutoAgentApp(runtime_event_sink=store)
            try:
                workflow = Workflow(
                    "state-range-integrity",
                    nodes=[Node("work", identity)],
                )
                first = await core.ainvoke(
                    workflow,
                    {"value": 1},
                    session_id="state-range-integrity-session",
                )
                second = await core.ainvoke(
                    workflow,
                    {"value": 2},
                    session_id="state-range-integrity-session",
                )
                app = create_tracing_app(store)
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://tracing.test",
                ) as client:
                    first_detail = (
                        await client.get(
                            "/api/v1/invocations/detail",
                            params={"invocation_id": first.invocation_id},
                        )
                    ).json()
                    second_detail = (
                        await client.get(
                            "/api/v1/invocations/detail",
                            params={"invocation_id": second.invocation_id},
                        )
                    ).json()
                    earlier_sequence = first_detail["first_event_sequence"]
                    self.assertLess(
                        earlier_sequence,
                        second_detail["first_event_sequence"],
                    )

                    outside = await client.get(
                        "/api/v1/invocations/state",
                        params={
                            "invocation_id": second.invocation_id,
                            "through_sequence": earlier_sequence,
                        },
                    )
                    self.assertEqual(outside.status_code, 400)

                    connection = sqlite3.connect(path)
                    try:
                        connection.execute(
                            "UPDATE invocations SET first_event_sequence = ? "
                            "WHERE invocation_id = ?",
                            (earlier_sequence, second.invocation_id),
                        )
                        connection.commit()
                    finally:
                        connection.close()

                    corrupt = await client.get(
                        "/api/v1/invocations/state",
                        params={
                            "invocation_id": second.invocation_id,
                            "through_sequence": earlier_sequence,
                        },
                    )
                    self.assertEqual(corrupt.status_code, 503)
                    self.assertEqual(
                        corrupt.json()["detail"]["code"],
                        "store_unavailable",
                    )
            finally:
                core.close()
                store.close()

    async def test_parent_child_routes_expose_durable_plan_lineage(self) -> None:
        """Navigate between a parent Invocation and its indexed Child plan."""

        assert httpx is not None
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            core = AutoAgentApp(runtime_event_sink=store)
            try:
                child = Workflow("traced-child", nodes=[Node("work", identity)])
                parent = Workflow(
                    "traced-parent",
                    nodes=[Node("spawn", child, execution_mode="spawn")],
                )
                result = core.invoke(
                    parent,
                    {"value": 8},
                    session_id="traced-parent-session",
                )
                handle = core.child_invocations(result.ref)[0]
                core.join(handle, timeout=1)
                app = create_tracing_app(store)
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://tracing.test",
                ) as client:
                    children = await client.get(
                        "/api/v1/invocations/children",
                        params={"invocation_id": result.invocation_id},
                    )
                    self.assertEqual(children.status_code, 200)
                    item = children.json()["items"][0]
                    self.assertEqual(item["parent_invocation_id"], result.invocation_id)
                    self.assertEqual(item["planned_workflow_id"], "traced-child")
                    self.assertEqual(
                        item["planned_invocation_id"], handle.invocation_id
                    )
                    self.assertEqual(
                        item["current_invocation_id"], handle.invocation_id
                    )
                    self.assertEqual(item["mode"], "spawn")
                    self.assertEqual(item["phase"], "terminal")

                    detail = await client.get(
                        "/api/v1/invocations/detail",
                        params={"invocation_id": handle.invocation_id},
                    )
                    self.assertEqual(
                        detail.json()["parent_invocation_id"],
                        result.invocation_id,
                    )
            finally:
                core.close()
                store.close()

    async def test_corrupt_persisted_records_are_not_reported_as_bad_requests(
        self,
    ) -> None:
        """Return one non-leaking Store error for corrupt canonical records."""

        assert httpx is not None
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            writer = SQLiteRuntimeStore(path)
            core = AutoAgentApp(runtime_event_sink=writer)
            try:
                compiled = core.register_workflow(
                    Workflow("corrupt-trace", nodes=[Node("work", identity)])
                )
                writer.save_workflow(
                    core.workflow_definition_snapshot(
                        compiled.workflow_revision_id
                    )
                )
                result = core.invoke(
                    compiled.workflow_revision_id,
                    {"value": 1},
                    session_id="corrupt-trace-session",
                )
            finally:
                core.close()
                writer.close()
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE workflow_definitions SET record_json = 'not-json' "
                    "WHERE revision_id = ?",
                    (compiled.workflow_revision_id,),
                )
                connection.execute(
                    "UPDATE trace_events SET record_json = 'not-json' "
                    "WHERE invocation_id = ? AND trace_sequence = ("
                    "SELECT MIN(trace_sequence) FROM trace_events "
                    "WHERE invocation_id = ?)",
                    (result.invocation_id, result.invocation_id),
                )
                connection.execute(
                    "UPDATE runtime_events SET record_json = 'not-json' "
                    "WHERE session_id = ? AND sequence = 1",
                    (result.session_id,),
                )
                connection.commit()
            finally:
                connection.close()

            reader = SQLiteRuntimeStore.open_read_only(path)
            reader.start()
            try:
                app = create_tracing_app(reader)
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://tracing.test",
                ) as client:
                    responses = [
                        await client.get(
                            "/api/v1/workflows/detail",
                            params={
                                "revision_id": compiled.workflow_revision_id
                            },
                        ),
                        await client.get(
                            "/api/v1/invocations/trace",
                            params={"invocation_id": result.invocation_id},
                        ),
                        await client.get(
                            "/api/v1/invocations/state",
                            params={"invocation_id": result.invocation_id},
                        ),
                    ]
                for response in responses:
                    self.assertEqual(response.status_code, 503)
                    self.assertEqual(
                        response.json()["detail"]["code"],
                        "store_unavailable",
                    )
                    self.assertNotIn("not-json", response.text)
            finally:
                reader.close()

    async def test_sse_replays_last_event_id_and_ends_at_terminal(self) -> None:
        """Resume terminal Trace streams from Last-Event-ID."""

        assert httpx is not None
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            core = AutoAgentApp(runtime_event_sink=store)
            try:
                result = core.invoke(
                    Workflow("sse-replay", nodes=[Node("work", identity)]),
                    {"value": 5},
                    session_id="sse-replay-session",
                )
                app = create_tracing_app(store, heartbeat_seconds=0.1)
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://tracing.test",
                ) as client:
                    path = "/api/v1/invocations/stream"
                    identity_params = {"invocation_id": result.invocation_id}
                    replay = await client.get(path, params=identity_params)
                    frames = _sse_events(replay.text)
                    events = [
                        frame for frame in frames if frame["event"] == "trace"
                    ]
                    self.assertGreater(len(events), 3)
                    stream_end = frames[-1]
                    self.assertEqual(stream_end["event"], "stream_end")
                    self.assertEqual(
                        stream_end["data"]["invocation_id"],
                        result.invocation_id,
                    )
                    self.assertEqual(stream_end["data"]["status"], "completed")
                    self.assertEqual(
                        stream_end["data"]["resume_cursor"], stream_end["id"]
                    )
                    self.assertEqual(
                        replay.headers["content-type"],
                        "text/event-stream; charset=utf-8",
                    )
                    self.assertEqual(
                        replay.headers["cache-control"],
                        "no-cache, no-transform",
                    )
                    resumed = await client.get(
                        path,
                        params=identity_params,
                        headers={"Last-Event-ID": str(events[0]["id"])},
                    )
                    resumed_frames = _sse_events(resumed.text)
                    resumed_events = [
                        frame
                        for frame in resumed_frames
                        if frame["event"] == "trace"
                    ]
                    self.assertEqual(len(resumed_events), len(events) - 1)
                    self.assertEqual(
                        resumed_events[0]["data"]["id"],
                        events[1]["data"]["id"],
                    )
                    numeric = await client.get(
                        path,
                        params={
                            **identity_params,
                            "after_sequence": events[0]["data"][
                                "trace_sequence"
                            ]
                        },
                    )
                    numeric_events = [
                        frame
                        for frame in _sse_events(numeric.text)
                        if frame["event"] == "trace"
                    ]
                    self.assertEqual(
                        len(numeric_events), len(events) - 1
                    )
                    ended = await client.get(
                        path,
                        params=identity_params,
                        headers={"Last-Event-ID": str(stream_end["id"])},
                    )
                    self.assertEqual(
                        [frame["event"] for frame in _sse_events(ended.text)],
                        ["stream_end"],
                    )
                    self.assertNotIn(": heartbeat", replay.text)
            finally:
                core.close()
                store.close()

    async def test_sse_emits_safe_error_when_store_fails_after_start(self) -> None:
        """End an established stream with a safe Store failure frame."""

        assert httpx is not None

        class _FailingLiveStore:
            async def get_invocation(
                self, invocation_id: str
            ) -> dict[str, object]:
                return {"invocation_id": invocation_id}

            async def latest_trace_sequence(self, invocation_id: str) -> int:
                del invocation_id
                return 0

            async def list_trace_events(
                self,
                invocation_id: str,
                *,
                after_sequence: int = 0,
                limit: int = 200,
            ) -> tuple[object, ...]:
                del invocation_id, after_sequence, limit
                raise RuntimeEventStoreError("secret SQLite failure")

        invocation_id = "sse-store-failure-invocation"
        app = create_tracing_app(
            _FailingLiveStore(),  # type: ignore[arg-type]
            heartbeat_seconds=0.1,
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://tracing.test",
        ) as client:
            response = await client.get(
                "/api/v1/invocations/stream",
                params={"invocation_id": invocation_id},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            _sse_events(response.text),
            [
                {
                    "id": None,
                    "event": "stream_error",
                    "data": {
                        "invocation_id": invocation_id,
                        "code": "store_unavailable",
                        "message": "Tracing data is unavailable or corrupt.",
                    },
                }
            ],
        )
        self.assertNotIn("secret SQLite failure", response.text)

    async def test_sse_wakes_live_and_emits_configured_heartbeat(self) -> None:
        """Wake live subscribers without a short polling loop."""

        events = _capture_events()
        self.assertGreaterEqual(len(events), 2)
        invocation_id = events[1].invocation_id
        assert invocation_id is not None
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(
                Path(directory) / "runtime.db",
                refresh_seconds=10.0,
            )
            try:
                await store.append(events[0])
                after_sequence = await store.latest_trace_sequence(invocation_id)
                stream = _iter_trace_stream(
                    store,
                    invocation_id,
                    after_sequence=after_sequence,
                    heartbeat_seconds=0.2,
                )
                waiting = asyncio.create_task(anext(stream))
                await asyncio.sleep(0.02)
                await store.append(events[1])
                frame = await asyncio.wait_for(waiting, timeout=0.5)
                self.assertIn(b'"kind":"invocation.started"', frame)

                heartbeat_started = time.monotonic()
                heartbeat = await asyncio.wait_for(anext(stream), timeout=0.6)
                heartbeat_elapsed = time.monotonic() - heartbeat_started
                self.assertEqual(heartbeat, b": heartbeat\n\n")
                self.assertGreaterEqual(heartbeat_elapsed, 0.1)
                await stream.aclose()

                after_sequence = await store.latest_trace_sequence(invocation_id)
                cancelled_stream = _iter_trace_stream(
                    store,
                    invocation_id,
                    after_sequence=after_sequence,
                    heartbeat_seconds=10,
                )
                pending = asyncio.create_task(anext(cancelled_stream))
                await asyncio.sleep(0.05)
                self.assertGreater(store._active_listener_count(), 0)
                pending.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await pending
                await cancelled_stream.aclose()
                self.assertEqual(store._active_listener_count(), 0)
            finally:
                store.close()

    async def test_sse_waits_for_spawned_children_before_stream_end(self) -> None:
        """Deliver late Child terminal traces after a Spawn parent completes."""

        entered = asyncio.Event()
        test_loop = asyncio.get_running_loop()
        release = threading.Event()

        async def slow_child(value: Value) -> Value:
            test_loop.call_soon_threadsafe(entered.set)
            while not release.is_set():
                await asyncio.sleep(0.01)
            return value

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            core = AutoAgentApp(runtime_event_sink=store)
            try:
                child = Workflow("slow-trace-child", nodes=[Node("work", slow_child)])
                parent = Workflow(
                    "spawn-trace-parent",
                    nodes=[Node("spawn", child, execution_mode="spawn")],
                )
                result = await core.ainvoke(
                    parent,
                    {"value": 1},
                    session_id="spawn-trace-parent-session",
                )
                self.assertEqual(result.status, "completed")
                await asyncio.wait_for(entered.wait(), timeout=1)
                handle = (await core.achild_invocations(result.ref))[0]
                position = await store.latest_trace_sequence(result.invocation_id)
                self.assertIsNone(
                    await store.terminal_trace_status(
                        result.invocation_id,
                        through_sequence=position,
                    )
                )
                stream = _iter_trace_stream(
                    store,
                    result.invocation_id,
                    after_sequence=position,
                    heartbeat_seconds=10,
                )
                pending = asyncio.create_task(anext(stream))
                await asyncio.sleep(0.05)
                self.assertFalse(pending.done())

                release.set()
                child_result = await core.ajoin(handle, timeout=1)
                self.assertEqual(child_result.status, "completed")
                terminal_phase = await asyncio.wait_for(pending, timeout=1)
                self.assertIn(b'"kind":"child_invocation.phase_changed"', terminal_phase)
                self.assertIn(b'"status":"terminal"', terminal_phase)
                stream_end = await asyncio.wait_for(anext(stream), timeout=1)
                self.assertIn(b"event: stream_end", stream_end)
                await stream.aclose()
            finally:
                release.set()
                core.close()
                store.close()


if __name__ == "__main__":
    unittest.main()
