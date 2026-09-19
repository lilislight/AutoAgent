from __future__ import annotations

from collections.abc import Iterator
import unittest
import warnings
from typing_extensions import TypedDict
from autoagent import AutoAgentApp, Node, Stream, StreamContext, Workflow
from autoagent.core.runtime import UserEvent


class Value(TypedDict):
    value: int


class Chunk(TypedDict):
    value: int


class Total(TypedDict):
    total: int


class CoreUserEventSinkTests(unittest.TestCase):
    def test_user_event_sink_failure_does_not_change_workflow_result(self) -> None:
        """Keep canonical execution successful when observation delivery fails."""

        class FailingSink:
            def __init__(self) -> None:
                self.invocation_ids: list[str] = []

            async def append_user_event(self, _event: UserEvent) -> None:
                self.invocation_ids.append(_event.invocation_id)
                raise RuntimeError("sink failed")

        def stream(value: Value) -> Iterator[Chunk]:
            yield value

        class Reducer:
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

        sink = FailingSink()
        app = AutoAgentApp(user_event_sink=sink)
        try:
            workflow = Workflow(
                "sink-failure",
                nodes=[
                    Node(
                        "work",
                        stream,
                        stream=Stream(Reducer()),
                    )
                ],
            )
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                result = app.invoke(workflow, {"value": 1})
                second = app.invoke(workflow, {"value": 2})
            self.assertEqual(result.status, "completed")
            self.assertEqual(second.status, "completed")
            self.assertIsInstance(app.user_event_sink_error, RuntimeError)
            self.assertEqual(len(app.user_event_sink_errors), 2)
            self.assertEqual(len(set(sink.invocation_ids)), 2)
            self.assertEqual(len(caught), 2)
        finally:
            app.close()


if __name__ == "__main__":
    unittest.main()
