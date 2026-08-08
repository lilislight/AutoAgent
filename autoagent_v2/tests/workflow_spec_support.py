"""Black-box helpers for the normative Workflow graph specification tests.

This module deliberately depends only on AutoAgent's public API.  The tests
that import it are derived from ``autoagent_v2/workflow.md``, not from the
Compiler, Scheduler, Executor, or Runtime implementation.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
import sys
import threading
from typing import Any
import unittest


V2_ROOT = Path(__file__).resolve().parents[1]
while str(V2_ROOT) in sys.path:
    sys.path.remove(str(V2_ROOT))
sys.path.insert(0, str(V2_ROOT))

from autoagent import (  # noqa: E402
    AutoAgentApp,
    ContextPatch,
    Edge,
    Invocation,
    InvocationState,
    Node,
    Workflow,
)
from autoagent.core import (  # noqa: E402
    ExecutionContext,
    WorkflowCompileError,
    WorkflowCompiler,
)


Payload = dict[str, int]


def passthrough(payload: Payload) -> Payload:
    return dict(payload)


def use_invocation_input(context: ExecutionContext) -> Payload:
    return dict(context.invocation_input)


def always(context: ExecutionContext) -> bool:
    del context
    return True


def never(context: ExecutionContext) -> bool:
    del context
    return False


def conditional_true(context: ExecutionContext) -> bool:
    """Semantically true but intentionally opaque to static analysis."""

    return bool(context.invocation_input.get("route", 1))


def conditional_false(context: ExecutionContext) -> bool:
    """Semantically false but intentionally opaque to static analysis."""

    return bool(context.invocation_input.get("never", 0))


def traced_operator(
    label: str,
    trace: list[str],
    *,
    started: threading.Event | None = None,
    release: threading.Event | None = None,
) -> Callable[[Payload], Payload]:
    def operator(payload: Payload) -> Payload:
        trace.append(label)
        if started is not None:
            started.set()
        if release is not None and not release.wait(timeout=3.0):
            raise TimeoutError(f"test operator {label!r} was not released")
        return dict(payload)

    return operator


def node(
    node_id: str,
    operator: Callable[[Payload], Payload] = passthrough,
    **kwargs: Any,
) -> Node:
    return Node(
        id=node_id,
        operator=operator,
        input_mapping=use_invocation_input,
        **kwargs,
    )


def workflow(
    workflow_id: str,
    node_ids: list[str],
    edges: list[Edge],
    *,
    nodes: list[Node] | None = None,
) -> Workflow:
    return Workflow(
        id=workflow_id,
        nodes=nodes if nodes is not None else [node(node_id) for node_id in node_ids],
        edges=edges,
    )


def compile_workflow(definition: Workflow):
    return WorkflowCompiler().compile(definition)


def compile_error_code(error: WorkflowCompileError) -> str | None:
    code = getattr(error, "code", None)
    if code is not None:
        return str(code)
    message = str(error)
    for candidate in (
        "WORKFLOW_NO_ENTRY",
        "WORKFLOW_NO_EXIT",
        "WORKFLOW_UNREACHABLE_NODE",
        "LOOP_IRREDUCIBLE",
        "LOOP_NON_HEADER_ENTRY",
        "LOOP_REGION_OVERLAP",
        "LOOP_MULTIPLE_BACK_EDGES",
        "LOOP_WITHOUT_EXIT",
        "CYCLIC_REGION_WITHOUT_EXIT",
        "LOOP_STATIC_CONTROL_CONFLICT",
    ):
        if candidate in message:
            return candidate
    return None


def assert_compile_error(
    case: unittest.TestCase,
    definition: Workflow,
    expected_code: str | set[str] | None = None,
) -> WorkflowCompileError:
    with case.assertRaises(WorkflowCompileError) as raised:
        compile_workflow(definition)
    if expected_code is not None:
        expected = {expected_code} if isinstance(expected_code, str) else expected_code
        case.assertIn(
            compile_error_code(raised.exception),
            expected,
            f"compile error should expose one of the stable codes {sorted(expected)}",
        )
    return raised.exception


def run_workflow(
    definition: Workflow,
    invocation_input: Payload | None = None,
    *,
    timeout: float = 5.0,
) -> tuple[Invocation, Any]:
    app = AutoAgentApp(max_thread_workers=8, max_parallel_units=8)
    try:
        app.register_workflow(definition)
        invocation = app.submit_invoke(
            definition.id,
            invocation_input or {"route": 1},
        )
        invocation.wait(timeout=timeout)
        return invocation, invocation.snapshot()
    finally:
        app.close()


def assert_completed(case: unittest.TestCase, snapshot: Any) -> None:
    case.assertEqual(snapshot.state, InvocationState.COMPLETED, snapshot.error)


def assert_runtime_error(
    case: unittest.TestCase,
    snapshot: Any,
    expected_code: str,
) -> None:
    case.assertEqual(snapshot.state, InvocationState.FAILED)
    case.assertIsNotNone(snapshot.error)
    error_type = str(snapshot.error.type)
    message = str(snapshot.error.message)
    case.assertTrue(
        error_type == expected_code or expected_code in message,
        f"expected runtime error {expected_code}, got {error_type}: {message}",
    )
