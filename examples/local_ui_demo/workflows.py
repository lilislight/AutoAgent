"""Registered definitions for the local browser; no executions are submitted."""

import asyncio
from contextlib import asynccontextmanager
from typing import cast
from typing_extensions import TypedDict

from autoagent import (Capability, ConditionContext, ContextOperation, ContextPatch, Edge, InputMappingContext, OutputBindingContext, TimerRequest,
                       Map, Node, Operator, Spawn, Wait, Workflow)
from autoagent.host import workflow
from examples.core_workflow_api_demo import CheckResult, CombinedChecks, Decision, Order, build_workflow, manual_review, order_from_context, risk_check
from examples.host_project_demo.workflows import finish


incident_review_operator = Operator(manual_review, id="review_incident")
incident_review = Capability("incident_review", incident_review_operator.contract)


@asynccontextmanager
async def lifespan(setup):
    """Bind the Capability to its default implementation before execution."""
    setup.register_operator(incident_review_operator, capability_id=incident_review.id, default=True)
    yield


def operational_risk_failure(context: ConditionContext) -> bool:
    """Review service failures while keeping invalid input errors unhandled."""
    return context.error is not None and context.error.type in {
        "RuntimeError", "TimeoutError", "ConnectionError",
    }


def available_checks(context: InputMappingContext) -> CombinedChecks:
    """Treat an inactive result edge as a failed check at the fan-in."""
    order = cast(Order, context.invocation_context["order"])
    return {
        "order_id": order["order_id"],
        "risk": cast(CheckResult, context.incoming.get("risk-result", {
            "check": "risk", "passed": False, "detail": "risk check failed",
        })),
        "inventory": cast(CheckResult, context.incoming.get("inventory-result", {
            "check": "inventory", "passed": False, "detail": "inventory check failed",
        })),
    }


def handle_risk_failure(order: Order) -> Decision:
    """Send failed risk checks to a separate review branch."""
    return {
        "order_id": order["order_id"],
        "approved": False,
        "summary": "risk check failed; manual investigation required",
    }


def demo_risk_check(order: Order) -> CheckResult:
    """Provide optional deterministic failure cases for the tracing demo."""
    if order["order_id"] == "risk-service-failure":
        raise RuntimeError("Risk service unavailable")
    if order["amount"] < 0:
        raise ValueError("Order amount must be nonnegative")
    return risk_check(order)


@workflow
def order_processing():
    """Approval, parallel checks, fan-in, branching and a spawned Workflow."""
    definition = build_workflow()
    next(node for node in definition.nodes if node.id == "risk").executable = demo_risk_check
    next(node for node in definition.nodes if node.id == "combine").input_mapping = available_checks
    definition.add_node(Node("risk_failed", handle_risk_failure, input_mapping=order_from_context))
    definition.add_node(Node("review_incident", incident_review))
    definition.add_edge(Edge("risk", "risk_failed", operational_risk_failure, on="error", id="risk-failed"))
    definition.add_edge(Edge("risk_failed", "review_incident", id="failure-to-review"))
    return definition


@workflow
def document_formatting():
    """A small independent Workflow for verifying catalogue selection."""
    return Workflow("document-formatting", nodes=[Node("format", finish)])


class ReviewRequest(TypedDict):
    text: str
    rounds: int


class ReviewState(TypedDict):
    text: str
    rounds: int
    iteration: int


def prepare_review(value: ReviewRequest) -> ReviewState:
    """Start a bounded review with distinct values in every Loop iteration."""
    if not 1 <= value["rounds"] <= 200:
        raise ValueError("Review rounds must be between 1 and 200")
    return {"text": value["text"], "rounds": value["rounds"], "iteration": 0}


async def refine_document(value: ReviewState) -> ReviewState:
    """Simulate one review pass and retain its own Input and Output."""
    await asyncio.sleep(0.025)
    iteration = value["iteration"] + 1
    return {"text": value["text"] + f"\nReview pass {iteration}: checked.",
            "rounds": value["rounds"], "iteration": iteration}


def review_again(context: ConditionContext) -> bool:
    value = cast(ReviewState, context.output)
    return value["iteration"] < value["rounds"]


def review_finished(context: ConditionContext) -> bool:
    return not review_again(context)


def publish_review(value: ReviewState) -> ReviewState:
    return value


@workflow
def document_review_loop():
    """A real graph Loop repeats a Node and both outgoing Edge evaluations."""
    return Workflow("document-review-loop", nodes=[
        Node("prepare", prepare_review), Node("refine", refine_document),
        Node("publish", publish_review),
    ], edges=[
        Edge("prepare", "refine", id="start-review"),
        Edge("refine", "refine", review_again, id="review-again"),
        Edge("refine", "publish", review_finished, id="review-complete"),
    ])


class DocumentBatch(TypedDict):
    documents: list[str]


class DocumentTask(TypedDict):
    index: int
    text: str


class DocumentResult(TypedDict):
    index: int
    text: str
    reviewed: bool


def map_documents(context: InputMappingContext) -> list[DocumentTask]:
    batch = cast(DocumentBatch, context.invocation_input)
    return [{"index": index, "text": text} for index, text in enumerate(batch["documents"])]


async def review_batch_document(value: DocumentTask) -> DocumentResult:
    """Stagger real Calls so their overlap is visible in the Timeline."""
    await asyncio.sleep(0.08 + value["index"] % 3 * 0.045)
    return {"index": value["index"], "text": value["text"], "reviewed": True}


def map_review_workers(context: InputMappingContext) -> list[DocumentResult]:
    return cast(list[DocumentResult], context.incoming["dispatch-reviewed"])


async def save_document_review(value: DocumentResult) -> DocumentResult:
    """Each spawned Worker finishes independently of the parent Map."""
    await asyncio.sleep(0.06 + value["index"] % 3 * 0.035)
    return value


@workflow
def document_batch_review():
    """Map Operators over documents, then Map Spawn into child Invocations."""
    worker = Workflow("document-review-worker", nodes=[Node("save_review", save_document_review)])
    return Workflow("document-batch-review", nodes=[
        Node("review_documents", review_batch_document, input_mapping=map_documents,
             map=Map(max_parallelism=3)),
        Node("spawn_review_workers", Spawn(worker, "save_review"), input_mapping=map_review_workers,
             map=Map(max_parallelism=2)),
    ], edges=[Edge("review_documents", "spawn_review_workers", id="dispatch-reviewed")])


class LiveReviewRequest(TypedDict):
    text: str
    rounds: int
    delay_ms: int
    children: int


class LiveReviewState(LiveReviewRequest):
    iteration: int


class LiveReviewTask(TypedDict):
    text: str
    index: int
    delay_ms: int


def prepare_live_review(value: LiveReviewRequest) -> LiveReviewState:
    if not 1 <= value['rounds'] <= 200 or not 1 <= value['children'] <= 20 or not 10 <= value['delay_ms'] <= 10000:
        raise ValueError('Expected 1–200 rounds, 1–20 children and 10–10000 ms per step')
    return {**value, 'iteration': 0}


async def live_review_step(value: LiveReviewState) -> LiveReviewState:
    await asyncio.sleep(value['delay_ms'] / 1000)
    return {**value, 'iteration': value['iteration'] + 1}


def live_review_again(context: ConditionContext) -> bool:
    return context.output['iteration'] < context.output['rounds']


def live_review_done(context: ConditionContext) -> bool:
    return not live_review_again(context)


def remember_live_review(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(invocation=(ContextOperation.set('review', context.output),))


async def live_save_review(value: LiveReviewTask) -> LiveReviewTask:
    await asyncio.sleep(value['delay_ms'] / 1000)
    return value


def live_review_context(context: InputMappingContext) -> LiveReviewState:
    return cast(LiveReviewState, dict(context.invocation_context['review']))


def live_review_timer(context: InputMappingContext) -> TimerRequest:
    return TimerRequest(delay_us=2_000_000)


def live_review_children(context: InputMappingContext) -> list[LiveReviewTask]:
    value = cast(LiveReviewState, context.incoming['approval-to-dispatch'])
    return [{'text':value['text'], 'index':index, 'delay_ms':5000 + index % 4 * 1000} for index in range(value['children'])]


@workflow
def live_document_review():
    """Slow real Loop, Timer, approval and owned children for live tracing."""
    from autoagent import Timer, Wait
    worker = Workflow('live-review-worker', nodes=[Node('save', live_save_review)])
    return Workflow('live-document-review', nodes=[
        Node('prepare', prepare_live_review),
        Node('refine', live_review_step, output_binding=remember_live_review),
        Node('cooldown', Timer(), input_mapping=live_review_timer),
        Node('approval', Wait(LiveReviewState, LiveReviewState), input_mapping=live_review_context),
        Node('dispatch', Spawn(worker,'save'), input_mapping=live_review_children, map=Map(max_parallelism=4)),
    ], edges=[
        Edge('prepare','refine',id='begin-review'),
        Edge('refine','refine',live_review_again,id='review-again'),
        Edge('refine','cooldown',live_review_done,id='review-done'),
        Edge('cooldown','approval',id='request-approval'),
        Edge('approval','dispatch',id='approval-to-dispatch'),
    ])


class LiveSignalMessage(TypedDict):
    text: str


@workflow
def live_signal_review():
    """Wait for a message sent from the execution UI's declared Signal Endpoint."""
    from autoagent import AwaitSignal, SignalEndpoint
    return Workflow('live-signal-review',nodes=[Node('receive',AwaitSignal('message'))],
        signal_endpoints=[SignalEndpoint('message',LiveSignalMessage)])


class ParallelApprovalRequest(TypedDict):
    subject: str


class ParallelApprovalResponse(TypedDict):
    approved: bool
    comment: str


def prepare_parallel_approvals(value: ParallelApprovalRequest) -> ParallelApprovalRequest:
    """Fan one request out to twenty independently resumable Wait Nodes."""
    return value


@workflow
def parallel_approvals():
    """Twenty concurrent external approvals in a single Invocation."""
    approvals = [Node(f"approval_{index:02d}", Wait(ParallelApprovalRequest, ParallelApprovalResponse)) for index in range(1, 21)]
    return Workflow("parallel-approvals", nodes=[Node("prepare", prepare_parallel_approvals), *approvals],
                    edges=[Edge(source="prepare", target=node.id, id=f"request_{index:02d}") for index, node in enumerate(approvals, 1)])
