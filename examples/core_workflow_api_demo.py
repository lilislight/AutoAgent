"""Use only AutoAgent Core to run, observe, checkpoint, and recover a Workflow.

Run from the repository root:

    .venv/bin/python -m examples.core_workflow_api_demo

The example writes ``core_workflow_checkpoint.json`` in the current directory.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import cast

from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp,
    ConditionContext,
    ContextOperation,
    ContextPatch,
    Edge,
    InputMappingContext,
    InvocationResult,
    InvocationUpdate,
    Node,
    OutputBindingContext,
    Recovery,
    RuntimeCheckpointBundle,
    SubWorkflow,
    UserEventMapping,
    Wait,
    Workflow,
)


CHECKPOINT_PATH = Path("core_workflow_checkpoint.json")


# Core requires durable Operator boundaries. TypedDict and strict Pydantic models
# are supported; this example uses module-level TypedDict contracts throughout.
class OrderInput(TypedDict):
    order_id: str
    amount: int
    stock: int


class Order(TypedDict):
    order_id: str
    amount: int
    stock: int
    currency: str


class ApprovalRequest(TypedDict):
    order_id: str
    amount: int
    question: str


class ApprovalResponse(TypedDict):
    approved: bool


class CheckResult(TypedDict):
    check: str
    passed: bool
    detail: str


class CombinedChecks(TypedDict):
    order_id: str
    risk: CheckResult
    inventory: CheckResult


class Decision(TypedDict):
    order_id: str
    approved: bool
    summary: str


class AuditRequest(TypedDict):
    order_id: str
    summary: str


class AuditRecord(TypedDict):
    order_id: str
    stored: bool


def normalize_order(value: OrderInput) -> Order:
    return {**value, "currency": "USD"}


def remember_order(context: OutputBindingContext) -> ContextPatch:
    """Save data needed after the Wait boundary in Invocation Context."""

    return ContextPatch(
        invocation=(ContextOperation.set("order", context.output),),
    )


def make_approval_request(context: InputMappingContext) -> ApprovalRequest:
    order = cast(Order, context.invocation_context["order"])
    return {
        "order_id": order["order_id"],
        "amount": order["amount"],
        "question": "Approve this order?",
    }


def approval_granted(context: ConditionContext) -> bool:
    response = cast(ApprovalResponse, context.output)
    return response["approved"]


def approval_rejected(context: ConditionContext) -> bool:
    return not approval_granted(context)


def order_from_context(context: InputMappingContext) -> Order:
    # Context views are immutable mappings; return a plain dict at the strict
    # TypedDict Operator boundary.
    return dict(cast(Order, context.invocation_context["order"]))  # type: ignore[return-value]


def risk_check(order: Order) -> CheckResult:
    # Pretend this calls a fraud/risk service.
    return {
        "check": "risk",
        "passed": order["amount"] <= 1_000,
        "detail": f"amount={order['amount']}",
    }


def inventory_check(order: Order) -> CheckResult:
    # This node and risk_check become ready together and run in parallel.
    return {
        "check": "inventory",
        "passed": order["stock"] > 0,
        "detail": f"stock={order['stock']}",
    }


def map_combined_checks(context: InputMappingContext) -> CombinedChecks:
    risk = cast(CheckResult, context.incoming["risk-result"])
    inventory = cast(CheckResult, context.incoming["inventory-result"])
    order = cast(Order, context.invocation_context["order"])
    return {
        "order_id": order["order_id"],
        "risk": risk,
        "inventory": inventory,
    }


def combine_checks(checks: CombinedChecks) -> Decision:
    passed = checks["risk"]["passed"] and checks["inventory"]["passed"]
    return {
        "order_id": checks["order_id"],
        "approved": passed,
        "summary": (
            f"risk={checks['risk']['passed']}, "
            f"inventory={checks['inventory']['passed']}"
        ),
    }


def decision_approved(context: ConditionContext) -> bool:
    decision = cast(Decision, context.output)
    return decision["approved"]


def decision_rejected(context: ConditionContext) -> bool:
    return not decision_approved(context)


def rejected_order_input(context: InputMappingContext) -> Order:
    return dict(cast(Order, context.invocation_context["order"]))  # type: ignore[return-value]


def reject_order(order: Order) -> Decision:
    return {
        "order_id": order["order_id"],
        "approved": False,
        "summary": "approval was rejected",
    }


def manual_review(decision: Decision) -> Decision:
    return {**decision, "summary": f"manual review required: {decision['summary']}"}


def audit_input(context: InputMappingContext) -> AuditRequest:
    decision = cast(Decision, next(iter(context.incoming.values())))
    return {
        "order_id": decision["order_id"],
        "summary": decision["summary"],
    }


def write_audit_record(request: AuditRequest) -> AuditRecord:
    # Sleep briefly so the parent can return its spawn handle first.
    time.sleep(0.1)
    return {"order_id": request["order_id"], "stored": True}


def audit_event(context: OutputBindingContext) -> AuditRecord:
    return cast(AuditRecord, context.output)


def decision_event(context: OutputBindingContext) -> Decision:
    return cast(Decision, context.output)


def build_workflow() -> Workflow:
    """Build the exact same definition in both the source and recovered App."""

    # SubWorkflow is a compile-time inline composition. Its nodes become
    # prepare.normalize and prepare.approval inside the parent's Workflow IR.
    prepare = Workflow(
        "prepare-order",
        nodes=[
            Node("normalize", normalize_order, output_binding=remember_order),
            Node(
                "approval",
                Wait(ApprovalRequest, ApprovalResponse),
                input_mapping=make_approval_request,
            ),
        ],
        edges=[Edge("normalize", "approval")],
    )

    # A Workflow used as a Node executable is a runtime Child Invocation.
    audit = Workflow(
        "audit-order",
        nodes=[
            Node(
                "write",
                write_audit_record,
                recovery_mode=Recovery(mode="replay_safe", max_attempts=2),
                user_events=(UserEventMapping("audit.stored", audit_event),),
            )
        ],
    )

    return Workflow(
        "order-processing-demo",
        sub_workflows=[SubWorkflow("prepare", prepare)],
        nodes=[
            Node("risk", risk_check, input_mapping=order_from_context),
            Node("inventory", inventory_check, input_mapping=order_from_context),
            Node(
                "combine",
                combine_checks,
                input_mapping=map_combined_checks,
                user_events=(UserEventMapping("order.decided", decision_event),),
            ),
            Node(
                "spawn_audit",
                audit,
                input_mapping=audit_input,
                execution_mode="spawn",
            ),
            Node("manual_review", manual_review),
            Node("approval_rejected", reject_order, input_mapping=rejected_order_input),
        ],
        edges=[
            # Approval branches. When approved, both checks become ready together.
            Edge(
                "prepare.approval",
                "risk",
                approval_granted,
                id="approval-to-risk",
            ),
            Edge(
                "prepare.approval",
                "inventory",
                approval_granted,
                id="approval-to-inventory",
            ),
            Edge(
                "prepare.approval",
                "approval_rejected",
                approval_rejected,
                id="approval-to-rejected",
            ),
            # combine is a fan-in and runs only after both branches resolve.
            Edge("risk", "combine", id="risk-result"),
            Edge("inventory", "combine", id="inventory-result"),
            # The combined result branches again.
            Edge(
                "combine",
                "spawn_audit",
                decision_approved,
                id="decision-to-audit",
            ),
            Edge(
                "combine",
                "manual_review",
                decision_rejected,
                id="decision-to-review",
            ),
        ],
    )


def print_result(label: str, result: InvocationResult) -> None:
    print(f"\n[{label}] status={result.status}")
    print(f"ref={result.ref}")
    print(f"output={result.output}")
    print(f"error={result.error}")
    print(f"waits={result.waits}")
    print("trace kinds:", [event.kind for event in result.trace_events])
    print(
        "user events:",
        [(event.kind, event.payload) for event in result.user_events],
    )
    print(
        "checkpoint:",
        result.checkpoint.id,
        "sessions=",
        tuple(result.checkpoint.states),
    )


def save_checkpoint(checkpoint: RuntimeCheckpointBundle) -> None:
    CHECKPOINT_PATH.write_text(
        json.dumps(checkpoint.to_record(), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"\nsaved checkpoint to {CHECKPOINT_PATH.resolve()}")


def load_checkpoint() -> RuntimeCheckpointBundle:
    record = json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
    return RuntimeCheckpointBundle.from_record(record)


def main() -> None:
    source = AutoAgentApp(max_operator_concurrency=4)
    workflow = build_workflow()
    compiled = source.register_workflow(workflow)
    print("registered revision:", compiled.workflow_revision_id)

    # stream() invokes the Workflow and exposes backpressured observations. Some
    # updates also carry a checkpoint when Core has reached a safe boundary.
    waiting: InvocationResult | None = None
    with source.stream(
        workflow.id,
        {"order_id": "order-42", "amount": 600, "stock": 3},
        session_id="core-api-demo-session",
    ) as updates:
        for item in updates:
            if isinstance(item, InvocationUpdate):
                print(
                    "stream update:",
                    item.event.kind,
                    "safe checkpoint=",
                    item.checkpoint is not None,
                )
            else:
                waiting = item
    assert waiting is not None
    # Attached stream observations are delivered once as InvocationUpdate; the
    # final Result does not duplicate them in trace_events/user_events.
    print_result("source reached Wait", waiting)

    # The checkpoint is a self-contained Runtime State graph. Core does not
    # choose storage, so this example persists its canonical record as JSON.
    save_checkpoint(waiting.checkpoint)
    source.close()

    # Simulate a new process: construct a fresh Core App and register the exact
    # Workflow revision before installing its checkpoint.
    recovered = AutoAgentApp(max_operator_concurrency=4)
    recovered_workflow = build_workflow()
    recovered.register_workflow(recovered_workflow)
    loaded = recovered.load_checkpoint(load_checkpoint())
    root_ref = loaded.roots[0]
    print("\nloaded root:", root_ref)

    # recover() validates recovery policy and drives runnable work. A Wait stays
    # waiting until the user supplies its exact wait id and typed response.
    restored_wait = recovered.recover(root_ref)
    print_result("recovered Wait", restored_wait)

    completed = recovered.resume(
        root_ref,
        restored_wait.waits[0].id,
        {"approved": True},
    )
    print_result("parent completed", completed)

    # The approved path returns after spawning the audit Child. Observe and wait
    # for it through the parent's durable ChildInvocationHandle.
    child_handle = recovered.child_handles(completed.ref)[0]
    print("\nspawned child handle:", child_handle)
    child = recovered.wait_child(child_handle, timeout=2.0)
    print_result("spawned audit child", child)

    # Any stable result boundary carries a fresh checkpoint. This one includes
    # the root plus the spawned Child Runtime State.
    save_checkpoint(child.checkpoint)
    recovered.close()


if __name__ == "__main__":
    main()
