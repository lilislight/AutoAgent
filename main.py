from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from autoagent import (
    AutoAgentApp,
    BackoffPolicy,
    CapabilityRef,
    CapabilitySelectionPolicy,
    EdgePolicy,
    MapPolicy,
    NodePolicy,
    OperatorRef,
    ReplicationPolicy,
    ResourcePolicy,
    RetryPolicy,
    TimeoutPolicy,
    Workflow,
)
from autoagent.core.runtime import ConditionContext, OutputBindingContext


def collect_incident_signal(
    incident_id: str,
    service: str,
    severity: int,
    symptoms: list[str],
) -> dict[str, object]:
    """Normalize an alert or human report into the incident domain."""

    return {
        "incident_id": incident_id,
        "service": service,
        "severity": severity,
        "symptoms": symptoms,
    }


def classify_incident(
    incident_id: str,
    service: str,
    severity: int,
    symptoms: list[str],
) -> dict[str, object]:
    """Produce routing fields consumed by several independent branches."""

    return {
        "incident_id": incident_id,
        "service": service,
        "severity": severity,
        "symptoms": symptoms,
        "query": f'{service} errors matching {" ".join(symptoms)}',
        "needs_security_review": "unauthorized" in symptoms,
    }


def local_log_analyzer(
    incident_id: str,
    query: str,
    **context: object,
) -> dict[str, object]:
    """Default implementation of the abstract incident_log_analysis capability."""

    return {
        "incident_id": incident_id,
        "log_findings": [f"local match for: {query}"],
        "confidence": 0.78,
    }


async def remote_log_analyzer(
    incident_id: str,
    query: str,
    **context: object,
) -> dict[str, object]:
    """Higher-priority async implementation selected at execution time."""

    await asyncio.sleep(0)
    return {
        "incident_id": incident_id,
        "log_findings": [f"remote match for: {query}"],
        "confidence": 0.91,
    }


def incompatible_log_analyzer(search_text: str) -> list[str]:
    """Intentionally incompatible with incident_log_analysis's contract.

    The Capability requires named inputs ``incident_id`` and ``query``. This
    Operator instead requires ``search_text`` and returns a different shape, so
    OperatorRegistry must reject it before any Workflow can reference it.
    """

    return [search_text]


def assess_blast_radius(
    incident_id: str,
    service: str,
    severity: int,
    **context: object,
) -> dict[str, object]:
    return {
        "incident_id": incident_id,
        "affected_services": [service, "api-gateway"] if severity >= 3 else [service],
        "customer_impact": "high" if severity >= 4 else "moderate",
    }


def review_security_risk(
    incident_id: str,
    symptoms: list[str],
    **context: object,
) -> dict[str, object]:
    return {
        "incident_id": incident_id,
        "security_risk": "high" if "unauthorized" in symptoms else "low",
        "containment_required": "unauthorized" in symptoms,
    }


def plan_remediation(
    analyze_logs: dict[str, object],
    assess_blast_radius: dict[str, object],
    security_review: dict[str, object] | None = None,
) -> list[dict[str, str]]:
    """Create a dynamic action list that the following Map edge fans out."""

    actions = [
        {
            "action_id": "rollback",
            "command": "deploy rollback payments-api",
            "owner": "release-engineering",
        },
        {
            "action_id": "scale",
            "command": "scale payments-api --replicas 12",
            "owner": "sre",
        },
    ]
    if security_review and security_review.get("containment_required"):
        actions.append(
            {
                "action_id": "contain",
                "command": "rotate credentials payments-api",
                "owner": "security",
            }
        )
    return actions


def select_action_items(
    actions: list[dict[str, str]],
) -> Iterable[Mapping[str, Any]]:
    """Map each planned action to one execute_action Operator input."""

    return [dict(action) for action in actions]


async def execute_action(
    action_id: str,
    command: str,
    owner: str,
) -> dict[str, object]:
    await asyncio.sleep(0)
    return {
        "action_id": action_id,
        "command": command,
        "owner": owner,
        "success": True,
    }


def aggregate_action_results(
    outputs: list[dict[str, object]],
) -> dict[str, object]:
    return {"results": outputs}


def review_remediation(results: list[dict[str, object]]) -> dict[str, object]:
    """One replicated review sample; three samples are aggregated below."""

    successful = sum(bool(item.get("success")) for item in results)
    score = int(successful / max(1, len(results)) * 100)
    return {
        "approved": score >= 80,
        "score": score,
        "reasons": [] if score >= 80 else ["Not all remediation actions succeeded"],
    }


def select_best_review(
    outputs: list[dict[str, object]],
) -> dict[str, object]:
    """Replication aggregator producing the logical review node output."""

    return max(outputs, key=lambda output: int(output["score"]))


def refine_remediation(
    approved: bool,
    score: int,
    reasons: list[str],
) -> dict[str, object]:
    """Loop body that prepares another review input when approval fails."""

    return {
        "results": [
            {
                "action_id": "refined-check",
                "success": True,
                "previous_score": score,
                "resolved_reasons": reasons,
            }
        ]
    }


def close_incident(
    approved: bool,
    score: int,
    reasons: list[str],
) -> dict[str, object]:
    return {
        "status": "resolved",
        "review_score": score,
        "remaining_reasons": reasons,
        "approved": approved,
    }


def remember_resolution(ctx: OutputBindingContext) -> None:
    """Persist a small cross-invocation incident summary in SessionContext."""

    ctx.session_context.data["latest_resolution"] = dict(ctx.output)


def notify_stakeholders(
    status: str,
    review_score: int,
    **context: object,
) -> dict[str, object]:
    return {"notification_sent": True, "status": status, "score": review_score}


def create_postmortem(
    status: str,
    review_score: int,
    **context: object,
) -> dict[str, object]:
    return {
        "postmortem_id": "pm-auto-generated",
        "status": status,
        "review_score": review_score,
    }


def archive_incident(
    notify_stakeholders: dict[str, object],
    create_postmortem: dict[str, object],
) -> dict[str, object]:
    return {
        "archived": True,
        "notification": notify_stakeholders,
        "postmortem": create_postmortem,
    }


def should_run_security_review(ctx: ConditionContext) -> bool:
    return bool(ctx.source_output["needs_security_review"])


def review_is_approved(ctx: ConditionContext) -> bool:
    return bool(ctx.source_output["approved"])


def review_requires_refinement(ctx: ConditionContext) -> bool:
    return not review_is_approved(ctx)


def build_app() -> AutoAgentApp:
    app = AutoAgentApp()
    app.register_capability(
        "incident_log_analysis",
        description="Search and interpret logs related to a production incident.",
    )
    app.register_operator(
        local_log_analyzer,
        operator_id="local_log_analyzer",
        capability_id="incident_log_analysis",
        default=True,
    )
    app.register_operator(
        remote_log_analyzer,
        operator_id="remote_log_analyzer",
        capability_id="incident_log_analysis",
        priority=100,
    )
    app.register_operator(
        archive_incident,
        operator_id="archive_incident_v1",
    )
    return app


def demonstrate_schema_registration_error(app: AutoAgentApp) -> str:
    """Attempt an invalid registration and return the expected error message."""

    try:
        app.register_operator(
            incompatible_log_analyzer,
            operator_id="incompatible_log_analyzer",
            capability_id="incident_log_analysis",
        )
    except ValueError as exc:
        return str(exc)
    raise AssertionError("Incompatible Operator registration unexpectedly succeeded.")


def build_workflow() -> Workflow:
    workflow = Workflow(
        id="production_incident_response",
        version=1,
        name="Production Incident Response",
        description=(
            "Investigate a production incident, fan out remediation actions, "
            "review the result, and publish the resolution."
        ),
    )

    workflow.add_node(
        collect_incident_signal,
        node_id="collect_signal",
        name="Collect incident signal",
        entry=True,
    )
    workflow.add_node(
        classify_incident,
        node_id="classify_incident",
        name="Classify incident",
    )
    workflow.add_node(
        CapabilityRef(id="incident_log_analysis"),
        node_id="analyze_logs",
        name="Analyze logs",
        policy=NodePolicy(
            selection=CapabilitySelectionPolicy(
                mode="priority",
                preferred_operator_ids=("remote_log_analyzer",),
            ),
            retry=RetryPolicy(
                max_attempts=3,
                backoff=BackoffPolicy(
                    mode="exponential",
                    initial_delay_ms=100,
                    max_delay_ms=1000,
                    jitter="full",
                ),
            ),
            timeout=TimeoutPolicy(timeout_ms=5000),
            resource=ResourcePolicy(
                max_node_executions_per_invocation=4,
                max_operator_calls_per_invocation=8,
                max_runtime_ms_per_invocation=15000,
            ),
            max_concurrency=4,
        ),
    )
    workflow.add_node(
        assess_blast_radius,
        node_id="assess_blast_radius",
        name="Assess blast radius",
    )
    workflow.add_node(
        review_security_risk,
        node_id="security_review",
        name="Security review",
    )
    workflow.add_node(
        plan_remediation,
        node_id="plan_remediation",
        name="Plan remediation",
    )
    workflow.add_node(
        execute_action,
        node_id="execute_action",
        name="Execute remediation actions",
        policy=NodePolicy(max_concurrency=8),
    )
    workflow.add_node(
        review_remediation,
        node_id="review_remediation",
        name="Review remediation",
        policy=NodePolicy(
            replication=ReplicationPolicy(
                count=3,
                output_aggregator=select_best_review,
                max_parallelism=3,
            )
        ),
    )
    workflow.add_node(
        refine_remediation,
        node_id="refine_remediation",
        name="Refine failed remediation",
    )
    workflow.add_node(
        close_incident,
        node_id="close_incident",
        name="Close incident",
        output_binding=remember_resolution,
    )
    workflow.add_node(
        notify_stakeholders,
        node_id="notify_stakeholders",
        name="Notify stakeholders",
    )
    workflow.add_node(
        create_postmortem,
        node_id="create_postmortem",
        name="Create postmortem",
    )
    workflow.add_node(
        OperatorRef(id="archive_incident_v1"),
        node_id="archive_incident",
        name="Archive incident",
    )

    workflow.add_edge("collect_signal", "classify_incident")
    workflow.add_edge("classify_incident", "analyze_logs")
    workflow.add_edge("classify_incident", "assess_blast_radius")
    workflow.add_edge(
        "classify_incident",
        "security_review",
        condition=should_run_security_review,
    )
    workflow.add_edge("analyze_logs", "plan_remediation")
    workflow.add_edge("assess_blast_radius", "plan_remediation")
    workflow.add_edge("security_review", "plan_remediation")
    workflow.add_edge(
        "plan_remediation",
        "execute_action",
        policy=EdgePolicy(
            map=MapPolicy(
                item_selector=select_action_items,
                output_aggregator=aggregate_action_results,
                max_parallelism=4,
            )
        ),
    )
    workflow.add_edge("execute_action", "review_remediation")
    workflow.add_edge(
        "review_remediation",
        "close_incident",
        condition=review_is_approved,
    )
    workflow.add_edge(
        "review_remediation",
        "refine_remediation",
        condition=review_requires_refinement,
    )
    workflow.add_edge("refine_remediation", "review_remediation")
    workflow.add_edge("close_incident", "notify_stakeholders")
    workflow.add_edge("close_incident", "create_postmortem")
    workflow.add_edge("notify_stakeholders", "archive_incident")
    workflow.add_edge("create_postmortem", "archive_incident")

    # ---------------------------------------------------------------------
    # INTENTIONAL ERROR NODES
    # These CapabilityRefs are deliberately not registered in build_app().
    # Compiler should report CAPABILITY_NOT_REGISTERED for both nodes.
    # ---------------------------------------------------------------------
    workflow.add_node(
        CapabilityRef(id="pager_delivery"),
        node_id="broken_pager_node",
        name="[ERROR] Unregistered pager capability",
    )
    workflow.add_node(
        CapabilityRef(id="compliance_ticket_writer"),
        node_id="broken_ticket_node",
        name="[ERROR] Unregistered compliance capability",
    )
    workflow.add_edge("archive_incident", "broken_pager_node")
    workflow.add_edge("archive_incident", "broken_ticket_node")

    # ---------------------------------------------------------------------
    # INTENTIONAL ERROR EDGES
    # 1. Unknown target creates a red missing-node placeholder in the preview.
    # 2. Unknown source creates another red placeholder and invalid edge.
    # 3. String conditions are reserved for YAML/UI but unsupported in V1.
    # 4. A loop may only have one external entry node. The normal entry is
    #    review_remediation; entering refine_remediation directly is forbidden.
    # 5. security_review -> assess_blast_radius is a cross-branch Map into a
    #    target that already has an incoming edge. V1 rejects Map plus fan-in.
    # ---------------------------------------------------------------------
    workflow.add_edge(
        "archive_incident",
        "missing_audit_sink",  # INTENTIONAL ERROR: target node does not exist.
        edge_id="error_missing_target",
    )
    workflow.add_edge(
        "ghost_manual_trigger",  # INTENTIONAL ERROR: source node does not exist.
        "collect_signal",
        edge_id="error_missing_source",
    )
    workflow.add_edge(
        "classify_incident",
        "archive_incident",
        edge_id="error_string_condition",
        condition="output.severity >= 5",  # INTENTIONAL ERROR: unsupported string.
    )
    workflow.add_edge(
        "classify_incident",
        "refine_remediation",
        # INTENTIONAL ERROR: second external entry into the review/refine loop.
        # No edge_id is supplied: Compiler generates
        # edge_classify_incident_refine_remediation automatically.
    )
    workflow.add_edge(
        "security_review",
        "assess_blast_radius",
        # INTENTIONAL ERROR: Map target already has classify_incident as an
        # incoming edge, so this violates the V1 Map/fan-in restriction.
        # This edge omits edge_id to demonstrate automatic ID generation.
        policy=EdgePolicy(
            map=MapPolicy(
                item_selector=select_action_items,
                max_parallelism=2,
            )
        ),
    )

    return workflow


def main() -> None:
    app = build_app()
    workflow = build_workflow()
    schema_registration_error = demonstrate_schema_registration_error(app)

    # The example contains intentional compiler errors, so preview it rather
    # than calling app.invoke(). Remove the marked block before execution.
    diagram = workflow.diagram(compiler=app.compiler)
    mermaid_path = app.preview(workflow, Path("workflow_preview.mmd"))

    print(f"Mermaid source: {mermaid_path}")
    print(
        f"Compiler result: {diagram.error_count} errors, "
        f"{diagram.warning_count} warnings"
    )
    print(f"Expected registration schema error: {schema_registration_error}")
    for diagnostic in diagram.diagnostics:
        subject = f" [{diagnostic.subject}]" if diagnostic.subject else ""
        print(
            f"- {diagnostic.severity.upper()} {diagnostic.code}{subject}: "
            f"{diagnostic.message}"
        )


if __name__ == "__main__":
    main()
