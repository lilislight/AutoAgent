from __future__ import annotations

import json
import random
import time
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from autoagent import (
    AutoAgentApp,
    AutoAgentServer,
    CapabilityRef,
    CapabilitySelectionPolicy,
    EdgePolicy,
    MapPolicy,
    NodePolicy,
    ReplicationPolicy,
    ResourcePolicy,
    RetryPolicy,
    DatabaseRuntimeStore,
    SystemCommand,
    TimeoutPolicy,
    Workflow,
)
from autoagent.core.runtime import OutputBindingContext, RuntimeStore


SECURITY_REVIEW_FAILURE_RATE = 0.4
RELIABILITY_REVIEW_TIMEOUT_MS = 120
RELIABILITY_REVIEW_PRIMARY_SLEEP_SECONDS = 0.35
RELIABILITY_REVIEW_CAPABILITY_ID = "reliability_review"
DEFAULT_RUNTIME_DATABASE = (
    Path(__file__).resolve().parent / ".autoagent" / "real-workflow.sqlite3"
)


class ServiceContext(BaseModel):
    service: str
    environment: Literal["development", "staging", "production"]
    region: str
    owner_team: str
    dependencies: list[str] = Field(default_factory=list)


class IncidentRequest(BaseModel):
    incident_id: str
    title: str
    severity: Literal["low", "medium", "high", "critical"]
    context: ServiceContext
    symptoms: list[str]
    tags: dict[str, str] = Field(default_factory=dict)
    requested_by: str


class ResumeRequest(BaseModel):
    incident: IncidentRequest
    previous_summary: str
    completed_steps: list[str]
    open_questions: list[str]
    resume_reason: str


class IncidentHistoryItem(BaseModel):
    event: str
    detail: str
    source: str


class RequestEnvelope(BaseModel):
    incident: IncidentRequest
    source: Literal["new", "resume"]
    history: list[IncidentHistoryItem]
    open_questions: list[str]


class NormalizedIncident(BaseModel):
    incident_id: str
    severity: Literal["low", "medium", "high", "critical"]
    service: ServiceContext
    problem_statement: str
    signals: list[str]
    risk_labels: set[str]
    history: list[IncidentHistoryItem]


class InvestigationTask(BaseModel):
    task_id: str
    goal: str
    data_sources: list[str]
    priority: int


class InvestigationPlan(BaseModel):
    incident: NormalizedIncident
    primary_hypothesis: str
    tasks: list[InvestigationTask]
    success_criteria: list[str]


class EvidenceItem(BaseModel):
    task_id: str
    source: str
    observations: list[str]
    metrics: dict[str, float]
    confidence: float


class InvestigationSynthesis(BaseModel):
    attempt: int
    summary: str
    suspected_causes: list[str]
    recommended_actions: list[str]
    evidence_task_ids: list[str]
    confidence: float


class QualityDecision(BaseModel):
    accepted: bool
    score: float
    feedback: list[str]
    reviewed_attempt: int


class InvestigationReport(BaseModel):
    incident: NormalizedIncident
    plan: InvestigationPlan
    evidence: list[EvidenceItem]
    synthesis: InvestigationSynthesis
    quality: QualityDecision


class AuditRequest(BaseModel):
    report: InvestigationReport
    requested_checks: list[str]


class AuditResult(BaseModel):
    incident_id: str
    passed_checks: list[str]
    warnings: list[str]
    audit_score: float


class ReviewRoute(BaseModel):
    security_required: bool
    reliability_required: bool
    rationale: list[str]


class SpecialistReview(BaseModel):
    domain: Literal["security", "reliability", "fast_track"]
    findings: list[str]
    recommendations: list[str]
    risk_score: float


class CompositeReview(BaseModel):
    report: InvestigationReport
    reviews: list[SpecialistReview]
    maximum_risk: float
    executive_summary: str


class FinalDecision(BaseModel):
    approved: bool
    requires_human: bool
    reason: str
    rollout_steps: list[str]
    rollback_conditions: list[str]


class PublishedResolution(BaseModel):
    incident_id: str
    status: Literal["resolved"]
    public_summary: str
    applied_actions: list[str]
    audit: dict[str, str]


class EscalationPacket(BaseModel):
    incident_id: str
    status: Literal["escalated"]
    reason: str
    required_approvers: list[str]
    evidence_summary: list[str]
    proposed_actions: list[str]


# Runtime values can outlive the Python process. These stable ids avoid using
# ``__main__`` when this example is started as ``python real_workflow.py``.
_RUNTIME_MODELS: tuple[type[BaseModel], ...] = (
    ServiceContext,
    IncidentRequest,
    ResumeRequest,
    IncidentHistoryItem,
    RequestEnvelope,
    NormalizedIncident,
    InvestigationTask,
    InvestigationPlan,
    EvidenceItem,
    InvestigationSynthesis,
    QualityDecision,
    InvestigationReport,
    AuditRequest,
    AuditResult,
    ReviewRoute,
    SpecialistReview,
    CompositeReview,
    FinalDecision,
    PublishedResolution,
    EscalationPacket,
)


def ingest_new_incident(request: IncidentRequest) -> RequestEnvelope:
    time.sleep(5)
    return RequestEnvelope(
        incident=request,
        source="new",
        history=[
            IncidentHistoryItem(
                event="incident_received",
                detail=request.title,
                source=request.requested_by,
            )
        ],
        open_questions=[],
    )


def resume_incident(checkpoint: ResumeRequest) -> RequestEnvelope:
    history = [
        IncidentHistoryItem(
            event="completed_step",
            detail=step,
            source="checkpoint",
        )
        for step in checkpoint.completed_steps
    ]
    history.append(
        IncidentHistoryItem(
            event="workflow_resumed",
            detail=checkpoint.previous_summary,
            source=checkpoint.resume_reason,
        )
    )
    return RequestEnvelope(
        incident=checkpoint.incident,
        source="resume",
        history=history,
        open_questions=checkpoint.open_questions,
    )


def record_manual_approval(
    approved: bool,
    reviewer: str = "unknown",
    note: str = "",
) -> dict[str, object]:
    return {
        "approved": approved,
        "reviewer": reviewer,
        "note": note,
    }


def normalize_incident(envelope: RequestEnvelope) -> NormalizedIncident:
    incident = envelope.incident
    labels = {incident.severity, incident.context.environment}
    labels.update(key for key, value in incident.tags.items() if value == "true")
    return NormalizedIncident(
        incident_id=incident.incident_id,
        severity=incident.severity,
        service=incident.context,
        problem_statement=f"{incident.title}: {'; '.join(incident.symptoms)}",
        signals=list(dict.fromkeys(incident.symptoms + envelope.open_questions)),
        risk_labels=labels,
        history=envelope.history,
    )


def mock_llm_plan_investigation(
    incident: NormalizedIncident,
) -> InvestigationPlan:
    """Deterministic stand-in for an LLM planning call."""

    tasks = [
        InvestigationTask(
            task_id="logs",
            goal="Correlate application errors with the incident start time.",
            data_sources=["application_logs", "error_tracker"],
            priority=1,
        ),
        InvestigationTask(
            task_id="metrics",
            goal="Compare latency, saturation, and dependency health.",
            data_sources=["service_metrics", "dependency_metrics"],
            priority=2,
        ),
        InvestigationTask(
            task_id="changes",
            goal="Identify deployments and configuration changes.",
            data_sources=["deployment_history", "configuration_audit"],
            priority=3,
        ),
    ]
    return InvestigationPlan(
        incident=incident,
        primary_hypothesis=(
            f"A recent change or dependency regression is affecting "
            f"{incident.service.service}."
        ),
        tasks=tasks,
        success_criteria=[
            "At least two independent signals support the primary cause.",
            "Recommended actions include measurable rollback conditions.",
        ],
    )


def select_investigation_tasks(
    plan: InvestigationPlan,
) -> list[dict[str, object]]:
    return [
        {"task": task, "incident": plan.incident}
        for task in sorted(plan.tasks, key=lambda item: item.priority)
    ]


def gather_evidence(
    task: InvestigationTask,
    incident: NormalizedIncident,
) -> EvidenceItem:
    confidence_by_task = {"logs": 0.86, "metrics": 0.79, "changes": 0.91}
    observations = [
        f"Completed {task.goal}",
        f"Sources checked: {', '.join(task.data_sources)}",
    ]
    if task.task_id == "changes":
        observations.append(
            f"A deployment preceded impact on {incident.service.service}."
        )
    return EvidenceItem(
        task_id=task.task_id,
        source=task.data_sources[0],
        observations=observations,
        metrics={"signal_strength": confidence_by_task[task.task_id]},
        confidence=confidence_by_task[task.task_id],
    )


def aggregate_evidence(outputs: list[EvidenceItem]) -> list[EvidenceItem]:
    return sorted(outputs, key=lambda item: item.task_id)


def mock_llm_synthesize_findings(
    plan: InvestigationPlan,
    evidence: list[EvidenceItem],
    attempt: int,
    previous_feedback: list[str],
) -> InvestigationSynthesis:
    """Deterministic stand-in for a looping LLM analysis call."""

    base_confidence = sum(item.confidence for item in evidence) / len(evidence)
    confidence = min(0.96, base_confidence + (0.08 if attempt > 1 else -0.12))
    actions = [
        "Pause the latest deployment.",
        "Shift traffic to healthy instances.",
        "Verify dependency latency before restoring traffic.",
    ]
    if previous_feedback:
        actions.append("Attach rollback metrics requested by the quality review.")
    return InvestigationSynthesis(
        attempt=attempt,
        summary=(
            f"Evidence supports the hypothesis: {plan.primary_hypothesis} "
            f"Analysis attempt {attempt}."
        ),
        suspected_causes=[
            "Latest deployment changed request behavior.",
            "Dependency latency amplified retry traffic.",
        ],
        recommended_actions=actions,
        evidence_task_ids=[item.task_id for item in evidence],
        confidence=round(confidence, 3),
    )


def evaluate_investigation_quality(
    synthesis: InvestigationSynthesis,
) -> QualityDecision:
    accepted = synthesis.attempt >= 2 and synthesis.confidence >= 0.8
    return QualityDecision(
        accepted=accepted,
        score=synthesis.confidence,
        feedback=(
            []
            if accepted
            else [
                "Correlate the deployment with dependency latency.",
                "Add explicit rollback verification metrics.",
            ]
        ),
        reviewed_attempt=synthesis.attempt,
    )


def finalize_investigation_report(
    plan: InvestigationPlan,
    evidence: list[EvidenceItem],
    synthesis: InvestigationSynthesis,
    quality: QualityDecision,
) -> InvestigationReport:
    return InvestigationReport(
        incident=plan.incident,
        plan=plan,
        evidence=evidence,
        synthesis=synthesis,
        quality=quality,
    )


def load_audit_request(request: AuditRequest) -> AuditRequest:
    return request


def audit_existing_report(request: AuditRequest) -> AuditResult:
    passed = [
        check
        for check in request.requested_checks
        if check in {"evidence", "rollback", "ownership"}
    ]
    return AuditResult(
        incident_id=request.report.incident.incident_id,
        passed_checks=passed,
        warnings=[] if len(passed) == len(request.requested_checks) else ["Unknown check"],
        audit_score=round(len(passed) / max(1, len(request.requested_checks)), 2),
    )


def mock_llm_route_reviews(report: InvestigationReport) -> ReviewRoute:
    """Deterministic stand-in for an LLM routing decision."""

    labels = report.incident.risk_labels
    security_required = bool(
        {"security", "active_exploit", "critical"}.intersection(labels)
    )
    reliability_required = report.incident.severity in {"high", "critical"}
    rationale = []
    if security_required:
        rationale.append("Security-sensitive labels require threat review.")
    if reliability_required:
        rationale.append("High severity requires reliability review.")
    if not rationale:
        rationale.append("Low-risk incident qualifies for fast-track review.")
    return ReviewRoute(
        security_required=security_required,
        reliability_required=reliability_required,
        rationale=rationale,
    )


def perform_security_review(report: InvestigationReport) -> SpecialistReview:
    if random.random() < SECURITY_REVIEW_FAILURE_RATE:
        raise RuntimeError(
            "Randomized security review failure for tracing demo."
        )
    active_exploit = "active_exploit" in report.incident.risk_labels
    return SpecialistReview(
        domain="security",
        findings=[
            "No credential exposure detected.",
            "Traffic shift must preserve audit logging.",
        ],
        recommendations=["Require security approval before restoring traffic."],
        risk_score=0.96 if active_exploit else 0.72,
    )


def slow_reliability_review(report: InvestigationReport) -> SpecialistReview:
    time.sleep(RELIABILITY_REVIEW_PRIMARY_SLEEP_SECONDS)
    return perform_reliability_review(report)


def perform_reliability_review(report: InvestigationReport) -> SpecialistReview:
    return SpecialistReview(
        domain="reliability",
        findings=[
            f"Confidence is {report.synthesis.confidence:.2f}.",
            "Rollback metrics cover latency and error rate.",
        ],
        recommendations=["Use a staged traffic restoration."],
        risk_score=0.74 if report.incident.severity == "critical" else 0.58,
    )


def perform_fast_track_review(report: InvestigationReport) -> SpecialistReview:
    return SpecialistReview(
        domain="fast_track",
        findings=["The investigation meets low-risk publication requirements."],
        recommendations=["Publish the resolution without additional approval."],
        risk_score=max(0.2, 1.0 - report.synthesis.confidence),
    )


def merge_specialist_reviews(
    report: InvestigationReport,
    reviews: list[SpecialistReview],
) -> CompositeReview:
    maximum_risk = max((review.risk_score for review in reviews), default=0.0)
    domains = ", ".join(review.domain for review in reviews)
    return CompositeReview(
        report=report,
        reviews=reviews,
        maximum_risk=maximum_risk,
        executive_summary=(
            f"Investigation confidence {report.synthesis.confidence:.2f}; "
            f"completed reviews: {domains}."
        ),
    )


def mock_llm_make_final_decision(review: CompositeReview) -> FinalDecision:
    """Deterministic stand-in for an LLM approval decision."""

    approved = review.maximum_risk < 0.9 and review.report.quality.accepted
    return FinalDecision(
        approved=approved,
        requires_human=not approved,
        reason=(
            "Automated evidence and specialist reviews are within risk limits."
            if approved
            else "Risk exceeds the automated approval threshold."
        ),
        rollout_steps=review.report.synthesis.recommended_actions,
        rollback_conditions=[
            "Error rate exceeds 2%.",
            "P95 latency exceeds 500 ms.",
        ],
    )


def select_conservative_decision(outputs: list[FinalDecision]) -> FinalDecision:
    return max(
        outputs,
        key=lambda decision: (
            decision.requires_human,
            not decision.approved,
            len(decision.rollback_conditions),
        ),
    )


def publish_resolution(
    review: CompositeReview,
    decision: FinalDecision,
) -> PublishedResolution:
    return PublishedResolution(
        incident_id=review.report.incident.incident_id,
        status="resolved",
        public_summary=review.executive_summary,
        applied_actions=decision.rollout_steps,
        audit={
            "quality_score": str(review.report.quality.score),
            "maximum_risk": str(review.maximum_risk),
        },
    )


def bind_normalized_incident_context(context: OutputBindingContext) -> None:
    """Expose the normalized incident as Invocation-scoped trace data."""

    incident = context.output
    context.invocation_context.data["normalized_incident"] = {
        "incident_id": incident.incident_id,
        "severity": incident.severity,
        "service": incident.service.service,
        "environment": incident.service.environment,
        "risk_labels": sorted(incident.risk_labels),
    }
    context.invocation_context.metadata["normalized_by"] = context.node_id


def bind_final_decision_context(context: OutputBindingContext) -> None:
    """Persist the latest decision for the next Invocation in this Session."""

    decision = context.output
    context.session_context.data["latest_incident_decision"] = {
        "approved": decision.approved,
        "requires_human": decision.requires_human,
        "reason": decision.reason,
        "rollout_steps": list(decision.rollout_steps),
    }
    context.session_context.metadata["updated_by"] = context.node_id


def create_escalation_packet(
    review: CompositeReview,
    decision: FinalDecision,
) -> EscalationPacket:
    return EscalationPacket(
        incident_id=review.report.incident.incident_id,
        status="escalated",
        reason=decision.reason,
        required_approvers=["incident_commander", "security_lead"],
        evidence_summary=review.report.synthesis.suspected_causes,
        proposed_actions=decision.rollout_steps,
    )


def build_investigation_workflow() -> Workflow:
    """Build a reusable child Workflow with map, loop, and two entry/exit pairs."""

    workflow = Workflow(
        id="incident_investigation",
        version=1,
        name="Incident investigation",
    )
    workflow.add_node(
        mock_llm_plan_investigation,
        node_id="plan_investigation",
        input_mapping=lambda ctx: {
            "incident": (
                ctx.incoming[0].value
                if ctx.incoming
                else ctx.invocation_input["incident"]
            )
        },
    )
    workflow.add_node(gather_evidence, node_id="gather_evidence")
    workflow.add_node(
        mock_llm_synthesize_findings,
        node_id="synthesize_findings",
        input_mapping=lambda ctx: {
            "plan": ctx.outputs.latest("plan_investigation"),
            # OutputContext exposes mutable containers as immutable snapshots.
            # Convert the map result back to the operator's declared list input.
            "evidence": list(ctx.outputs.latest("gather_evidence")),
            "attempt": len(ctx.outputs.all("synthesize_findings")) + 1,
            "previous_feedback": (
                ctx.outputs.latest("quality_gate").feedback
                if ctx.outputs.has("quality_gate")
                else []
            ),
        },
        policy=NodePolicy(
            timeout=TimeoutPolicy(timeout_ms=5_000),
            resource=ResourcePolicy(
                max_node_executions_per_invocation=3,
                max_operator_calls_per_invocation=3,
                max_runtime_ms_per_invocation=10_000,
            ),
        ),
    )
    workflow.add_node(
        evaluate_investigation_quality,
        node_id="quality_gate",
        input_mapping=lambda ctx: {
            "synthesis": ctx.outputs.latest("synthesize_findings")
        },
    )
    workflow.add_node(
        finalize_investigation_report,
        node_id="finalize_report",
        input_mapping=lambda ctx: {
            "plan": ctx.outputs.latest("plan_investigation"),
            "evidence": list(ctx.outputs.latest("gather_evidence")),
            "synthesis": ctx.outputs.latest("synthesize_findings"),
            "quality": ctx.outputs.latest("quality_gate"),
        },
    )

    # Independent audit entry/exit pair makes this a genuine multi-entry and
    # multi-exit child Workflow. The parent explicitly selects the investigation
    # pair, so this disconnected audit component is not expanded into the parent.
    workflow.add_node(load_audit_request, node_id="audit_entry")
    workflow.add_node(
        audit_existing_report,
        node_id="audit_exit",
        input_mapping=lambda ctx: {"request": ctx.outputs.latest("audit_entry")},
    )

    workflow.add_edge(
        "plan_investigation",
        "gather_evidence",
        edge_id="map_evidence_tasks",
        policy=EdgePolicy(
            map=MapPolicy(
                item_selector=select_investigation_tasks,
                output_aggregator=aggregate_evidence,
                max_parallelism=3,
            )
        ),
    )
    workflow.add_edge("gather_evidence", "synthesize_findings")
    workflow.add_edge("synthesize_findings", "quality_gate")
    workflow.add_edge(
        "quality_gate",
        "synthesize_findings",
        edge_id="retry_investigation",
        condition=lambda ctx: not ctx.source_output.accepted,
    )
    workflow.add_edge(
        "quality_gate",
        "finalize_report",
        edge_id="accept_investigation",
        condition=lambda ctx: ctx.source_output.accepted,
    )
    workflow.add_edge("audit_entry", "audit_exit")
    return workflow


def build_incident_response_workflow() -> Workflow:
    """Build the top-level production incident response Workflow."""

    investigation = build_investigation_workflow()
    workflow = Workflow(
        id="production_incident_response_v1",
        version=1,
        name="Production incident response",
        description=(
            "Normalize an incident, investigate it, perform parallel specialist "
            "reviews, and either publish or escalate the resolution."
        ),
    )

    workflow.add_node(ingest_new_incident, node_id="new_incident")
    workflow.add_node(resume_incident, node_id="resume_incident")
    workflow.add_node(
        SystemCommand(id="wait"),
        node_id="manual_approval_wait",
        name="Manual approval wait",
        description=(
            "Independent wait entry used to demonstrate UI-driven resume. "
            "It is not connected to the normal incident response paths."
        ),
    )
    workflow.add_node(
        record_manual_approval,
        node_id="manual_approval_result",
        input_mapping=lambda ctx: dict(ctx.outputs.latest("manual_approval_wait")),
    )
    workflow.add_node(
        normalize_incident,
        node_id="normalize_incident",
        input_mapping=lambda ctx: {
            "envelope": (
                ctx.outputs.latest("new_incident")
                if ctx.outputs.has("new_incident")
                else ctx.outputs.latest("resume_incident")
            )
        },
        output_binding=bind_normalized_incident_context,
    )
    workflow.add_node(
        investigation,
        node_id="investigation",
        child_entry_node_id="plan_investigation",
        child_exit_node_id="finalize_report",
    )
    workflow.add_node(
        mock_llm_route_reviews,
        node_id="route_reviews",
        input_mapping=lambda ctx: {
            "report": ctx.outputs.latest("investigation")
        },
    )
    workflow.add_node(
        perform_security_review,
        node_id="security_review",
        input_mapping=lambda ctx: {
            "report": ctx.outputs.latest("investigation")
        },
        policy=NodePolicy(
            retry=RetryPolicy(max_attempts=2),
        ),
    )
    workflow.add_node(
        CapabilityRef(id=RELIABILITY_REVIEW_CAPABILITY_ID),
        node_id="reliability_review",
        input_mapping=lambda ctx: {
            "report": ctx.outputs.latest("investigation")
        },
        policy=NodePolicy(
            selection=CapabilitySelectionPolicy(allow_fallback=True),
            timeout=TimeoutPolicy(timeout_ms=RELIABILITY_REVIEW_TIMEOUT_MS),
        ),
    )
    workflow.add_node(
        perform_fast_track_review,
        node_id="fast_track_review",
        input_mapping=lambda ctx: {
            "report": ctx.outputs.latest("investigation")
        },
    )
    workflow.add_node(
        merge_specialist_reviews,
        node_id="merge_reviews",
        input_mapping=lambda ctx: {
            "report": ctx.outputs.latest("investigation"),
            "reviews": [
                ctx.outputs.latest(node_id)
                for node_id in (
                    "security_review",
                    "reliability_review",
                    "fast_track_review",
                )
                if ctx.outputs.has(node_id)
            ],
        },
    )
    workflow.add_node(
        mock_llm_make_final_decision,
        node_id="final_decision",
        input_mapping=lambda ctx: {"review": ctx.outputs.latest("merge_reviews")},
        output_binding=bind_final_decision_context,
        policy=NodePolicy(
            replication=ReplicationPolicy(
                count=3,
                output_aggregator=select_conservative_decision,
                max_parallelism=3,
            )
        ),
    )
    workflow.add_node(
        publish_resolution,
        node_id="publish_resolution",
        input_mapping=lambda ctx: {
            "review": ctx.outputs.latest("merge_reviews"),
            "decision": ctx.outputs.latest("final_decision"),
        },
    )
    workflow.add_node(
        create_escalation_packet,
        node_id="escalate_incident",
        input_mapping=lambda ctx: {
            "review": ctx.outputs.latest("merge_reviews"),
            "decision": ctx.outputs.latest("final_decision"),
        },
    )

    workflow.add_edge("new_incident", "normalize_incident")
    workflow.add_edge("resume_incident", "normalize_incident")
    workflow.add_edge("manual_approval_wait", "manual_approval_result")
    workflow.add_edge("normalize_incident", "investigation")
    workflow.add_edge("investigation", "route_reviews")
    workflow.add_edge(
        "route_reviews",
        "security_review",
        condition=lambda ctx: ctx.source_output.security_required,
    )
    workflow.add_edge(
        "route_reviews",
        "reliability_review",
        condition=lambda ctx: ctx.source_output.reliability_required,
    )
    workflow.add_edge(
        "route_reviews",
        "fast_track_review",
        condition=lambda ctx: not (
            ctx.source_output.security_required
            or ctx.source_output.reliability_required
        ),
    )
    workflow.add_edge("security_review", "merge_reviews")
    workflow.add_edge("reliability_review", "merge_reviews")
    workflow.add_edge("fast_track_review", "merge_reviews")
    workflow.add_edge("merge_reviews", "final_decision")
    workflow.add_edge(
        "final_decision",
        "publish_resolution",
        condition=lambda ctx: ctx.source_output.approved,
    )
    workflow.add_edge(
        "final_decision",
        "escalate_incident",
        condition=lambda ctx: not ctx.source_output.approved,
    )
    return workflow


def build_incident_response_app(
    *,
    runtime_store: RuntimeStore | None = None,
    database_path: str | Path | None = None,
) -> tuple[AutoAgentApp, Workflow]:
    """Create the demo App with runtime-resolved Operators registered.

    The reliability review node intentionally uses a CapabilityRef so the UI can
    show a primary OperatorCall timing out and a fallback OperatorCall finishing
    successfully inside the same NodeExecution. By default the example stores
    durable runtime data in ``.autoagent/real-workflow.sqlite3`` so the tracing
    server and UI can inspect historical invocations across restarts.
    """

    if runtime_store is not None and database_path is not None:
        raise ValueError("Pass either runtime_store or database_path, not both.")
    if runtime_store is None:
        resolved_database_path = Path(database_path or DEFAULT_RUNTIME_DATABASE)
        resolved_database_path.parent.mkdir(parents=True, exist_ok=True)
        runtime_store = DatabaseRuntimeStore.from_path(resolved_database_path)

    app = AutoAgentApp(namespace="real-workflow", runtime_store=runtime_store)
    for model_type in _RUNTIME_MODELS:
        stable_type_id = f"real_workflow:{model_type.__qualname__}"
        app.register_runtime_model(model_type, type_id=stable_type_id)
        # The first version of this example was executed as a script and wrote
        # ``__main__`` ids. Keep this alias so its current local demo database
        # can open once; all new writes use the stable id above.
        app.register_runtime_model(
            model_type,
            type_id=f"__main__:{model_type.__qualname__}",
        )
    app.register_capability(
        RELIABILITY_REVIEW_CAPABILITY_ID,
        description="Review incident mitigation from a reliability perspective.",
    )
    app.register_operator(
        slow_reliability_review,
        operator_id="slow_reliability_review",
        capability_id=RELIABILITY_REVIEW_CAPABILITY_ID,
        default=True,
    )
    app.register_operator(
        perform_reliability_review,
        operator_id="reliability_review_fallback",
        capability_id=RELIABILITY_REVIEW_CAPABILITY_ID,
    )
    workflow = build_incident_response_workflow()
    app.register_workflow(workflow)
    return app, workflow


def _new_incident_sample() -> IncidentRequest:
    return IncidentRequest(
        incident_id="INC-2048",
        title="Checkout API latency and elevated errors",
        severity="high",
        context=ServiceContext(
            service="checkout-api",
            environment="production",
            region="us-east-1",
            owner_team="commerce-platform",
            dependencies=["payments-api", "inventory-api"],
        ),
        symptoms=[
            "P95 latency increased from 180 ms to 900 ms",
            "HTTP 5xx rate reached 4.2%",
        ],
        tags={"security": "true", "customer_impact": "true"},
        requested_by="on-call-engineer",
    )


def _resume_incident_sample() -> ResumeRequest:
    incident = IncidentRequest(
        incident_id="INC-4096",
        title="Authentication failures during active exploit investigation",
        severity="critical",
        context=ServiceContext(
            service="identity-api",
            environment="production",
            region="eu-west-1",
            owner_team="identity-platform",
            dependencies=["token-service", "user-directory"],
        ),
        symptoms=[
            "Token validation failures increased sharply",
            "Unusual request signatures detected",
        ],
        tags={"security": "true", "active_exploit": "true"},
        requested_by="security-operations",
    )
    return ResumeRequest(
        incident=incident,
        previous_summary="Initial containment is complete; root cause is unconfirmed.",
        completed_steps=["Blocked suspicious source ranges", "Rotated signing key"],
        open_questions=["Did the latest identity deployment widen token acceptance?"],
        resume_reason="security_handoff",
    )


def _print_result(label: str, invocation) -> None:
    if invocation.state != "completed":
        error = (
            invocation.error.to_record()
            if invocation.error is not None
            else {"message": "Unknown invocation failure."}
        )
        print(f"\n{label} failed")
        print(json.dumps(error, indent=2, sort_keys=True))
        print("Executed nodes:")
        print(" -> ".join(item.node_id for item in invocation.node_executions))
        return
    output = invocation.result["output"]
    payload = (
        output.model_dump(mode="json")
        if isinstance(output, BaseModel)
        else output
    )
    print(f"\n{label}")
    print(json.dumps(payload, indent=2, sort_keys=True))
    print("Executed nodes:")
    print(" -> ".join(item.node_id for item in invocation.node_executions))


def main() -> None:
    app, workflow = build_incident_response_app()

    compile_result = app.compiler.compile(workflow)
    if not compile_result.ok:
        diagnostics = [
            item.model_dump(mode="json")
            for item in compile_result.diagnostics
        ]
        raise RuntimeError(json.dumps(diagnostics, indent=2))

    print("Compiled entries:", compile_result.workflow_ir.entry_node_ids)
    print("Compiled exits:", compile_result.workflow_ir.exit_node_ids)
    print("Expanded node count:", len(compile_result.workflow_ir.nodes))

    new_invocation = app.invoke(
        workflow,
        input={"request": _new_incident_sample()},
        entry_node_id="new_incident",
        session_id="incident-new-example",
    )
    _print_result("New incident path", new_invocation)

    resumed_invocation = app.invoke(
        workflow,
        input={"checkpoint": _resume_incident_sample()},
        entry_node_id="resume_incident",
        session_id="incident-resume-example",
    )
    _print_result("Resumed incident path", resumed_invocation)

    print("\nAutoAgentServer is serving this workflow at http://0.0.0.0:8765")
    print("Start the tracing UI with:")
    print(
        "cd /home/chengqian/projects/AutoAgent/ui && "
        "AUTOAGENT_SERVER_URL=http://127.0.0.1:8765 npm run dev -- --host 0.0.0.0"
    )
    AutoAgentServer(app).run(host="0.0.0.0", port=8765)


if __name__ == "__main__":
    main()
