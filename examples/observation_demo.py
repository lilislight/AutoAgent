"""Seed a realistic trace and start the local AutoAgent Server API."""

from __future__ import annotations

import time

from autoagent import AutoAgentApp, AutoAgentServer
from autoagent.workflow import Workflow


def receive_incident(summary: str, severity: int) -> dict[str, object]:
    time.sleep(0.02)
    return {"summary": summary, "severity": severity}


def assess_incident(summary: str, severity: int) -> dict[str, object]:
    time.sleep(0.04)
    route = "remediate" if severity >= 4 else "notify" if severity >= 2 else "archive"
    return {"summary": summary, "severity": severity, "route": route}


def remediate(summary: str, severity: int, route: str) -> dict[str, str]:
    del severity, route
    time.sleep(0.08)
    return {"result": f"Mitigation started for {summary}"}


def notify(summary: str, severity: int, route: str) -> dict[str, str]:
    del severity, route
    time.sleep(0.05)
    return {"result": f"On-call notified about {summary}"}


def archive(summary: str, severity: int, route: str) -> dict[str, str]:
    del severity, route
    time.sleep(0.01)
    return {"result": f"Archived low-severity report: {summary}"}


def close_incident(result: str) -> str:
    time.sleep(0.02)
    return result


def build_workflow() -> Workflow:
    workflow = Workflow(
        id="incident_triage",
        version=1,
        name="Incident triage",
        description="Route incidents by severity and record the selected response path.",
    )
    workflow.add_node(receive_incident, node_id="receive")
    workflow.add_node(assess_incident, node_id="assess")
    workflow.add_node(remediate, node_id="remediate")
    workflow.add_node(notify, node_id="notify")
    workflow.add_node(archive, node_id="archive")
    workflow.add_node(close_incident, node_id="close")
    workflow.add_edge("receive", "assess")
    workflow.add_edge(
        "assess",
        "remediate",
        condition=lambda ctx: ctx.source_output["route"] == "remediate",
    )
    workflow.add_edge(
        "assess",
        "notify",
        condition=lambda ctx: ctx.source_output["route"] == "notify",
    )
    workflow.add_edge(
        "assess",
        "archive",
        condition=lambda ctx: ctx.source_output["route"] == "archive",
    )
    workflow.add_edge("remediate", "close")
    workflow.add_edge("notify", "close")
    workflow.add_edge("archive", "close")
    return workflow


def main() -> None:
    workflow = build_workflow()
    app = AutoAgentApp()
    app.invoke(
        workflow,
        input={"summary": "Checkout latency spike", "severity": 5},
        session_id="operations",
    )
    app.invoke(
        workflow,
        input={"summary": "Elevated API error rate", "severity": 3},
        session_id="operations",
    )
    app.invoke(
        workflow,
        input={"summary": "Single failed health check", "severity": 1},
        session_id="support",
    )
    app.register_workflow(workflow)
    AutoAgentServer(app).run(host="0.0.0.0", port=8765)


if __name__ == "__main__":
    main()
