from __future__ import annotations

from typing import Any

from autoagent.debug.models import InvocationRerunResult


def build_rerun_result(admitted: Any, invocation: Any) -> InvocationRerunResult:
    """Build the stable public result from one internal App admission."""

    session = admitted.prepared.session
    session_key = session.session_key
    if session_key is None:
        raise RuntimeError("Rerun Session has no external key.")
    return InvocationRerunResult(
        source_invocation_id=str(admitted.source_invocation_id),
        source_workflow_revision_id=admitted.source_workflow_revision_id,
        candidate_invocation_id=str(invocation.id),
        candidate_workflow_revision_id=invocation.workflow_revision_id,
        workflow_id=invocation.workflow_id,
        session_id=str(session.id),
        session_key=session_key,
        state=invocation.state,
        event_mode=invocation.event_mode,
    )
