"""Lookup nested Command calls without admitting them as Map unit outputs."""

from types import SimpleNamespace
from urllib.parse import unquote


def runtime_call(invocation, call_id):
    call = invocation.scheduler.operator_calls.get(call_id)
    if call is not None:
        return call
    if not call_id.startswith("inline:"):
        return None
    occurrence_id = unquote(call_id.split(":", 2)[1])
    occurrence = invocation.scheduler.occurrences.get(occurrence_id)
    record = occurrence.execution.inline_commands.get(call_id) if occurrence is not None else None
    if record is None:
        # Terminal recovery may rename the owning occurrence while retaining
        # the accepted effect's identity and receipt.
        for occurrence in invocation.scheduler.occurrences.values():
            record = occurrence.execution.inline_commands.get(call_id)
            if record is not None:
                break
    if record is None:
        return None
    return SimpleNamespace(**record, occurrence_id=occurrence.id)
