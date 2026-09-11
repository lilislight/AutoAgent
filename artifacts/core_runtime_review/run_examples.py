"""Run the existing Core example and export complete, replay-verified Events.

Run from the repository root:
    .venv/bin/python artifacts/core_runtime_review/run_examples.py
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from autoagent import AutoAgentApp
from autoagent.core.runtime import RuntimeEvent, StateReducer
from examples import core_workflow_api_demo as demo

OUT = Path(__file__).resolve().parent


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


class EventRecorder:
    def __init__(self, directory):
        self.directory = directory
        directory.mkdir(exist_ok=True)
        self.events = []
        self.by_session = defaultdict(list)
        self.ids = {}
        self.checks = []
        self.file = (directory / "runtime_events.jsonl").open("w", encoding="utf-8")

    async def append(self, event):
        record = event.to_record()
        if event.id in self.ids:
            assert self.ids[event.id] == record, "Conflicting duplicate Event"
            return
        assert event.sequence == len(self.by_session[event.session_id]) + 1
        assert RuntimeEvent.from_record(json.loads(json.dumps(record))) == event
        self.file.write(json.dumps(record, ensure_ascii=False) + "\n")
        self.file.flush()
        os.fsync(self.file.fileno())
        self.ids[event.id] = record
        self.events.append(event)
        self.by_session[event.session_id].append(event)

    def verify_checkpoint(self, checkpoint):
        record = checkpoint.to_record()
        sid = checkpoint.session_id
        events = self.by_session[sid]
        prefix = [event for event in events if event.sequence <= checkpoint.sequence]
        replay = StateReducer().reduce(prefix)
        assert replay == checkpoint.state, f"Checkpoint replay mismatch: {sid}"
        self.checks.append({"session_id": sid, "sequence": checkpoint.sequence, "checkpoint_equals_replay": True})
        write_json(self.directory / f"state-{sid}-{checkpoint.sequence}.json", replay.to_record())

    def finish(self):
        self.file.close()
        for sid, events in self.by_session.items():
            for end in range(1, len(events) + 1):
                # reduce also performs full Runtime State validation.
                state = StateReducer().reduce(events[:end])
                assert state.sequence == end
            write_json(self.directory / f"events-{sid}.json", [event.to_record() for event in events])
        write_json(self.directory / "runtime_events.json", [event.to_record() for event in self.events])
        lines = ["Runtime Events in sink arrival order; sequence is ordered per Session.", ""]
        for index, event in enumerate(self.events, 1):
            payload = event.to_record()["payload"]
            detail = {key: value for key, value in payload.items() if key != "kind"}
            count = len(event.delta.operations) if event.delta else 0
            lines.append(f"{index:03d} [{event.session_id} #{event.sequence:03d}] {event.event_name}  delta_ops={count}")
            lines.append("    " + json.dumps(detail, ensure_ascii=False))
        (self.directory / "timeline.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
        return {
            "events": len(self.events),
            "sessions": {sid: len(events) for sid, events in self.by_session.items()},
            "event_types": dict(Counter(event.event_name for event in self.events)),
            "all_prefixes_valid": True,
            "all_records_roundtrip": True,
            "checkpoint_checks": self.checks,
        }


def app_type(recorder):
    class RecordedApp(AutoAgentApp):
        def __init__(self, **kwargs):
            super().__init__(runtime_event_sink=recorder, **kwargs)

        def unload_session(self, ref):
            checkpoint = super().unload_session(ref)
            recorder.verify_checkpoint(checkpoint)
            return checkpoint

        def close(self, timeout=30.0):
            checkpoint = super().close(timeout)
            # The bundle contains every Session still resident at close.
            for session in checkpoint.sessions:
                recorder.verify_checkpoint(session)
            return checkpoint
    return RecordedApp


def original_example():
    recorder = EventRecorder(OUT / "approved_recovery_child")
    original_app = demo.AutoAgentApp
    demo.AutoAgentApp = app_type(recorder)
    demo.PARENT_CHECKPOINT_PATH = recorder.directory / "parent_checkpoint.json"
    demo.CHILD_CHECKPOINT_PATH = recorder.directory / "child_checkpoint.json"
    try:
        demo.main()
        return recorder.finish()
    finally:
        demo.AutoAgentApp = original_app
        recorder.file.close()


def alternate_branch(name, approved, amount, expected_terminal):
    recorder = EventRecorder(OUT / name)
    app = app_type(recorder)(max_operator_concurrency=4)
    try:
        workflow = demo.build_workflow()
        waiting = app.invoke(workflow, {"order_id": name, "amount": amount, "stock": 3}, session_id=name)
        assert waiting.status == "waiting", waiting.error
        result = app.resume(waiting.ref, waiting.waits[0].id, {"approved": approved})
        assert result.status == "completed", result.error
        assert not app.child_invocations(result.ref)
        completed = [event.payload.occurrence_id for event in recorder.events if event.event_name == "node_occurrence.completed"]
        assert any(item.startswith(expected_terminal + "@") for item in completed), completed
        write_json(recorder.directory / "result.json", {"status": result.status, "output": result.output})
        print(f"[{name}] completed: {result.output}")
    finally:
        app.close()
    return recorder.finish()


def main():
    report = {"approved_recovery_child": original_example()}
    report["approval_rejected"] = alternate_branch("approval_rejected", False, 600, "approval_rejected")
    report["manual_review"] = alternate_branch("manual_review", True, 1600, "manual_review")
    write_json(OUT / "example_verification.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
