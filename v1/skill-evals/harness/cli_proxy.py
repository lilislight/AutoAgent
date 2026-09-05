#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from threading import Thread
import time
from typing import BinaryIO


_SECRET_FLAGS = {"--access-token", "--api-key", "--token"}


def main() -> int:
    evaluation_root = Path(__file__).resolve().parents[1]
    config = json.loads(
        (evaluation_root / "config.json").read_text(encoding="utf-8")
    )
    commands = evaluation_root / "commands"
    commands.mkdir(parents=True, exist_ok=True)
    sequence = _claim_sequence(commands)
    stem = f"{sequence:04d}"
    stdout_path = commands / f"{stem}.stdout.txt"
    stderr_path = commands / f"{stem}.stderr.txt"
    started_at_ms = time.time_ns() // 1_000_000
    started_ns = time.perf_counter_ns()
    raw_arguments = sys.argv[1:]
    command = [config["real_cli"], *raw_arguments]

    try:
        process = subprocess.Popen(
            command,
            cwd=Path.cwd(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as exc:
        stderr_path.write_text(f"{exc}\n", encoding="utf-8")
        print(exc, file=sys.stderr)
        return_code = 127
    else:
        assert process.stdout is not None
        assert process.stderr is not None
        with stdout_path.open("wb") as stdout_file, stderr_path.open(
            "wb"
        ) as stderr_file:
            stdout_thread = Thread(
                target=_pump,
                args=(process.stdout, sys.stdout.buffer, stdout_file),
                daemon=True,
            )
            stderr_thread = Thread(
                target=_pump,
                args=(process.stderr, sys.stderr.buffer, stderr_file),
                daemon=True,
            )
            stdout_thread.start()
            stderr_thread.start()
            try:
                return_code = process.wait()
            except KeyboardInterrupt:
                process.send_signal(2)
                return_code = process.wait()
            stdout_thread.join()
            stderr_thread.join()

    completed_at_ms = time.time_ns() // 1_000_000
    record = {
        "sequence": sequence,
        "started_at_ms": started_at_ms,
        "completed_at_ms": completed_at_ms,
        "duration_ms": max(0, (time.perf_counter_ns() - started_ns) // 1_000_000),
        "cwd": str(Path.cwd().resolve()),
        "arguments": _redact_arguments(raw_arguments),
        "exit_code": return_code,
        "stdout_file": str(stdout_path.relative_to(evaluation_root)),
        "stderr_file": str(stderr_path.relative_to(evaluation_root)),
        "stdout_sha256": _sha256(stdout_path),
        "stderr_sha256": _sha256(stderr_path),
    }
    record_path = commands / f"{stem}.json"
    payload = json.dumps(record, sort_keys=True, separators=(",", ":"))
    record_path.write_text(payload + "\n", encoding="utf-8")
    with (evaluation_root / "audit.jsonl").open("ab") as audit:
        audit.write(payload.encode("utf-8") + b"\n")
    return return_code


def _claim_sequence(commands: Path) -> int:
    for sequence in range(1, 100_000):
        claim = commands / f"{sequence:04d}.claim"
        try:
            descriptor = os.open(
                claim,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError:
            continue
        os.close(descriptor)
        return sequence
    raise RuntimeError("CLI audit sequence space is exhausted.")


def _pump(source: BinaryIO, target: BinaryIO, capture: BinaryIO) -> None:
    while True:
        chunk = source.read(64 * 1024)
        if not chunk:
            return
        capture.write(chunk)
        capture.flush()
        target.write(chunk)
        target.flush()


def _redact_arguments(arguments: list[str]) -> list[str]:
    values: list[str] = []
    redact_next = False
    for argument in arguments:
        if redact_next:
            values.append("<redacted>")
            redact_next = False
            continue
        flag, separator, _ = argument.partition("=")
        if flag in _SECRET_FLAGS:
            values.append(f"{flag}=<redacted>" if separator else flag)
            redact_next = not separator
        else:
            values.append(argument)
    return values


def _sha256(path: Path) -> str:
    if not path.exists():
        return hashlib.sha256(b"").hexdigest()
    digest = hashlib.sha256()
    with path.open("rb") as value:
        while chunk := value.read(64 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
