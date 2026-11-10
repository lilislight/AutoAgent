"""Deterministic CLI probes: no network, secrets, or file mutations.

From this demo's Workspace: python3 -u -m agent_demo.cli_probe info
"""

import argparse
import json
from pathlib import Path
import sys
import time


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("info", "count", "echo", "fail", "sleep", "approval", "deny"))
    parser.add_argument("--steps", type=int, default=4)
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("--seconds", type=float, default=5)
    args = parser.parse_args(argv)
    if not 1 <= args.steps <= 20 or not 0 <= args.interval <= 2 or not 0 <= args.seconds <= 60:
        parser.error("Probe limits: steps 1–20, interval 0–2, seconds 0–60.")
    if args.action == "info":
        print(json.dumps({"cwd": str(Path.cwd()), "python": sys.executable, "version": sys.version.split()[0]}))
    elif args.action == "count":
        for index in range(args.steps):
            print(f"STEP {index + 1}/{args.steps}", flush=True)
            time.sleep(args.interval)
    elif args.action == "echo":
        print("WAITING_FOR_INPUT", flush=True)
        print("ECHO: " + input(), flush=True)
    elif args.action == "fail":
        print("EXPECTED_FAILURE", file=sys.stderr, flush=True)
        return 3
    elif args.action == "sleep":
        print("SLEEP_STARTED", flush=True)
        time.sleep(args.seconds)
        print("SLEEP_FINISHED", flush=True)
    elif args.action == "approval":
        print("APPROVAL_OK", flush=True)
    elif args.action == "deny":
        print("DENY_PROBE_WAS_EXECUTED", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
