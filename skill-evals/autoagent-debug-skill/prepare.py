"""Copy a debug task and create its original SQLite execution history."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess


def prepare(case, workspace, cli):
    fixtures = Path(__file__).resolve().parent
    skill = fixtures.parents[1] / "skills" / "autoagent-debug-skill"
    shutil.copytree(fixtures / case, workspace, ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copytree(skill, workspace / skill.name)
    (workspace / ".env").write_text(
        "AUTOAGENT_DATABASE_URL=sqlite+aiosqlite:///.autoagent/incident.db\n",
        encoding="utf-8",
    )
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith("AUTOAGENT_") and key != "PYTHONPATH"
    }

    def run(*arguments):
        result = subprocess.run(
            [cli, *arguments, "--format", "json"],
            cwd=workspace,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=60,
        )
        if result.returncode:
            raise RuntimeError(result.stdout or result.stderr)
        return json.loads(result.stdout)["data"]

    run("project", "check")
    if case == "invocation":
        waiting = run(
            "invoke", "quote-review", "--entry-node", "quote",
            "--input-json", '{"quote_id":"Q-1","total_cents":12500}', "--wait", "boundary",
        )
        identity = waiting["invocation_id"]
        wait, = waiting["waits"]
        finished = run(
            "invocation", "respond", identity, "--wait-id", wait["wait_id"],
            "--input-json", '{"approved":true}', "--wait", "terminal",
        )
    else:
        initial = run(
            "invoke", "set-expense-limit", "--entry-node", "save",
            "--input-json", '{"limit_cents":12500}', "--wait", "terminal",
        )
        identity = initial["session_id"]
        finished = run(
            "invoke", "classify-expense", "--entry-node", "classify",
            "--session-mode", "existing", "--session-id", identity,
            "--input-json", '{"amount_cents":12400}', "--wait", "terminal",
        )
    if finished["status"] != "completed":
        raise RuntimeError(f"Incident seed did not finish: {finished}")
    requirements = workspace / "REQUIREMENTS.md"
    requirements.write_text(
        requirements.read_text(encoding="utf-8").replace("INCIDENT_ID", identity),
        encoding="utf-8",
    )
    print(f"Prepared {case} task in {workspace}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("case", choices=("invocation", "session"))
    parser.add_argument("workspace", type=Path)
    parser.add_argument("--cli", required=True, type=Path)
    arguments = parser.parse_args()
    prepare(arguments.case, arguments.workspace.resolve(), arguments.cli.resolve())


if __name__ == "__main__":
    main()
