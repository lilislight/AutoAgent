"""PostgreSQL test configuration and local cluster control.

Run .venv/bin/python -m tests.postgres to start the installed local cluster and
run the complete V2 suite with PostgreSQL enabled. Explicit
AUTOAGENT_TEST_POSTGRES_URL takes precedence over the ignored local test-url.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
import subprocess
import unittest
from uuid import uuid4


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_CLUSTER = PROJECT_ROOT / ".autoagent" / "postgres"


def configured_url():
    value = os.environ.get("AUTOAGENT_TEST_POSTGRES_URL")
    if value:
        return value
    path = LOCAL_CLUSTER / "test-url"
    if not path.exists():
        return None
    value = path.read_text().strip()
    if not value:
        raise ValueError("The local PostgreSQL test-url is empty.")
    return value


POSTGRES_TEST_URL = configured_url()


class PostgreSQLDatabase:
    """Create and remove one disposable database using the configured admin URL."""

    def __init__(self):
        self.name = "autoagent_test_" + uuid4().hex
        self.created = False

    async def create(self):
        import asyncpg
        from sqlalchemy.engine import make_url

        if not POSTGRES_TEST_URL:
            raise RuntimeError("PostgreSQL tests require AUTOAGENT_TEST_POSTGRES_URL or a local installation.")
        self.admin = make_url(POSTGRES_TEST_URL)
        connection = await asyncpg.connect(self.admin.set(drivername="postgresql").render_as_string(hide_password=False))
        try:
            await connection.execute('CREATE DATABASE "' + self.name + '"')
            self.created = True
        finally:
            await connection.close()
        return self.admin.set(drivername="postgresql+asyncpg", database=self.name).render_as_string(hide_password=False)

    async def close(self):
        if not self.created:
            return
        import asyncpg

        connection = await asyncpg.connect(self.admin.set(drivername="postgresql").render_as_string(hide_password=False))
        try:
            await connection.execute('DROP DATABASE "' + self.name + '" WITH (FORCE)')
            self.created = False
        finally:
            await connection.close()


def control(command):
    candidates = sorted((LOCAL_CLUSTER / "install/usr/lib/postgresql").glob("*/bin/pg_ctl"))
    if not candidates or not (LOCAL_CLUSTER / "data/PG_VERSION").is_file():
        raise RuntimeError("No local PostgreSQL test installation is available.")
    executable = str(candidates[-1])
    environment = dict(os.environ)
    libraries = str(LOCAL_CLUSTER / "install/usr/lib/x86_64-linux-gnu")
    environment["LD_LIBRARY_PATH"] = libraries + (":" + environment["LD_LIBRARY_PATH"] if environment.get("LD_LIBRARY_PATH") else "")
    arguments = [executable, "-D", str(LOCAL_CLUSTER / "data")]
    if command == "start":
        state = subprocess.run(arguments + ["status"], env=environment, stdout=subprocess.DEVNULL, check=False)
        if state.returncode == 0:
            return
        if state.returncode != 3:
            raise RuntimeError("Cannot determine local PostgreSQL status.")
        arguments += ["-l", str(LOCAL_CLUSTER / "server.log"), "-w", "start"]
    elif command == "stop":
        arguments += ["-m", "fast", "-w", "stop"]
    else:
        arguments += ["status"]
    subprocess.run(arguments, env=environment, check=True)


async def check_connection():
    import asyncpg
    from sqlalchemy.engine import make_url

    if not POSTGRES_TEST_URL:
        raise RuntimeError("PostgreSQL tests are required: configure AUTOAGENT_TEST_POSTGRES_URL or install the local cluster.")
    connection = await asyncpg.connect(make_url(POSTGRES_TEST_URL).set(drivername="postgresql").render_as_string(hide_password=False))
    try:
        print("PostgreSQL tests enabled:", await connection.fetchval("SELECT version()"), flush=True)
    finally:
        await connection.close()


def postgres_cases(suite):
    for case in suite:
        if isinstance(case, unittest.TestSuite):
            yield from postgres_cases(case)
        elif type(case).__name__.startswith("PostgreSQL"):
            yield case


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", nargs="?", choices=("test", "start", "stop", "status"), default="test")
    options = parser.parse_args()
    if options.command != "test":
        control(options.command)
        return
    if not os.environ.get("AUTOAGENT_TEST_POSTGRES_URL") and POSTGRES_TEST_URL:
        control("start")
    asyncio.run(check_connection())
    suite = unittest.defaultTestLoader.discover(str(PROJECT_ROOT / "tests"))
    cases = list(postgres_cases(suite))
    if not cases:
        raise RuntimeError("No PostgreSQL tests were discovered; check the installed test dependencies.")
    print(f"Running {len(cases)} PostgreSQL tests as part of the full suite.", flush=True)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    skipped = [case.id() for case, _reason in result.skipped if type(case).__name__.startswith("PostgreSQL")]
    if skipped:
        print(f"Required PostgreSQL tests were skipped: {len(skipped)}", flush=True)
    raise SystemExit(not result.wasSuccessful() or bool(skipped))


if __name__ == "__main__":
    main()
