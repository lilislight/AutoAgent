from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from alembic import command
from alembic.config import Config


class RuntimeMigrationTests(unittest.TestCase):
    def test_initial_migration_matches_runtime_metadata(self) -> None:
        """Guard the checked-in migration against ORM schema drift."""

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "runtime.db"
            config = Config("alembic.ini")
            config.set_main_option(
                "sqlalchemy.url",
                f"sqlite+aiosqlite:///{database}",
            )

            command.upgrade(config, "head")
            command.check(config)


if __name__ == "__main__":
    unittest.main()
