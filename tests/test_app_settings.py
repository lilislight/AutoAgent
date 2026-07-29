from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from dotenv import dotenv_values

from autoagent import AutoAgentApp, AutoAgentSettings, DatabaseBackend
from autoagent.ai.providers.factory import LLM_PROVIDER_ENV_KEYS
from autoagent.core.app.settings import AUTOAGENT_ENV_KEYS
from autoagent.core.server import SERVER_ENV_KEYS


class AutoAgentSettingsTests(unittest.IsolatedAsyncioTestCase):
    def test_default_persistence_budget_is_128_256_512_mib(self) -> None:
        settings = AutoAgentSettings.from_env(env_file=None, environ={})

        self.assertEqual(
            128 * 1024 * 1024,
            settings.persistence_queue_low_watermark_bytes,
        )
        self.assertEqual(
            256 * 1024 * 1024,
            settings.persistence_queue_high_watermark_bytes,
        )
        self.assertEqual(
            512 * 1024 * 1024,
            settings.persistence_queue_hard_watermark_bytes,
        )

    async def test_environment_builds_the_complete_database_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "runtime.db"
            settings = AutoAgentSettings.from_env(
                env_file=None,
                environ={
                    "AUTOAGENT_DATABASE_URL": (
                        f"sqlite+aiosqlite:///{database_path}"
                    ),
                    "AUTOAGENT_DATABASE_ECHO": "true",
                    "AUTOAGENT_SERIALIZER_MAX_INLINE_BYTES": "65536",
                    "AUTOAGENT_PERSISTENCE_QUEUE_LOW_WATERMARK_BYTES": "1000",
                    "AUTOAGENT_PERSISTENCE_QUEUE_HIGH_WATERMARK_BYTES": "2000",
                    "AUTOAGENT_PERSISTENCE_QUEUE_HARD_WATERMARK_BYTES": "4000",
                    "AUTOAGENT_PERSISTENCE_ADMISSION_TIMEOUT_MS": "2500",
                    "AUTOAGENT_DATABASE_BATCH_MAX_ITEMS": "12",
                    "AUTOAGENT_DATABASE_BATCH_MAX_BYTES": "3000",
                    "AUTOAGENT_DATABASE_BATCH_MAX_DELAY_MS": "7",
                    "AUTOAGENT_DATABASE_RECOVERY_EVENT_INTERVAL": "15",
                    "AUTOAGENT_SQLITE_SYNCHRONOUS": "normal",
                    "AUTOAGENT_EXECUTOR_MAX_THREAD_WORKERS": "5",
                    "AUTOAGENT_EXECUTOR_MAX_PARALLEL_UNITS": "3",
                    "AUTOAGENT_SHUTDOWN_GRACE_TIMEOUT_MS": "1250",
                    "AUTOAGENT_ARTIFACT_ENABLED": "false",
                    "AUTOAGENT_ARTIFACT_INLINE_MAX_BYTES": "4096",
                    "AUTOAGENT_RETENTION_MODE": "lru_durable_terminal",
                    "AUTOAGENT_RETENTION_MAX_TERMINAL_INVOCATIONS": "9",
                    (
                        "AUTOAGENT_RETENTION_MAX_REPLAY_"
                        "CHECKPOINTS_PER_INVOCATION"
                    ): "3",
                },
            )
            app = AutoAgentApp(settings=settings)

            self.assertEqual(65_536, app.runtime_serializer.max_inline_bytes)
            self.assertIsInstance(app.runtime_store.backend, DatabaseBackend)
            backend = app.runtime_store.backend
            assert isinstance(backend, DatabaseBackend)
            self.assertTrue(backend.engine.echo)
            self.assertEqual(12, backend.batch_max_items)
            self.assertEqual(3_000, backend.batch_max_bytes)
            self.assertEqual(7, backend.batch_max_delay_ms)
            self.assertEqual(15, backend.recovery_event_interval)
            self.assertEqual("NORMAL", backend.sqlite_synchronous)
            self.assertEqual(1_250, backend.shutdown_timeout_ms)
            self.assertEqual(
                5,
                app.workflow_executor.node_executor.thread_pool._max_workers,
            )
            self.assertEqual(
                3,
                app.workflow_executor.node_executor.max_parallel_units,
            )
            self.assertEqual(1_250, app.settings.shutdown_grace_timeout_ms)
            self.assertFalse(backend.artifact_policy.enabled)
            self.assertEqual(4_096, backend.artifact_policy.inline_max_bytes)

            persistence = app.runtime_store.persistence
            assert persistence is not None
            self.assertEqual(
                1_000,
                persistence.policy.queue_low_watermark_bytes,
            )
            self.assertEqual(
                2_000,
                persistence.policy.queue_high_watermark_bytes,
            )
            self.assertEqual(
                4_000,
                persistence.policy.queue_hard_watermark_bytes,
            )
            self.assertEqual(
                2_500,
                persistence.policy.admission_timeout_ms,
            )
            self.assertEqual(
                "lru_durable_terminal",
                app.runtime_store.retention_policy.mode,
            )
            self.assertEqual(
                9,
                app.runtime_store.retention_policy.max_terminal_invocations,
            )
            self.assertEqual(
                3,
                (
                    app.runtime_store.retention_policy
                    .max_replay_checkpoints_per_invocation
                ),
            )
            await app.aclose()

    def test_process_environment_overrides_dotenv_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text(
                "AUTOAGENT_DATABASE_ECHO=false\n",
                encoding="utf-8",
            )

            settings = AutoAgentSettings.from_env(
                env_file=env_file,
                environ={
                    "AUTOAGENT_DATABASE_ECHO": "true",
                },
            )

        self.assertTrue(settings.database_echo)

    async def test_autoagent_app_loads_default_environment(self) -> None:
        with patch.dict(
            os.environ,
            {},
            clear=True,
        ), patch(
            "autoagent.core.app.settings.dotenv_values",
            return_value={},
        ):
            app = AutoAgentApp()
        try:
            self.assertIsNone(app.runtime_store.backend)
        finally:
            await app.aclose()

    def test_invalid_environment_value_names_the_configuration_key(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "AUTOAGENT_ARTIFACT_ENABLED",
        ):
            AutoAgentSettings.from_env(
                env_file=None,
                environ={"AUTOAGENT_ARTIFACT_ENABLED": "sometimes"},
            )

        with self.assertRaisesRegex(ValueError, "retention mode"):
            AutoAgentSettings.from_env(
                env_file=None,
                environ={"AUTOAGENT_RETENTION_MODE": "unknown"},
            )

    def test_env_example_documents_every_supported_setting(self) -> None:
        example = dotenv_values(
            Path(__file__).parents[1] / ".env.example"
        )
        documented = set(example)

        self.assertEqual(
            AUTOAGENT_ENV_KEYS
            | SERVER_ENV_KEYS
            | LLM_PROVIDER_ENV_KEYS,
            documented,
        )
