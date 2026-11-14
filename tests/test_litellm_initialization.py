"""Cold LiteLLM imports cannot reenter its modules from another logging thread."""

import importlib.util
import logging
import os
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from uuid import uuid4

from autoagent.agent.providers._litellm_sdk import load_litellm_sdk


class InitializationTests(unittest.TestCase):
    def test_new_sdk_filters_wait_until_import_completes_and_keep_redacting(self):
        """Concurrent logging waits for initialization; original filters and redaction are restored."""
        logger = logging.getLogger('litellm-import-test-' + uuid4().hex)
        logger.propagate = False
        logger.setLevel(logging.WARNING)
        captured, calls = [], []
        entered = threading.Event()
        original_add = logging.Filterer.addFilter

        class Capture(logging.Handler):
            def emit(self, record):
                captured.append(record.getMessage())

        class SDKFilter(logging.Filter):
            __module__ = 'litellm._logging'

            def filter(self, record):
                calls.append(record)
                record.msg = 'REDACTED'
                return True

        existing, delegate = logging.Filter(), SDKFilter()
        logger.addFilter(existing)
        handler = Capture()
        logger.addHandler(handler)
        worker = None

        def write():
            entered.set()
            logger.warning('PRIVATE_TEST_VALUE')

        def importing(name):
            nonlocal worker
            self.assertEqual(name, 'litellm')
            logger.addFilter(delegate)
            worker = threading.Thread(target=write, daemon=True)
            worker.start()
            self.assertTrue(entered.wait(2))
            self.assertEqual(calls, [])
            return SimpleNamespace(acompletion=object(), aresponses=object(), aembedding=object())

        try:
            with patch('autoagent.agent.providers._litellm_sdk.importlib.import_module', side_effect=importing):
                load_litellm_sdk()
            worker.join(2)
            self.assertFalse(worker.is_alive())
            self.assertEqual(captured, ['REDACTED'])
            self.assertEqual(logger.filters, [existing, delegate])
            self.assertIs(logging.Filterer.addFilter, original_add)
        finally:
            logger.filters.clear()
            logger.removeHandler(handler)
            handler.close()
            if worker is not None:
                worker.join(2)

    def test_failed_import_rolls_back_its_logging_registration(self):
        """An initialization failure removes incomplete SDK filters and preserves unrelated logging."""
        logger = logging.getLogger('litellm-failed-import-' + uuid4().hex)
        existing = logging.Filter()
        logger.addFilter(existing)
        original_add = logging.Filterer.addFilter

        class SDKFilter(logging.Filter):
            __module__ = 'litellm._logging'

        def importing(name):
            logger.addFilter(SDKFilter())
            raise ImportError('Optional SDK is unavailable')

        try:
            with patch('autoagent.agent.providers._litellm_sdk.importlib.import_module', side_effect=importing):
                with self.assertRaises(ImportError):
                    load_litellm_sdk()
            self.assertEqual(logger.filters, [existing])
            self.assertIs(logging.Filterer.addFilter, original_add)
        finally:
            logger.filters.clear()

    @unittest.skipUnless(importlib.util.find_spec('litellm'), 'Install autoagent[litellm] for cold SDK initialization')
    def test_real_cold_sdk_import_survives_concurrent_asyncio_logging(self):
        """Independent Provider loops initialize under concurrent logging without making model requests."""
        script = r'''
import asyncio, logging, threading, traceback
from autoagent.agent import LiteLLMProvider, LiteLLMEmbeddingProvider

logger = logging.getLogger('asyncio')
logger.handlers = [logging.NullHandler()]
logger.propagate = False
logger.setLevel(logging.WARNING)
stop, errors = threading.Event(), []
original_add = logging.Filterer.addFilter

def log():
    while not stop.wait(.0005):
        try:
            logger.warning('Concurrent Runtime startup observation')
        except BaseException:
            errors.append(traceback.format_exc())
            break

worker = threading.Thread(target=log, daemon=True)
worker.start()
async def main():
    for provider in (LiteLLMProvider(), LiteLLMProvider(protocol='openai.responses'), LiteLLMEmbeddingProvider()):
        async with provider:
            assert provider._sdk is not None

start = threading.Barrier(3)
def prepare():
    try:
        start.wait(5)
        asyncio.run(main())
    except BaseException:
        errors.append(traceback.format_exc())

providers = [threading.Thread(target=prepare, daemon=True) for _ in range(3)]
try:
    for provider in providers:
        provider.start()
    for provider in providers:
        provider.join(20)
    assert all(not provider.is_alive() for provider in providers), 'Provider initialization deadlocked'
finally:
    stop.set()
    worker.join(3)
assert not worker.is_alive(), 'Logging thread deadlocked'
assert not errors, '\n'.join(errors)
assert logging.Filterer.addFilter is original_add
assert all(type(value).__module__ != 'autoagent.agent.providers._litellm_sdk' for value in logger.filters)
print('COLD_IMPORT_OK')
'''
        environment = dict(os.environ, LITELLM_LOCAL_MODEL_COST_MAP='True', LITELLM_MODE='PRODUCTION')
        for attempt in range(3):
            with self.subTest(attempt=attempt):
                result = subprocess.run([sys.executable, '-c', script], env=environment,
                    cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=35)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn('COLD_IMPORT_OK', result.stdout)
