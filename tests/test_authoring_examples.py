from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


EXAMPLE_TEST = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "authoring"
    / "tests"
    / "test_examples.py"
)


def load_tests(
    loader: unittest.TestLoader,
    tests: unittest.TestSuite,
    pattern: str | None,
) -> unittest.TestSuite:
    del tests, pattern
    spec = importlib.util.spec_from_file_location(
        "autoagent_authoring_example_tests",
        EXAMPLE_TEST,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load authoring example tests: {EXAMPLE_TEST}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return loader.loadTestsFromModule(module)
