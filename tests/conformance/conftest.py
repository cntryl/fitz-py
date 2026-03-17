"""
Conformance test session hook — writes the JSON results file after all tests run.
The _results list and _write_results() live in test_conformance.py and are
imported lazily here to avoid circular-import issues at conftest load time.
"""

from __future__ import annotations

import importlib
import sys


def pytest_sessionfinish(session, exitstatus) -> None:  # noqa: ANN001
    # Lazy import — test_conformance is fully initialised by the time this hook fires.
    module = None
    for loaded in sys.modules.values():
        if loaded is None:
            continue
        if not getattr(loaded, "__name__", "").endswith("test_conformance"):
            continue
        if hasattr(loaded, "_results") and hasattr(loaded, "_write_results"):
            module = loaded
            break

    for module_name in (
        "tests.conformance.test_conformance",
        "conformance.test_conformance",
    ):
        if module is not None:
            break
        try:
            module = importlib.import_module(module_name)
            break
        except ImportError:
            continue
    if module is None:
        return
    _results = getattr(module, "_results", None)
    _write_results = getattr(module, "_write_results", None)
    if _results is None or _write_results is None:
        return
    if _results:
        _write_results()
