"""Release reports must account for every required shared scenario."""

import json
from pathlib import Path

import pytest

from tests.conformance import test_conformance as conformance


@pytest.mark.parametrize(
    ("missing", "expected_status", "rate_field", "expected_rate"),
    [(20, "fail", "p0_pass_rate", 10 / 11), (18, "partial", "p1_pass_rate", 12 / 13)],
)
def test_missing_scenario_counts_against_release_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing: int,
    expected_status: str,
    rate_field: str,
    expected_rate: float,
) -> None:
    results = [
        conformance.ScenarioResult(
            f"CS-{number:03d}",
            "scenario",
            "P0" if number <= 8 or number in (19, 20, 24) else "P1",
            "fitz-py",
            "tcp",
            "anonymous",
            "pass",
            [],
            0,
        )
        for number in range(1, 25)
        if number != missing
    ]
    results.append(
        conformance.ScenarioResult(
            "PY-EXTRA", "extra", "P0", "fitz-py", "tcp", "anonymous", "pass", [], 0
        )
    )
    output = tmp_path / "results.json"
    monkeypatch.setattr(conformance, "_results", results)
    monkeypatch.setattr(conformance, "CONFORMANCE_OUTPUT", str(output))

    status = conformance._write_results()
    aggregate = json.loads(output.read_text(encoding="utf-8"))

    assert status == expected_status
    assert aggregate["p0_total"] == 11
    assert aggregate["p1_total"] == 13
    assert aggregate["p0_pass"] == (10 if missing == 20 else 11)
    assert aggregate["p1_pass"] == (12 if missing == 18 else 13)
    assert aggregate[rate_field] == expected_rate
    assert aggregate["missing_scenarios"] == [f"CS-{missing:03d}"]
