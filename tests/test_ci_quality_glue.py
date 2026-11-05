"""Unit tests for product-repo quality CI glue under .ci/."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CI = ROOT / ".ci"
if str(CI) not in sys.path:
    sys.path.insert(0, str(CI))

import lib
import run_mutmut_full
import slack_notify


def test_finding_id_includes_start_line() -> None:
    assert lib.finding_id("rule", "src/a.py", 12, "msg") == "rule|src/a.py|12|msg"


def test_findings_from_semgrep_json_null_start_extra() -> None:
    payload = {
        "results": [
            {"check_id": "r", "path": "src/a.py", "start": None, "extra": None},
            {
                "check_id": "r",
                "path": "src/a.py",
                "start": {"line": 3},
                "extra": {"message": "m"},
            },
        ]
    }
    findings = lib.findings_from_semgrep_json(payload)
    assert findings[0]["id"] == "r|src/a.py|0|"
    assert findings[1]["id"] == "r|src/a.py|3|m"
    assert findings[0]["id"] != findings[1]["id"]


def test_mutation_score_formula() -> None:
    assert (
        lib.mutation_score({"total": 10, "skipped": 0, "killed": 7, "timeout": 1}) == 80
    )
    assert lib.mutation_score({"total": 0, "skipped": 0}) == 0


def test_temporary_only_mutate_writes_and_restores(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        "[project]\nname = 'x'\n\n[tool.mutmut]\nsource_paths = [\"src\"]\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(lib, "PYPROJECT", pyproject)
    original = pyproject.read_text(encoding="utf-8")
    with lib.temporary_only_mutate(["src/a.py"]):
        text = pyproject.read_text(encoding="utf-8")
        assert 'only_mutate = ["src/a.py"]' in text
        assert "[tool.mutmut]" in text
    assert pyproject.read_text(encoding="utf-8") == original


def test_temporary_only_mutate_appends_missing_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("[project]\nname = 'x'\n", encoding="utf-8")
    monkeypatch.setattr(lib, "PYPROJECT", pyproject)
    with lib.temporary_only_mutate(["src/b.py"]):
        text = pyproject.read_text(encoding="utf-8")
        assert "[tool.mutmut]" in text
        assert 'only_mutate = ["src/b.py"]' in text


def test_run_mutmut_full_compare_survived_growth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = tmp_path / "out"
    out.mkdir()
    baseline = tmp_path / "mutmut-score.json"
    baseline.write_text(
        json.dumps(
            {
                "score": 80,
                "killed": 8,
                "survived": 0,
                "total": 10,
                "skipped": 0,
                "timeout": 0,
                "no_tests": 0,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(run_mutmut_full, "MUTMUT_BASELINE", baseline)
    monkeypatch.setattr(run_mutmut_full, "OUT_DIR", out)
    monkeypatch.setattr(lib, "OUT_DIR", out)
    monkeypatch.setattr(
        run_mutmut_full,
        "_export_stats",
        lambda: {
            "killed": 8,
            "survived": 2,
            "total": 10,
            "skipped": 0,
            "timeout": 0,
            "no_tests": 0,
        },
    )
    monkeypatch.setattr(
        run_mutmut_full,
        "collect_survived_mutants",
        lambda: [{"id": "m1", "path": "src/a.py"}],
    )
    code = run_mutmut_full.main(
        ["--skip-run", "--compare-baseline", "--worsen-out", str(out / "w.json")]
    )
    assert code == 2
    worsen = json.loads((out / "w.json").read_text(encoding="utf-8"))
    assert worsen["worsened"] is True
    assert worsen["survived_delta"] == 2


def test_slack_collect_factors_and_skip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clean = tmp_path / "clean.json"
    clean.write_text(
        json.dumps({"tool": "semgrep", "worsened": False}), encoding="utf-8"
    )
    bad = tmp_path / "bad.json"
    bad.write_text(
        json.dumps(
            {"tool": "semgrep", "worsened": True, "new_findings": [{"id": "x"}]}
        ),
        encoding="utf-8",
    )
    assert slack_notify.collect_factors([clean]) == []
    assert slack_notify.collect_factors([bad]) == [
        "semgrep: 1 new finding(s) vs baseline"
    ]

    out = tmp_path / "payload.json"
    monkeypatch.delenv("SLACK_CHANNEL_ID", raising=False)
    code = slack_notify.main(
        ["--dry-run", "--worsen", str(clean), "--output", str(out)]
    )
    assert code == 0
    skipped = json.loads(out.read_text(encoding="utf-8"))
    assert skipped.get("skipped") is True
    assert "text" not in skipped

    code = slack_notify.main(["--dry-run", "--worsen", str(bad), "--output", str(out)])
    assert code == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert "text" in payload
    assert payload["channel"] == slack_notify.CHANNEL_PLACEHOLDER
