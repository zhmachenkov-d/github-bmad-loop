#!/usr/bin/env python3
"""HITL dry-run: emit fixture SARIF + aggregated Slack payload without posting."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from lib import CI_DIR, OUT_DIR, ensure_out_dir, write_json
from slack_notify import main as slack_main

FIXTURE_DIR = CI_DIR / "fixtures" / "worsen"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-url",
        default="https://example.invalid/actions/runs/fixture",
        help="Placeholder workflow run URL",
    )
    args = parser.parse_args(argv)
    ensure_out_dir()

    for name in ("semgrep.sarif", "mutmut.sarif"):
        src = FIXTURE_DIR / name
        if not src.is_file():
            print(f"missing fixture {src}", file=sys.stderr)
            return 1
        dest = OUT_DIR / f"fixture-{name}"
        shutil.copyfile(src, dest)
        print(f"Copied SARIF artifact {dest}")

    worsen_semgrep = {
        "tool": "semgrep",
        "worsened": True,
        "new_findings": [
            {
                "id": "python.lang.security.audit.fixture|src/fixture.py|fixture finding",
                "check_id": "python.lang.security.audit.fixture",
                "path": "src/fixture.py",
                "start_line": 1,
                "message": "fixture finding",
            }
        ],
        "baseline_count": 0,
        "current_count": 1,
    }
    worsen_mutmut = {
        "tool": "mutmut",
        "worsened": True,
        "baseline_score": 80,
        "current_score": 70,
        "drop": 10,
        "threshold": 1,
    }
    semgrep_path = OUT_DIR / "fixture-semgrep-worsen.json"
    mutmut_path = OUT_DIR / "fixture-mutmut-worsen.json"
    write_json(semgrep_path, worsen_semgrep)
    write_json(mutmut_path, worsen_mutmut)

    return slack_main(
        [
            "--dry-run",
            "--run-url",
            args.run_url,
            "--worsen",
            str(semgrep_path),
            str(mutmut_path),
            "--output",
            str(OUT_DIR / "fixture-slack-payload.json"),
        ]
    )


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    raise SystemExit(main())
