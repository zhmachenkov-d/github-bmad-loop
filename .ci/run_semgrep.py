#!/usr/bin/env python3
"""Run Semgrep (optionally diff-aware), emit SARIF/JSON, compare to baseline."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from lib import (
    OUT_DIR,
    REPO_ROOT,
    SEMGREP_BASELINE,
    die,
    ensure_out_dir,
    findings_from_semgrep_json,
    load_json,
    write_json,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline-commit",
        default=None,
        help="Git ref for Semgrep diff-aware scan (PR base)",
    )
    parser.add_argument(
        "--sarif-output",
        type=Path,
        default=OUT_DIR / "semgrep.sarif",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=OUT_DIR / "semgrep.json",
    )
    parser.add_argument(
        "--compare-baseline",
        action="store_true",
        help="Fail if findings introduce ids absent from .ci/baselines/semgrep-findings.json",
    )
    parser.add_argument(
        "--write-baseline",
        action="store_true",
        help="Overwrite the Semgrep findings baseline (intentional bump)",
    )
    parser.add_argument(
        "--worsen-out",
        type=Path,
        default=OUT_DIR / "semgrep-worsen.json",
        help="Write worsen factor details when comparing",
    )
    args = parser.parse_args(argv)
    ensure_out_dir()

    cmd = [
        "uv",
        "run",
        "semgrep",
        "scan",
        "--config=p/python",
        "--metrics=off",
        "--quiet",
        f"--sarif-output={args.sarif_output}",
        f"--json-output={args.json_output}",
        "src",
    ]
    if args.baseline_commit:
        cmd.insert(-1, f"--baseline-commit={args.baseline_commit}")

    # Semgrep exits 1 when findings exist; treat that as success for analysis.
    result = subprocess.run(cmd, cwd=REPO_ROOT, check=False, text=True)
    if result.returncode not in (0, 1):
        die(f"semgrep failed with exit {result.returncode}", code=result.returncode or 1)

    payload = load_json(args.json_output)
    findings = findings_from_semgrep_json(payload)
    snapshot = {"findings": findings}
    write_json(OUT_DIR / "semgrep-findings-current.json", snapshot)
    print(f"Semgrep findings: {len(findings)}")
    print(f"SARIF: {args.sarif_output}")

    if args.write_baseline:
        write_json(SEMGREP_BASELINE, snapshot)
        print(f"Wrote baseline {SEMGREP_BASELINE}")
        return 0

    if not args.compare_baseline:
        return 0

    if not SEMGREP_BASELINE.is_file():
        die(f"Missing Semgrep baseline: {SEMGREP_BASELINE}")

    baseline = load_json(SEMGREP_BASELINE)
    known = {f["id"] for f in baseline.get("findings", [])}
    new = [f for f in findings if f["id"] not in known]
    worsen = {
        "tool": "semgrep",
        "worsened": bool(new),
        "new_findings": new,
        "baseline_count": len(known),
        "current_count": len(findings),
    }
    write_json(args.worsen_out, worsen)
    if new:
        print(f"Semgrep worsen: {len(new)} new finding(s) vs baseline")
        for item in new:
            print(f"  - {item['id']}: {item.get('message', '')}")
        return 2
    print("Semgrep baseline: clean or improved")
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    raise SystemExit(main())
