#!/usr/bin/env python3
"""Full mutmut over src/; emit SARIF + score; optionally gate vs baseline (drop ≥ 1)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from lib import (
    MUTMUT_BASELINE,
    OUT_DIR,
    collect_survived_mutants,
    die,
    ensure_out_dir,
    load_json,
    mutation_score,
    run,
    write_json,
)
from mutmut_to_sarif import build_sarif


def _export_stats() -> dict:
    run(["uv", "run", "mutmut", "export-cicd-stats"], check=True)
    from lib import REPO_ROOT

    stats_path = REPO_ROOT / "mutants" / "mutmut-cicd-stats.json"
    if not stats_path.is_file():
        die(f"missing mutmut cicd stats: {stats_path}")
    return load_json(stats_path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sarif-output",
        type=Path,
        default=OUT_DIR / "mutmut-full.sarif",
    )
    parser.add_argument(
        "--compare-baseline",
        action="store_true",
        help="Fail when score drops by ≥ 1 vs .ci/baselines/mutmut-score.json",
    )
    parser.add_argument(
        "--write-baseline",
        action="store_true",
        help="Overwrite the mutmut score baseline (intentional bump)",
    )
    parser.add_argument(
        "--worsen-out",
        type=Path,
        default=OUT_DIR / "mutmut-worsen.json",
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="Reuse existing mutants/ results (no mutmut run)",
    )
    args = parser.parse_args(argv)
    ensure_out_dir()

    if not args.skip_run:
        run(["uv", "run", "mutmut", "run"], check=True)

    stats = _export_stats()
    score = mutation_score(stats)
    current = {
        "score": score,
        "killed": int(stats.get("killed", 0)),
        "survived": int(stats.get("survived", 0)),
        "total": int(stats.get("total", 0)),
        "skipped": int(stats.get("skipped", 0)),
        "timeout": int(stats.get("timeout", 0)),
        "no_tests": int(stats.get("no_tests", 0)),
    }
    write_json(OUT_DIR / "mutmut-score-current.json", current)
    write_json(args.sarif_output, build_sarif())
    print(
        f"Mutation score: {score} (killed={current['killed']} survived={current['survived']} total={current['total']})"
    )
    print(f"SARIF: {args.sarif_output}")

    if args.write_baseline:
        write_json(MUTMUT_BASELINE, current)
        print(f"Wrote baseline {MUTMUT_BASELINE}")
        return 0

    if not args.compare_baseline:
        return 0

    if not MUTMUT_BASELINE.is_file():
        die(f"Missing mutmut baseline: {MUTMUT_BASELINE}")

    baseline = load_json(MUTMUT_BASELINE)
    baseline_score = int(baseline.get("score", 0))
    baseline_survived = int(baseline.get("survived", 0))
    baseline_no_tests = int(baseline.get("no_tests", 0))
    drop = baseline_score - score
    survived_up = current["survived"] - baseline_survived
    no_tests_up = current["no_tests"] - baseline_no_tests
    worsened = drop >= 1 or survived_up > 0 or no_tests_up > 0
    survivors = collect_survived_mutants() if worsened else []
    worsen = {
        "tool": "mutmut",
        "worsened": worsened,
        "baseline_score": baseline_score,
        "current_score": score,
        "drop": drop,
        "threshold": 1,
        "survived_delta": survived_up,
        "no_tests_delta": no_tests_up,
        "survivors": survivors,
    }
    write_json(args.worsen_out, worsen)
    if worsened:
        print(
            f"Mutmut worsen: score {baseline_score} → {score} "
            f"(drop {drop}, survived_delta {survived_up}, no_tests_delta {no_tests_up})"
        )
        return 2
    print(f"Mutmut baseline: clean or improved ({baseline_score} → {score})")
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    raise SystemExit(main())
