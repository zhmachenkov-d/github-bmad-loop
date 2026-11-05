#!/usr/bin/env python3
"""Convert mutmut meta results under mutants/ into a SARIF document."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from lib import REPO_ROOT, empty_sarif, ensure_out_dir, write_json

# mutmut exit codes → status (aligned with mutmut.stats.status_by_exit_code)
_STATUS = {
    0: "survived",
    1: "killed",
    3: "killed",
    5: "no_tests",
    33: "no_tests",
    34: "skipped",
    36: "timeout",
}


def _status(exit_code: int | None) -> str:
    if exit_code is None:
        return "not_checked"
    return _STATUS.get(exit_code, "suspicious")


def _level(status: str) -> str:
    if status == "survived":
        return "warning"
    if status in {"killed", "timeout", "skipped", "no_tests"}:
        return "note"
    return "none"


def collect_results(paths_filter: set[str] | None) -> list[dict]:
    mutants_root = REPO_ROOT / "mutants"
    results: list[dict] = []
    if not mutants_root.is_dir():
        return results

    for meta_path in sorted(mutants_root.rglob("*.meta")):
        # mutants/src/foo.py.meta → src/foo.py
        rel = meta_path.relative_to(mutants_root)
        source_path = str(rel)[: -len(".meta")]
        if paths_filter is not None and source_path not in paths_filter:
            continue
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        for key, exit_code in sorted(payload.get("exit_code_by_key", {}).items()):
            status = _status(exit_code)
            results.append(
                {
                    "ruleId": f"mutmut/{status}",
                    "level": _level(status),
                    "message": {"text": f"Mutant {key}: {status}"},
                    "locations": [
                        {
                            "physicalLocation": {
                                "artifactLocation": {"uri": source_path},
                            }
                        }
                    ],
                    "properties": {
                        "mutant": key,
                        "status": status,
                        "exitCode": exit_code,
                    },
                }
            )
    return results


def build_sarif(paths_filter: set[str] | None = None) -> dict:
    sarif = empty_sarif("mutmut")
    sarif["runs"][0]["results"] = collect_results(paths_filter)
    sarif["runs"][0]["tool"]["driver"]["rules"] = [
        {
            "id": "mutmut/survived",
            "shortDescription": {"text": "Surviving mutant"},
            "defaultConfiguration": {"level": "warning"},
        },
        {
            "id": "mutmut/killed",
            "shortDescription": {"text": "Killed mutant"},
            "defaultConfiguration": {"level": "note"},
        },
        {
            "id": "mutmut/no_tests",
            "shortDescription": {"text": "Mutant with no covering tests"},
            "defaultConfiguration": {"level": "note"},
        },
        {
            "id": "mutmut/timeout",
            "shortDescription": {"text": "Mutant timed out"},
            "defaultConfiguration": {"level": "note"},
        },
        {
            "id": "mutmut/skipped",
            "shortDescription": {"text": "Skipped mutant"},
            "defaultConfiguration": {"level": "note"},
        },
        {
            "id": "mutmut/suspicious",
            "shortDescription": {"text": "Suspicious mutant result"},
            "defaultConfiguration": {"level": "note"},
        },
        {
            "id": "mutmut/not_checked",
            "shortDescription": {"text": "Mutant not checked"},
            "defaultConfiguration": {"level": "none"},
        },
    ]
    return sarif


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=ensure_out_dir() / "mutmut.sarif",
        help="SARIF output path",
    )
    parser.add_argument(
        "--paths",
        nargs="*",
        default=None,
        help="Optional src/ paths to include (diff slice)",
    )
    args = parser.parse_args(argv)
    paths_filter = set(args.paths) if args.paths else None
    write_json(args.output, build_sarif(paths_filter))
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    # Allow `python .ci/mutmut_to_sarif.py` from repo root
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    raise SystemExit(main())
