#!/usr/bin/env python3
"""Diff-scoped mutmut on changed src/ files; emit SARIF; never fail on survivors."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from lib import (
    OUT_DIR,
    die,
    ensure_out_dir,
    git_changed_src_files,
    run,
    temporary_only_mutate,
    write_json,
)
from mutmut_to_sarif import build_sarif


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-ref",
        default=None,
        help="Git ref to diff against (CI: PR base SHA). Omit for local hook (vs HEAD).",
    )
    parser.add_argument(
        "--sarif-output",
        type=Path,
        default=OUT_DIR / "mutmut-diff.sarif",
    )
    args = parser.parse_args(argv)
    ensure_out_dir()

    changed = git_changed_src_files(args.base_ref)
    if not changed:
        print("No changed src/ Python files; writing empty mutmut SARIF")
        write_json(args.sarif_output, build_sarif(set()))
        return 0

    print(f"Diff-scoped mutmut targets ({len(changed)}):")
    for path in changed:
        print(f"  - {path}")

    try:
        with temporary_only_mutate(changed):
            # Tool crash fails the job; surviving mutants must not.
            run(["uv", "run", "mutmut", "run"], check=True)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 — surface analysis failure
        die(f"mutmut failed: {exc}")

    write_json(args.sarif_output, build_sarif(set(changed)))
    print(f"SARIF: {args.sarif_output} (survivors do not fail this step)")
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    raise SystemExit(main())
