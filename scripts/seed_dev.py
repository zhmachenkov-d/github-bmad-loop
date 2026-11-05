#!/usr/bin/env python3
"""Reset control-plane tables and insert one run + unit(stub_ready) + unit job.

Idempotent for local verify: always truncate then seed a single actionable cycle.
Pass --keep to skip truncate (insert another cycle; UNIQUE(kind, ref_id) still holds).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from uuid import uuid4

# Allow running without editable install.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from adapters.postgres import connect
from core.stub_unit import STUB_READY


def seed(*, reset: bool = True) -> dict[str, str]:
    run_id = uuid4()
    unit_id = uuid4()
    job_id = uuid4()
    with connect() as conn, conn.transaction():
        if reset:
            conn.execute(
                """
                    TRUNCATE signals, outbox, event_log, jobs, units, runs
                    RESTART IDENTITY CASCADE
                    """
            )
        conn.execute("INSERT INTO runs (id) VALUES (%s)", (run_id,))
        conn.execute(
            "INSERT INTO units (id, run_id, state) VALUES (%s, %s, %s)",
            (unit_id, run_id, STUB_READY),
        )
        conn.execute(
            """
                INSERT INTO jobs (id, kind, ref_id, version)
                VALUES (%s, 'unit', %s, 0)
                """,
            (job_id, unit_id),
        )
    return {
        "run_id": str(run_id),
        "unit_id": str(unit_id),
        "job_id": str(job_id),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Do not truncate; insert an additional run/unit/job cycle",
    )
    args = parser.parse_args()
    ids = seed(reset=not args.keep)
    print(
        f"seeded run={ids['run_id']} unit={ids['unit_id']} "
        f"job={ids['job_id']} state={STUB_READY}"
    )


if __name__ == "__main__":
    main()
