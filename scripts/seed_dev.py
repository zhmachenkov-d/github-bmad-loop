#!/usr/bin/env python3
"""Insert one run + unit(stub_ready) + unit job for local verification."""

from __future__ import annotations

import sys
from pathlib import Path
from uuid import uuid4

# Allow running without editable install.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from adapters.postgres import connect  # noqa: E402
from core.stub_unit import STUB_READY  # noqa: E402


def seed() -> dict[str, str]:
    run_id = uuid4()
    unit_id = uuid4()
    job_id = uuid4()
    with connect() as conn:
        with conn.transaction():
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
    ids = seed()
    print(
        f"seeded run={ids['run_id']} unit={ids['unit_id']} "
        f"job={ids['job_id']} state={STUB_READY}"
    )


if __name__ == "__main__":
    main()
