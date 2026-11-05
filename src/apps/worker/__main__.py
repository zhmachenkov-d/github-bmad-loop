"""Run one stub unit cycle: lease → stub_start → stub_complete (one hold).

Exit codes: 0 applied full cycle, 1 lease/SM apply error, 2 idle (empty queue).
"""

from __future__ import annotations

import os
import sys
from dataclasses import replace

from adapters.postgres import PostgresJobStore, connect
from core.stub_unit import (
    STUB_COMPLETE,
    STUB_START,
    IllegalTransitionError,
    InvalidUnitStateError,
    LeaseLostError,
    UnsupportedResultError,
)


def run_once(worker_id: str | None = None) -> int:
    worker_id = worker_id or os.environ.get("WORKER_ID", "worker-1")
    with connect() as conn:
        store = PostgresJobStore(conn)
        lease = store.lease_next_unit_job(worker_id)
        if lease is None:
            print("no actionable unit job", file=sys.stderr)
            return 2
        try:
            start = store.apply_transition(lease, worker_id, STUB_START)
            lease = replace(lease, job_version=start.job_version_after)
            complete = store.apply_transition(lease, worker_id, STUB_COMPLETE)
        except (
            LeaseLostError,
            IllegalTransitionError,
            InvalidUnitStateError,
            UnsupportedResultError,
        ) as exc:
            print(f"apply failed: {exc}", file=sys.stderr)
            return 1
        print(
            f"applied {start.from_state}→{start.to_state} then "
            f"{complete.from_state}→{complete.to_state} "
            f"key={complete.idempotency_key}"
        )
        return 0


def main() -> None:
    raise SystemExit(run_once())


if __name__ == "__main__":
    main()
