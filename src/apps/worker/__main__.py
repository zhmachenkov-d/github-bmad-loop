"""Run one stub unit cycle: lease → apply.

Exit codes: 0 applied, 1 lease lost, 2 idle (empty queue or terminal no-op).
"""

from __future__ import annotations

import os
import sys

from adapters.postgres import PostgresJobStore, connect
from core.stub_unit import LeaseLostError


def run_once(worker_id: str | None = None) -> int:
    worker_id = worker_id or os.environ.get("WORKER_ID", "worker-1")
    with connect() as conn:
        store = PostgresJobStore(conn)
        lease = store.lease_next_unit_job(worker_id)
        if lease is None:
            print("no actionable unit job", file=sys.stderr)
            return 2
        try:
            result = store.apply_stub_transition(lease, worker_id)
        except LeaseLostError as exc:
            print(f"lease lost: {exc}", file=sys.stderr)
            return 1
        if result.applied:
            print(
                f"applied {result.from_state}→{result.to_state} "
                f"key={result.idempotency_key}"
            )
            return 0
        print("apply no-op (unit already terminal)", file=sys.stderr)
        return 2


def main() -> None:
    raise SystemExit(run_once())


if __name__ == "__main__":
    main()
