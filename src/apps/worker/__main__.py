"""Worker composition root: continuous lease loop (default) or ``--once``.

Exit codes for ``--once``: 0 applied/dispatched, 1 lease/SM/handler error, 2 idle.
Continuous mode polls until stopped (SIGINT releases any held lease) or a
non-idle handler error. Reconcile ticks in-process every 60s (try-lock).
``--reconcile-once`` runs one reconcile tick then exits (tests / scripts).
"""

from __future__ import annotations

import argparse
import os
import sys

from adapters.postgres import PostgresJobStore, connect
from apps.worker.loop import claim_and_dispatch, run_poll_loop
from apps.worker.reconcile import run_reconcile_tick


def run_once(worker_id: str | None = None) -> int:
    """One claim+dispatch then exit (seed / AGENT / ``--once``)."""
    worker_id = worker_id or os.environ.get("WORKER_ID", "worker-1")
    with connect() as conn:
        store = PostgresJobStore(conn)
        code = claim_and_dispatch(store, worker_id)
        if code == 2:
            print("no actionable job", file=sys.stderr)
        return code


def reconcile_once(worker_id: str | None = None) -> int:
    """One reconcile tick under try-lock then exit (0 locked pass, 2 lock miss)."""
    worker_id = worker_id or os.environ.get("WORKER_ID", "worker-1")
    with connect() as conn:
        store = PostgresJobStore(conn)
        result = run_reconcile_tick(store, worker_id)
        if not result.locked:
            print("reconcile lock not acquired", file=sys.stderr)
            return 2
        print(
            "reconcile "
            f"grace={result.grace_cleared} units={result.units_failed} "
            f"runs={result.runs_alerted} dead={result.dead_alerts}"
        )
        return 0


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Shared typed-job worker: lease loop for kind∈{run,unit}."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Claim and dispatch at most one job, then exit",
    )
    parser.add_argument(
        "--reconcile-once",
        action="store_true",
        help="Run one reconcile tick under try-lock, then exit",
    )
    args = parser.parse_args(argv)
    worker_id = os.environ.get("WORKER_ID", "worker-1")

    if args.once and args.reconcile_once:
        parser.error("use only one of --once / --reconcile-once")

    if args.reconcile_once:
        raise SystemExit(reconcile_once(worker_id))

    if args.once:
        raise SystemExit(run_once(worker_id))

    with connect() as conn:
        store = PostgresJobStore(conn)
        raise SystemExit(run_poll_loop(store, worker_id, once=False))


if __name__ == "__main__":
    main()
