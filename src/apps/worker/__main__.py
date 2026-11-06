"""Worker composition root: continuous lease loop (default) or ``--once``.

Exit codes for ``--once``: 0 applied/dispatched, 1 lease/SM/handler error, 2 idle.
Continuous mode polls until stopped (SIGINT) or a non-idle handler error.
"""

from __future__ import annotations

import argparse
import os
import sys

from adapters.postgres import PostgresJobStore, connect
from apps.worker.loop import claim_and_dispatch, run_poll_loop


def run_once(worker_id: str | None = None) -> int:
    """One claim+dispatch then exit (seed / AGENT / ``--once``)."""
    worker_id = worker_id or os.environ.get("WORKER_ID", "worker-1")
    with connect() as conn:
        store = PostgresJobStore(conn)
        code = claim_and_dispatch(store, worker_id)
        if code == 2:
            print("no actionable job", file=sys.stderr)
        return code


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Shared typed-job worker: lease loop for kind∈{run,unit}."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Claim and dispatch at most one job, then exit",
    )
    args = parser.parse_args(argv)
    worker_id = os.environ.get("WORKER_ID", "worker-1")

    if args.once:
        raise SystemExit(run_once(worker_id))

    with connect() as conn:
        store = PostgresJobStore(conn)
        try:
            raise SystemExit(run_poll_loop(store, worker_id, once=False))
        except KeyboardInterrupt:
            print("worker stopped", file=sys.stderr)
            raise SystemExit(0) from None


if __name__ == "__main__":
    main()
