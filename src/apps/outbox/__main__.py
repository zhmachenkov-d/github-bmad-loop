"""Mark pending outbox rows as sent. No GitHub/Slack I/O in this stub."""

from __future__ import annotations

import sys

from adapters.postgres import PostgresJobStore, connect


def run_once(*, limit: int = 20) -> int:
    with connect() as conn:
        store = PostgresJobStore(conn)
        n = store.mark_outbox_sent(limit=limit)
        if n == 0:
            print("no pending outbox rows", file=sys.stderr)
        else:
            print(f"marked {n} outbox row(s) sent")
        return 0


def main() -> None:
    raise SystemExit(run_once())


if __name__ == "__main__":
    main()
