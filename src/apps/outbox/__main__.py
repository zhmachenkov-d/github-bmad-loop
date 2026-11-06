"""Outbox dispatcher composition root: continuous poll or ``--once``.

Default: claim due pending → channel stub send → sent / retry / dead.
Exit codes for ``--once``: 0 processed, 2 idle.
"""

from __future__ import annotations

import argparse

from adapters.postgres import connect
from apps.outbox.dispatcher import (
    DEFAULT_CLAIM_LIMIT,
    default_senders_router,
    open_store,
    run_once,
    run_poll_loop,
)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Outbox dispatcher: claim→send with stub channel senders."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Claim and process at most one batch, then exit",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_CLAIM_LIMIT,
        help=f"Claim batch size (default {DEFAULT_CLAIM_LIMIT})",
    )
    args = parser.parse_args(argv)
    router = default_senders_router()

    if args.once:
        with connect() as conn:
            store = open_store(conn)
            raise SystemExit(run_once(store, router.send, limit=args.limit))

    with connect() as conn:
        store = open_store(conn)
        raise SystemExit(
            run_poll_loop(store, router.send, once=False, limit=args.limit)
        )


if __name__ == "__main__":
    main()
