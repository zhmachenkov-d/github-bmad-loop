"""Claim → route → send → per-row outcome updates for the outbox dispatcher."""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from typing import Any

import psycopg

from adapters.postgres.store import PostgresJobStore
from core.outbox_policy import OUTBOX_MAX_ATTEMPTS, outbox_backoff_seconds
from ports.outbox_sender import OutboxMessage, PermanentError, TransientError

IDLE_SLEEP_SECONDS = 2.0
DEFAULT_CLAIM_LIMIT = 20

SenderFn = Callable[[OutboxMessage], str]


def process_claimed_row(
    store: PostgresJobStore,
    msg: OutboxMessage,
    send: SenderFn,
) -> str:
    """Apply one send outcome. Returns new status: sent | pending | dead."""
    try:
        external_id = send(msg)
    except TransientError:
        new_attempts = msg.attempts + 1
        if new_attempts >= OUTBOX_MAX_ATTEMPTS:
            store.mark_outbox_dead(msg.id, attempts=new_attempts)
            return "dead"
        delay = outbox_backoff_seconds(msg.attempts)
        store.mark_outbox_retry(msg.id, attempts=new_attempts, delay_seconds=delay)
        return "pending"
    except PermanentError:
        store.mark_outbox_dead(msg.id, attempts=msg.attempts)
        return "dead"
    store.mark_outbox_success(msg.id, external_id)
    return "sent"


def dispatch_batch(
    store: PostgresJobStore,
    send: SenderFn,
    *,
    limit: int = DEFAULT_CLAIM_LIMIT,
) -> int:
    """Claim due rows (locks held) and process each; returns claimed count."""
    if limit < 1:
        return 0
    with store.connection.transaction():
        rows = store.claim_due_outbox(limit=limit)
        for msg in rows:
            status = process_claimed_row(store, msg, send)
            print(f"outbox id={msg.id} channel={msg.channel} → {status}")
        return len(rows)


def run_once(
    store: PostgresJobStore,
    send: SenderFn,
    *,
    limit: int = DEFAULT_CLAIM_LIMIT,
) -> int:
    """One claim batch then exit. Exit: 0 processed, 2 idle."""
    n = dispatch_batch(store, send, limit=limit)
    if n == 0:
        print("no pending outbox rows", file=sys.stderr)
        return 2
    return 0


def run_poll_loop(
    store: PostgresJobStore,
    send: SenderFn,
    *,
    once: bool = False,
    limit: int = DEFAULT_CLAIM_LIMIT,
    idle_sleep: float = IDLE_SLEEP_SECONDS,
) -> int:
    """Continuous poll until stopped; with ``once``, one claim batch then exit."""
    try:
        while True:
            n = dispatch_batch(store, send, limit=limit)
            if once:
                if n == 0:
                    print("no pending outbox rows", file=sys.stderr)
                    return 2
                return 0
            if n == 0:
                time.sleep(idle_sleep)
    except KeyboardInterrupt:
        print("outbox dispatcher stopped", file=sys.stderr)
        return 0


def default_senders_router():
    """Composition helper: stub/github/slack stub senders wired to a router."""
    from adapters.outbox.router import ChannelRouter
    from adapters.outbox.senders import GitHubStubSender, SlackStubSender, StubSender

    return ChannelRouter(
        {
            "stub": StubSender(),
            "github": GitHubStubSender(),
            "slack": SlackStubSender(),
        }
    )


def open_store(conn: psycopg.Connection[dict[str, Any]]) -> PostgresJobStore:
    return PostgresJobStore(conn)
