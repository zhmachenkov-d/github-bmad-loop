"""Outbox claim→send: retry, dead@10, permanent, concurrency, send idempotency."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from adapters.outbox.router import ChannelRouter
from adapters.outbox.senders import GitHubStubSender, SlackStubSender, StubSender
from adapters.postgres import PostgresJobStore
from apps.outbox import __main__ as outbox_main
from apps.outbox.dispatcher import dispatch_batch, run_once
from core.outbox_policy import OUTBOX_MAX_ATTEMPTS, outbox_backoff_seconds
from core.stub_unit import (
    STUB_COMPLETE,
    STUB_EFFECT,
    STUB_OUTBOX_CHANNEL,
    STUB_START,
)
from ports.outbox_sender import PermanentError, TransientError


def _router(
    *,
    stub: StubSender | None = None,
    github: GitHubStubSender | None = None,
    slack: SlackStubSender | None = None,
) -> ChannelRouter:
    return ChannelRouter(
        {
            "stub": stub or StubSender(),
            "github": github or GitHubStubSender(),
            "slack": slack or SlackStubSender(),
        }
    )


def _seed_pending_outbox(
    conn, ids, *, channel: str = STUB_OUTBOX_CHANNEL, key: str | None = None
):
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    start = store.apply_transition(lease, "w", STUB_START)
    lease = replace(
        lease, job_version=start.job_version_after, unit_state=start.to_state
    )
    store.apply_transition(lease, "w", STUB_COMPLETE)
    if channel != STUB_OUTBOX_CHANNEL:
        with conn.transaction():
            conn.execute("DELETE FROM outbox")
            conn.execute(
                """
                INSERT INTO outbox (
                    run_id, unit_id, channel, idempotency_key, payload, status
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, 'pending')
                """,
                (
                    ids["run_id"],
                    ids["unit_id"],
                    channel,
                    key or f"unit:{ids['unit_id']}:{channel}:1",
                    Jsonb({"effect": STUB_EFFECT}),
                ),
            )
    return store


def test_happy_send_sets_external_id(conn, seed_unit_job, db_url):
    ids = seed_unit_job()
    stub = StubSender()
    store = _seed_pending_outbox(conn, ids)
    conn.commit()
    assert dispatch_batch(store, _router(stub=stub).send) == 1
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        row = other.execute(
            "SELECT status, external_id, sent_at FROM outbox"
        ).fetchone()
    assert row["status"] == "sent"
    assert row["external_id"]
    assert row["sent_at"] is not None
    assert len(stub.effects) == 1


def test_transient_retry_then_success(conn, seed_unit_job, db_url):
    ids = seed_unit_job()
    stub = StubSender()
    stub.fail_next(TransientError("boom"))
    store = _seed_pending_outbox(conn, ids)
    conn.commit()

    assert dispatch_batch(store, _router(stub=stub).send) == 1
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        row = other.execute(
            """
            SELECT status, attempts, next_attempt_at,
                   next_attempt_at > now() AS future
            FROM outbox
            """
        ).fetchone()
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert row["future"] is True

    # Not due yet → claim skips
    assert dispatch_batch(store, _router(stub=stub).send) == 0

    with store.connection.transaction():
        store.connection.execute(
            "UPDATE outbox SET next_attempt_at = now() - interval '1 second'"
        )
    stub_ok = StubSender()
    assert dispatch_batch(store, _router(stub=stub_ok).send) == 1
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        row = other.execute(
            "SELECT status, external_id, attempts FROM outbox"
        ).fetchone()
    assert row["status"] == "sent"
    assert row["external_id"]
    assert row["attempts"] == 1


def test_dead_at_max_attempts(conn, seed_unit_job, db_url):
    ids = seed_unit_job()
    stub = StubSender()
    store = _seed_pending_outbox(conn, ids)
    conn.commit()

    for i in range(OUTBOX_MAX_ATTEMPTS):
        stub.fail_next(TransientError(f"t{i}"))
        with store.connection.transaction():
            store.connection.execute(
                "UPDATE outbox SET next_attempt_at = now() - interval '1 second'"
            )
        assert dispatch_batch(store, _router(stub=stub).send) == 1

    with psycopg.connect(db_url, row_factory=dict_row) as other:
        row = other.execute("SELECT status, attempts FROM outbox").fetchone()
    assert row["status"] == "dead"
    assert row["attempts"] == OUTBOX_MAX_ATTEMPTS

    # Further ticks leave untouched
    assert dispatch_batch(store, _router(stub=StubSender()).send) == 0
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        row = other.execute("SELECT status, attempts FROM outbox").fetchone()
    assert row["status"] == "dead"
    assert row["attempts"] == OUTBOX_MAX_ATTEMPTS


def test_permanent_dead_immediately(conn, seed_unit_job, db_url):
    ids = seed_unit_job()
    stub = StubSender()
    stub.fail_next(PermanentError("nope"))
    store = _seed_pending_outbox(conn, ids)
    conn.commit()
    assert dispatch_batch(store, _router(stub=stub).send) == 1
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        row = other.execute("SELECT status, attempts FROM outbox").fetchone()
    assert row["status"] == "dead"
    assert row["attempts"] == 0


def test_unknown_channel_dead(conn, seed_unit_job, db_url):
    ids = seed_unit_job()
    store = _seed_pending_outbox(conn, ids, channel="teams")
    conn.commit()
    assert dispatch_batch(store, _router().send) == 1
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        row = other.execute("SELECT status, channel FROM outbox").fetchone()
    assert row["channel"] == "teams"
    assert row["status"] == "dead"


def test_enqueue_on_conflict_dedupe(conn, seed_unit_job, counts):
    """Duplicate insert same idempotency_key → exactly one outbox row."""
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    start = store.apply_transition(lease, "w", STUB_START)
    lease = replace(
        lease, job_version=start.job_version_after, unit_state=start.to_state
    )
    from core.stub_unit import outbox_idempotency_key

    pre_key = outbox_idempotency_key(
        unit_id=ids["unit_id"],
        effect=STUB_EFFECT,
        transition_version=lease.job_version,
    )
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO outbox (
                run_id, unit_id, channel, idempotency_key, payload, status
            )
            VALUES (%s, %s, %s, %s, %s::jsonb, 'pending')
            """,
            (
                ids["run_id"],
                ids["unit_id"],
                STUB_OUTBOX_CHANNEL,
                pre_key,
                Jsonb({"effect": "preseed"}),
            ),
        )
    store.apply_transition(lease, "w", STUB_COMPLETE)
    assert counts() == {"event_log": 2, "outbox": 1}


def test_concurrent_dispatcher_claim(db_url, seed_unit_job, conn):
    ids = seed_unit_job()
    _seed_pending_outbox(conn, ids)
    conn.commit()

    shared = StubSender()
    results: list[int] = []
    errors: list = []
    barrier = threading.Barrier(2)

    def worker() -> None:
        try:
            with psycopg.connect(db_url, row_factory=dict_row) as c:
                store = PostgresJobStore(c)
                barrier.wait(timeout=10)
                n = dispatch_batch(store, _router(stub=shared).send)
                results.append(n)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(worker), pool.submit(worker)]
        for f in futs:
            f.result(timeout=30)

    assert not errors, errors
    assert sorted(results) == [0, 1]
    assert len(shared.effects) == 1
    with psycopg.connect(db_url, row_factory=dict_row) as c:
        row = c.execute("SELECT status, external_id FROM outbox").fetchone()
    assert row["status"] == "sent"
    assert row["external_id"]


def test_send_idempotency_github_slack():
    from uuid import uuid4

    from ports.outbox_sender import OutboxMessage

    github = GitHubStubSender()
    slack = SlackStubSender()
    run_id = uuid4()
    unit_id = uuid4()
    gh_msg = OutboxMessage(
        id=1,
        run_id=run_id,
        unit_id=unit_id,
        channel="github",
        idempotency_key="unit:u:github:1",
        payload={},
        attempts=0,
    )
    first = github.send(gh_msg)
    second = github.send(gh_msg)
    assert first == second
    assert github.effects == [gh_msg.idempotency_key]

    slack_msg = OutboxMessage(
        id=2,
        run_id=run_id,
        unit_id=unit_id,
        channel="slack",
        idempotency_key="unit:u:slack:1",
        payload={},
        attempts=0,
    )
    a = slack.send(slack_msg)
    b = slack.send(slack_msg)
    assert a == b
    assert slack.effects == [slack_msg.idempotency_key]


def test_github_channel_happy_path(conn, seed_unit_job, db_url):
    ids = seed_unit_job()
    github = GitHubStubSender()
    store = _seed_pending_outbox(conn, ids, channel="github")
    conn.commit()
    assert dispatch_batch(store, _router(github=github).send) == 1
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        row = other.execute(
            "SELECT status, external_id, channel FROM outbox"
        ).fetchone()
    assert row["channel"] == "github"
    assert row["status"] == "sent"
    assert row["external_id"].startswith("github-")
    assert len(github.effects) == 1


def test_backoff_constant_shared():
    assert OUTBOX_MAX_ATTEMPTS == 10
    assert outbox_backoff_seconds(0) == 1
    assert outbox_backoff_seconds(8) == 256
    assert outbox_backoff_seconds(9) == 300
    assert outbox_backoff_seconds(20) == 300


def test_run_once_idle_and_happy(conn, seed_unit_job, db_url):
    store = PostgresJobStore(conn)
    assert run_once(store, _router().send) == 2

    ids = seed_unit_job()
    _seed_pending_outbox(conn, ids)
    conn.commit()
    assert run_once(store, _router().send) == 0
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        assert other.execute("SELECT status FROM outbox").fetchone()["status"] == "sent"


def test_outbox_main_once(conn, seed_unit_job, db_url):
    ids = seed_unit_job()
    _seed_pending_outbox(conn, ids)
    conn.commit()

    with pytest.raises(SystemExit) as exc:
        outbox_main.main(["--once"])
    assert exc.value.code == 0
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        row = other.execute("SELECT status, external_id FROM outbox").fetchone()
    assert row["status"] == "sent"
    assert row["external_id"]


def test_claim_limit_zero(conn, seed_unit_job):
    ids = seed_unit_job()
    store = _seed_pending_outbox(conn, ids)
    conn.commit()
    assert dispatch_batch(store, _router().send, limit=0) == 0
    assert dispatch_batch(store, _router().send, limit=-1) == 0
