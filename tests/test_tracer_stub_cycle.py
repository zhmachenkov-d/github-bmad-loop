"""I/O matrix: happy path, reclaim, held lease, lease lost, outbox dedupe, concurrency."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from adapters.postgres import PostgresJobStore
from apps.worker import __main__ as worker_main
from core.stub_unit import (
    STUB_COMPLETE,
    STUB_DONE,
    STUB_EFFECT,
    STUB_OUTBOX_CHANNEL,
    STUB_READY,
    STUB_RUNNING,
    STUB_START,
    IllegalTransitionError,
    LeaseLostError,
    UnitJobLease,
    outbox_idempotency_key,
)


def test_happy_path(conn, db_url, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)

    lease = store.lease_next("worker-a")
    assert lease is not None
    assert lease.unit_id == ids["unit_id"]
    assert lease.unit_state == STUB_READY

    start = store.apply_transition(lease, "worker-a", STUB_START)
    assert start.to_state == STUB_RUNNING
    lease = replace(
        lease, job_version=start.job_version_after, unit_state=start.to_state
    )
    complete = store.apply_transition(lease, "worker-a", STUB_COMPLETE)
    assert complete.from_state == STUB_RUNNING
    assert complete.to_state == STUB_DONE
    expected_key = outbox_idempotency_key(
        unit_id=ids["unit_id"],
        effect=STUB_EFFECT,
        transition_version=lease.job_version,
    )
    assert complete.idempotency_key == expected_key

    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_DONE

    events = conn.execute(
        """
        SELECT from_state, to_state, event_type, worker_id, run_id, payload
        FROM event_log ORDER BY id
        """
    ).fetchall()
    assert len(events) == 2
    assert events[0]["event_type"] == STUB_START
    assert events[1]["event_type"] == STUB_COMPLETE
    assert events[1]["payload"]["effect"] == STUB_EFFECT
    assert events[1]["payload"]["result"] is None
    assert events[1]["payload"]["job_version"] == lease.job_version
    assert events[1]["payload"]["lease_id"] == str(lease.lease_id)

    outbox = conn.execute(
        "SELECT status, idempotency_key, channel, payload FROM outbox"
    ).fetchone()
    assert outbox["status"] == "pending"
    assert outbox["idempotency_key"] == expected_key
    assert outbox["channel"] == "stub"
    assert outbox["payload"] == {
        "effect": STUB_EFFECT,
        "from_state": STUB_RUNNING,
        "to_state": STUB_DONE,
        "unit_id": str(ids["unit_id"]),
    }

    conn.commit()
    marked = store.mark_outbox_sent()
    assert marked == 1
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        sent = other.execute("SELECT status FROM outbox").fetchone()
        assert sent["status"] == "sent"
    assert counts() == {"event_log": 2, "outbox": 1}


def test_worker_run_once_exit_zero(conn, seed_unit_job, db_url):
    seed_unit_job()
    conn.commit()
    assert worker_main.run_once("worker-a") == 0
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        unit = other.execute("SELECT state FROM units").fetchone()
        assert unit["state"] == STUB_DONE


def test_worker_run_once_exit_one_on_apply_failure(conn, seed_unit_job, monkeypatch):
    seed_unit_job()
    conn.commit()

    def boom(self, lease, worker_id, event, result=None):
        raise LeaseLostError("forced apply failure")

    monkeypatch.setattr(PostgresJobStore, "apply_transition", boom)
    assert worker_main.run_once("worker-a") == 1


def test_terminal_apply_illegal_not_noop(conn, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("worker-a")
    assert lease is not None
    start = store.apply_transition(lease, "worker-a", STUB_START)
    lease = replace(
        lease, job_version=start.job_version_after, unit_state=start.to_state
    )
    store.apply_transition(lease, "worker-a", STUB_COMPLETE)
    before = counts()

    assert store.lease_next("worker-b") is None

    forged_lease_id = uuid4()
    with conn.transaction():
        conn.execute(
            """
            UPDATE jobs
            SET lease_id = %s, leased_by = %s,
                lease_expires_at = now() + interval '90 seconds',
                version = version + 1
            WHERE id = %s
            """,
            (forged_lease_id, "forger", ids["job_id"]),
        )
    job = conn.execute(
        "SELECT version, lease_id FROM jobs WHERE id = %s", (ids["job_id"],)
    ).fetchone()
    forged = UnitJobLease(
        job_id=ids["job_id"],
        run_id=ids["run_id"],
        unit_id=ids["unit_id"],
        lease_id=job["lease_id"],
        job_version=job["version"],
        unit_state=STUB_DONE,
    )
    with pytest.raises(IllegalTransitionError):
        store.apply_transition(forged, "forger", STUB_START)
    assert counts() == before


def test_empty_queue(conn, counts):
    store = PostgresJobStore(conn)
    assert store.lease_next("worker-empty") is None
    assert store.mark_outbox_sent() == 0
    assert store.mark_outbox_sent(limit=0) == 0
    assert store.mark_outbox_sent(limit=-1) == 0
    assert counts() == {"event_log": 0, "outbox": 0}


def test_held_lease_blocks_second_claim(conn, seed_unit_job):
    seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w1")
    assert lease is not None
    assert store.lease_next("w2") is None


def test_expired_lease_reclaim(conn, seed_unit_job):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    first = store.lease_next("w1")
    assert first is not None
    with conn.transaction():
        conn.execute(
            """
            UPDATE jobs
            SET lease_expires_at = now() - interval '1 second'
            WHERE id = %s
            """,
            (ids["job_id"],),
        )
    reclaimed = store.lease_next("w2")
    assert reclaimed is not None
    assert reclaimed.unit_id == ids["unit_id"]
    assert reclaimed.lease_id != first.lease_id

    start = store.apply_transition(reclaimed, "w2", STUB_START)
    lease = replace(
        reclaimed, job_version=start.job_version_after, unit_state=start.to_state
    )
    complete = store.apply_transition(lease, "w2", STUB_COMPLETE)
    assert complete.to_state == STUB_DONE


def test_lease_mismatch_raises(conn, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    bad = UnitJobLease(
        job_id=lease.job_id,
        run_id=lease.run_id,
        unit_id=lease.unit_id,
        lease_id=uuid4(),
        job_version=lease.job_version,
        unit_state=STUB_READY,
    )
    with pytest.raises(LeaseLostError, match="lease_id mismatch"):
        store.apply_transition(bad, "w", STUB_START)
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_READY
    assert counts() == {"event_log": 0, "outbox": 0}


def test_run_id_mismatch_raises(conn, seed_unit_job, counts):
    seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    bad = UnitJobLease(
        job_id=lease.job_id,
        run_id=uuid4(),
        unit_id=lease.unit_id,
        lease_id=lease.lease_id,
        job_version=lease.job_version,
        unit_state=STUB_READY,
    )
    with pytest.raises(LeaseLostError, match="run_id mismatch"):
        store.apply_transition(bad, "w", STUB_START)
    assert counts() == {"event_log": 0, "outbox": 0}


def test_outbox_on_conflict_dedupe(conn, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    assert lease.job_version == 1
    start = store.apply_transition(lease, "w", STUB_START)
    lease = replace(
        lease, job_version=start.job_version_after, unit_state=start.to_state
    )
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
    row = conn.execute(
        "SELECT payload->>'effect' AS effect FROM outbox WHERE idempotency_key = %s",
        (pre_key,),
    ).fetchone()
    assert row["effect"] == "preseed"


def test_mark_outbox_sent_stub_channel_only(conn, seed_unit_job, db_url):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    start = store.apply_transition(lease, "w", STUB_START)
    lease = replace(
        lease, job_version=start.job_version_after, unit_state=start.to_state
    )
    store.apply_transition(lease, "w", STUB_COMPLETE)
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO outbox (
                run_id, unit_id, channel, idempotency_key, payload, status
            )
            VALUES (%s, %s, 'github', %s, '{}'::jsonb, 'pending')
            """,
            (ids["run_id"], ids["unit_id"], f"unit:{ids['unit_id']}:github:1"),
        )
    conn.commit()
    marked = store.mark_outbox_sent()
    assert marked == 1
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        rows = {
            r["channel"]: r["status"]
            for r in other.execute("SELECT channel, status FROM outbox").fetchall()
        }
    assert rows["stub"] == "sent"
    assert rows["github"] == "pending"


def test_concurrent_claim(db_url, seed_unit_job, conn, counts):
    ids = seed_unit_job()
    conn.commit()

    results: list = []
    errors: list = []
    barrier = threading.Barrier(2)

    def worker(name: str) -> None:
        try:
            with psycopg.connect(db_url, row_factory=dict_row) as c:
                store = PostgresJobStore(c)
                barrier.wait(timeout=10)
                lease = store.lease_next(name)
                if lease is None:
                    results.append((name, None))
                    return
                try:
                    start = store.apply_transition(lease, name, STUB_START)
                    lease = replace(
                        lease,
                        job_version=start.job_version_after,
                        unit_state=start.to_state,
                    )
                    complete = store.apply_transition(lease, name, STUB_COMPLETE)
                    results.append((name, complete))
                except LeaseLostError as exc:
                    errors.append((name, exc))
        except Exception as exc:  # noqa: BLE001
            errors.append((name, exc))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(worker, "w1"), pool.submit(worker, "w2")]
        for f in futs:
            f.result(timeout=30)

    assert not errors, errors
    winners = [r for r in results if r[1] is not None]
    losers = [r for r in results if r[1] is None]
    assert len(winners) == 1, results
    assert len(losers) == 1, results

    with psycopg.connect(db_url, row_factory=dict_row) as c:
        unit = c.execute(
            "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
        ).fetchone()
        assert unit["state"] == STUB_DONE
        event_n = c.execute("SELECT count(*) AS n FROM event_log").fetchone()["n"]
        outbox_n = c.execute("SELECT count(*) AS n FROM outbox").fetchone()["n"]
        assert event_n == 2
        assert outbox_n == 1
