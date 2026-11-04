"""I/O matrix: happy path, idempotent reclaim, empty queue, concurrent claim."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row

from adapters.postgres import PostgresJobStore
from core.stub_unit import (
    STUB_DONE,
    STUB_EVENT_TYPE,
    STUB_READY,
    LeaseLostError,
    outbox_idempotency_key,
)


def test_happy_path(conn, db_url, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)

    lease = store.lease_next_unit_job("worker-a")
    assert lease is not None
    assert lease.unit_id == ids["unit_id"]
    assert lease.unit_state == STUB_READY

    result = store.apply_stub_transition(lease, "worker-a")
    assert result.applied is True
    assert result.from_state == STUB_READY
    assert result.to_state == STUB_DONE
    expected_key = outbox_idempotency_key(
        unit_id=ids["unit_id"],
        transition_version=lease.job_version,
    )
    assert result.idempotency_key == expected_key

    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_DONE

    event = conn.execute(
        "SELECT from_state, to_state, event_type, worker_id FROM event_log"
    ).fetchone()
    assert event["from_state"] == STUB_READY
    assert event["to_state"] == STUB_DONE
    assert event["event_type"] == STUB_EVENT_TYPE
    assert event["worker_id"] == "worker-a"

    outbox = conn.execute(
        "SELECT status, idempotency_key, channel FROM outbox"
    ).fetchone()
    assert outbox["status"] == "pending"
    assert outbox["idempotency_key"] == expected_key
    assert outbox["channel"] == "stub"

    # End implicit txn from SELECTs so mark_outbox_sent commits durably.
    conn.commit()
    marked = store.mark_outbox_sent()
    assert marked == 1
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        sent = other.execute("SELECT status FROM outbox").fetchone()
        assert sent["status"] == "sent"
    assert counts() == {"event_log": 1, "outbox": 1}


def test_idempotent_reclaim(conn, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)

    lease = store.lease_next_unit_job("worker-a")
    assert lease is not None
    store.apply_stub_transition(lease, "worker-a")
    store.mark_outbox_sent()
    before = counts()

    # Second claim: terminal unit → no actionable job.
    assert store.lease_next_unit_job("worker-b") is None

    # Forced apply against a fabricated lease after clear: no-op on stub_done.
    forced = store.apply_stub_transition(lease, "worker-forced")
    assert forced.applied is False
    assert counts() == before

    # Even with a fresh lease row forged on a done unit, apply stays no-op.
    with conn.transaction():
        conn.execute(
            """
            UPDATE jobs
            SET lease_id = %s, leased_by = %s,
                lease_expires_at = now() + interval '90 seconds',
                version = version + 1
            WHERE id = %s
            """,
            (uuid4(), "forger", ids["job_id"]),
        )
    job = conn.execute(
        "SELECT version, lease_id FROM jobs WHERE id = %s", (ids["job_id"],)
    ).fetchone()
    from core.stub_unit import UnitJobLease

    forged = UnitJobLease(
        job_id=ids["job_id"],
        run_id=ids["run_id"],
        unit_id=ids["unit_id"],
        lease_id=job["lease_id"],
        job_version=job["version"],
        unit_state=STUB_DONE,
    )
    again = store.apply_stub_transition(forged, "worker-forced")
    assert again.applied is False
    assert counts() == before


def test_empty_queue(conn, counts):
    store = PostgresJobStore(conn)
    assert store.lease_next_unit_job("worker-empty") is None
    assert store.mark_outbox_sent() == 0
    assert counts() == {"event_log": 0, "outbox": 0}


def test_concurrent_claim(db_url, seed_unit_job, conn, counts):
    # seed via session conn, then race on separate connections
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
                lease = store.lease_next_unit_job(name)
                if lease is None:
                    results.append((name, None))
                    return
                try:
                    applied = store.apply_stub_transition(lease, name)
                    results.append((name, applied))
                except LeaseLostError as exc:
                    errors.append((name, exc))
        except Exception as exc:  # noqa: BLE001
            errors.append((name, exc))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(worker, "w1"), pool.submit(worker, "w2")]
        for f in futs:
            f.result(timeout=30)

    assert not errors, errors
    winners = [r for r in results if r[1] is not None and getattr(r[1], "applied", False)]
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
        assert event_n == 1
        assert outbox_n == 1
