"""I/O matrix: happy path, reclaim, held lease, lease lost, outbox dedupe, concurrency."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import psycopg
import pytest
from adapters.postgres import PostgresJobStore
from core.stub_unit import (
    STUB_DONE,
    STUB_EVENT_TYPE,
    STUB_OUTBOX_CHANNEL,
    STUB_READY,
    LeaseLostError,
    UnitJobLease,
    outbox_idempotency_key,
)
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


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
    expected_key = f"unit:{ids['unit_id']}:stub_transition:{lease.job_version}"
    assert result.idempotency_key == expected_key
    assert result.idempotency_key == outbox_idempotency_key(
        unit_id=ids["unit_id"],
        transition_version=lease.job_version,
    )

    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_DONE

    event = conn.execute(
        "SELECT from_state, to_state, event_type, worker_id, run_id FROM event_log"
    ).fetchone()
    assert event["from_state"] == STUB_READY
    assert event["to_state"] == STUB_DONE
    assert event["event_type"] == STUB_EVENT_TYPE
    assert event["worker_id"] == "worker-a"
    assert event["run_id"] == ids["run_id"]

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

    # Even with a fresh lease row forged on a done unit, apply stays no-op and clears lease.
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
    again = store.apply_stub_transition(forged, "worker-forced")
    assert again.applied is False
    assert counts() == before
    cleared = conn.execute(
        "SELECT lease_id, leased_by, lease_expires_at FROM jobs WHERE id = %s",
        (ids["job_id"],),
    ).fetchone()
    assert cleared["lease_id"] is None
    assert cleared["leased_by"] is None
    assert cleared["lease_expires_at"] is None


def test_empty_queue(conn, counts):
    store = PostgresJobStore(conn)
    assert store.lease_next_unit_job("worker-empty") is None
    assert store.mark_outbox_sent() == 0
    assert store.mark_outbox_sent(limit=0) == 0
    assert store.mark_outbox_sent(limit=-1) == 0
    assert counts() == {"event_log": 0, "outbox": 0}


def test_held_lease_blocks_second_claim(conn, seed_unit_job):
    seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next_unit_job("w1")
    assert lease is not None
    assert store.lease_next_unit_job("w2") is None


def test_expired_lease_reclaim(conn, seed_unit_job):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    first = store.lease_next_unit_job("w1")
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
    reclaimed = store.lease_next_unit_job("w2")
    assert reclaimed is not None
    assert reclaimed.unit_id == ids["unit_id"]
    assert reclaimed.lease_id != first.lease_id

    result = store.apply_stub_transition(reclaimed, "w2")
    assert result.applied is True


def test_lease_mismatch_raises(conn, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next_unit_job("w")
    assert lease is not None
    bad = UnitJobLease(
        job_id=lease.job_id,
        run_id=lease.run_id,
        unit_id=lease.unit_id,
        lease_id=uuid4(),
        job_version=lease.job_version,
        unit_state=STUB_READY,
    )
    with pytest.raises(LeaseLostError, match="job version or lease mismatch"):
        store.apply_stub_transition(bad, "w")
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_READY
    assert counts() == {"event_log": 0, "outbox": 0}


def test_run_id_mismatch_raises(conn, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next_unit_job("w")
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
        store.apply_stub_transition(bad, "w")
    assert counts() == {"event_log": 0, "outbox": 0}


def test_outbox_on_conflict_dedupe(conn, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    # Version after lease will be 1 (seed inserts version 0, lease bumps).
    pre_key = outbox_idempotency_key(unit_id=ids["unit_id"], transition_version=1)
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
    lease = store.lease_next_unit_job("w")
    assert lease is not None
    assert lease.job_version == 1
    result = store.apply_stub_transition(lease, "w")
    assert result.applied is True
    assert counts() == {"event_log": 1, "outbox": 1}
    row = conn.execute(
        "SELECT payload->>'effect' AS effect FROM outbox WHERE idempotency_key = %s",
        (pre_key,),
    ).fetchone()
    assert row["effect"] == "preseed"


def test_mark_outbox_sent_stub_channel_only(conn, seed_unit_job, db_url):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next_unit_job("w")
    assert lease is not None
    store.apply_stub_transition(lease, "w")
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
    winners = [
        r for r in results if r[1] is not None and getattr(r[1], "applied", False)
    ]
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
