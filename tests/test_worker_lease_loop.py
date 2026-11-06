"""Worker lease loop: heartbeat, reclaim, run no-op, SKIP LOCKED, fleet 2×100."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from adapters.postgres import PostgresJobStore
from apps.worker import __main__ as worker_main
from apps.worker.loop import claim_and_dispatch, handle_unit, run_poll_loop
from core.stub_unit import (
    JOB_KIND_RUN,
    JOB_KIND_UNIT,
    STUB_COMPLETE,
    STUB_DONE,
    STUB_EFFECT,
    STUB_FAILED,
    STUB_PAUSED,
    STUB_READY,
    STUB_RUNNING,
    STUB_START,
)

ROOT = Path(__file__).resolve().parents[1]


def test_heartbeat_extends_lease(conn, seed_unit_job):
    seed_unit_job()
    store = PostgresJobStore(conn, lease_ttl=timedelta(seconds=30))
    lease = store.lease_next("w")
    assert lease is not None
    before = conn.execute(
        "SELECT lease_expires_at FROM jobs WHERE id = %s", (lease.job_id,)
    ).fetchone()["lease_expires_at"]
    assert store.heartbeat(lease.job_id, lease.lease_id, "w") is True
    after = conn.execute(
        "SELECT lease_expires_at FROM jobs WHERE id = %s", (lease.job_id,)
    ).fetchone()["lease_expires_at"]
    assert after > before


def test_heartbeat_loss_mismatch_worker(conn, seed_unit_job):
    seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    assert store.heartbeat(lease.job_id, lease.lease_id, "other") is False


def test_heartbeat_loss_expired(conn, seed_unit_job):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    with conn.transaction():
        conn.execute(
            """
            UPDATE jobs
            SET lease_expires_at = now() - interval '1 second'
            WHERE id = %s
            """,
            (ids["job_id"],),
        )
    assert store.heartbeat(lease.job_id, lease.lease_id, "w") is False


def test_heartbeat_loss_stops_handler_zero_further_writes(
    conn, seed_unit_job, counts, monkeypatch
):
    seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None

    calls: list[str] = []
    real_apply = store.apply_transition

    def tracking_apply(lease_arg, worker_id, event, result=None):
        calls.append(event)
        return real_apply(lease_arg, worker_id, event, result)

    monkeypatch.setattr(store, "apply_transition", tracking_apply)
    monkeypatch.setattr(store, "heartbeat", lambda *a, **k: False)

    code = handle_unit(store, lease, "w")
    assert code == 1
    assert calls == [STUB_START]
    assert counts() == {"event_log": 1, "outbox": 0}
    unit = conn.execute("SELECT state FROM units").fetchone()
    assert unit["state"] == STUB_RUNNING


def test_release_clears_matching_lease(conn, seed_unit_job):
    seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    assert store.release(lease.job_id, lease.lease_id) is True
    job = conn.execute(
        "SELECT lease_id, leased_by, lease_expires_at FROM jobs WHERE id = %s",
        (lease.job_id,),
    ).fetchone()
    assert job["lease_id"] is None
    assert job["leased_by"] is None
    assert job["lease_expires_at"] is None


def test_release_mismatch_no_clear(conn, seed_unit_job):
    seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    assert store.release(lease.job_id, uuid4()) is False
    job = conn.execute(
        "SELECT lease_id FROM jobs WHERE id = %s", (lease.job_id,)
    ).fetchone()
    assert job["lease_id"] == lease.lease_id


@pytest.mark.parametrize("state", [STUB_PAUSED, STUB_DONE, STUB_FAILED])
def test_non_claimable_unit_states_skipped(conn, seed_unit_job, state):
    seed_unit_job(state=state)
    store = PostgresJobStore(conn)
    assert store.lease_next("w") is None


def test_mid_cycle_reclaim_complete_only(conn, seed_unit_job, counts):
    ids = seed_unit_job(state=STUB_RUNNING)
    lease_id = uuid4()
    with conn.transaction():
        conn.execute(
            """
            UPDATE jobs
            SET lease_id = %s,
                leased_by = %s,
                lease_expires_at = now() - interval '1 second',
                version = 1
            WHERE id = %s
            """,
            (lease_id, "dead-worker", ids["job_id"]),
        )
    # Pre-seed one stub_start event as if the killed worker had started.
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO event_log (
                run_id, unit_id, from_state, to_state, event_type, worker_id, payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, '{}'::jsonb)
            """,
            (
                ids["run_id"],
                ids["unit_id"],
                STUB_READY,
                STUB_RUNNING,
                STUB_START,
                "dead-worker",
            ),
        )

    store = PostgresJobStore(conn)
    code = claim_and_dispatch(store, "reclaimer")
    assert code == 0

    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_DONE

    starts = conn.execute(
        "SELECT count(*) AS n FROM event_log WHERE event_type = %s AND unit_id = %s",
        (STUB_START, ids["unit_id"]),
    ).fetchone()["n"]
    completes = conn.execute(
        "SELECT count(*) AS n FROM event_log WHERE event_type = %s AND unit_id = %s",
        (STUB_COMPLETE, ids["unit_id"]),
    ).fetchone()["n"]
    effects = conn.execute(
        "SELECT count(*) AS n FROM outbox WHERE payload->>'effect' = %s",
        (STUB_EFFECT,),
    ).fetchone()["n"]
    assert starts == 1
    assert completes == 1
    assert effects == 1
    assert counts() == {"event_log": 2, "outbox": 1}


def test_run_noop_release_no_unit_side_effects(conn, seed_run_job, counts):
    ids = seed_run_job()
    store = PostgresJobStore(conn)
    code = claim_and_dispatch(store, "w")
    assert code == 0
    job = conn.execute(
        "SELECT lease_id, leased_by, lease_expires_at, kind FROM jobs WHERE id = %s",
        (ids["job_id"],),
    ).fetchone()
    assert job["kind"] == JOB_KIND_RUN
    assert job["lease_id"] is None
    assert job["leased_by"] is None
    assert job["lease_expires_at"] is None
    assert counts() == {"event_log": 0, "outbox": 0}
    units_n = conn.execute("SELECT count(*) AS n FROM units").fetchone()["n"]
    assert units_n == 0


def test_lease_next_returns_run_kind(conn, seed_run_job):
    ids = seed_run_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    assert lease.kind == JOB_KIND_RUN
    assert lease.job_id == ids["job_id"]
    assert lease.run_id == ids["run_id"]


def test_run_job_does_not_block_unit_fleet_path(conn, seed_run_job, seed_unit_job):
    seed_run_job()
    unit_ids = seed_unit_job()
    store = PostgresJobStore(conn)
    seen_kinds: list[str] = []
    for _ in range(2):
        lease = store.lease_next("w")
        assert lease is not None
        seen_kinds.append(lease.kind)
        if lease.kind == JOB_KIND_RUN:
            assert store.release(lease.job_id, lease.lease_id)
        else:
            from dataclasses import replace

            start = store.apply_transition(lease, "w", STUB_START)
            lease = replace(
                lease, job_version=start.job_version_after, unit_state=start.to_state
            )
            store.apply_transition(lease, "w", STUB_COMPLETE)
    assert seen_kinds == [JOB_KIND_RUN, JOB_KIND_UNIT]
    assert store.lease_next("w") is None  # released run not re-claimed
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (unit_ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_DONE


def test_run_crash_mid_hold_is_reclaimable(conn, seed_run_job):
    ids = seed_run_job()
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
    assert reclaimed.job_id == ids["job_id"]
    assert store.release(reclaimed.job_id, reclaimed.lease_id)
    assert store.lease_next("w3") is None


def test_concurrent_lease_next_skip_locked(db_url, seed_unit_job, conn):
    ids = seed_unit_job()
    conn.commit()

    results: list = []
    barrier = threading.Barrier(2)

    def worker(name: str) -> None:
        with psycopg.connect(db_url, row_factory=dict_row) as c:
            store = PostgresJobStore(c)
            barrier.wait(timeout=10)
            lease = store.lease_next(name)
            results.append((name, lease.job_id if lease else None))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(worker, "w1"), pool.submit(worker, "w2")]
        for f in futs:
            f.result(timeout=30)

    winners = [r for r in results if r[1] is not None]
    losers = [r for r in results if r[1] is None]
    assert len(winners) == 1, results
    assert len(losers) == 1, results
    assert winners[0][1] == ids["job_id"]


def test_once_flag_exits_after_one_dispatch(conn, seed_fleet_unit_jobs, db_url):
    seed_fleet_unit_jobs(3)
    conn.commit()
    assert worker_main.run_once("once-w") == 0
    with psycopg.connect(db_url, row_factory=dict_row) as other:
        done = other.execute(
            "SELECT count(*) AS n FROM units WHERE state = %s", (STUB_DONE,)
        ).fetchone()["n"]
        ready = other.execute(
            "SELECT count(*) AS n FROM units WHERE state = %s", (STUB_READY,)
        ).fetchone()["n"]
    assert done == 1
    assert ready == 2


def test_poll_loop_once_idle(conn):
    store = PostgresJobStore(conn)
    assert run_poll_loop(store, "w", once=True) == 2


def test_fleet_two_workers_100_units(db_url, seed_fleet_unit_jobs, conn):
    seed_fleet_unit_jobs(100)
    conn.commit()

    env = {
        **os.environ,
        "DATABASE_URL": db_url,
        "PYTHONPATH": str(ROOT / "src"),
    }
    procs = [
        subprocess.Popen(
            [sys.executable, "-m", "apps.worker"],
            cwd=ROOT,
            env={**env, "WORKER_ID": f"fleet-w{i}"},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for i in range(2)
    ]

    deadline = time.time() + 120
    try:
        while time.time() < deadline:
            with psycopg.connect(db_url, row_factory=dict_row) as c:
                done = c.execute(
                    "SELECT count(*) AS n FROM units WHERE state = %s", (STUB_DONE,)
                ).fetchone()["n"]
                if done == 100:
                    break
            time.sleep(0.2)
        else:
            pytest.fail("fleet did not reach 100 stub_done within timeout")
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()

    with psycopg.connect(db_url, row_factory=dict_row) as c:
        done = c.execute(
            "SELECT count(*) AS n FROM units WHERE state = %s", (STUB_DONE,)
        ).fetchone()["n"]
        starts = c.execute(
            "SELECT count(*) AS n FROM event_log WHERE event_type = %s", (STUB_START,)
        ).fetchone()["n"]
        completes = c.execute(
            "SELECT count(*) AS n FROM event_log WHERE event_type = %s",
            (STUB_COMPLETE,),
        ).fetchone()["n"]
        effects = c.execute(
            "SELECT count(*) AS n FROM outbox WHERE payload->>'effect' = %s",
            (STUB_EFFECT,),
        ).fetchone()["n"]
        # Duplicate completions would show as >100 completes or >100 effects.
        per_unit_starts = c.execute(
            """
            SELECT unit_id, count(*) AS n
            FROM event_log
            WHERE event_type = %s
            GROUP BY unit_id
            HAVING count(*) > 1
            """,
            (STUB_START,),
        ).fetchall()
        per_unit_effects = c.execute(
            """
            SELECT unit_id, count(*) AS n
            FROM outbox
            WHERE payload->>'effect' = %s
            GROUP BY unit_id
            HAVING count(*) > 1
            """,
            (STUB_EFFECT,),
        ).fetchall()

    assert done == 100
    assert starts == 100
    assert completes == 100
    assert effects == 100
    assert per_unit_starts == []
    assert per_unit_effects == []
