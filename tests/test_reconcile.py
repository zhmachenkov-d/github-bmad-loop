"""Reconcile tick: grace clear, stuck attempts/deadline, dead alerts, lock."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from psycopg.rows import dict_row

from adapters.postgres import PostgresJobStore
from apps.worker import __main__ as worker_main
from apps.worker.loop import run_poll_loop
from apps.worker.reconcile import run_reconcile_tick
from core.stub_unit import (
    STUB_FAILED,
    STUB_PAUSED,
    STUB_READY,
    STUB_RUNNING,
    STUCK_ALERT_EFFECT,
    STUCK_WHY_ATTEMPTS,
    STUCK_WHY_DEADLINE,
    dead_alert_idempotency_key,
    stuck_alert_idempotency_key,
)


def _stuck_alert_rows(conn):
    return conn.execute(
        """
        SELECT idempotency_key, payload
        FROM outbox
        WHERE payload->>'effect' = %s
        ORDER BY id
        """,
        (STUCK_ALERT_EFFECT,),
    ).fetchall()


def _dead_alert_rows(conn):
    return conn.execute(
        """
        SELECT idempotency_key, payload
        FROM outbox
        WHERE payload->>'effect' = 'dead_alert'
        ORDER BY id
        """
    ).fetchall()


def test_grace_force_clear_very_stale_lease(conn, seed_unit_job):
    ids = seed_unit_job(
        state=STUB_RUNNING,
        lease_expires_age="6 minutes",
        version=2,
    )
    before = conn.execute(
        "SELECT version, lease_id FROM jobs WHERE id = %s", (ids["job_id"],)
    ).fetchone()
    assert before["lease_id"] is not None
    assert before["version"] == 2

    store = PostgresJobStore(conn)
    result = run_reconcile_tick(store, "reconcile-w")
    assert result.locked is True
    assert result.grace_cleared == 1

    job = conn.execute(
        """
        SELECT lease_id, leased_by, lease_expires_at, version
        FROM jobs WHERE id = %s
        """,
        (ids["job_id"],),
    ).fetchone()
    assert job["lease_id"] is None
    assert job["leased_by"] is None
    assert job["lease_expires_at"] is None
    assert job["version"] == 3

    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_RUNNING


def test_stuck_attempts_fail_via_apply(conn, seed_unit_job, counts):
    ids = seed_unit_job(
        state=STUB_RUNNING,
        attempts=3,
        max_attempts=3,
        lease_expires_age="30 seconds",
        version=4,
    )
    store = PostgresJobStore(conn)
    result = run_reconcile_tick(store, "reconcile-w")
    assert result.locked is True
    assert result.units_failed == 1

    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_FAILED

    job = conn.execute(
        """
        SELECT lease_id, leased_by, lease_expires_at FROM jobs WHERE id = %s
        """,
        (ids["job_id"],),
    ).fetchone()
    assert job["lease_id"] is None
    assert job["leased_by"] is None
    assert job["lease_expires_at"] is None

    events = conn.execute(
        """
        SELECT event_type, payload FROM event_log WHERE unit_id = %s
        """,
        (ids["unit_id"],),
    ).fetchall()
    assert len(events) == 1
    assert events[0]["event_type"] == "stub_fail"
    assert events[0]["payload"]["result"] == STUCK_WHY_ATTEMPTS

    alerts = _stuck_alert_rows(conn)
    assert len(alerts) == 1
    assert alerts[0]["idempotency_key"] == stuck_alert_idempotency_key(
        unit_id=ids["unit_id"], pre_steal_version=4
    )
    assert alerts[0]["payload"]["why"] == STUCK_WHY_ATTEMPTS

    before = counts()
    second = run_reconcile_tick(store, "reconcile-w")
    assert second.locked is True
    assert second.units_failed == 0
    assert counts() == before
    assert len(_stuck_alert_rows(conn)) == 1
    assert (
        conn.execute(
            "SELECT count(*) AS n FROM event_log WHERE unit_id = %s",
            (ids["unit_id"],),
        ).fetchone()["n"]
        == 1
    )


def test_stuck_deadline_fail_via_apply(conn, seed_unit_job):
    past = datetime.now(UTC) - timedelta(minutes=1)
    ids = seed_unit_job(
        state=STUB_RUNNING,
        attempts=0,
        max_attempts=3,
        deadline_at=past,
        lease_expires_age="10 seconds",
        version=1,
    )
    store = PostgresJobStore(conn)
    result = run_reconcile_tick(store, "reconcile-w")
    assert result.units_failed == 1

    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_FAILED

    event = conn.execute(
        "SELECT payload FROM event_log WHERE unit_id = %s", (ids["unit_id"],)
    ).fetchone()
    assert event["payload"]["result"] == STUCK_WHY_DEADLINE

    alerts = _stuck_alert_rows(conn)
    assert len(alerts) == 1
    assert alerts[0]["payload"]["why"] == STUCK_WHY_DEADLINE

    before_events = conn.execute("SELECT count(*) AS n FROM event_log").fetchone()["n"]
    before_alerts = len(_stuck_alert_rows(conn))
    run_reconcile_tick(store, "reconcile-w")
    assert (
        conn.execute("SELECT count(*) AS n FROM event_log").fetchone()["n"]
        == before_events
    )
    assert len(_stuck_alert_rows(conn)) == before_alerts


def test_stuck_prefers_attempts_when_both_breach(conn, seed_unit_job):
    past = datetime.now(UTC) - timedelta(minutes=1)
    ids = seed_unit_job(
        state=STUB_RUNNING,
        attempts=5,
        max_attempts=3,
        deadline_at=past,
        version=0,
    )
    store = PostgresJobStore(conn)
    run_reconcile_tick(store, "reconcile-w")
    event = conn.execute(
        "SELECT payload FROM event_log WHERE unit_id = %s", (ids["unit_id"],)
    ).fetchone()
    assert event["payload"]["result"] == STUCK_WHY_ATTEMPTS


@pytest.mark.parametrize("state", [STUB_READY, STUB_PAUSED])
def test_non_running_breach_no_stuck_apply(conn, seed_unit_job, counts, state):
    past = datetime.now(UTC) - timedelta(minutes=1)
    ids = seed_unit_job(
        state=state,
        attempts=9,
        max_attempts=3,
        deadline_at=past,
    )
    store = PostgresJobStore(conn)
    result = run_reconcile_tick(store, "reconcile-w")
    assert result.units_failed == 0
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == state
    assert counts()["event_log"] == 0
    assert _stuck_alert_rows(conn) == []


def test_dead_outbox_alert_idempotent(conn, seed_dead_outbox):
    ids = seed_dead_outbox()
    store = PostgresJobStore(conn)
    first = run_reconcile_tick(store, "reconcile-w")
    assert first.dead_alerts == 1
    alerts = _dead_alert_rows(conn)
    assert len(alerts) == 1
    expected = dead_alert_idempotency_key(dead_outbox_id=ids["dead_outbox_id"])
    assert alerts[0]["idempotency_key"] == expected

    dead = conn.execute(
        "SELECT status FROM outbox WHERE id = %s", (ids["dead_outbox_id"],)
    ).fetchone()
    assert dead["status"] == "dead"

    second = run_reconcile_tick(store, "reconcile-w")
    assert second.dead_alerts == 0
    assert len(_dead_alert_rows(conn)) == 1


def test_reconcile_lock_contention(conn, seed_unit_job, db_url):
    ids = seed_unit_job(
        state=STUB_RUNNING,
        attempts=3,
        max_attempts=3,
        version=2,
    )
    conn.commit()

    barrier = threading.Barrier(2)
    results: list = []

    def _tick(worker: str) -> None:
        with psycopg.connect(db_url, row_factory=dict_row) as c:
            store = PostgresJobStore(c)
            barrier.wait(timeout=5)
            results.append(run_reconcile_tick(store, worker))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(_tick, f"w-{i}") for i in range(2)]
        for f in futs:
            f.result(timeout=30)

    locked = [r for r in results if r.locked]
    missed = [r for r in results if not r.locked]
    assert len(locked) == 1
    assert len(missed) == 1
    assert locked[0].units_failed == 1

    with psycopg.connect(db_url, row_factory=dict_row) as c:
        unit = c.execute(
            "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
        ).fetchone()
        assert unit["state"] == STUB_FAILED
        events = c.execute(
            "SELECT count(*) AS n FROM event_log WHERE unit_id = %s",
            (ids["unit_id"],),
        ).fetchone()["n"]
        assert events == 1
        alerts = c.execute(
            """
            SELECT count(*) AS n FROM outbox
            WHERE payload->>'effect' = %s
            """,
            (STUCK_ALERT_EFFECT,),
        ).fetchone()["n"]
        assert alerts == 1


def test_continuous_reconcile_does_not_block_lease(conn, seed_unit_job, monkeypatch):
    """Lease dispatch still works while reconcile is composed into the poll loop."""
    seed_unit_job(state=STUB_READY)
    store = PostgresJobStore(conn)
    called = {"reconcile": False, "dispatch_ok": False}

    def fake_reconcile(store_arg, worker_id):
        called["reconcile"] = True
        from apps.worker.reconcile import ReconcileTickResult

        return ReconcileTickResult(locked=True)

    monkeypatch.setattr("apps.worker.loop.run_reconcile_tick", fake_reconcile)

    stop = {"n": 0}
    real_dispatch = None

    from apps.worker import loop as loop_mod

    real_dispatch = loop_mod.dispatch

    def dispatch_once(store_arg, lease, worker_id):
        code = real_dispatch(store_arg, lease, worker_id)
        called["dispatch_ok"] = code == 0
        stop["n"] += 1
        raise KeyboardInterrupt

    monkeypatch.setattr(loop_mod, "dispatch", dispatch_once)
    code = run_poll_loop(store, "w", once=False, reconcile_interval=0.0)
    assert code == 0
    assert called["reconcile"] is True
    assert called["dispatch_ok"] is True


def test_reconcile_once_cli(conn, seed_unit_job, monkeypatch, db_url):
    seed_unit_job(
        state=STUB_RUNNING,
        attempts=3,
        max_attempts=3,
        version=1,
    )
    conn.commit()

    monkeypatch.setenv("DATABASE_URL", db_url)
    monkeypatch.setenv("WORKER_ID", "cli-reconcile")
    assert worker_main.reconcile_once("cli-reconcile") == 0

    with psycopg.connect(db_url, row_factory=dict_row) as c:
        unit = c.execute("SELECT state FROM units").fetchone()
        assert unit["state"] == STUB_FAILED


def test_grace_does_not_clear_fresh_expiry(conn, seed_unit_job):
    ids = seed_unit_job(
        state=STUB_RUNNING,
        lease_expires_age="1 minute",
        version=1,
    )
    store = PostgresJobStore(conn)
    result = run_reconcile_tick(store, "reconcile-w")
    assert result.grace_cleared == 0
    job = conn.execute(
        "SELECT lease_id, version FROM jobs WHERE id = %s", (ids["job_id"],)
    ).fetchone()
    assert job["lease_id"] is not None
    assert job["version"] == 1


def test_grace_skips_stuck_eligible_very_stale_lease(conn, seed_unit_job):
    """Grace must not strip leases that the stuck step needs to steal."""
    ids = seed_unit_job(
        state=STUB_RUNNING,
        attempts=3,
        max_attempts=3,
        lease_expires_age="6 minutes",
        version=2,
    )
    store = PostgresJobStore(conn)
    result = run_reconcile_tick(store, "reconcile-w")
    assert result.grace_cleared == 0
    assert result.units_failed == 1
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_FAILED
    event = conn.execute(
        "SELECT payload FROM event_log WHERE unit_id = %s", (ids["unit_id"],)
    ).fetchone()
    assert event["payload"]["result"] == STUCK_WHY_ATTEMPTS


def test_grace_clears_paused_with_breach_and_very_stale_lease(conn, seed_unit_job):
    """Non-running non-terminal work still gets grace; stuck only handles running."""
    ids = seed_unit_job(
        state=STUB_PAUSED,
        attempts=3,
        max_attempts=3,
        lease_expires_age="6 minutes",
        version=2,
    )
    store = PostgresJobStore(conn)
    result = run_reconcile_tick(store, "reconcile-w")
    assert result.locked is True
    assert result.grace_cleared == 1
    assert result.units_failed == 0
    job = conn.execute(
        """
        SELECT lease_id, leased_by, lease_expires_at, version
        FROM jobs WHERE id = %s
        """,
        (ids["job_id"],),
    ).fetchone()
    assert job["lease_id"] is None
    assert job["leased_by"] is None
    assert job["lease_expires_at"] is None
    assert job["version"] == 3
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_PAUSED


def test_reconcile_exception_keeps_poll_loop_alive(
    conn, seed_unit_job, monkeypatch, capsys
):
    seed_unit_job(state=STUB_READY)
    store = PostgresJobStore(conn)

    def boom(_store, _worker_id):
        raise RuntimeError("reconcile boom")

    monkeypatch.setattr("apps.worker.loop.run_reconcile_tick", boom)

    from apps.worker import loop as loop_mod

    real_dispatch = loop_mod.dispatch

    def dispatch_once(store_arg, lease, worker_id):
        real_dispatch(store_arg, lease, worker_id)
        raise KeyboardInterrupt

    monkeypatch.setattr(loop_mod, "dispatch", dispatch_once)
    code = run_poll_loop(store, "w", once=False, reconcile_interval=0.0)
    assert code == 0
    err = capsys.readouterr().err
    assert "reconcile tick failed: reconcile boom" in err
