"""Cross-cutting restart/failure suite: process kill, outbox, reconcile, last stop.

Fail-closed: missing dispatcher or reconcile modules error at import/collection
(no skip/xfail). One recovery path each — not the story 3/4/5 full matrices.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest
from psycopg.rows import dict_row

from adapters.outbox.router import ChannelRouter
from adapters.outbox.senders import StubSender
from adapters.postgres import PostgresJobStore
from apps.outbox.dispatcher import dispatch_batch
from apps.worker.reconcile import run_reconcile_tick
from core.stub_unit import (
    STUB_COMPLETE,
    STUB_DONE,
    STUB_EFFECT,
    STUB_FAILED,
    STUB_RUNNING,
    STUB_START,
    STUCK_ALERT_EFFECT,
    STUCK_WHY_ATTEMPTS,
    STUCK_WHY_DEADLINE,
    dead_alert_idempotency_key,
)
from ports.outbox_sender import TransientError

ROOT = Path(__file__).resolve().parents[1]


def _router(*, stub: StubSender | None = None) -> ChannelRouter:
    return ChannelRouter({"stub": stub or StubSender()})


def _last_stop(conn, unit_id):
    return conn.execute(
        """
        SELECT event_type, payload
        FROM event_log
        WHERE unit_id = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (unit_id,),
    ).fetchone()


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


def test_process_kill_mid_hold_peer_reclaims(db_url, seed_unit_job, conn):
    """Real worker SIGKILL mid-hold; peer --once after short TTL → stub_done once."""
    ids = seed_unit_job()
    conn.commit()

    lease_ttl = 3
    env = {
        **os.environ,
        "DATABASE_URL": db_url,
        "PYTHONPATH": str(ROOT / "src"),
        "WORKER_ID": "kill-victim",
        "LEASE_TTL_SECONDS": str(lease_ttl),
        "BMAD_WORKER_HOLD_SECONDS": "30",
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "apps.worker"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        deadline = time.time() + 20
        while time.time() < deadline:
            if proc.poll() is not None:
                err = (proc.stderr.read() if proc.stderr else b"").decode()
                pytest.fail(f"victim exited early rc={proc.returncode} stderr={err!r}")
            with psycopg.connect(db_url, row_factory=dict_row) as c:
                row = c.execute(
                    """
                    SELECT u.state, j.lease_id, j.leased_by, j.lease_expires_at
                    FROM units u
                    JOIN jobs j ON j.ref_id = u.id AND j.kind = 'unit'
                    WHERE u.id = %s
                    """,
                    (ids["unit_id"],),
                ).fetchone()
                if (
                    row
                    and row["state"] == STUB_RUNNING
                    and row["lease_id"] is not None
                    and row["leased_by"] == "kill-victim"
                    and row["lease_expires_at"] is not None
                ):
                    break
            time.sleep(0.05)
        else:
            pytest.fail("unit never reached stub_running under live victim lease")

        proc.send_signal(signal.SIGKILL)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)

        # Wait out victim TTL, then peer --once (or any concurrent worker) reclaims.
        time.sleep(lease_ttl + 1)
        peer_env = {
            **os.environ,
            "DATABASE_URL": db_url,
            "PYTHONPATH": str(ROOT / "src"),
            "WORKER_ID": "kill-peer",
            "LEASE_TTL_SECONDS": str(lease_ttl),
        }
        reclaim_deadline = time.time() + 30
        while time.time() < reclaim_deadline:
            with psycopg.connect(db_url, row_factory=dict_row) as c:
                state = c.execute(
                    "SELECT state FROM units WHERE id = %s",
                    (ids["unit_id"],),
                ).fetchone()["state"]
                if state == STUB_DONE:
                    break
            peer = subprocess.run(
                [sys.executable, "-m", "apps.worker", "--once"],
                cwd=ROOT,
                env=peer_env,
                capture_output=True,
                text=True,
                check=False,
            )
            if peer.returncode == 0:
                break
            if peer.returncode not in (0, 2):
                pytest.fail(
                    f"peer --once error rc={peer.returncode} stderr={peer.stderr!r}"
                )
            time.sleep(0.2)
        else:
            pytest.fail("unit did not reach stub_done after victim kill + reclaim")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)

    with psycopg.connect(db_url, row_factory=dict_row) as c:
        unit = c.execute(
            "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
        ).fetchone()
        starts = c.execute(
            """
            SELECT count(*) AS n FROM event_log
            WHERE unit_id = %s AND event_type = %s
            """,
            (ids["unit_id"], STUB_START),
        ).fetchone()["n"]
        effects = c.execute(
            """
            SELECT count(*) AS n FROM outbox
            WHERE unit_id = %s AND payload->>'effect' = %s
            """,
            (ids["unit_id"], STUB_EFFECT),
        ).fetchone()["n"]
        completes = c.execute(
            """
            SELECT count(*) AS n FROM event_log
            WHERE unit_id = %s AND event_type = %s
            """,
            (ids["unit_id"], STUB_COMPLETE),
        ).fetchone()["n"]

    assert unit["state"] == STUB_DONE
    assert starts == 1
    assert effects == 1
    assert completes == 1


def test_outbox_transient_then_sent(conn, seed_unit_job, db_url):
    """One TransientError via fail_next → retry → sent + external_id."""
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    start = store.apply_transition(lease, "w", STUB_START)
    lease = replace(
        lease, job_version=start.job_version_after, unit_state=start.to_state
    )
    store.apply_transition(lease, "w", STUB_COMPLETE)
    conn.commit()

    stub = StubSender()
    stub.fail_next(TransientError("boom"))
    assert dispatch_batch(store, _router(stub=stub).send) == 1

    with psycopg.connect(db_url, row_factory=dict_row) as other:
        row = other.execute(
            "SELECT status, attempts FROM outbox WHERE unit_id = %s",
            (ids["unit_id"],),
        ).fetchone()
    assert row["status"] == "pending"
    assert row["attempts"] == 1

    with store.connection.transaction():
        store.connection.execute(
            "UPDATE outbox SET next_attempt_at = now() - interval '1 second'"
        )
    stub_ok = StubSender()
    assert dispatch_batch(store, _router(stub=stub_ok).send) == 1

    with psycopg.connect(db_url, row_factory=dict_row) as other:
        row = other.execute(
            "SELECT status, external_id FROM outbox WHERE unit_id = %s",
            (ids["unit_id"],),
        ).fetchone()
    assert row["status"] == "sent"
    assert row["external_id"]


def test_reconcile_grace_clears_stale_lease(conn, seed_unit_job):
    """Lease >5m past expiry, non-stuck: clear lease + version bump; state unchanged."""
    ids = seed_unit_job(
        state=STUB_RUNNING,
        lease_expires_age="6 minutes",
        version=2,
    )
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


def test_reconcile_stuck_and_last_stop(conn, seed_unit_job, counts):
    """Attempts stuck → stub_failed + one stub_fail why + one stuck-alert; 2nd idle."""
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

    last = _last_stop(conn, ids["unit_id"])
    assert last["event_type"] == "stub_fail"
    why = last["payload"]["result"]
    # Distinguishable criterion: attempts vs deadline (this path is attempts).
    assert why in {STUCK_WHY_ATTEMPTS, STUCK_WHY_DEADLINE}
    assert why == STUCK_WHY_ATTEMPTS

    alerts = _stuck_alert_rows(conn)
    assert len(alerts) == 1
    assert alerts[0]["payload"]["why"] == STUCK_WHY_ATTEMPTS

    before = counts()
    second = run_reconcile_tick(store, "reconcile-w")
    assert second.locked is True
    assert second.units_failed == 0
    assert counts() == before
    assert len(_stuck_alert_rows(conn)) == 1


def test_reconcile_dead_outbox_alert_idempotent(conn, seed_dead_outbox):
    """Story-4-shaped dead outbox → one dead-alert; second tick adds none."""
    ids = seed_dead_outbox()
    store = PostgresJobStore(conn)
    first = run_reconcile_tick(store, "reconcile-w")
    assert first.dead_alerts == 1
    alerts = _dead_alert_rows(conn)
    assert len(alerts) == 1
    assert alerts[0]["idempotency_key"] == dead_alert_idempotency_key(
        dead_outbox_id=ids["dead_outbox_id"]
    )

    second = run_reconcile_tick(store, "reconcile-w")
    assert second.dead_alerts == 0
    assert len(_dead_alert_rows(conn)) == 1


def test_happy_cycle_last_stop_stub_complete(conn, seed_unit_job):
    """Full stub cycle last event_log row is stub_complete."""
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    start = store.apply_transition(lease, "w", STUB_START)
    lease = replace(
        lease, job_version=start.job_version_after, unit_state=start.to_state
    )
    store.apply_transition(lease, "w", STUB_COMPLETE)

    last = _last_stop(conn, ids["unit_id"])
    assert last["event_type"] == STUB_COMPLETE

    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_DONE
