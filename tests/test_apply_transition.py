"""Table-driven stub Unit SM apply: legal edges, guards, corrupt state."""

from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest

from adapters.postgres import PostgresJobStore
from core.stub_unit import (
    STUB_COMPLETE,
    STUB_DONE,
    STUB_EFFECT,
    STUB_FAIL,
    STUB_FAILED,
    STUB_NOTIFY,
    STUB_PAUSE,
    STUB_PAUSED,
    STUB_READY,
    STUB_RESUME,
    STUB_RUNNING,
    STUB_START,
    TRANSITIONS,
    IllegalTransitionError,
    InvalidUnitStateError,
    LeaseLostError,
    UnitJobLease,
    UnsupportedResultError,
    outbox_idempotency_key,
)


def _lease_and_apply(
    store: PostgresJobStore,
    conn,
    seed_unit_job,
    *,
    worker: str = "w",
    from_state: str = STUB_READY,
    event: str,
):
    ids = seed_unit_job(state=from_state)
    if from_state != STUB_READY:
        lease_id = uuid4()
        with conn.transaction():
            conn.execute(
                """
                UPDATE jobs
                SET lease_id = %s,
                    leased_by = %s,
                    lease_expires_at = now() + interval '90 seconds',
                    version = version + 1
                WHERE id = %s
                """,
                (lease_id, worker, ids["job_id"]),
            )
        job = conn.execute(
            "SELECT version, lease_id FROM jobs WHERE id = %s", (ids["job_id"],)
        ).fetchone()
        lease = UnitJobLease(
            job_id=ids["job_id"],
            run_id=ids["run_id"],
            unit_id=ids["unit_id"],
            lease_id=job["lease_id"],
            job_version=job["version"],
            unit_state=from_state,
        )
    else:
        lease = store.lease_next_unit_job(worker)
        assert lease is not None
    return ids, store.apply_transition(lease, worker, event)


@pytest.mark.parametrize(
    ("from_state", "event", "to_state", "effect"),
    [
        (STUB_READY, STUB_START, STUB_RUNNING, None),
        (STUB_RUNNING, STUB_COMPLETE, STUB_DONE, STUB_EFFECT),
        (STUB_RUNNING, STUB_PAUSE, STUB_PAUSED, STUB_NOTIFY),
        (STUB_PAUSED, STUB_RESUME, STUB_RUNNING, None),
        (STUB_RUNNING, STUB_FAIL, STUB_FAILED, STUB_EFFECT),
    ],
)
def test_legal_edges(conn, seed_unit_job, counts, from_state, event, to_state, effect):
    store = PostgresJobStore(conn)
    ids, result = _lease_and_apply(
        store, conn, seed_unit_job, from_state=from_state, event=event
    )
    assert result.from_state == from_state
    assert result.to_state == to_state

    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == to_state

    event_row = conn.execute(
        """
        SELECT event_type, from_state, to_state, payload
        FROM event_log WHERE unit_id = %s
        """,
        (ids["unit_id"],),
    ).fetchone()
    assert event_row["event_type"] == event
    assert event_row["from_state"] == from_state
    assert event_row["to_state"] == to_state
    assert event_row["payload"]["effect"] == effect
    assert event_row["payload"]["result"] is None
    assert set(event_row["payload"].keys()) == {
        "job_version",
        "lease_id",
        "effect",
        "result",
    }

    job = conn.execute(
        "SELECT version, lease_id, leased_by FROM jobs WHERE id = %s",
        (ids["job_id"],),
    ).fetchone()
    assert job["version"] == result.job_version_after
    terminal = to_state in {STUB_DONE, STUB_FAILED}
    if terminal:
        assert job["lease_id"] is None
        assert job["leased_by"] is None
    else:
        assert job["lease_id"] is not None
        assert job["leased_by"] == "w"

    if effect is None:
        assert result.idempotency_key is None
        assert counts() == {"event_log": 1, "outbox": 0}
    else:
        lease_version = event_row["payload"]["job_version"]
        expected_key = outbox_idempotency_key(
            unit_id=ids["unit_id"],
            effect=effect,
            transition_version=lease_version,
        )
        assert result.idempotency_key == expected_key
        outbox = conn.execute(
            "SELECT idempotency_key, payload FROM outbox WHERE unit_id = %s",
            (ids["unit_id"],),
        ).fetchone()
        assert outbox["idempotency_key"] == expected_key
        assert outbox["payload"]["effect"] == effect
        assert counts() == {"event_log": 1, "outbox": 1}


def test_every_declared_transition_has_test():
    covered = {
        (STUB_READY, STUB_START),
        (STUB_RUNNING, STUB_COMPLETE),
        (STUB_RUNNING, STUB_PAUSE),
        (STUB_PAUSED, STUB_RESUME),
        (STUB_RUNNING, STUB_FAIL),
    }
    assert covered == set(TRANSITIONS.keys())


@pytest.mark.parametrize(
    ("from_state", "event"),
    [
        (STUB_READY, STUB_COMPLETE),
        (STUB_RUNNING, STUB_START),
        (STUB_PAUSED, STUB_COMPLETE),
        (STUB_DONE, STUB_START),
        (STUB_FAILED, STUB_START),
    ],
)
def test_illegal_transition_zero_writes(conn, seed_unit_job, counts, from_state, event):
    store = PostgresJobStore(conn)
    ids = seed_unit_job(state=from_state)
    lease_id = uuid4()
    with conn.transaction():
        conn.execute(
            """
            UPDATE jobs
            SET lease_id = %s,
                leased_by = %s,
                lease_expires_at = now() + interval '90 seconds',
                version = version + 1
            WHERE id = %s
            """,
            (lease_id, "w", ids["job_id"]),
        )
    job = conn.execute(
        "SELECT version, lease_id FROM jobs WHERE id = %s", (ids["job_id"],)
    ).fetchone()
    lease = UnitJobLease(
        job_id=ids["job_id"],
        run_id=ids["run_id"],
        unit_id=ids["unit_id"],
        lease_id=job["lease_id"],
        job_version=job["version"],
        unit_state=from_state,
    )
    with pytest.raises(IllegalTransitionError):
        store.apply_transition(lease, "w", event)
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == from_state
    assert counts() == {"event_log": 0, "outbox": 0}


def test_stale_lease_before_illegal_transition(conn, seed_unit_job, counts):
    """Guard order: bad lease/version must fail before SM table (illegal event)."""
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next_unit_job("w")
    assert lease is not None
    stale = replace(lease, job_version=lease.job_version - 1)
    with pytest.raises(LeaseLostError):
        store.apply_transition(stale, "w", STUB_COMPLETE)
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_READY
    assert counts() == {"event_log": 0, "outbox": 0}


def test_unsupported_result(conn, seed_unit_job, counts):
    seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next_unit_job("w")
    assert lease is not None
    with pytest.raises(UnsupportedResultError):
        store.apply_transition(lease, "w", STUB_START, result={"x": 1})
    assert counts() == {"event_log": 0, "outbox": 0}


def test_corrupt_unit_state(conn, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next_unit_job("w")
    assert lease is not None
    conn.execute("ALTER TABLE units DROP CONSTRAINT units_state_check")
    try:
        conn.execute(
            "UPDATE units SET state = %s WHERE id = %s",
            ("not_a_stub_state", ids["unit_id"]),
        )
        with pytest.raises(InvalidUnitStateError):
            store.apply_transition(lease, "w", STUB_START)
        assert counts() == {"event_log": 0, "outbox": 0}
    finally:
        conn.execute(
            "UPDATE units SET state = %s WHERE id = %s",
            (STUB_READY, ids["unit_id"]),
        )
        conn.execute(
            """
            ALTER TABLE units
                ADD CONSTRAINT units_state_check
                CHECK (state IN (
                    'stub_ready', 'stub_running', 'stub_paused',
                    'stub_done', 'stub_failed'
                ))
            """
        )


def test_stale_lease_after_terminal_clear(conn, seed_unit_job, counts):
    store = PostgresJobStore(conn)
    ids, start = _lease_and_apply(
        store, conn, seed_unit_job, from_state=STUB_READY, event=STUB_START
    )
    job = conn.execute(
        "SELECT version, lease_id FROM jobs WHERE id = %s", (ids["job_id"],)
    ).fetchone()
    held = UnitJobLease(
        job_id=ids["job_id"],
        run_id=ids["run_id"],
        unit_id=ids["unit_id"],
        lease_id=job["lease_id"],
        job_version=start.job_version_after,
        unit_state=STUB_RUNNING,
    )
    store.apply_transition(held, "w", STUB_COMPLETE)
    stale = replace(held, job_version=start.job_version_after)
    with pytest.raises(LeaseLostError):
        store.apply_transition(stale, "w", STUB_START)
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_DONE
    assert counts() == {"event_log": 2, "outbox": 1}


def test_version_refresh_between_applies(conn, seed_unit_job):
    seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next_unit_job("w")
    assert lease is not None
    v0 = lease.job_version
    start = store.apply_transition(lease, "w", STUB_START)
    assert start.job_version_after == v0 + 1
    lease = replace(lease, job_version=start.job_version_after)
    complete = store.apply_transition(lease, "w", STUB_COMPLETE)
    assert complete.job_version_after == start.job_version_after + 1


def test_expired_lease_raises(conn, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next_unit_job("w")
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
    with pytest.raises(LeaseLostError, match="lease expired"):
        store.apply_transition(lease, "w", STUB_START)
    assert counts() == {"event_log": 0, "outbox": 0}
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_READY


def test_cleared_lease_raises(conn, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next_unit_job("w")
    assert lease is not None
    with conn.transaction():
        conn.execute(
            """
            UPDATE jobs
            SET lease_id = NULL,
                leased_by = NULL,
                lease_expires_at = NULL
            WHERE id = %s
            """,
            (ids["job_id"],),
        )
    with pytest.raises(LeaseLostError):
        store.apply_transition(lease, "w", STUB_START)
    assert counts() == {"event_log": 0, "outbox": 0}


def test_missing_unit_raises(conn, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next_unit_job("w")
    assert lease is not None
    with conn.transaction():
        conn.execute("DELETE FROM jobs WHERE id = %s", (ids["job_id"],))
        conn.execute("DELETE FROM units WHERE id = %s", (ids["unit_id"],))
    with pytest.raises(LeaseLostError, match="unit missing"):
        store.apply_transition(lease, "w", STUB_START)
    assert counts() == {"event_log": 0, "outbox": 0}
