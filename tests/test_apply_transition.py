"""Table-driven stub Unit SM apply: legal edges, guards, corrupt state."""

from __future__ import annotations

import re
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

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
    STUB_TERMINAL_STATES,
    STUB_UNIT_STATES,
    TRANSITIONS,
    IllegalTransitionError,
    InvalidUnitStateError,
    LeaseLostError,
    UnitJobLease,
    UnsupportedResultError,
    outbox_idempotency_key,
)

LEGAL_EDGES: list[tuple[str, str, str, str | None]] = [
    (STUB_READY, STUB_START, STUB_RUNNING, None),
    (STUB_RUNNING, STUB_COMPLETE, STUB_DONE, STUB_EFFECT),
    (STUB_RUNNING, STUB_PAUSE, STUB_PAUSED, STUB_NOTIFY),
    (STUB_PAUSED, STUB_RESUME, STUB_RUNNING, None),
    (STUB_RUNNING, STUB_FAIL, STUB_FAILED, STUB_EFFECT),
]


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
        lease = store.lease_next(worker)
        assert lease is not None
    return ids, lease, store.apply_transition(lease, worker, event)


@pytest.mark.parametrize(
    ("from_state", "event", "to_state", "effect"),
    LEGAL_EDGES,
)
def test_legal_edges(conn, seed_unit_job, counts, from_state, event, to_state, effect):
    store = PostgresJobStore(conn)
    ids, lease, result = _lease_and_apply(
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
    assert event_row["payload"]["job_version"] == lease.job_version
    assert event_row["payload"]["lease_id"] == str(lease.lease_id)
    assert set(event_row["payload"].keys()) == {
        "job_version",
        "lease_id",
        "effect",
        "result",
    }

    job = conn.execute(
        "SELECT version, lease_id, leased_by, lease_expires_at FROM jobs WHERE id = %s",
        (ids["job_id"],),
    ).fetchone()
    assert job["version"] == result.job_version_after
    terminal = to_state in STUB_TERMINAL_STATES
    if terminal:
        assert job["lease_id"] is None
        assert job["leased_by"] is None
        assert job["lease_expires_at"] is None
    else:
        assert job["lease_id"] is not None
        assert job["leased_by"] == "w"
        assert job["lease_expires_at"] is not None

    if effect is None:
        assert result.idempotency_key is None
        assert counts() == {"event_log": 1, "outbox": 0}
    else:
        expected_key = outbox_idempotency_key(
            unit_id=ids["unit_id"],
            effect=effect,
            transition_version=lease.job_version,
        )
        assert result.idempotency_key == expected_key
        outbox = conn.execute(
            "SELECT idempotency_key, payload FROM outbox WHERE unit_id = %s",
            (ids["unit_id"],),
        ).fetchone()
        assert outbox["idempotency_key"] == expected_key
        assert outbox["payload"] == {
            "effect": effect,
            "from_state": from_state,
            "to_state": to_state,
            "unit_id": str(ids["unit_id"]),
        }
        assert counts() == {"event_log": 1, "outbox": 1}


def test_every_declared_transition_has_test():
    covered = {(from_state, event) for from_state, event, _to, _effect in LEGAL_EDGES}
    assert covered == set(TRANSITIONS.keys())
    for from_state, event, to_state, effect in LEGAL_EDGES:
        edge = TRANSITIONS[(from_state, event)]
        assert edge.to_state == to_state
        assert edge.effect == effect


def test_migration_0003_check_matches_stub_unit_states():
    migration = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "0003_unit_sm_states_and_event_payload.py"
    )
    text = migration.read_text(encoding="utf-8")
    upgrade_block = text.split("def upgrade()", 1)[1].split("def downgrade()", 1)[0]
    states = set(re.findall(r"'stub_[a-z]+'", upgrade_block))
    states = {s.strip("'") for s in states}
    assert states == set(STUB_UNIT_STATES)


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
    job_before = conn.execute(
        """
        SELECT version, lease_id, leased_by, lease_expires_at
        FROM jobs WHERE id = %s
        """,
        (ids["job_id"],),
    ).fetchone()
    lease = UnitJobLease(
        job_id=ids["job_id"],
        run_id=ids["run_id"],
        unit_id=ids["unit_id"],
        lease_id=job_before["lease_id"],
        job_version=job_before["version"],
        unit_state=from_state,
    )
    with pytest.raises(IllegalTransitionError):
        store.apply_transition(lease, "w", event)
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == from_state
    job_after = conn.execute(
        """
        SELECT version, lease_id, leased_by, lease_expires_at
        FROM jobs WHERE id = %s
        """,
        (ids["job_id"],),
    ).fetchone()
    assert job_after["version"] == job_before["version"]
    assert job_after["lease_id"] == job_before["lease_id"]
    assert job_after["leased_by"] == job_before["leased_by"]
    assert job_after["lease_expires_at"] == job_before["lease_expires_at"]
    assert counts() == {"event_log": 0, "outbox": 0}


def test_stale_lease_before_illegal_transition(conn, seed_unit_job, counts):
    """Guard order: bad lease/version must fail before SM table (illegal event)."""
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    stale = replace(lease, job_version=lease.job_version - 1)
    with pytest.raises(LeaseLostError):
        store.apply_transition(stale, "w", STUB_COMPLETE)
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_READY
    assert counts() == {"event_log": 0, "outbox": 0}


def test_leased_by_mismatch_before_illegal_transition(conn, seed_unit_job, counts):
    """Guard order: wrong worker_id must fail before SM table (illegal event)."""
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    with pytest.raises(LeaseLostError, match="leased_by mismatch"):
        store.apply_transition(lease, "other-worker", STUB_COMPLETE)
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_READY
    assert counts() == {"event_log": 0, "outbox": 0}


def test_allowed_why_result_written_to_event_log(conn, seed_unit_job, counts):
    """Stuck why values pass through into event_log.payload.result."""
    ids = seed_unit_job(state=STUB_RUNNING)
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
        unit_state=STUB_RUNNING,
    )
    store = PostgresJobStore(conn)
    result = store.apply_transition(lease, "w", STUB_FAIL, result="attempts")
    assert result.to_state == STUB_FAILED
    event = conn.execute(
        "SELECT payload FROM event_log WHERE unit_id = %s", (ids["unit_id"],)
    ).fetchone()
    assert event["payload"]["result"] == "attempts"
    assert counts()["event_log"] == 1


def test_unsupported_result_shape(conn, seed_unit_job, counts):
    seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    with pytest.raises(UnsupportedResultError):
        store.apply_transition(lease, "w", STUB_START, result={"x": 1})
    assert counts() == {"event_log": 0, "outbox": 0}


def test_corrupt_unit_state(conn, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
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
        states_sql = ", ".join(f"'{s}'" for s in sorted(STUB_UNIT_STATES))
        conn.execute(
            f"""
            ALTER TABLE units
                ADD CONSTRAINT units_state_check
                CHECK (state IN ({states_sql}))
            """
        )


def test_stale_lease_after_terminal_clear(conn, seed_unit_job, counts):
    store = PostgresJobStore(conn)
    ids, _lease, start = _lease_and_apply(
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
    lease = store.lease_next("w")
    assert lease is not None
    v0 = lease.job_version
    start = store.apply_transition(lease, "w", STUB_START)
    assert start.job_version_after == v0 + 1
    lease = replace(
        lease, job_version=start.job_version_after, unit_state=start.to_state
    )
    complete = store.apply_transition(lease, "w", STUB_COMPLETE)
    assert complete.job_version_after == start.job_version_after + 1


def test_expired_lease_raises(conn, seed_unit_job, counts):
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
    lease = store.lease_next("w")
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
    lease = store.lease_next("w")
    assert lease is not None
    with conn.transaction():
        conn.execute("DELETE FROM jobs WHERE id = %s", (ids["job_id"],))
        conn.execute("DELETE FROM units WHERE id = %s", (ids["unit_id"],))
    with pytest.raises(LeaseLostError, match="unit missing"):
        store.apply_transition(lease, "w", STUB_START)
    assert counts() == {"event_log": 0, "outbox": 0}


def test_missing_job_raises(conn, seed_unit_job, counts):
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    with conn.transaction():
        conn.execute("DELETE FROM jobs WHERE id = %s", (ids["job_id"],))
    with pytest.raises(LeaseLostError, match="job missing"):
        store.apply_transition(lease, "w", STUB_START)
    unit = conn.execute(
        "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
    ).fetchone()
    assert unit["state"] == STUB_READY
    assert counts() == {"event_log": 0, "outbox": 0}


def test_concurrent_same_lease_apply(db_url, seed_unit_job, conn):
    """Two threads apply the same held lease: one winner write path, loser zero writes."""
    ids = seed_unit_job(state=STUB_RUNNING)
    lease_id = uuid4()
    worker = "w-concurrent"
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
        unit_state=STUB_RUNNING,
    )
    conn.commit()

    results: list = []
    errors: list = []
    barrier = threading.Barrier(2)

    def rival() -> None:
        try:
            with psycopg.connect(db_url, row_factory=dict_row) as c:
                store = PostgresJobStore(c)
                barrier.wait(timeout=10)
                try:
                    results.append(store.apply_transition(lease, worker, STUB_COMPLETE))
                except LeaseLostError as exc:
                    errors.append(exc)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(rival), pool.submit(rival)]
        for f in futs:
            f.result(timeout=30)

    assert len(results) == 1, (results, errors)
    assert len(errors) == 1, (results, errors)
    assert isinstance(errors[0], LeaseLostError)

    with psycopg.connect(db_url, row_factory=dict_row) as c:
        unit = c.execute(
            "SELECT state FROM units WHERE id = %s", (ids["unit_id"],)
        ).fetchone()
        assert unit["state"] == STUB_DONE
        event_n = c.execute(
            "SELECT count(*) AS n FROM event_log WHERE unit_id = %s",
            (ids["unit_id"],),
        ).fetchone()["n"]
        outbox_n = c.execute(
            "SELECT count(*) AS n FROM outbox WHERE unit_id = %s",
            (ids["unit_id"],),
        ).fetchone()["n"]
    assert event_n == 1
    assert outbox_n == 1


def test_stale_version_outbox_dedupe(conn, seed_unit_job, counts):
    """After a successful apply, stale job_version fails closed with no second outbox row."""
    ids = seed_unit_job()
    store = PostgresJobStore(conn)
    lease = store.lease_next("w")
    assert lease is not None
    start = store.apply_transition(lease, "w", STUB_START)
    held = replace(
        lease, job_version=start.job_version_after, unit_state=start.to_state
    )
    pre_complete_version = held.job_version
    expected_key = outbox_idempotency_key(
        unit_id=ids["unit_id"],
        effect=STUB_EFFECT,
        transition_version=pre_complete_version,
    )
    complete = store.apply_transition(held, "w", STUB_COMPLETE)
    assert complete.idempotency_key == expected_key
    assert counts() == {"event_log": 2, "outbox": 1}

    stale = replace(held, job_version=pre_complete_version)
    with pytest.raises(LeaseLostError):
        store.apply_transition(stale, "w", STUB_COMPLETE)
    assert counts() == {"event_log": 2, "outbox": 1}
    rows = conn.execute(
        "SELECT count(*) AS n FROM outbox WHERE idempotency_key = %s",
        (expected_key,),
    ).fetchone()
    assert rows["n"] == 1


def test_lease_expiry_equality_boundary(conn, seed_unit_job, counts):
    """At lease_expires_at == now() (TX-stable): reclaimable; heartbeat/apply lost."""
    ids = seed_unit_job(state=STUB_RUNNING)
    lease_id = uuid4()
    worker = "w-eq"
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
        unit_state=STUB_RUNNING,
    )
    store = PostgresJobStore(conn)

    # Outer TX keeps Postgres now() stable across nested store savepoints.
    with conn.transaction():
        conn.execute(
            """
            UPDATE jobs
            SET lease_expires_at = now()
            WHERE id = %s
            """,
            (ids["job_id"],),
        )
        assert store.heartbeat(ids["job_id"], lease.lease_id, worker) is False
        with pytest.raises(LeaseLostError, match="lease expired"):
            store.apply_transition(lease, worker, STUB_COMPLETE)
        assert counts() == {"event_log": 0, "outbox": 0}
        reclaimed = store.lease_next(worker)
        assert reclaimed is not None
        assert reclaimed.job_id == ids["job_id"]
        assert reclaimed.unit_state == STUB_RUNNING
