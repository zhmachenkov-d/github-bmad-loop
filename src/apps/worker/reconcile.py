"""Worker-owned reconcile tick: grace clear → stuck → dead-outbox alerts."""

from __future__ import annotations

from dataclasses import dataclass

from core.stub_unit import (
    JOB_KIND_RUN,
    JOB_KIND_UNIT,
    STUB_FAIL,
    run_stuck_alert_idempotency_key,
    stuck_alert_idempotency_key,
    stuck_why,
)
from ports.job_store import JobStorePort

RECONCILE_INTERVAL_SECONDS = 60.0


@dataclass(frozen=True, slots=True)
class ReconcileTickResult:
    locked: bool
    grace_cleared: int = 0
    units_failed: int = 0
    runs_alerted: int = 0
    dead_alerts: int = 0


def run_reconcile_tick(store: JobStorePort, worker_id: str) -> ReconcileTickResult:
    """Ordered reconcile under pg_try_advisory_lock(42). Lock miss → no-op."""
    if not store.try_reconcile_lock():
        return ReconcileTickResult(locked=False)
    try:
        grace = store.grace_force_clear_stale_leases()
        units = _reconcile_stuck_units(store, worker_id)
        runs = _reconcile_stuck_runs(store)
        dead = _reconcile_dead_outbox(store)
        return ReconcileTickResult(
            locked=True,
            grace_cleared=grace,
            units_failed=units,
            runs_alerted=runs,
            dead_alerts=dead,
        )
    finally:
        store.release_reconcile_lock()


def _reconcile_stuck_units(store: JobStorePort, worker_id: str) -> int:
    failed = 0
    for row in store.select_stuck_unit_jobs():
        why = stuck_why(
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            deadline_passed=bool(row["deadline_passed"]),
        )
        pre_steal_version = int(row["job_version"])
        lease = store.steal_reconcile_lease(row["job_id"], worker_id)
        if lease is None:
            continue
        store.apply_transition(lease, worker_id, STUB_FAIL, result=why)
        store.enqueue_stuck_alert(
            run_id=row["run_id"],
            unit_id=row["unit_id"],
            idempotency_key=stuck_alert_idempotency_key(
                unit_id=row["unit_id"],
                pre_steal_version=pre_steal_version,
            ),
            why=why,
            kind=JOB_KIND_UNIT,
            ref_id=row["unit_id"],
        )
        failed += 1
    return failed


def _reconcile_stuck_runs(store: JobStorePort) -> int:
    alerted = 0
    for row in store.select_stuck_run_jobs():
        why = stuck_why(
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            deadline_passed=bool(row["deadline_passed"]),
        )
        store.clear_run_lease_if_held(row["job_id"])
        inserted = store.enqueue_stuck_alert(
            run_id=row["run_id"],
            unit_id=None,
            idempotency_key=run_stuck_alert_idempotency_key(
                run_id=row["run_id"],
                why=why,
            ),
            why=why,
            kind=JOB_KIND_RUN,
            ref_id=row["run_id"],
        )
        if inserted:
            alerted += 1
    return alerted


def _reconcile_dead_outbox(store: JobStorePort) -> int:
    inserted = 0
    for row in store.select_dead_outbox():
        if store.enqueue_dead_alert(
            run_id=row["run_id"],
            unit_id=row["unit_id"],
            dead_outbox_id=int(row["id"]),
        ):
            inserted += 1
    return inserted
