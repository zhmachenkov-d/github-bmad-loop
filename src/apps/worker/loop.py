"""Shared typed-job lease loop: poll, dispatch by kind, heartbeat under hold."""

from __future__ import annotations

import sys
import time
from dataclasses import replace

from core.stub_unit import (
    JOB_KIND_RUN,
    JOB_KIND_UNIT,
    STUB_COMPLETE,
    STUB_READY,
    STUB_RUNNING,
    STUB_START,
    IllegalTransitionError,
    InvalidUnitStateError,
    LeaseLostError,
    UnitJobLease,
    UnsupportedResultError,
)
from ports.job_store import JobStorePort

IDLE_SLEEP_SECONDS = 0.5

_APPLY_ERRORS = (
    LeaseLostError,
    IllegalTransitionError,
    InvalidUnitStateError,
    UnsupportedResultError,
)


def handle_unit(store: JobStorePort, lease: UnitJobLease, worker_id: str) -> int:
    """Unit stub: ready → start+complete; reclaimed running → complete only."""
    try:
        if lease.unit_state == STUB_READY:
            start = store.apply_transition(lease, worker_id, STUB_START)
            lease = replace(
                lease,
                job_version=start.job_version_after,
                unit_state=start.to_state,
            )
            if not store.heartbeat(lease.job_id, lease.lease_id, worker_id):
                print("heartbeat lost after stub_start; stopping", file=sys.stderr)
                return 1
            complete = store.apply_transition(lease, worker_id, STUB_COMPLETE)
            print(
                f"applied {start.from_state}→{start.to_state} then "
                f"{complete.from_state}→{complete.to_state} "
                f"key={complete.idempotency_key}"
            )
            return 0

        if lease.unit_state == STUB_RUNNING:
            if not store.heartbeat(lease.job_id, lease.lease_id, worker_id):
                print("heartbeat lost on reclaim; stopping", file=sys.stderr)
                return 1
            complete = store.apply_transition(lease, worker_id, STUB_COMPLETE)
            print(
                f"reclaimed complete {complete.from_state}→{complete.to_state} "
                f"key={complete.idempotency_key}"
            )
            return 0

        print(
            f"unexpected unit state {lease.unit_state!r} under lease; releasing",
            file=sys.stderr,
        )
        store.release(lease.job_id, lease.lease_id)
        return 1
    except _APPLY_ERRORS as exc:
        print(f"apply failed: {exc}", file=sys.stderr)
        return 1


def handle_run(store: JobStorePort, lease: UnitJobLease, worker_id: str) -> int:
    """Run stub: no Unit SM / event_log / outbox — release only."""
    del worker_id  # identity checked at claim; run stub does not apply
    cleared = store.release(lease.job_id, lease.lease_id)
    if cleared:
        print(f"run job {lease.job_id} no-op released")
        return 0
    print(f"run job {lease.job_id} release missed (lease mismatch)", file=sys.stderr)
    return 1


def dispatch(store: JobStorePort, lease: UnitJobLease, worker_id: str) -> int:
    if lease.kind == JOB_KIND_UNIT:
        return handle_unit(store, lease, worker_id)
    if lease.kind == JOB_KIND_RUN:
        return handle_run(store, lease, worker_id)
    print(f"unknown job kind {lease.kind!r}; releasing", file=sys.stderr)
    store.release(lease.job_id, lease.lease_id)
    return 1


def claim_and_dispatch(store: JobStorePort, worker_id: str) -> int:
    """One claim+dispatch. Exit: 0 ok, 1 error, 2 idle."""
    lease = store.lease_next(worker_id)
    if lease is None:
        return 2
    return dispatch(store, lease, worker_id)


def run_poll_loop(
    store: JobStorePort,
    worker_id: str,
    *,
    once: bool = False,
    idle_sleep: float = IDLE_SLEEP_SECONDS,
) -> int:
    """Continuous poll until stopped; with ``once``, one claim+dispatch then exit."""
    while True:
        code = claim_and_dispatch(store, worker_id)
        if once:
            if code == 2:
                print("no actionable job", file=sys.stderr)
            return code
        if code == 2:
            time.sleep(idle_sleep)
            continue
        if code != 0:
            return code
