"""Postgres JobStore: lease + AD-12-subset apply + stub outbox mark-sent."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from core.stub_unit import (
    STUB_OUTBOX_CHANNEL,
    STUB_READY,
    STUB_TERMINAL_STATES,
    STUB_UNIT_STATES,
    ApplyTransitionResult,
    InvalidUnitStateError,
    LeaseLostError,
    UnitJobLease,
    UnsupportedResultError,
    lookup_transition,
    outbox_idempotency_key,
)

LEASE_TTL = timedelta(seconds=90)


class PostgresJobStore:
    def __init__(
        self,
        conn: psycopg.Connection[dict[str, Any]],
        *,
        lease_ttl: timedelta = LEASE_TTL,
    ) -> None:
        self._conn = conn
        self._lease_ttl = lease_ttl

    def lease_next_unit_job(self, worker_id: str) -> UnitJobLease | None:
        with self._conn.transaction():
            row = self._conn.execute(
                """
                SELECT j.id AS job_id,
                       u.run_id AS run_id,
                       u.id AS unit_id,
                       j.version AS job_version,
                       u.state AS unit_state
                FROM jobs j
                JOIN units u ON u.id = j.ref_id
                WHERE j.kind = 'unit'
                  AND u.state = %s
                  AND (j.lease_expires_at IS NULL OR j.lease_expires_at < now())
                ORDER BY j.created_at ASC
                FOR UPDATE OF j, u SKIP LOCKED
                LIMIT 1
                """,
                (STUB_READY,),
            ).fetchone()
            if row is None:
                return None

            lease_id = uuid4()
            updated = self._conn.execute(
                """
                UPDATE jobs
                SET lease_id = %s,
                    leased_by = %s,
                    lease_expires_at = now() + %s::interval,
                    version = version + 1,
                    updated_at = now()
                WHERE id = %s
                RETURNING version
                """,
                (lease_id, worker_id, self._lease_ttl, row["job_id"]),
            ).fetchone()
            assert updated is not None
            return UnitJobLease(
                job_id=row["job_id"],
                run_id=row["run_id"],
                unit_id=row["unit_id"],
                lease_id=lease_id,
                job_version=updated["version"],
                unit_state=row["unit_state"],
            )

    def apply_transition(
        self,
        lease: UnitJobLease,
        worker_id: str,
        event: str,
        result: Any = None,
    ) -> ApplyTransitionResult:
        if result is not None:
            raise UnsupportedResultError("result must be None")

        with self._conn.transaction():
            unit = self._conn.execute(
                """
                SELECT id, run_id, state
                FROM units
                WHERE id = %s
                FOR UPDATE
                """,
                (lease.unit_id,),
            ).fetchone()
            if unit is None:
                raise LeaseLostError("unit missing")

            if unit["run_id"] != lease.run_id:
                raise LeaseLostError("run_id mismatch")

            job_row = self._conn.execute(
                """
                SELECT id, version, lease_id, leased_by, lease_expires_at
                FROM jobs
                WHERE id = %s
                  AND kind = 'unit'
                  AND ref_id = %s
                FOR UPDATE
                """,
                (lease.job_id, lease.unit_id),
            ).fetchone()
            if job_row is None:
                raise LeaseLostError("job missing")
            if job_row["version"] != lease.job_version:
                raise LeaseLostError("job version mismatch")
            if job_row["lease_id"] != lease.lease_id:
                raise LeaseLostError("lease_id mismatch")
            if job_row["leased_by"] != worker_id:
                raise LeaseLostError("leased_by mismatch")
            if job_row["lease_expires_at"] is None:
                raise LeaseLostError("lease expired or cleared")
            expired = self._conn.execute(
                "SELECT (lease_expires_at <= now()) AS expired FROM jobs WHERE id = %s",
                (lease.job_id,),
            ).fetchone()
            if expired is None or expired["expired"]:
                raise LeaseLostError("lease expired")

            from_state = unit["state"]
            if from_state not in STUB_UNIT_STATES:
                raise InvalidUnitStateError(f"unknown unit state {from_state!r}")

            edge = lookup_transition(from_state=from_state, event=event)

            pre_bump_version = lease.job_version
            job_version_after = self._bump_job_version(
                lease=lease,
                worker_id=worker_id,
                clear_lease=edge.to_state in STUB_TERMINAL_STATES,
            )

            updated_unit = self._conn.execute(
                """
                UPDATE units
                SET state = %s, updated_at = now()
                WHERE id = %s AND state = %s
                RETURNING id
                """,
                (edge.to_state, lease.unit_id, from_state),
            ).fetchone()
            if updated_unit is None:
                raise LeaseLostError("unit state changed under lease")

            run_id = unit["run_id"]
            payload = Jsonb(
                {
                    "job_version": pre_bump_version,
                    "lease_id": str(lease.lease_id),
                    "effect": edge.effect,
                    "result": None,
                }
            )
            self._conn.execute(
                """
                INSERT INTO event_log (
                    run_id, unit_id, from_state, to_state, event_type, worker_id, payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    lease.unit_id,
                    from_state,
                    edge.to_state,
                    event,
                    worker_id,
                    payload,
                ),
            )

            idempotency_key: str | None = None
            if edge.effect is not None:
                idempotency_key = outbox_idempotency_key(
                    unit_id=lease.unit_id,
                    effect=edge.effect,
                    transition_version=pre_bump_version,
                )
                self._conn.execute(
                    """
                    INSERT INTO outbox (
                        run_id, unit_id, channel, idempotency_key, payload, status
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb, 'pending')
                    ON CONFLICT (idempotency_key) DO NOTHING
                    """,
                    (
                        run_id,
                        lease.unit_id,
                        STUB_OUTBOX_CHANNEL,
                        idempotency_key,
                        Jsonb(
                            {
                                "effect": edge.effect,
                                "from_state": from_state,
                                "to_state": edge.to_state,
                                "unit_id": str(lease.unit_id),
                            }
                        ),
                    ),
                )

            return ApplyTransitionResult(
                from_state=from_state,
                to_state=edge.to_state,
                job_version_after=job_version_after,
                idempotency_key=idempotency_key,
            )

    def _bump_job_version(
        self,
        *,
        lease: UnitJobLease,
        worker_id: str,
        clear_lease: bool,
    ) -> int:
        if clear_lease:
            job = self._conn.execute(
                """
                UPDATE jobs
                SET lease_id = NULL,
                    leased_by = NULL,
                    lease_expires_at = NULL,
                    version = version + 1,
                    updated_at = now()
                WHERE id = %s
                  AND version = %s
                  AND lease_id = %s
                  AND leased_by = %s
                  AND kind = 'unit'
                  AND ref_id = %s
                  AND lease_expires_at > now()
                RETURNING version
                """,
                (
                    lease.job_id,
                    lease.job_version,
                    lease.lease_id,
                    worker_id,
                    lease.unit_id,
                ),
            ).fetchone()
        else:
            job = self._conn.execute(
                """
                UPDATE jobs
                SET version = version + 1,
                    updated_at = now()
                WHERE id = %s
                  AND version = %s
                  AND lease_id = %s
                  AND leased_by = %s
                  AND kind = 'unit'
                  AND ref_id = %s
                  AND lease_expires_at > now()
                RETURNING version
                """,
                (
                    lease.job_id,
                    lease.job_version,
                    lease.lease_id,
                    worker_id,
                    lease.unit_id,
                ),
            ).fetchone()
        if job is None:
            raise LeaseLostError("job version or lease mismatch")
        return int(job["version"])

    def mark_outbox_sent(self, *, limit: int = 20) -> int:
        if limit < 1:
            return 0
        with self._conn.transaction():
            rows = self._conn.execute(
                """
                SELECT id
                FROM outbox
                WHERE status = 'pending'
                  AND channel = %s
                  AND next_attempt_at <= now()
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT %s
                """,
                (STUB_OUTBOX_CHANNEL, limit),
            ).fetchall()
            if not rows:
                return 0
            ids = [r["id"] for r in rows]
            self._conn.execute(
                """
                UPDATE outbox
                SET status = 'sent',
                    sent_at = now()
                WHERE id = ANY(%s)
                """,
                (ids,),
            )
            return len(ids)
