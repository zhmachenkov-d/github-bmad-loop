"""Postgres JobStore: lease + atomic stub transition + stub outbox mark-sent."""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from core.stub_unit import (
    STUB_DONE,
    STUB_EVENT_TYPE,
    STUB_OUTBOX_CHANNEL,
    STUB_READY,
    LeaseLostError,
    StubTransition,
    UnitJobLease,
    outbox_idempotency_key,
)

LEASE_TTL = timedelta(seconds=90)


class PostgresJobStore:
    def __init__(self, conn: psycopg.Connection, *, lease_ttl: timedelta = LEASE_TTL) -> None:
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
                FOR UPDATE OF j SKIP LOCKED
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

    def apply_stub_transition(
        self,
        lease: UnitJobLease,
        worker_id: str,
    ) -> StubTransition:
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

            # Terminal unit: forced second apply is a no-op (no side effects).
            if unit["state"] == STUB_DONE:
                return StubTransition(
                    applied=False,
                    from_state=STUB_DONE,
                    to_state=STUB_DONE,
                    job_version_after=None,
                    idempotency_key=None,
                )

            if unit["state"] != STUB_READY:
                raise LeaseLostError(f"unit not actionable: {unit['state']}")

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
                RETURNING version
                """,
                (lease.job_id, lease.job_version, lease.lease_id),
            ).fetchone()
            if job is None:
                raise LeaseLostError("job version or lease mismatch")

            self._conn.execute(
                """
                UPDATE units
                SET state = %s, updated_at = now()
                WHERE id = %s AND state = %s
                """,
                (STUB_DONE, lease.unit_id, STUB_READY),
            )

            self._conn.execute(
                """
                INSERT INTO event_log (
                    run_id, unit_id, from_state, to_state, event_type, worker_id
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    lease.run_id,
                    lease.unit_id,
                    STUB_READY,
                    STUB_DONE,
                    STUB_EVENT_TYPE,
                    worker_id,
                ),
            )

            # Dedupe from the job version observed at lease time (pre-clear bump).
            key = outbox_idempotency_key(
                unit_id=lease.unit_id,
                transition_version=lease.job_version,
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
                    lease.run_id,
                    lease.unit_id,
                    STUB_OUTBOX_CHANNEL,
                    key,
                    Jsonb(
                        {
                            "effect": "stub_transition",
                            "from_state": STUB_READY,
                            "to_state": STUB_DONE,
                            "unit_id": str(lease.unit_id),
                        }
                    ),
                ),
            )

            return StubTransition(
                applied=True,
                from_state=STUB_READY,
                to_state=STUB_DONE,
                job_version_after=job["version"],
                idempotency_key=key,
            )

    def mark_outbox_sent(self, *, limit: int = 20) -> int:
        with self._conn.transaction():
            rows = self._conn.execute(
                """
                SELECT id
                FROM outbox
                WHERE status = 'pending'
                  AND next_attempt_at <= now()
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT %s
                """,
                (limit,),
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
