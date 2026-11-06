"""Postgres JobStore: lease + AD-12-subset apply + outbox claim/send + reconcile."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from core.stub_unit import (
    ALLOWED_APPLY_RESULTS,
    DEAD_ALERT_EFFECT,
    JOB_KIND_RUN,
    JOB_KIND_UNIT,
    RECONCILE_ADVISORY_LOCK_KEY,
    STUB_DONE,
    STUB_FAILED,
    STUB_OUTBOX_CHANNEL,
    STUB_PAUSED,
    STUB_READY,
    STUB_RUNNING,
    STUB_TERMINAL_STATES,
    STUB_UNIT_STATES,
    STUCK_ALERT_EFFECT,
    ApplyTransitionResult,
    InvalidUnitStateError,
    LeaseLostError,
    UnitJobLease,
    UnsupportedResultError,
    dead_alert_idempotency_key,
    lookup_transition,
    outbox_idempotency_key,
)
from ports.outbox_sender import OutboxMessage

LEASE_TTL = timedelta(seconds=90)
RECONCILE_LEASE_TTL = timedelta(seconds=30)


class PostgresJobStore:
    def __init__(
        self,
        conn: psycopg.Connection[dict[str, Any]],
        *,
        lease_ttl: timedelta = LEASE_TTL,
    ) -> None:
        self._conn = conn
        self._lease_ttl = lease_ttl

    @property
    def connection(self) -> psycopg.Connection[dict[str, Any]]:
        return self._conn

    def lease_next(self, worker_id: str) -> UnitJobLease | None:
        with self._conn.transaction():
            row = self._conn.execute(
                """
                SELECT j.id AS job_id,
                       j.kind AS kind,
                       j.ref_id AS ref_id,
                       j.version AS job_version,
                       u.run_id AS run_id,
                       u.id AS unit_id,
                       u.state AS unit_state
                FROM jobs j
                LEFT JOIN units u ON j.kind = %s AND u.id = j.ref_id
                WHERE (
                    (
                        j.kind = %s
                        AND u.state IN (%s, %s)
                        AND u.state NOT IN (%s, %s, %s)
                        AND (j.lease_expires_at IS NULL OR j.lease_expires_at <= now())
                    )
                    OR (
                        j.kind = %s
                        AND (j.lease_expires_at IS NULL OR j.lease_expires_at <= now())
                        -- Stub stand-in for Run SM: after successful release
                        -- (lease cleared, version already bumped by claim) do not
                        -- re-claim. Still reclaim crashed holds (lease_id set, expired).
                        AND (j.version = 0 OR j.lease_id IS NOT NULL)
                    )
                )
                ORDER BY j.created_at ASC
                FOR UPDATE OF j SKIP LOCKED
                LIMIT 1
                """,
                (
                    JOB_KIND_UNIT,
                    JOB_KIND_UNIT,
                    STUB_READY,
                    STUB_RUNNING,
                    STUB_PAUSED,
                    STUB_DONE,
                    STUB_FAILED,
                    JOB_KIND_RUN,
                ),
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

            kind = row["kind"]
            if kind == JOB_KIND_UNIT:
                assert row["unit_id"] is not None
                assert row["run_id"] is not None
                assert row["unit_state"] is not None
                return UnitJobLease(
                    job_id=row["job_id"],
                    run_id=row["run_id"],
                    unit_id=row["unit_id"],
                    lease_id=lease_id,
                    job_version=updated["version"],
                    unit_state=row["unit_state"],
                    kind=JOB_KIND_UNIT,
                )
            return UnitJobLease(
                job_id=row["job_id"],
                run_id=row["ref_id"],
                unit_id=row["ref_id"],
                lease_id=lease_id,
                job_version=updated["version"],
                unit_state="",
                kind=JOB_KIND_RUN,
            )

    def heartbeat(self, job_id: UUID, lease_id: UUID, worker_id: str) -> bool:
        with self._conn.transaction():
            updated = self._conn.execute(
                """
                UPDATE jobs
                SET lease_expires_at = now() + %s::interval,
                    updated_at = now()
                WHERE id = %s
                  AND lease_id = %s
                  AND leased_by = %s
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at > now()
                RETURNING id
                """,
                (self._lease_ttl, job_id, lease_id, worker_id),
            ).fetchone()
            return updated is not None

    def release(self, job_id: UUID, lease_id: UUID) -> bool:
        with self._conn.transaction():
            updated = self._conn.execute(
                """
                UPDATE jobs
                SET lease_id = NULL,
                    leased_by = NULL,
                    lease_expires_at = NULL,
                    updated_at = now()
                WHERE id = %s
                  AND lease_id = %s
                RETURNING id
                """,
                (job_id, lease_id),
            ).fetchone()
            return updated is not None

    def apply_transition(
        self,
        lease: UnitJobLease,
        worker_id: str,
        event: str,
        result: Any = None,
    ) -> ApplyTransitionResult:
        if result is not None and (
            not isinstance(result, str) or result not in ALLOWED_APPLY_RESULTS
        ):
            raise UnsupportedResultError(
                f"result must be None or one of {sorted(ALLOWED_APPLY_RESULTS)}"
            )

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
                    "result": result,
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

    def try_reconcile_lock(self) -> bool:
        # Commit immediately so we do not leave an open TX around the lease loop.
        # Session advisory locks survive commit.
        with self._conn.transaction():
            row = self._conn.execute(
                "SELECT pg_try_advisory_lock(%s) AS got",
                (RECONCILE_ADVISORY_LOCK_KEY,),
            ).fetchone()
            assert row is not None
            return bool(row["got"])

    def release_reconcile_lock(self) -> None:
        with self._conn.transaction():
            self._conn.execute(
                "SELECT pg_advisory_unlock(%s)",
                (RECONCILE_ADVISORY_LOCK_KEY,),
            )

    def grace_force_clear_stale_leases(self) -> int:
        with self._conn.transaction():
            unit_cleared = self._conn.execute(
                """
                UPDATE jobs j
                SET lease_id = NULL,
                    leased_by = NULL,
                    lease_expires_at = NULL,
                    version = version + 1,
                    updated_at = now()
                FROM units u
                WHERE j.kind = %s
                  AND j.ref_id = u.id
                  AND j.lease_expires_at IS NOT NULL
                  AND j.lease_expires_at < now() - interval '5 minutes'
                  AND u.state NOT IN (%s, %s)
                  AND NOT (
                      u.state = %s
                      AND (
                          j.attempts >= j.max_attempts
                          OR (
                              j.deadline_at IS NOT NULL
                              AND j.deadline_at < now()
                          )
                      )
                  )
                RETURNING j.id
                """,
                (JOB_KIND_UNIT, STUB_DONE, STUB_FAILED, STUB_RUNNING),
            ).fetchall()
            run_cleared = self._conn.execute(
                """
                UPDATE jobs
                SET lease_id = NULL,
                    leased_by = NULL,
                    lease_expires_at = NULL,
                    version = version + 1,
                    updated_at = now()
                WHERE kind = %s
                  AND lease_id IS NOT NULL
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < now() - interval '5 minutes'
                RETURNING id
                """,
                (JOB_KIND_RUN,),
            ).fetchall()
            return len(unit_cleared) + len(run_cleared)

    def select_stuck_unit_jobs(self) -> list[dict[str, Any]]:
        with self._conn.transaction():
            rows = self._conn.execute(
                """
                SELECT j.id AS job_id,
                       j.version AS job_version,
                       j.attempts AS attempts,
                       j.max_attempts AS max_attempts,
                       j.deadline_at AS deadline_at,
                       (j.deadline_at IS NOT NULL AND j.deadline_at < now())
                           AS deadline_passed,
                       u.id AS unit_id,
                       u.run_id AS run_id,
                       u.state AS unit_state
                FROM jobs j
                JOIN units u ON u.id = j.ref_id
                WHERE j.kind = %s
                  AND u.state = %s
                  AND (
                      j.attempts >= j.max_attempts
                      OR (j.deadline_at IS NOT NULL AND j.deadline_at < now())
                  )
                ORDER BY j.created_at ASC
                FOR UPDATE OF j SKIP LOCKED
                """,
                (JOB_KIND_UNIT, STUB_RUNNING),
            ).fetchall()
            return [dict(r) for r in rows]

    def steal_reconcile_lease(
        self, job_id: UUID, worker_id: str
    ) -> UnitJobLease | None:
        with self._conn.transaction():
            lease_id = uuid4()
            row = self._conn.execute(
                """
                UPDATE jobs j
                SET lease_id = %s,
                    leased_by = %s,
                    lease_expires_at = now() + %s::interval,
                    version = version + 1,
                    updated_at = now()
                FROM units u
                WHERE j.id = %s
                  AND j.kind = %s
                  AND j.ref_id = u.id
                  AND u.state = %s
                RETURNING j.id AS job_id,
                          j.version AS job_version,
                          j.lease_id AS lease_id,
                          u.id AS unit_id,
                          u.run_id AS run_id,
                          u.state AS unit_state
                """,
                (
                    lease_id,
                    worker_id,
                    RECONCILE_LEASE_TTL,
                    job_id,
                    JOB_KIND_UNIT,
                    STUB_RUNNING,
                ),
            ).fetchone()
            if row is None:
                return None
            return UnitJobLease(
                job_id=row["job_id"],
                run_id=row["run_id"],
                unit_id=row["unit_id"],
                lease_id=row["lease_id"],
                job_version=row["job_version"],
                unit_state=row["unit_state"],
                kind=JOB_KIND_UNIT,
            )

    def enqueue_stuck_alert(
        self,
        *,
        run_id: UUID,
        unit_id: UUID | None,
        idempotency_key: str,
        why: str,
        kind: str,
        ref_id: UUID,
    ) -> bool:
        with self._conn.transaction():
            inserted = self._conn.execute(
                """
                INSERT INTO outbox (
                    run_id, unit_id, channel, idempotency_key, payload, status
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, 'pending')
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING id
                """,
                (
                    run_id,
                    unit_id,
                    STUB_OUTBOX_CHANNEL,
                    idempotency_key,
                    Jsonb(
                        {
                            "effect": STUCK_ALERT_EFFECT,
                            "why": why,
                            "kind": kind,
                            "ref_id": str(ref_id),
                        }
                    ),
                ),
            ).fetchone()
            return inserted is not None

    def select_stuck_run_jobs(self) -> list[dict[str, Any]]:
        with self._conn.transaction():
            rows = self._conn.execute(
                """
                SELECT id AS job_id,
                       ref_id AS run_id,
                       version AS job_version,
                       attempts AS attempts,
                       max_attempts AS max_attempts,
                       deadline_at AS deadline_at,
                       (deadline_at IS NOT NULL AND deadline_at < now())
                           AS deadline_passed,
                       lease_id AS lease_id
                FROM jobs
                WHERE kind = %s
                  AND (
                      attempts >= max_attempts
                      OR (deadline_at IS NOT NULL AND deadline_at < now())
                  )
                ORDER BY created_at ASC
                FOR UPDATE OF jobs SKIP LOCKED
                """,
                (JOB_KIND_RUN,),
            ).fetchall()
            return [dict(r) for r in rows]

    def clear_run_lease_if_held(self, job_id: UUID) -> int | None:
        with self._conn.transaction():
            row = self._conn.execute(
                """
                UPDATE jobs
                SET lease_id = NULL,
                    leased_by = NULL,
                    lease_expires_at = NULL,
                    version = version + 1,
                    updated_at = now()
                WHERE id = %s
                  AND kind = %s
                  AND lease_id IS NOT NULL
                RETURNING version - 1 AS pre_clear_version
                """,
                (job_id, JOB_KIND_RUN),
            ).fetchone()
            if row is None:
                existing = self._conn.execute(
                    "SELECT version FROM jobs WHERE id = %s AND kind = %s",
                    (job_id, JOB_KIND_RUN),
                ).fetchone()
                if existing is None:
                    return None
                return int(existing["version"])
            return int(row["pre_clear_version"])

    def select_dead_outbox(self) -> list[dict[str, Any]]:
        with self._conn.transaction():
            rows = self._conn.execute(
                """
                SELECT id, run_id, unit_id, idempotency_key, payload
                FROM outbox
                WHERE status = 'dead'
                ORDER BY id ASC
                """
            ).fetchall()
            return [dict(r) for r in rows]

    def enqueue_dead_alert(
        self,
        *,
        run_id: UUID,
        unit_id: UUID | None,
        dead_outbox_id: int,
    ) -> bool:
        key = dead_alert_idempotency_key(dead_outbox_id=dead_outbox_id)
        with self._conn.transaction():
            inserted = self._conn.execute(
                """
                INSERT INTO outbox (
                    run_id, unit_id, channel, idempotency_key, payload, status
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, 'pending')
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING id
                """,
                (
                    run_id,
                    unit_id,
                    STUB_OUTBOX_CHANNEL,
                    key,
                    Jsonb(
                        {
                            "effect": DEAD_ALERT_EFFECT,
                            "dead_outbox_id": dead_outbox_id,
                        }
                    ),
                ),
            ).fetchone()
            return inserted is not None

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

    def claim_due_outbox(self, *, limit: int = 20) -> list[OutboxMessage]:
        """Claim due pending rows (all channels). Caller must hold an open TX."""
        if limit < 1:
            return []
        rows = self._conn.execute(
            """
            SELECT id, run_id, unit_id, channel, idempotency_key, payload, attempts
            FROM outbox
            WHERE status = 'pending'
              AND next_attempt_at <= now()
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
        return [
            OutboxMessage(
                id=int(r["id"]),
                run_id=r["run_id"],
                unit_id=r["unit_id"],
                channel=r["channel"],
                idempotency_key=r["idempotency_key"],
                payload=dict(r["payload"]),
                attempts=int(r["attempts"]),
            )
            for r in rows
        ]

    def mark_outbox_success(self, msg_id: int, external_id: str) -> None:
        self._conn.execute(
            """
            UPDATE outbox
            SET status = 'sent',
                external_id = %s,
                sent_at = now()
            WHERE id = %s
            """,
            (external_id, msg_id),
        )

    def mark_outbox_retry(
        self,
        msg_id: int,
        *,
        attempts: int,
        delay_seconds: int,
    ) -> None:
        self._conn.execute(
            """
            UPDATE outbox
            SET status = 'pending',
                attempts = %s,
                next_attempt_at = now() + (%s * interval '1 second')
            WHERE id = %s
            """,
            (attempts, delay_seconds, msg_id),
        )

    def mark_outbox_dead(self, msg_id: int, *, attempts: int) -> None:
        self._conn.execute(
            """
            UPDATE outbox
            SET status = 'dead',
                attempts = %s
            WHERE id = %s
            """,
            (attempts, msg_id),
        )
