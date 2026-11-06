"""Job / unit transition port surface used by the stub worker."""

from __future__ import annotations

from typing import Any, Protocol
from uuid import UUID

from core.stub_unit import ApplyTransitionResult, UnitJobLease
from ports.outbox_sender import OutboxMessage


class JobStorePort(Protocol):
    def lease_next(self, worker_id: str) -> UnitJobLease | None:
        """Short TX: claim one actionable run|unit job with FOR UPDATE SKIP LOCKED."""

    def heartbeat(self, job_id: UUID, lease_id: UUID, worker_id: str) -> bool:
        """Extend lease_expires_at by TTL when job+lease+worker match. False if 0 rows."""

    def release(self, job_id: UUID, lease_id: UUID) -> bool:
        """Clear lease columns when lease_id still matches. False if 0 rows."""

    def apply_transition(
        self,
        lease: UnitJobLease,
        worker_id: str,
        event: str,
        result: Any = None,
    ) -> ApplyTransitionResult:
        """AD-12 subset: one TX, lease/version guards, SM edge, event_log, outbox."""

    def try_reconcile_lock(self) -> bool:
        """Non-blocking pg_try_advisory_lock(42). True if this session holds it."""

    def release_reconcile_lock(self) -> None:
        """Release session advisory lock 42 (no-op if not held)."""

    def grace_force_clear_stale_leases(self) -> int:
        """Clear leases expired >5m on non-terminal work; bump version. Return rows."""

    def select_stuck_unit_jobs(self) -> list[dict[str, Any]]:
        """stub_running units past attempts or deadline (FOR UPDATE SKIP LOCKED)."""

    def steal_reconcile_lease(
        self, job_id: UUID, worker_id: str
    ) -> UnitJobLease | None:
        """Force a short reconcile-held lease (even if live). None if race-lost."""

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
        """Insert stub stuck-alert outbox. True if inserted, False on conflict."""

    def select_stuck_run_jobs(self) -> list[dict[str, Any]]:
        """Run-kind jobs past attempts or deadline (FOR UPDATE SKIP LOCKED)."""

    def clear_run_lease_if_held(self, job_id: UUID) -> int | None:
        """Clear lease cols + bump version when held. Return pre-clear version."""

    def select_dead_outbox(self) -> list[dict[str, Any]]:
        """Outbox rows with status='dead' (stable id order)."""

    def enqueue_dead_alert(
        self,
        *,
        run_id: UUID,
        unit_id: UUID | None,
        dead_outbox_id: int,
    ) -> bool:
        """Insert stub dead-alert outbox. True if inserted, False on conflict."""

    def claim_due_outbox(self, *, limit: int = 20) -> list[OutboxMessage]:
        """Claim due pending outbox rows (all channels). Caller holds the TX."""

    def mark_outbox_success(self, msg_id: int, external_id: str) -> None:
        """Mark row sent with external_id + sent_at."""

    def mark_outbox_retry(
        self,
        msg_id: int,
        *,
        attempts: int,
        delay_seconds: int,
    ) -> None:
        """Stay pending with bumped attempts and future next_attempt_at."""

    def mark_outbox_dead(self, msg_id: int, *, attempts: int) -> None:
        """Mark row dead (transient exhausted or permanent failure)."""
