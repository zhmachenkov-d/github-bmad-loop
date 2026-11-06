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
