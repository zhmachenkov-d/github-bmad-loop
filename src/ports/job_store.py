"""Job / unit transition port surface used by the stub worker."""

from __future__ import annotations

from typing import Any, Protocol

from core.stub_unit import ApplyTransitionResult, UnitJobLease


class JobStorePort(Protocol):
    def lease_next_unit_job(self, worker_id: str) -> UnitJobLease | None:
        """Short TX: claim one actionable unit job with FOR UPDATE SKIP LOCKED."""

    def apply_transition(
        self,
        lease: UnitJobLease,
        worker_id: str,
        event: str,
        result: Any = None,
    ) -> ApplyTransitionResult:
        """AD-12 subset: one TX, lease/version guards, SM edge, event_log, outbox."""

    def mark_outbox_sent(self, *, limit: int = 20) -> int:
        """Stub dispatcher: pending → sent. Returns number of rows marked."""
