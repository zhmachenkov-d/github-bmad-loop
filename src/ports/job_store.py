"""Job / unit transition port surface used by the stub worker."""

from __future__ import annotations

from typing import Protocol

from core.stub_unit import StubTransition, UnitJobLease


class JobStorePort(Protocol):
    def lease_next_unit_job(self, worker_id: str) -> UnitJobLease | None:
        """Short TX: claim one actionable unit job with FOR UPDATE SKIP LOCKED."""

    def apply_stub_transition(
        self,
        lease: UnitJobLease,
        worker_id: str,
    ) -> StubTransition:
        """One TX: version+lease guard, unit state, event_log, outbox (AD-16)."""

    def mark_outbox_sent(self, *, limit: int = 20) -> int:
        """Stub dispatcher: pending → sent. Returns number of rows marked."""
