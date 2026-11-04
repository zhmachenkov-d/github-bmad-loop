"""Stub Unit SM for story 1.1 only: stub_ready → stub_done. Story 1.2 replaces this."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

STUB_READY = "stub_ready"
STUB_DONE = "stub_done"
STUB_EVENT_TYPE = "stub_unit_complete"
STUB_OUTBOX_CHANNEL = "stub"
STUB_OUTBOX_EFFECT = "stub_transition"


class LeaseLostError(Exception):
    """Raised when apply cannot proceed because version or lease_id no longer match."""


@dataclass(frozen=True, slots=True)
class UnitJobLease:
    job_id: UUID
    run_id: UUID
    unit_id: UUID
    lease_id: UUID
    job_version: int
    unit_state: str


@dataclass(frozen=True, slots=True)
class StubTransition:
    """Result of a successful stub_ready → stub_done apply (or a documented no-op)."""

    applied: bool
    from_state: str | None
    to_state: str | None
    job_version_after: int | None
    idempotency_key: str | None


def outbox_idempotency_key(*, unit_id: UUID, transition_version: int) -> str:
    """AD-16: aggregate_type:aggregate_id:effect_kind:dedupe_token."""
    dedupe_token = str(transition_version)
    return f"unit:{unit_id}:{STUB_OUTBOX_EFFECT}:{dedupe_token}"
