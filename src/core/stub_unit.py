"""Stub Unit SM (story 1.2): normative states, transition table, AD-16 keys."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final
from uuid import UUID

STUB_READY = "stub_ready"
STUB_RUNNING = "stub_running"
STUB_PAUSED = "stub_paused"
STUB_DONE = "stub_done"
STUB_FAILED = "stub_failed"

STUB_START = "stub_start"
STUB_COMPLETE = "stub_complete"
STUB_PAUSE = "stub_pause"
STUB_RESUME = "stub_resume"
STUB_FAIL = "stub_fail"

STUB_EFFECT = "stub_effect"
STUB_NOTIFY = "stub_notify"

STUB_OUTBOX_CHANNEL = "stub"

JOB_KIND_UNIT: Final[str] = "unit"
JOB_KIND_RUN: Final[str] = "run"

STUB_UNIT_STATES: Final[frozenset[str]] = frozenset(
    {
        STUB_READY,
        STUB_RUNNING,
        STUB_PAUSED,
        STUB_DONE,
        STUB_FAILED,
    }
)
STUB_TERMINAL_STATES: Final[frozenset[str]] = frozenset({STUB_DONE, STUB_FAILED})


class LeaseLostError(Exception):
    """Lease, version, expiry, or unit row no longer matches apply guards."""


class IllegalTransitionError(Exception):
    """Valid lease but (from_state, event) is not a declared SM edge."""


class InvalidUnitStateError(Exception):
    """Unit state is outside the stub Unit SM enum."""


class UnsupportedResultError(Exception):
    """Apply `result` is not supported (story 1.2: only None)."""


@dataclass(frozen=True, slots=True)
class UnitJobLease:
    """Lease claim DTO for the shared typed-job pool.

    Unit path: ``kind=unit`` with ``run_id`` / ``unit_id`` / ``unit_state`` set.
    Run path: ``kind=run`` with ``run_id`` = job ``ref_id``; unit fields unused.
    """

    job_id: UUID
    run_id: UUID
    unit_id: UUID
    lease_id: UUID
    job_version: int
    unit_state: str
    kind: str = JOB_KIND_UNIT


@dataclass(frozen=True, slots=True)
class TransitionDef:
    to_state: str
    effect: str | None


@dataclass(frozen=True, slots=True)
class ApplyTransitionResult:
    from_state: str
    to_state: str
    job_version_after: int
    idempotency_key: str | None


TRANSITIONS: Final[dict[tuple[str, str], TransitionDef]] = {
    (STUB_READY, STUB_START): TransitionDef(STUB_RUNNING, None),
    (STUB_RUNNING, STUB_COMPLETE): TransitionDef(STUB_DONE, STUB_EFFECT),
    (STUB_RUNNING, STUB_PAUSE): TransitionDef(STUB_PAUSED, STUB_NOTIFY),
    (STUB_PAUSED, STUB_RESUME): TransitionDef(STUB_RUNNING, None),
    (STUB_RUNNING, STUB_FAIL): TransitionDef(STUB_FAILED, STUB_EFFECT),
}


def lookup_transition(*, from_state: str, event: str) -> TransitionDef:
    if from_state in STUB_TERMINAL_STATES:
        raise IllegalTransitionError(f"terminal state {from_state!r}")
    edge = TRANSITIONS.get((from_state, event))
    if edge is None:
        raise IllegalTransitionError(f"unlisted ({from_state!r}, {event!r})")
    if edge.to_state == from_state:
        raise IllegalTransitionError("self-transition")
    return edge


def outbox_idempotency_key(
    *, unit_id: UUID, effect: str, transition_version: int
) -> str:
    """AD-16: aggregate_type:aggregate_id:effect_kind:dedupe_token."""
    dedupe_token = str(transition_version)
    return f"unit:{unit_id}:{effect}:{dedupe_token}"
