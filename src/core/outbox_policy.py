"""Outbox retry / dead-letter policy constants (story 1.4)."""

from __future__ import annotations

OUTBOX_MAX_ATTEMPTS = 10


def outbox_backoff_seconds(attempts: int) -> int:
    """Delay after a transient failure given current (pre-bump) attempts."""
    return min(2**attempts, 300)
