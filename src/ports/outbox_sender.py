"""Outbox sender port: channel delivery without real network I/O (story 1.4)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID


class TransientError(Exception):
    """Retryable send failure → backoff / dead at max attempts."""


class PermanentError(Exception):
    """Non-retryable send failure → dead immediately."""


@dataclass(frozen=True)
class OutboxMessage:
    id: int
    run_id: UUID
    unit_id: UUID | None
    channel: str
    idempotency_key: str
    payload: dict[str, Any]
    attempts: int


class OutboxSenderPort(Protocol):
    def send(self, msg: OutboxMessage) -> str:
        """Deliver ``msg`` and return ``external_id``.

        Raises:
            TransientError: retry with backoff.
            PermanentError: mark dead immediately.
        """
