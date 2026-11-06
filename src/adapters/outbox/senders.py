"""Stub GitHub/Slack/stub channel senders with injectable failures."""

from __future__ import annotations

from uuid import uuid4

from ports.outbox_sender import OutboxMessage


class StubSender:
    """Default ``stub`` channel: success unless a failure is queued."""

    def __init__(self) -> None:
        self.effects: list[str] = []
        self._failures: list[BaseException] = []

    def fail_next(self, exc: BaseException) -> None:
        self._failures.append(exc)

    def send(self, msg: OutboxMessage) -> str:
        if self._failures:
            raise self._failures.pop(0)
        external_id = f"stub-{uuid4()}"
        self.effects.append(msg.idempotency_key)
        return external_id


class _IdempotentChannelSender:
    """In-memory key→external_id store; second send returns same id, no new effect."""

    def __init__(self, *, prefix: str) -> None:
        self._prefix = prefix
        self._ids: dict[str, str] = {}
        self.effects: list[str] = []
        self._failures: list[BaseException] = []

    def fail_next(self, exc: BaseException) -> None:
        self._failures.append(exc)

    def send(self, msg: OutboxMessage) -> str:
        if self._failures:
            raise self._failures.pop(0)
        existing = self._ids.get(msg.idempotency_key)
        if existing is not None:
            return existing
        external_id = f"{self._prefix}-{uuid4()}"
        self._ids[msg.idempotency_key] = external_id
        self.effects.append(msg.idempotency_key)
        return external_id


class GitHubStubSender(_IdempotentChannelSender):
    def __init__(self) -> None:
        super().__init__(prefix="github")


class SlackStubSender(_IdempotentChannelSender):
    def __init__(self) -> None:
        super().__init__(prefix="slack")
