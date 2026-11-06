"""Route outbox rows to channel senders; unknown channels fail closed."""

from __future__ import annotations

from ports.outbox_sender import OutboxMessage, OutboxSenderPort, PermanentError


class ChannelRouter:
    def __init__(self, senders: dict[str, OutboxSenderPort]) -> None:
        self._senders = senders

    def send(self, msg: OutboxMessage) -> str:
        sender = self._senders.get(msg.channel)
        if sender is None:
            raise PermanentError(f"unknown outbox channel: {msg.channel!r}")
        return sender.send(msg)
