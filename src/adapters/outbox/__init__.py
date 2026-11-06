"""Stub channel senders for the outbox dispatcher (no real API I/O)."""

from adapters.outbox.router import ChannelRouter
from adapters.outbox.senders import GitHubStubSender, SlackStubSender, StubSender

__all__ = [
    "ChannelRouter",
    "GitHubStubSender",
    "SlackStubSender",
    "StubSender",
]
