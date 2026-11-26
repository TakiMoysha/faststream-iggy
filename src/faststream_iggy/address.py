"""Address helpers for Iggy streams and topics."""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_STREAM = "default"


@dataclass(frozen=True, slots=True)
class IggyAddress:
    """A parsed ``stream:topic`` address."""

    stream: str
    topic: str

    @property
    def address(self) -> str:
        return f"{self.stream}:{self.topic}"


def add_prefix(
    address: str,
    prefix: str,
    default_stream: str = DEFAULT_STREAM,
) -> str:
    """Apply a broker prefix to the topic part of a ``stream:topic`` address."""
    parsed = parse_address(address, default_stream)
    if not prefix:
        return parsed.address
    return IggyAddress(stream=parsed.stream, topic=f"{prefix}{parsed.topic}").address


def parse_address(address: str, default_stream: str = DEFAULT_STREAM) -> IggyAddress:
    """Split a FastStream address into an Iggy stream and topic.

    Accepted forms: ``stream:topic``, ``stream/topic`` and plain ``topic``
    (which uses ``default_stream``).
    """
    if not address:
        msg = "Iggy address must not be empty."
        raise ValueError(msg)

    for separator in (":", "/"):
        if separator in address:
            stream, topic = address.split(separator, 1)
            break
    else:
        stream, topic = default_stream, address

    if not stream or not topic:
        msg = f"Invalid Iggy address {address!r}: both stream and topic are required."
        raise ValueError(msg)

    return IggyAddress(stream=stream, topic=topic)
