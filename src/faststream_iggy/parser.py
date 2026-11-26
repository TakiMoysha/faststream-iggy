"""Parsing and header conversion for Iggy messages."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from faststream.message import decode_message

from .message import FAKE_CONSUMER, IggyConsumerProto, IggyMessage, IggyRawMessageProto

if TYPE_CHECKING:
    from faststream._internal.basic_types import DecodedMessage


def decode_user_headers(headers: Any | None) -> dict[str, Any]:
    """Convert Iggy ``UserHeaders`` to plain FastStream headers."""
    if headers is None:
        return {}

    scalar = headers.to_scalar_dict() if hasattr(headers, "to_scalar_dict") else headers

    result: dict[str, Any] = {}
    for raw_key, raw_value in dict(scalar).items():
        key = (
            raw_key.decode(errors="replace")
            if isinstance(raw_key, bytes)
            else str(raw_key)
        )
        value = (
            raw_value.decode(errors="replace")
            if isinstance(raw_value, bytes)
            else raw_value
        )
        result[key] = value
    return result


def encode_user_headers(headers: Mapping[str, Any]) -> dict[str, Any]:
    """Convert FastStream headers to values accepted by ``SendMessage``.

    Iggy supports str/bytes/bool/int/float header values; anything else is
    serialized with ``str``. ``None`` values are dropped.
    """
    result: dict[str, Any] = {}
    for raw_key, raw_value in headers.items():
        if raw_value is None:
            continue

        key = raw_key.decode(errors="replace") if isinstance(raw_key, bytes) else str(raw_key)
        if isinstance(raw_value, (str, bytes, bool, int, float)):
            result[key] = raw_value
        else:
            result[key] = str(raw_value)
    return result


class IggyParser:
    """Build a FastStream message from an Iggy ``ReceiveMessage``."""

    __slots__ = ("_consumer",)

    def __init__(self) -> None:
        self._consumer: IggyConsumerProto = FAKE_CONSUMER

    def _setup(self, consumer: IggyConsumerProto) -> None:
        self._consumer = consumer

    async def parse_message(self, message: IggyRawMessageProto) -> IggyMessage:
        headers = decode_user_headers(message.user_headers())

        return IggyMessage(
            body=message.payload(),
            headers=headers,
            reply_to=str(headers.get("reply_to", "")),
            content_type=str(headers["content-type"]) if "content-type" in headers else None,
            message_id=f"{message.partition_id()}-{message.offset()}",
            correlation_id=str(headers["correlation_id"])
            if "correlation_id" in headers
            else None,
            raw_message=message,
            consumer=getattr(message, "consumer", self._consumer),
        )

    async def decode_message(  # ruff: ignore[no-self-use]
        self,
        msg: IggyMessage,
    ) -> "DecodedMessage":
        return decode_message(msg)
