"""Iggy-specific publish commands and responses."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from faststream.response.publish_type import PublishType
from faststream.response.response import BatchPublishCommand, Response

from .address import DEFAULT_STREAM, parse_address

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage


class IggyPublishMessage(Response):
    """Iggy response object with partition/key metadata."""

    def __init__(
        self,
        body: "SendableMessage",
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        key: str | bytes | None = None,
        partition: int | None = None,
    ) -> None:
        super().__init__(body, headers=headers, correlation_id=correlation_id)
        self.key = key
        self.partition = partition

    def get_publish_key(self) -> str | bytes | None:
        return self.key

    def as_publish_command(self) -> "IggyPublishCommand":
        return IggyPublishCommand(
            self.body,
            topic=f"{DEFAULT_STREAM}:_reply",
            key=self.key,
            partition=self.partition,
            headers=self.headers,
            correlation_id=self.correlation_id,
            _publish_type=PublishType.PUBLISH,
        )


class IggyPublishCommand(BatchPublishCommand):
    """Publish command carrying an Iggy stream/topic and partitioning options."""

    def __init__(
        self,
        message: "SendableMessage",
        /,
        *messages: "SendableMessage",
        topic: str,
        stream: str | None = None,
        _publish_type: PublishType,
        partition: int | None = None,
        key: str | bytes | None = None,
        headers: Mapping[str, Any] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
    ) -> None:
        address = parse_address(topic or f"{DEFAULT_STREAM}:_reply", stream or DEFAULT_STREAM)
        super().__init__(
            message,
            *messages,
            destination=address.address,
            reply_to=reply_to,
            correlation_id=correlation_id,
            headers=dict(headers) if headers is not None else None,
            _publish_type=_publish_type,
        )

        self.stream = address.stream
        self.topic = address.topic
        self.partition = partition
        self.key = key

    @classmethod
    def from_cmd(
        cls,
        cmd: "IggyPublishCommand | Any",
        *,
        batch: bool = False,
    ) -> "IggyPublishCommand":
        if isinstance(cmd, IggyPublishCommand):
            return cmd

        body, extra_bodies = cls._parse_bodies(cmd.body, batch=batch)
        key = cmd.get_publish_key()

        return cls(
            body,
            *extra_bodies,
            topic=cmd.destination,
            key=key if isinstance(key, (str, bytes)) else None,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            reply_to=cmd.reply_to,
            _publish_type=cmd.publish_type,
        )

    def headers_to_publish(self) -> dict[str, Any]:
        headers = dict(self.headers)
        if self.correlation_id:
            headers["correlation_id"] = self.correlation_id
        if self.reply_to:
            headers["reply_to"] = self.reply_to
        return headers
