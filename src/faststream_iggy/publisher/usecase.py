"""Iggy publisher endpoint."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from faststream._internal.endpoint.publisher import PublisherUsecase
from faststream.exceptions import FeatureNotSupportedException
from faststream.response.publish_type import PublishType

from ..address import add_prefix, parse_address
from ..broker.config import ensure_topic
from ..response import IggyPublishCommand

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import PublisherMiddleware
    from faststream.response.response import PublishCommand

    from .config import IggyPublisherConfig


class IggyPublisher(PublisherUsecase):
    """A long-living publisher to an Iggy topic."""

    __slots__ = (
        "declare",
        "headers",
        "key",
        "partition",
        "partitions_count",
        "reply_to",
        "stream",
        "topic",
    )

    def __init__(self, config: "IggyPublisherConfig", specification: Any) -> None:
        super().__init__(config, specification)

        address = parse_address(config.address, config._outer_config.default_stream)
        prefixed = add_prefix(
            address.address,
            config._outer_config.prefix,
            config._outer_config.default_stream,
        )
        address = parse_address(prefixed)

        self.stream = address.stream
        self.topic = address.topic
        self.key = config.key
        self.partition = config.partition
        self.headers = dict(config.headers or {})
        self.reply_to = config.reply_to
        self.declare = config.declare
        self.partitions_count = config.partitions_count

    @property
    def address(self) -> str:
        return f"{self.stream}:{self.topic}"

    @override
    async def start(self) -> None:
        if not (self.declare or self._outer_config.auto_create_topics):
            return

        client = self._outer_config.client
        if client is not None:
            await ensure_topic(client, self.stream, self.topic, self.partitions_count)

    @override
    async def publish(
        self,
        message: "SendableMessage" = None,
        topic: str = "",
        *,
        stream: str = "",
        key: str | bytes | None = None,
        partition: int | None = None,
        headers: Mapping[str, Any] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
    ) -> Any:
        """Publish a message to an Iggy topic."""
        destination = (
            add_prefix(
                topic,
                self._outer_config.prefix,
                self._outer_config.default_stream,
            )
            if topic
            else reply_to or self.address
        )
        cmd = IggyPublishCommand(
            message,
            topic=destination,
            stream=stream or None,
            key=key if key is not None else self.key,
            partition=partition if partition is not None else self.partition,
            headers=self.headers | dict(headers or {}),
            correlation_id=correlation_id or self._outer_config.id_generator(),
            reply_to=reply_to,
            _publish_type=PublishType.PUBLISH,
        )
        return await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=(),
        )

    @override
    async def _publish(
        self,
        cmd: "PublishCommand | IggyPublishCommand",
        *,
        _extra_middlewares: "Iterable[PublisherMiddleware]",
    ) -> None:
        """Publish a command produced inside a subscriber flow."""
        real_cmd = IggyPublishCommand.from_cmd(cmd)
        real_cmd.stream = self.stream
        real_cmd.topic = self.topic
        real_cmd.destination = self.address
        real_cmd.add_headers(self.headers, override=False)
        real_cmd.reply_to = real_cmd.reply_to or self.reply_to
        real_cmd.partition = (
            real_cmd.partition if real_cmd.partition is not None else self.partition
        )
        real_cmd.key = real_cmd.key if real_cmd.key is not None else self.key

        await self._basic_publish(
            real_cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=_extra_middlewares,
        )

    @override
    async def publish_batch(
        self,
        *messages: "SendableMessage",
        topic: str = "",
        stream: str = "",
        key: str | bytes | None = None,
        partition: int | None = None,
        headers: Mapping[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> Any:
        """Publish several messages to one Iggy topic."""
        if not messages:
            msg = "At least one message is required for `publish_batch`."
            raise ValueError(msg)

        destination = (
            add_prefix(
                topic,
                self._outer_config.prefix,
                self._outer_config.default_stream,
            )
            if topic
            else self.address
        )
        cmd = IggyPublishCommand(
            *messages,
            topic=destination,
            stream=stream or None,
            key=key if key is not None else self.key,
            partition=partition if partition is not None else self.partition,
            headers=self.headers | dict(headers or {}),
            correlation_id=correlation_id or self._outer_config.id_generator(),
            _publish_type=PublishType.PUBLISH,
        )
        return await self._basic_publish_batch(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=(),
        )

    @override
    async def request(
        self,
        message: "SendableMessage" = None,
        topic: str = "",
        *,
        timeout: float = 0.5,
        correlation_id: str | None = None,
        headers: Mapping[str, Any] | None = None,
    ) -> Any:
        msg = "Iggy doesn't support `request` without a reply-topic implementation."
        raise FeatureNotSupportedException(msg)
