"""Iggy subscriber endpoint."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import anyio
from apache_iggy import (
    CommitPolicy,
    IggyConsumer,
    PollingStrategy,
    PollOptions,
)
from typing_extensions import override

from faststream._internal.endpoint.subscriber import SubscriberUsecase
from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.utils import process_msg
from faststream.message import StreamMessage

from ..broker.config import ensure_topic
from ..parser import IggyParser
from ..publisher.fake import IggyFakePublisher

if TYPE_CHECKING:
    from apache_iggy import IggyClient
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection

    from .config import IggySubscriberConfig


class IggySubscriber(TasksMixin, SubscriberUsecase[Any]):
    """Consume messages from one Iggy stream/topic."""

    __slots__ = (
        "_consumer",
        "_iggy_parser",
        "_iterator",
        "allow_replay",
        "create_consumer_group_if_not_exists",
        "declare",
        "group",
        "init_retries",
        "init_retry_interval",
        "partitions",
        "partitions_count",
        "poll_options",
        "polling_retry_interval",
        "polling_strategy",
        "stream",
        "topic",
    )

    def __init__(
        self,
        config: "IggySubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        parser = IggyParser()
        config.parser = parser.parse_message
        config.decoder = parser.decode_message

        super().__init__(config, specification, calls)

        self._iggy_parser = parser
        self._consumer: IggyConsumer | None = None
        self._iterator: AsyncIterator[Any] | None = None

        self.stream = config.stream
        self.topic = config.topic
        self.group = config.group
        self.create_consumer_group_if_not_exists = (
            config.create_consumer_group_if_not_exists
        )
        self.partitions = tuple(config.partitions)
        self.polling_strategy = config.polling_strategy
        self.poll_options = config.poll_options
        self.polling_retry_interval = config.polling_retry_interval
        self.init_retries = config.init_retries
        self.init_retry_interval = config.init_retry_interval
        self.allow_replay = config.allow_replay
        self.declare = config.declare
        self.partitions_count = config.partitions_count

    @property
    def address(self) -> str:
        return f"{self.stream}:{self._outer_config.prefix}{self.topic}"

    @staticmethod
    def build_log_context(
        message: "StreamMessage[Any] | None",
        address: str = "",
    ) -> dict[str, str]:
        return {
            "topic": address,
            "message_id": getattr(message, "message_id", ""),
        }

    def get_log_context(
        self,
        message: "StreamMessage[Any] | None",
    ) -> dict[str, str]:
        return self.build_log_context(message=message, address=self.address)

    async def _ensure_topic(self) -> None:
        if not (self.declare or self._outer_config.auto_create_topics):
            return

        client = self._outer_config.client
        if client is None:
            return

        await ensure_topic(client, self.stream, self.topic, self.partitions_count)

    async def _create_consumer(self) -> None:
        client: "IggyClient | None" = self._outer_config.client
        if client is None:
            msg = "Iggy subscriber requires an active broker connection."
            raise RuntimeError(msg)

        await self._ensure_topic()

        poll_options = self.poll_options or PollOptions(
            commit=CommitPolicy.Manual(),
            max_wait=timedelta(seconds=1),
        )
        polling_strategy = self.polling_strategy or PollingStrategy.Next()

        common: dict[str, Any] = {
            "polling_strategy": polling_strategy,
            "poll_options": poll_options,
            "polling_retry_interval": self.polling_retry_interval,
            "init_retries": self.init_retries,
            "init_retry_interval": self.init_retry_interval,
            "allow_replay": self.allow_replay,
        }

        if self.group:
            consumer = await client.consumer_group(
                name=self.group,
                stream=self.stream,
                topic=self.topic,
                create_consumer_group_if_not_exists=(
                    self.create_consumer_group_if_not_exists
                ),
                **common,
            )
        else:
            partitions = self.partitions or (0,)
            consumer = await client.consumer(
                name=f"fs-{self.stream}-{self.topic}",
                stream=self.stream,
                topic=self.topic,
                partitions=partitions,
                **common,
            )

        self._iggy_parser._setup(consumer)
        self._consumer = consumer
        self._iterator = consumer.iter_messages()

    @override
    async def start(self) -> None:
        await super().start()
        await self._create_consumer()
        self._post_start()

        if self.calls:
            self.add_task(self._consume_loop, restart_on_failure=False)

    @override
    async def stop(self) -> None:
        self.running = False

        if self._consumer is not None:
            await self._consumer.shutdown()
            self._consumer = None
            self._iterator = None

        await super().stop()

    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> tuple["PublisherProto"]:
        return (
            IggyFakePublisher(
                producer=self._outer_config.producer,
                address=message.reply_to or self.address,
            ),
        )

    async def _process_raw(self, raw_message: Any) -> "StreamMessage[Any]":
        context = self._outer_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        return await process_msg(  # type: ignore[no-any-return]
            msg=raw_message,
            middlewares=(
                m(raw_message, context=context) for m in self._broker_middlewares
            ),
            parser=async_parser,
            decoder=async_decoder,
        )

    async def _consume_loop(self) -> None:
        assert self._iterator is not None

        async for raw_message in self._iterator:
            if not self.running:
                break
            await self.consume(raw_message)

    @override
    async def get_one(self, *, timeout: float = 5.0) -> "StreamMessage[Any] | None":
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        if self._iterator is None:
            await self._create_consumer()

        assert self._iterator is not None
        raw_message: Any | None = None
        with anyio.move_on_after(timeout):
            raw_message = await self._iterator.__anext__()

        if raw_message is None:
            return None

        return await self._process_raw(raw_message)

    @override
    async def __aiter__(self) -> AsyncIterator["StreamMessage[Any]"]:
        assert not self.calls, (
            "You can't use `__aiter__` method if subscriber has registered handlers."
        )

        if self._iterator is None:
            await self._create_consumer()

        assert self._iterator is not None
        async for raw_message in self._iterator:
            yield await self._process_raw(raw_message)
