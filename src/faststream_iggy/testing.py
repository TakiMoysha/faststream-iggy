"""In-memory test double for :class:`IggyBroker`."""

from __future__ import annotations

from collections.abc import Generator, Iterable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast, overload, override

from apache_iggy import UserHeaders
from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.parser import DefaultCodec
from faststream._internal.testing.broker import (
    EnterType,
    TestBroker,
    change_producer,
)

from .address import parse_address
from .broker.broker import IggyBroker
from .message import FAKE_CONSUMER
from .parser import IggyParser
from .publisher.producer import IggyProducer
from .response import IggyPublishCommand
from .subscriber.usecase import IggySubscriber

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.parser import CodecProto

__all__ = ("TestIggyBroker",)


@dataclass(slots=True)
class FakeIggyMessage:
    """Minimal ``ReceiveMessage``-shaped object used by the fake producer."""

    body: bytes
    offset_: int
    partition_id_: int
    headers: dict[str, Any] | None = None
    consumer: Any = FAKE_CONSUMER
    checkpoint_: Any = field(default_factory=object)

    def payload(self) -> bytes:
        return self.body

    def offset(self) -> int:
        return self.offset_

    def partition_id(self) -> int:
        return self.partition_id_

    def checkpoint(self) -> Any:
        return self.checkpoint_

    def user_headers(self) -> UserHeaders | None:
        return UserHeaders(self.headers) if self.headers else None


class FakeIggyClient:
    """No-op client so declared topics do not hit a real Iggy server."""

    async def get_stream(self, name: str) -> None:
        return None

    async def create_stream(self, name: str) -> None:
        return None

    async def get_topic(self, stream: str, topic: str) -> None:
        return None

    async def create_topic(self, stream: str, topic: str, options: Any = None) -> None:
        return None

    async def ping(self) -> None:
        return None

    async def disconnect(self) -> None:
        return None


class FakeProducer(IggyProducer):
    """Route published messages directly to matching subscribers."""

    def __init__(
        self,
        broker: IggyBroker,
        brokers: Sequence[IggyBroker],
    ) -> None:
        self.broker = broker
        self.brokers = brokers
        self.serializer: "SerializerProto | None" = None

        default = IggyParser()
        self._parser = ParserComposition(broker._parser, default.parse_message)
        self._decoder = ParserComposition(broker._decoder, default.decode_message)
        self.codec: "CodecProto" = broker.config.broker_codec or DefaultCodec()
        self._offsets: dict[int, int] = {}

    def __bool__(self) -> bool:
        return True

    @property
    def closed(self) -> bool:
        return False

    @property
    def subscribers(self) -> Iterable[IggySubscriber]:
        return (
            cast("IggySubscriber", sub)
            for broker in self.brokers
            for sub in broker.subscribers
        )

    @override
    async def publish(self, cmd: IggyPublishCommand) -> None:
        headers = {"content-type": "", **cmd.headers_to_publish()}
        partition = cmd.partition if cmd.partition is not None else 0

        for body in cmd.batch_bodies:
            payload, content_type = await self.codec.encode(body, self.serializer)
            final_headers = dict(headers)
            if content_type:
                final_headers["content-type"] = content_type

            offset = self._offsets.get(partition, 0)
            self._offsets[partition] = offset + 1

            raw = FakeIggyMessage(
                body=payload,
                offset_=offset,
                partition_id_=partition,
                headers=final_headers,
            )

            for handler in self.subscribers:
                address = parse_address(handler.address)
                if address.stream != cmd.stream or address.topic != cmd.topic:
                    continue
                await handler.process_message(raw)

    @override
    async def publish_batch(self, cmd: IggyPublishCommand) -> None:
        await self.publish(cmd)

    @override
    async def request(self, cmd: IggyPublishCommand) -> Any:
        msg = "Iggy doesn't support `request` without a reply-topic implementation."
        from faststream.exceptions import FeatureNotSupportedException  # noqa: PLC0415

        raise FeatureNotSupportedException(msg)


class TestIggyBroker(
    TestBroker[IggyBroker, EnterType],
    broker=IggyBroker,
):
    """In-memory test client for Iggy handlers."""

    @overload
    def __init__(
        self: "TestIggyBroker[IggyBroker]",
        broker: IggyBroker,
        /,
        *,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: "TestIggyBroker[tuple[IggyBroker, ...]]",
        *brokers: IggyBroker,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None: ...

    def __init__(
        self,
        *brokers: IggyBroker,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None:
        super().__init__(
            *brokers,
            with_real=with_real,
            connect_only=connect_only,
        )

    @contextmanager
    def _patch_producer(self, broker: IggyBroker) -> Generator[None, None, None]:
        with change_producer(
            broker.config.broker_config,
            FakeProducer(broker, self.brokers),
        ):
            yield

    @override
    async def _fake_connect(
        self,
        broker: IggyBroker,
        *args: Any,
        **kwargs: Any,
    ) -> FakeIggyClient:
        client = FakeIggyClient()
        broker.config.broker_config.client = client
        return client

    def create_publisher_fake_subscriber(
        self,
        broker: IggyBroker,
        publisher: Any,
    ) -> tuple[IggySubscriber, bool]:
        for handler in (
            cast("IggySubscriber", sub) for br in self.brokers for sub in br.subscribers
        ):
            address = parse_address(handler.address)
            if address.stream == publisher.stream and address.topic == publisher.topic:
                return handler, True

        prefix = broker.config.prefix
        topic = publisher.topic.removeprefix(prefix)
        sub = broker.subscriber(
            f"{publisher.stream}:{topic}",
            persistent=False,
        )
        return cast("IggySubscriber", sub), False
