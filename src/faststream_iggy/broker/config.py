"""Configuration for the Iggy broker."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from apache_iggy import (
    IggyClient,
    PollingStrategy,
    PollOptions,
    TopicOptions,
)

from faststream._internal.configs import (
    BrokerConfig,
    UnderlyingDriverAnnotation,
    UnderlyingDriverAnnotations,
)
from faststream._internal.parser import DefaultCodec

from ..address import DEFAULT_STREAM
from ..publisher.producer import IggyProducer

if TYPE_CHECKING:
    from faststream._internal.parser import CodecProto


def _context_annotations_factory() -> UnderlyingDriverAnnotations:
    from apache_iggy import IggyClient as IggyClientDriver  # noqa: PLC0415

    from faststream_iggy.annotations import (  # noqa: PLC0415
        IggyBroker,
        IggyClient,
        IggyMessage,
        IggyProducer,
    )
    from faststream_iggy.broker.broker import IggyBroker as IggyBrokerDriver  # noqa: PLC0415
    from faststream_iggy.message import IggyMessage as IggyMessageDriver  # noqa: PLC0415
    from faststream_iggy.publisher.producer import (  # noqa: PLC0415
        IggyProducer as IggyProducerDriver,
    )

    return MappingProxyType(
        {
            IggyClientDriver: UnderlyingDriverAnnotation(
                type_hint=IggyClient,
                module="faststream_iggy.annotations",
                name="IggyClient",
            ),
            IggyBrokerDriver: UnderlyingDriverAnnotation(
                type_hint=IggyBroker,
                module="faststream_iggy.annotations",
                name="IggyBroker",
            ),
            IggyMessageDriver: UnderlyingDriverAnnotation(
                type_hint=IggyMessage,
                module="faststream_iggy.annotations",
                name="IggyMessage",
            ),
            IggyProducerDriver: UnderlyingDriverAnnotation(
                type_hint=IggyProducer,
                module="faststream_iggy.annotations",
                name="IggyProducer",
            ),
        },
    )


async def ensure_topic(
    client: Any,
    stream: str,
    topic: str,
    partitions_count: int = 1,
) -> None:
    """Create a stream/topic pair if it does not exist yet."""
    if await client.get_stream(stream) is None:
        await client.create_stream(stream)

    if await client.get_topic(stream, topic) is None:
        await client.create_topic(
            stream,
            topic,
            TopicOptions(partitions_count=partitions_count),
        )


@dataclass(kw_only=True)
class IggyBrokerConfig(BrokerConfig):
    producer: IggyProducer = field(
        default_factory=lambda: IggyProducer(parser=None, decoder=None),
    )
    client: IggyClient | Any | None = None

    default_stream: str = DEFAULT_STREAM
    auto_create_topics: bool = True
    partitions_count: int = 1

    polling_strategy: PollingStrategy | None = None
    poll_options: PollOptions | None = None

    default_driver_annotations: UnderlyingDriverAnnotations = field(
        default_factory=_context_annotations_factory,
    )

    async def connect(
        self,
        client: IggyClient,
        codec: "CodecProto | None" = None,
    ) -> None:
        self.client = client
        await self.producer.connect(
            client,
            serializer=self.fd_config._serializer,
            codec=codec or DefaultCodec(),
        )

    async def disconnect(self) -> None:
        await self.producer.disconnect()
        self.client = None
