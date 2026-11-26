"""Factory for Iggy subscribers."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from apache_iggy import PollingStrategy, PollOptions

from faststream._internal.endpoint.subscriber.call_item import CallsCollection

from ..address import parse_address
from .config import IggySubscriberConfig, IggySubscriberSpecificationConfig
from .specification import IggySubscriberSpecification
from .usecase import IggySubscriber

if TYPE_CHECKING:
    from ..broker.config import IggyBrokerConfig


def create_subscriber(
    topic: str,
    *,
    group: str | None,
    partitions: Sequence[int],
    polling_strategy: PollingStrategy | None,
    poll_options: PollOptions | None,
    create_consumer_group_if_not_exists: bool,
    polling_retry_interval: timedelta | None,
    init_retries: int | None,
    init_retry_interval: timedelta | None,
    allow_replay: bool,
    declare: bool,
    partitions_count: int,
    config: "IggyBrokerConfig",
    ack_policy: Any,
    no_reply: bool,
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> IggySubscriber:
    address = parse_address(topic, config.default_stream)

    subscriber_config = IggySubscriberConfig(
        stream=address.stream,
        topic=address.topic,
        group=group,
        partitions=tuple(partitions),
        polling_strategy=polling_strategy,
        poll_options=poll_options,
        create_consumer_group_if_not_exists=create_consumer_group_if_not_exists,
        polling_retry_interval=polling_retry_interval,
        init_retries=init_retries,
        init_retry_interval=init_retry_interval,
        allow_replay=allow_replay,
        declare=declare,
        partitions_count=partitions_count,
        no_reply=no_reply,
        _ack_policy=ack_policy,
        _outer_config=config,
    )

    calls = CallsCollection[Any]()
    specification = IggySubscriberSpecification(
        _outer_config=config,
        calls=calls,
        specification_config=IggySubscriberSpecificationConfig(
            stream=address.stream,
            topic=address.topic,
            group=group,
            partitions=tuple(partitions),
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
    )

    return IggySubscriber(subscriber_config, specification, calls)


__all__ = ("create_subscriber",)
