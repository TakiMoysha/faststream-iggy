"""Factory for Iggy publishers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from ..address import parse_address
from .config import IggyPublisherConfig, IggyPublisherSpecificationConfig
from .specification import IggyPublisherSpecification
from .usecase import IggyPublisher

if TYPE_CHECKING:
    from ..broker.config import IggyBrokerConfig


def create_publisher(
    topic: str,
    *,
    key: str | bytes | None,
    partition: int | None,
    headers: Mapping[str, Any] | None,
    reply_to: str,
    declare: bool,
    partitions_count: int,
    config: "IggyBrokerConfig",
    schema_: Any | None,
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> IggyPublisher:
    address = parse_address(topic, config.default_stream).address

    publisher_config = IggyPublisherConfig(
        address=address,
        key=key,
        partition=partition,
        headers=headers,
        reply_to=reply_to,
        declare=declare,
        partitions_count=partitions_count,
        _outer_config=config,
    )
    specification = IggyPublisherSpecification(
        _outer_config=config,
        specification_config=IggyPublisherSpecificationConfig(
            address=address,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
    )
    return IggyPublisher(publisher_config, specification)
