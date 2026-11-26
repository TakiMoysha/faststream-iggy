"""Configuration objects for Iggy publishers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)

from ..broker.config import IggyBrokerConfig

if TYPE_CHECKING:
    pass


@dataclass(kw_only=True)
class IggyPublisherSpecificationConfig(PublisherSpecificationConfig):
    address: str


@dataclass(kw_only=True)
class IggyPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "IggyBrokerConfig" = field(default_factory=IggyBrokerConfig)

    address: str
    key: str | bytes | None = None
    partition: int | None = None
    headers: Mapping[str, Any] | None = None
    reply_to: str = ""

    declare: bool = False
    partitions_count: int = 1
