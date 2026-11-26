"""Configuration objects for Iggy subscribers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING

from apache_iggy import PollingStrategy, PollOptions

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy

from ..broker.config import IggyBrokerConfig

if TYPE_CHECKING:
    pass


@dataclass(kw_only=True)
class IggySubscriberSpecificationConfig(SubscriberSpecificationConfig):
    stream: str = ""
    topic: str = ""
    group: str | None = None
    partitions: Sequence[int] = field(default_factory=tuple)


@dataclass(kw_only=True)
class IggySubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "IggyBrokerConfig" = field(default_factory=IggyBrokerConfig)

    stream: str = ""
    topic: str = ""
    group: str | None = None
    partitions: Sequence[int] = field(default_factory=tuple)

    polling_strategy: PollingStrategy | None = None
    poll_options: PollOptions | None = None
    create_consumer_group_if_not_exists: bool = True
    polling_retry_interval: timedelta | None = None
    init_retries: int | None = None
    init_retry_interval: timedelta | None = None
    allow_replay: bool = False

    declare: bool = False
    partitions_count: int = 1

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is not EMPTY:
            return self._ack_policy
        if self._outer_config.ack_policy is not EMPTY:
            return self._outer_config.ack_policy
        return AckPolicy.ACK
