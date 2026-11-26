"""Registration API shared by :class:`IggyBroker` and :class:`IggyRouter`."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

from apache_iggy import PollingStrategy, PollOptions
from typing_extensions import override

from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy

from ..publisher.factory import create_publisher
from ..publisher.usecase import IggyPublisher
from ..subscriber.factory import create_subscriber
from ..subscriber.usecase import IggySubscriber
from .config import IggyBrokerConfig

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable


class IggyRegistrator(Registrator[Any, IggyBrokerConfig]):
    """Includable to IggyBroker router."""

    @override
    def subscriber(
        self,
        *topics: str,
        group: str | None = None,
        partitions: Sequence[int] = (),
        polling_strategy: PollingStrategy | None = None,
        poll_options: PollOptions | None = None,
        create_consumer_group_if_not_exists: bool = True,
        polling_retry_interval: timedelta | None = None,
        init_retries: int | None = None,
        init_retry_interval: timedelta | None = None,
        allow_replay: bool = False,
        declare: bool = False,
        partitions_count: int = 1,
        persistent: bool = True,
        dependencies: Sequence["Dependant"] = (),
        parser: "CustomCallable | None" = None,
        decoder: "CustomCallable | None" = None,
        codec: "CodecProto | None" = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> IggySubscriber:
        """Create a subscriber for one Iggy ``stream:topic`` address."""
        if len(topics) != 1:
            msg = "Iggy subscriber requires exactly one `stream:topic` address."
            raise SetupError(msg)

        config = cast("IggyBrokerConfig", self.config)
        subscriber = create_subscriber(
            topics[0],
            group=group,
            partitions=partitions,
            polling_strategy=polling_strategy,
            poll_options=poll_options,
            create_consumer_group_if_not_exists=create_consumer_group_if_not_exists,
            polling_retry_interval=polling_retry_interval,
            init_retries=init_retries,
            init_retry_interval=init_retry_interval,
            allow_replay=allow_replay,
            declare=declare,
            partitions_count=partitions_count,
            config=config,
            ack_policy=ack_policy,
            no_reply=no_reply,
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
        )

        super().subscriber(subscriber, persistent=persistent)
        subscriber.add_call(
            parser_=parser,
            decoder_=decoder,
            dependencies_=dependencies,
            codec_=codec,
        )
        return subscriber

    @override
    def publisher(
        self,
        topic: str,
        *,
        key: str | bytes | None = None,
        partition: int | None = None,
        headers: Mapping[str, Any] | None = None,
        reply_to: str = "",
        declare: bool = False,
        partitions_count: int = 1,
        persistent: bool = True,
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> IggyPublisher:
        """Create a long-living publisher to an Iggy topic."""
        config = cast("IggyBrokerConfig", self.config)
        publisher = create_publisher(
            topic,
            key=key,
            partition=partition,
            headers=headers,
            reply_to=reply_to,
            declare=declare,
            partitions_count=partitions_count,
            config=config,
            schema_=schema,
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
        )

        super().publisher(publisher, persistent=persistent)
        return publisher
