"""FastStream broker for Apache Iggy."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable, Mapping, Sequence
from functools import partial
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import urlsplit, urlunsplit

from apache_iggy import IggyClient, PollingStrategy, PollOptions
from fast_depends import Provider, dependency_provider
from typing_extensions import override

from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream._internal.logger import DefaultLoggerStorage, make_logger_state
from faststream._internal.types import IdGenerator
from faststream.exceptions import FeatureNotSupportedException
from faststream.message import gen_cor_id
from faststream.middlewares import AckPolicy
from faststream.response.publish_type import PublishType
from faststream.specification.schema import BrokerSpec

from ..publisher.producer import IggyProducer
from ..response import IggyPublishCommand
from .config import IggyBrokerConfig, ensure_topic
from .registrator import IggyRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import LoggerProto, SendableMessage
    from faststream._internal.configs import UnderlyingDriverAnnotations
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict

make_iggy_logger_state = partial(
    make_logger_state,
    default_storage_cls=DefaultLoggerStorage,
)


class IggyBroker(
    IggyRegistrator,
    BrokerUsecase[Any, IggyClient, IggyBrokerConfig],
):
    """FastStream broker backed by the ``apache-iggy`` Python SDK."""

    def __init__(
        self,
        url: str | None = None,
        *,
        default_stream: str = "default",
        auto_create_topics: bool = True,
        partitions_count: int = 1,
        polling_strategy: PollingStrategy | None = None,
        poll_options: PollOptions | None = None,
        graceful_timeout: float | None = 15.0,
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        codec: CodecProto | None = None,
        dependencies: Sequence[Dependant] = (),
        middlewares: Sequence[BrokerMiddleware[Any, Any]] = (),
        routers: Iterable[IggyRegistrator] = (),
        ack_policy: AckPolicy = EMPTY,
        id_generator: IdGenerator = gen_cor_id,
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = None,
        description: str | None = None,
        tags: Iterable["Tag | TagDict"] = (),
        security: Optional["BaseSecurity"] = None,
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
        underlying_driver_annotations: "UnderlyingDriverAnnotations | None" = None,
    ) -> None:
        resolved_url = (
            url or os.getenv("IGGY_URL") or "iggy+tcp://iggy:iggy@127.0.0.1:8090"
        )
        producer = IggyProducer(parser=parser, decoder=decoder)
        spec_url = specification_url or resolved_url

        super().__init__(
            url=resolved_url,
            config=IggyBrokerConfig(
                producer=producer,
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                broker_codec=codec,
                logger=make_iggy_logger_state(
                    logger=logger if logger is not EMPTY else None,
                    log_level=log_level,
                ),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    provider=provider or dependency_provider,
                    context=context or ContextRepo(),
                ),
                broker_dependencies=dependencies,
                graceful_timeout=graceful_timeout,
                ack_policy=ack_policy,
                id_generator=id_generator,
                extra_context={"broker": self},
                underlying_driver_annotations=(underlying_driver_annotations or {}),
                default_stream=default_stream,
                auto_create_topics=auto_create_topics,
                partitions_count=partitions_count,
                polling_strategy=polling_strategy,
                poll_options=poll_options,
            ),
            specification=BrokerSpec(
                description=description,
                url=[spec_url],
                protocol=protocol or "iggy",
                protocol_version=protocol_version,
                tags=tags,
                security=security,
            ),
            routers=routers,
        )

    @staticmethod
    def _normalize_localhost(url: str) -> str:
        """Prefer IPv4 for ``localhost``; Iggy's TCP listener binds 0.0.0.0 only."""
        if "://" not in url:
            if url.startswith("localhost:"):
                return f"127.0.0.1:{url.removeprefix('localhost:')}"
            return url

        parts = urlsplit(url)
        if parts.hostname != "localhost":
            return url

        hostport = parts.netloc.rsplit("@", 1)[-1]
        new_hostport = hostport.replace("localhost", "127.0.0.1", 1)
        netloc = parts.netloc[: -len(hostport)] + new_hostport
        return urlunsplit(
            (parts.scheme, netloc, parts.path, parts.query, parts.fragment),
        )

    @classmethod
    def _create_client(cls, url: str) -> IggyClient:
        url = cls._normalize_localhost(url)
        if "://" in url:
            return IggyClient.from_connection_string(url)
        return IggyClient(url)

    @override
    async def _connect(self) -> IggyClient:
        client = self._create_client(self._connection_kwargs["url"])
        await client.connect()
        await self.config.broker_config.connect(
            client,
            codec=self.config.broker_codec,
        )
        return client

    @override
    async def start(self) -> None:
        await self.connect()
        await super().start()

    @override
    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        try:
            await super().stop(exc_type, exc_val, exc_tb)
        finally:
            if self._connection is not None:
                await self._connection.disconnect()
                self._connection = None

            await self.config.broker_config.disconnect()

    @override
    async def ping(self, timeout: float | None = None) -> bool:
        if self._connection is None:
            return False
        try:
            await self._connection.ping()
        except Exception:
            return False
        return True

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
        """Publish a message to an Iggy ``stream:topic`` address."""
        cmd = IggyPublishCommand(
            message,
            topic=topic or reply_to,
            stream=stream or self.config.default_stream,
            key=key,
            partition=partition,
            headers=headers,
            correlation_id=correlation_id or self.config.id_generator(),
            reply_to=reply_to,
            _publish_type=PublishType.PUBLISH,
        )
        await self._ensure_publish_destination(cmd)
        return await self._basic_publish(cmd, producer=self.config.producer)

    async def _ensure_publish_destination(self, cmd: IggyPublishCommand) -> None:
        if self.config.auto_create_topics and self._connection is not None:
            await ensure_topic(
                self._connection,
                cmd.stream,
                cmd.topic,
                self.config.partitions_count,
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
        """Publish several messages in one Iggy batch."""
        if not messages:
            msg = "At least one message is required for `publish_batch`."
            raise ValueError(msg)

        cmd = IggyPublishCommand(
            *messages,
            topic=topic,
            stream=stream or self.config.default_stream,
            key=key,
            partition=partition,
            headers=headers,
            correlation_id=correlation_id or self.config.id_generator(),
            _publish_type=PublishType.PUBLISH,
        )
        await self._ensure_publish_destination(cmd)
        return await self._basic_publish_batch(cmd, producer=self.config.producer)

    @override
    async def request(
        self,
        message: "SendableMessage" = None,
        topic: str = "",
        /,
        timeout: float = 0.5,
        correlation_id: str | None = None,
        headers: Mapping[str, Any] | None = None,
    ) -> Any:
        msg = "Iggy doesn't support `request` without a reply-topic implementation."
        raise FeatureNotSupportedException(msg)
