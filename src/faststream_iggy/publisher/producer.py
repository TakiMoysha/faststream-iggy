"""Iggy producer used by FastStream publishers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from apache_iggy import IggyClient, Partitioning, SendMessage
from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.parser import DefaultCodec
from faststream._internal.producer import ProducerProto
from faststream.exceptions import FeatureNotSupportedException

from ..parser import IggyParser, encode_user_headers
from ..response import IggyPublishCommand

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.parser import CodecProto
    from faststream._internal.types import CustomCallable


class IggyProducer(ProducerProto[IggyPublishCommand]):
    """Publish FastStream commands through an ``IggyClient``."""

    __slots__ = (
        "_client",
        "_decoder",
        "_parser",
        "codec",
        "serializer",
    )

    def __init__(
        self,
        parser: "CustomCallable | None",
        decoder: "CustomCallable | None",
    ) -> None:
        self._client: IggyClient | None = None
        self.serializer: "SerializerProto | None" = None
        self.codec: "CodecProto" = DefaultCodec()

        default = IggyParser()
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)

    async def connect(
        self,
        client: IggyClient,
        serializer: "SerializerProto | None",
        codec: "CodecProto | None" = None,
    ) -> None:
        self._client = client
        self.serializer = serializer
        self.codec = codec or DefaultCodec()

    async def disconnect(self) -> None:
        self._client = None

    def __bool__(self) -> bool:
        return self._client is not None

    @property
    def closed(self) -> bool:
        return self._client is None

    @staticmethod
    def _partitioning(cmd: IggyPublishCommand) -> Partitioning:
        if cmd.partition is not None:
            return Partitioning.partition_id(cmd.partition)
        if cmd.key is not None:
            return Partitioning.messages_key(cmd.key)
        return Partitioning.balanced()

    @override
    async def publish(self, cmd: IggyPublishCommand) -> Any:
        if self._client is None:
            msg = "Iggy producer is not connected."
            raise RuntimeError(msg)

        headers = {"content-type": "", **cmd.headers_to_publish()}
        messages: list[SendMessage] = []
        for body in cmd.batch_bodies:
            payload, content_type = await self.codec.encode(body, self.serializer)
            final_headers = dict(headers)
            if content_type:
                final_headers["content-type"] = content_type
            messages.append(
                SendMessage(
                    payload,
                    user_headers=encode_user_headers(final_headers),
                ),
            )

        return await self._client.send_messages(
            cmd.stream,
            cmd.topic,
            self._partitioning(cmd),
            messages,
        )

    @override
    async def publish_batch(self, cmd: IggyPublishCommand) -> Any:
        return await self.publish(cmd)

    @override
    async def request(self, cmd: IggyPublishCommand) -> Any:
        msg = "Iggy doesn't support `request` without a reply-topic implementation."
        raise FeatureNotSupportedException(msg)
