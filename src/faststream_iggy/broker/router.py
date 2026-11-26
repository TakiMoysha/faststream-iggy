"""Iggy router for splitting handlers across modules."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any

from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy

from ..address import DEFAULT_STREAM
from .config import IggyBrokerConfig
from .registrator import IggyRegistrator

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable


class IggyRouter(IggyRegistrator):
    """A minimal includable router for Iggy handlers."""

    def __init__(
        self,
        *,
        prefix: str = "",
        dependencies: Sequence["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[IggyRegistrator] = (),
        parser: "CustomCallable | None" = None,
        decoder: "CustomCallable | None" = None,
        codec: "CodecProto | None" = None,
        ack_policy: AckPolicy = EMPTY,
        default_stream: str = DEFAULT_STREAM,
        include_in_schema: bool | None = True,
    ) -> None:
        super().__init__(
            config=IggyBrokerConfig(
                prefix=prefix,
                include_in_schema=include_in_schema,
                broker_dependencies=dependencies,
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                broker_codec=codec,
                ack_policy=ack_policy,
                default_stream=default_stream,
            ),
            routers=routers,
        )
