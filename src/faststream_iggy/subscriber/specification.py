"""AsyncAPI specification for Iggy subscribers."""

from __future__ import annotations

from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec

from ..broker.config import IggyBrokerConfig
from .config import IggySubscriberSpecificationConfig


class IggySubscriberSpecification(
    SubscriberSpecification[IggyBrokerConfig, IggySubscriberSpecificationConfig],
):
    __slots__ = ()

    @property
    def address(self) -> str:
        prefix = self._outer_config.prefix
        return f"{self.config.stream}:{prefix}{self.config.topic}"

    @property
    def channel_labels(self) -> list[str]:
        return [self.address]

    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()
        address = self.address
        handler_name = self._channel_key(address, split=False)

        return {
            handler_name: SubscriberSpec(
                address=address,
                description=self.description,
                operation=Operation(
                    message=Message(
                        title=f"{handler_name}:Message",
                        payload=resolve_payloads(payloads),
                    ),
                    bindings=None,
                ),
                bindings=None,
            ),
        }
