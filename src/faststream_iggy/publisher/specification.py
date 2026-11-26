"""AsyncAPI specification for Iggy publishers."""

from __future__ import annotations

from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec

from ..address import add_prefix
from ..broker.config import IggyBrokerConfig
from .config import IggyPublisherSpecificationConfig


class IggyPublisherSpecification(
    PublisherSpecification[IggyBrokerConfig, IggyPublisherSpecificationConfig],
):
    __slots__ = ()

    @property
    def address(self) -> str:
        return add_prefix(self.config.address, self._outer_config.prefix)

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_
        return f"{self.address}:Publisher"

    def get_schema(self) -> dict[str, PublisherSpec]:
        payloads = self.get_payloads()

        return {
            self.name: PublisherSpec(
                address=self.address,
                description=self.config.description_,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads, "Publisher"),
                    ),
                    bindings=None,
                ),
                bindings=None,
            ),
        }
