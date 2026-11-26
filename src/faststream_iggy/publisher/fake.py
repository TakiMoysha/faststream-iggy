"""Fake publisher used for reply-to responses."""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

from faststream._internal.endpoint.publisher.fake import FakePublisher

from ..address import parse_address
from ..response import IggyPublishCommand

if TYPE_CHECKING:
    from faststream._internal.producer import ProducerProto
    from faststream.response.response import PublishCommand


class IggyFakePublisher(FakePublisher):
    """Publish handler responses to an ``IggyMessage.reply_to`` address."""

    __slots__ = ("address",)

    def __init__(
        self,
        producer: "ProducerProto[IggyPublishCommand]",
        address: str,
    ) -> None:
        super().__init__(producer=producer)
        self.address = address

    def patch_command(
        self,
        cmd: Union["PublishCommand", "IggyPublishCommand"],
    ) -> "IggyPublishCommand":
        cmd = super().patch_command(cmd)
        real_cmd = IggyPublishCommand.from_cmd(cmd)
        parsed = parse_address(self.address)
        real_cmd.stream = parsed.stream
        real_cmd.topic = parsed.topic
        real_cmd.destination = parsed.address
        return real_cmd
