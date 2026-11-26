"""FastStream message wrapper for Apache Iggy."""

from __future__ import annotations

from typing import Any, Protocol

from faststream.message import AckStatus, StreamMessage


class IggyConsumerProto(Protocol):
    """The subset of ``IggyConsumer`` used by message acknowledgement."""

    async def commit(self, checkpoint: Any) -> None: ...


class IggyRawMessageProto(Protocol):
    """The subset of ``ReceiveMessage`` used by the parser."""

    def payload(self) -> bytes: ...

    def offset(self) -> int: ...

    def partition_id(self) -> int: ...

    def checkpoint(self) -> Any: ...

    def user_headers(self) -> Any | None: ...


class FakeIggyConsumer:
    """Consumer used when messages are produced outside a running subscription."""

    async def commit(self, checkpoint: Any) -> None:
        return None


FAKE_CONSUMER = FakeIggyConsumer()


class IggyMessage(StreamMessage[IggyRawMessageProto]):
    """A consumed Iggy message with checkpoint-based acknowledgement."""

    def __init__(self, *args: Any, consumer: IggyConsumerProto, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.consumer = consumer

    @property
    def raw_headers(self) -> dict[str, Any]:
        headers = self.raw_message.user_headers()
        if headers is None:
            return {}
        if hasattr(headers, "to_scalar_dict"):
            return {
                str(key): value for key, value in headers.to_scalar_dict().items()
            }
        return {str(key): value for key, value in dict(headers).items()}

    @property
    def partition_id(self) -> int | None:
        return getattr(self.raw_message, "partition_id", lambda: None)()

    @property
    def offset(self) -> int | None:
        return getattr(self.raw_message, "offset", lambda: None)()

    async def _commit(self) -> None:
        if self.committed is None:
            await self.consumer.commit(self.raw_message.checkpoint())

    async def ack(self) -> None:
        """Commit this message and every earlier message of its partition."""
        await self._commit()
        await super().ack()

    async def reject(self) -> None:
        """Iggy has no reject primitive; commit so the message is not redelivered."""
        await self._commit()
        self.committed = AckStatus.REJECTED

    async def nack(self) -> None:
        """Leave the checkpoint uncommitted so Iggy redelivers it after restart."""
        await super().nack()
