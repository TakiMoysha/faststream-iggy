import unittest
from unittest.mock import AsyncMock

from faststream.specification import AsyncAPI

from faststream_iggy import IggyBroker, IggyRouter, TestIggyBroker
from faststream_iggy.address import parse_address
from faststream_iggy.annotations import IggyMessage as IggyMessageContext
from faststream_iggy.parser import IggyParser
from faststream_iggy.testing import FakeIggyMessage


class AddressTest(unittest.TestCase):
    def test_stream_topic_and_default(self) -> None:
        parsed = parse_address("events:created")
        self.assertEqual(("events", "created"), (parsed.stream, parsed.topic))

        parsed = parse_address("orders/created")
        self.assertEqual(("orders", "created"), (parsed.stream, parsed.topic))

        parsed = parse_address("created", default_stream="s")
        self.assertEqual(("s", "created"), (parsed.stream, parsed.topic))

    def test_empty_address_is_invalid(self) -> None:
        with self.assertRaises(ValueError):
            parse_address("")


class BrokerUrlTest(unittest.TestCase):
    def test_localhost_is_normalized_to_ipv4(self) -> None:
        self.assertEqual(
            "iggy+tcp://u:p@127.0.0.1:8090",
            IggyBroker._normalize_localhost("iggy+tcp://u:p@localhost:8090"),
        )
        self.assertEqual(
            "127.0.0.1:8090",
            IggyBroker._normalize_localhost("localhost:8090"),
        )


class ParserTest(unittest.IsolatedAsyncioTestCase):
    async def test_ack_commits_checkpoint(self) -> None:
        consumer = AsyncMock()
        raw = FakeIggyMessage(
            body=b"{}",
            offset_=3,
            partition_id_=1,
            headers={"content-type": "application/json"},
            consumer=consumer,
        )

        msg = await IggyParser().parse_message(raw)
        self.assertEqual("1-3", msg.message_id)
        self.assertEqual("application/json", msg.content_type)
        self.assertEqual(1, msg.partition_id)
        self.assertEqual(3, msg.offset)

        await msg.ack()

        consumer.commit.assert_awaited_once_with(raw.checkpoint_)


class BrokerTest(unittest.IsolatedAsyncioTestCase):
    async def test_handler_and_publisher(self) -> None:
        broker = IggyBroker(default_stream="s")

        @broker.subscriber("events")
        @broker.publisher("out")
        async def handler(msg: str) -> str:
            return msg.upper()

        async with TestIggyBroker(broker) as test_broker:
            await test_broker.publish("hello", "events")

            handler.mock.assert_called_once_with("hello")
            broker.publishers[0].mock.assert_called_once_with("HELLO")

    async def test_headers_are_passed_to_handler(self) -> None:
        broker = IggyBroker(default_stream="s")
        received: list[IggyMessageContext] = []

        @broker.subscriber("events")
        async def handler(body: dict, msg: IggyMessageContext) -> None:
            received.append(msg)

        async with TestIggyBroker(broker) as test_broker:
            await test_broker.publish(
                {"id": 1},
                "events",
                headers={"trace-id": "abc"},
            )

        self.assertEqual("abc", received[0].headers["trace-id"])
        self.assertEqual("application/json", received[0].content_type)

    async def test_publisher_explicit_topic_uses_broker_stream(self) -> None:
        broker = IggyBroker(default_stream="s")
        called: list[str] = []

        @broker.subscriber("events")
        async def handler(msg: str) -> None:
            called.append(msg)

        publisher = broker.publisher("out")

        async with TestIggyBroker(broker):
            await publisher.publish("x", topic="events")

        self.assertEqual(["x"], called)

    async def test_router_prefix(self) -> None:
        router = IggyRouter(prefix="pre-", default_stream="s")
        called: list[str] = []

        @router.subscriber("events")
        async def handler(msg: str) -> None:
            called.append(msg)

        broker = IggyBroker(default_stream="s", routers=[router])

        async with TestIggyBroker(broker) as test_broker:
            await test_broker.publish("hello", "pre-events")

        self.assertEqual(["hello"], called)

        spec = AsyncAPI(broker).to_specification().to_jsonable()
        self.assertIn("s:pre-events:Handler", spec["channels"])

    async def test_asyncapi_schema(self) -> None:
        broker = IggyBroker(default_stream="s")

        @broker.subscriber("events")
        async def handler(msg: str) -> None:
            pass

        spec = AsyncAPI(broker).to_specification().to_jsonable()
        self.assertIn("s:events:Handler", spec["channels"])
        self.assertEqual(
            "iggy",
            spec["servers"]["development"]["protocol"],
        )
