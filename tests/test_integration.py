"""Optional smoke test against a real Iggy server."""

import asyncio
import os
import unittest
from uuid import uuid4

from faststream_iggy import IggyBroker

IGGY_URL = os.getenv("IGGY_URL")


@unittest.skipUnless(IGGY_URL, "set IGGY_URL to run integration tests")
class IntegrationTest(unittest.IsolatedAsyncioTestCase):
    async def test_publish_consume(self) -> None:
        topic = f"fs-test-{uuid4().hex[:8]}"

        broker = IggyBroker(IGGY_URL, default_stream="fs-smoke")
        received: asyncio.Future[str] = asyncio.Future()

        @broker.subscriber(topic)
        async def handler(msg: str) -> None:
            if not received.done():
                received.set_result(msg)

        await broker.start()
        try:
            await broker.publish("hello", topic)
            self.assertEqual("hello", await asyncio.wait_for(received, timeout=10))
        finally:
            await broker.stop()
