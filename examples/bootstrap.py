import asyncio
import logging
import os
from socket import if_nameindex

from faststream_iggy import IggyBroker, TestIggyBroker

IGGY_URL = os.getenv("IGGY_URL")

if not IGGY_URL:
    broker = IggyBroker(default_stream="test", log_level=logging.WARNING)
else:
    broker = IggyBroker(IGGY_URL, default_stream="test", log_level=logging.WARNING)


@broker.subscriber("events")
async def handle(msg: str) -> None:
    print("HANDLED", msg)


@broker.publisher("out")
async def produce(msg: str) -> str:
    return msg.upper()


async def main() -> None:
    is_running = True
    async with broker as br:
        try:
            while is_running:
                await br.publish("hello", "events")
                await br.publish("world", "out")
        except KeyboardInterrupt:
            is_running = False
            print("publisher mock", br.publishers[0].mock.call_count)
            print(
                "handler mock",
                broker.subscribers[0].calls[0].handler.mock.call_count,
            )


if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(main(), 10))
