# faststream-iggy

Скелет кастомного брокера [FastStream](https://faststream.airt.ai/) для
[Apache Iggy](https://iggy.apache.org/). Пакет поверх `apache-iggy` Python SDK
даёт знакомый API `broker.subscriber`, `broker.publisher`, `broker.publish`,
`IggyRouter`, `TestIggyBroker` и генерацию AsyncAPI-спецификации.

> Статус: минимальный работающий скелет. RPC/`request` намеренно не реализован —
> у Iggy нет нативного reply-примитива.

## Установка

```bash
uv add faststream-iggy
# или, если вы в этом репозитории:
uv sync
```

Зависимости: `faststream>=0.7.9`, `apache-iggy>=0.10.0`, Python `>=3.13`.

## Адреса

Iggy адресует сообщения через `stream` + `topic`. В FastStream используется
один строковый адрес:

| Форма | Что получается |
| --- | --- |
| `events` | stream=`default`, topic=`events` |
| `orders:created` | stream=`orders`, topic=`created` |
| `orders/created` | то же самое, `/` тоже поддерживается |

Stream по умолчанию задаётся параметром брокера `default_stream`.

## Быстрый старт

```python
import asyncio

from faststream_iggy import IggyBroker

broker = IggyBroker(
    "iggy+tcp://iggy:iggy@127.0.0.1:8090",
    default_stream="orders",
    auto_create_topics=True,  # создаст stream/topic при старте
)


@broker.subscriber("created", group="checkout")
@broker.publisher("confirmed")
async def confirm(msg: str) -> str:
    return msg.upper()


async def main() -> None:
    await broker.start()
    try:
        await broker.publish("order-42", "created")
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

Запуск `broker.start()` подключает `IggyClient`, создаёт подписчиков и
publishers. `broker.stop()` останавливает consumer-ов и отключает клиента.
Можно также использовать `async with IggyBroker(...) as broker`.

### Прямая публикация

```python
await broker.publish(
    {"id": 42},
    "orders:created",
    key="order-42",      # Partitioning.messages_key(...)
    partition=1,         # или явный partition_id (имеет приоритет над key)
    headers={"trace-id": "abc"},
)

await broker.publish_batch({"id": 1}, {"id": 2}, topic="orders:created")
```

Тело сообщения кодируется через `codec` FastStream (по умолчанию JSON для
словарей); `content-type` и пользовательские заголовки кладутся в
`SendMessage.user_headers`.

### Подписка и подтверждение

Consumer создаётся с `CommitPolicy.Manual()`. `message.ack()` вызывает
`IggyConsumer.commit(message.checkpoint())`, то есть фиксирует сообщение и все
более ранние сообщения его партиции. `message.nack()` оставляет checkpoint
незафиксированным, `message.reject()` фиксирует его (в Iggy нет reject).

```python
from faststream_iggy.annotations import IggyMessage


@broker.subscriber("created", group="checkout")
async def handle(body: str, msg: IggyMessage) -> None:
    try:
        process(body)
    except Exception:
        await msg.nack()
    else:
        await msg.ack()
```

Доступные атрибуты сообщения: `msg.partition_id`, `msg.offset`, `msg.headers`,
`msg.correlation_id`, `msg.reply_to`, `msg.raw_message` (оригинальный
`ReceiveMessage`).

### Группы и одиночные consumer-ы

- `group="name"` → `client.consumer_group(...)` (недоступно поверх HTTP-транспорта Iggy).
- без `group` → `client.consumer(...)` с `partitions=(0,)` по умолчанию.
  Список партиций задаётся через `partitions=(0, 1, 2)`.

Дополнительные параметры подписчика: `polling_strategy`, `poll_options`,
`polling_retry_interval`, `init_retries`, `init_retry_interval`,
`allow_replay`, `ack_policy`, `no_reply`, `declare`, `partitions_count`.

### Роутеры и префиксы

```python
from faststream_iggy import IggyBroker, IggyRouter

router = IggyRouter(prefix="checkout-", default_stream="orders")


@router.subscriber("created")
async def handle(msg: str) -> None: ...


broker = IggyBroker(default_stream="orders", routers=[router])
```

Префикс применяется только к именам topic-ов. Адрес разбирается в момент
создания подписчика, поэтому у роутера стоит указать тот же `default_stream`,
что и у брокера, если адреса записываются без явного stream.

## Тестирование

`TestIggyBroker` подменяет продюсера in-memory маршрутизатором и не требует
живого сервера Iggy:

```python
from faststream_iggy import IggyBroker, TestIggyBroker

broker = IggyBroker(default_stream="orders")


@broker.subscriber("created")
@broker.publisher("confirmed")
async def confirm(msg: str) -> str:
    return msg.upper()


async def test_confirm() -> None:
    async with TestIggyBroker(broker) as test_broker:
        await test_broker.publish("abc", "created")
        confirm.mock.assert_called_once_with("abc")
        broker.publishers[0].mock.assert_called_once_with("ABC")
```

## AsyncAPI

```python
from faststream.specification import AsyncAPI

spec = AsyncAPI(broker).to_specification().to_jsonable()
```

Серверы получают `protocol: "iggy"`. Специальных Iggy channel/operation binding
в FastStream нет, поэтому channel/operation bindings пока пустые; адреса
channel-ов выглядят как `stream:topic:HandlerName`.

## Локальный сервер

В репозитории есть recipe:

```bash
just podman-run-iggy
# старое имя `podman-rund-iggy` оставлено как алиас
```

Поднимается TCP `:8090` и HTTP `:3010`. Рабочий connection string для recipe
из `justfile`:

```
iggy+tcp://takimoysha:abc123@127.0.0.1:8090
```

Можно передать и обычный `host:port`. URL можно не передавать вручную —
`IggyBroker()` возьмёт его из переменной окружения `IGGY_URL`.

**Важно:** используйте `127.0.0.1`, а не `localhost`. На некоторых системах
`localhost` резолвится сначала в IPv6 `::1`, а Iggy слушает только IPv4 —
подключение выглядит как зависшее. Брокер всё же нормализует `localhost` в
`127.0.0.1`, но лучше задавать IPv4 явно.

## Тесты пакета

```bash
# юнит-тесты (без сервера)
just unittest

# плюс smoke-тест против живого сервера
just fulltest
```

Оба recipe сами выставляют `IGGY_URL` с `127.0.0.1`.

## Ограничения

- `broker.request()` / `publisher.request()` бросают `FeatureNotSupportedException`.
- Consumer groups не работают по HTTP-транспорту Iggy (ограничение сервера).
- `nack()` не открывает новую доставку сразу — он лишь не фиксирует offset;
  повторная доставка произойдёт после перезапуска/пересмотра offset-а.
- Схема AsyncAPI пока не содержит Iggy-specific bindings.
