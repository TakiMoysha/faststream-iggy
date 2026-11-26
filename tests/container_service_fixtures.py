# TODO: updated
from __future__ import annotations

import asyncio
import os
import re
import subprocess
import sys
import timeit
from collections.abc import Awaitable, Callable, Generator
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import pytest
from redis.asyncio import Redis as AsyncRedis
from redis.exceptions import ConnectionError as RedisConnectionError
from valkey.asyncio import Valkey as AsyncValkey
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from litestar.utils import ensure_async_callable


async def wait_until_responsive(
    check: Callable[..., Awaitable],
    timeout: float,
    pause: float,
    **kwargs: Any,
) -> None:
    """Wait until a service is responsive.

    Args:
        check: Coroutine, return truthy value when waiting should stop.
        timeout: Maximum seconds to wait.
        pause: Seconds to wait between calls to `check`.
        **kwargs: Given as kwargs to `check`.
    """
    ref = timeit.default_timer()
    now = ref
    while (now - ref) < timeout:
        if await check(**kwargs):
            return
        await asyncio.sleep(pause)
        now = timeit.default_timer()

    raise RuntimeError("Timeout reached while waiting on service!")


class ContainerServiceRegistry:
    def __init__(self, worker_id: str) -> None:
        self._running_services: set[str] = set()
        self.docker_ip = self._get_docker_ip()
        self._base_command = [
            "docker",
            "compose",
            f"--file={Path(__file__).parent / 'docker-compose.yml'}",
            f"--project-name=litestar_pytest-{worker_id}",
        ]

    def _get_docker_ip(self) -> str:
        docker_host = os.environ.get("DOCKER_HOST", "").strip()
        if not docker_host or docker_host.startswith("unix://"):
            return "127.0.0.1"

        if match := re.match(r"^tcp://(.+?):\d+$", docker_host):
            return match[1]

        raise ValueError(f'Invalid value for DOCKER_HOST: "{docker_host}".')

    def run_command(self, *args: str) -> None:
        command = [*self._base_command, *args]
        subprocess.run(command, check=True, capture_output=True)

    async def start(
        self,
        name: str,
        *,
        check: Callable[..., Awaitable],
        timeout: float = 30,
        pause: float = 0.1,
        **kwargs: Any,
    ) -> None:
        if name not in self._running_services:
            self.run_command("up", "-d", name)
            self._running_services.add(name)

            await wait_until_responsive(
                check=ensure_async_callable(check),
                timeout=timeout,
                pause=pause,
                host=self.docker_ip,
                **kwargs,
            )

    def stop(self, name: str) -> None:
        pass

    def down(self) -> None:
        self.run_command("down", "-t", "5")


@pytest.fixture(scope="session")
def docker_services(worker_id: str) -> Generator[ContainerServiceRegistry, None, None]:
    if os.getenv("GITHUB_ACTIONS") == "true" and sys.platform != "linux":
        pytest.skip("Docker not available on this platform")

    registry = ContainerServiceRegistry(worker_id)
    try:
        yield registry
    finally:
        registry.down()


@pytest.fixture(scope="session")
def docker_ip(docker_services: ContainerServiceRegistry) -> str:
    return docker_services.docker_ip


async def redis_responsive(host: str) -> bool:
    client: AsyncRedis = AsyncRedis(host=host, port=6397)
    try:
        return await client.ping()
    except (ConnectionError, RedisConnectionError):
        return False
    finally:
        await client.aclose()


@pytest.fixture()
async def redis_service(docker_services: ContainerServiceRegistry) -> None:
    await docker_services.start("redis", check=redis_responsive)


async def valkey_responsive(host: str) -> bool:
    client: AsyncValkey = AsyncValkey(host=host, port=6381)
    try:
        return await client.ping()
    except (ConnectionError, ValkeyConnectionError):
        return False
    finally:
        await client.aclose()


async def iggy_healthcheck(host: str) -> bool:
    pass


@pytest.fixture()
async def iggy_provider(container_service: ContainerServiceRegistry) -> None:
    await container_service.start("iggy", check=iggy_healthcheck)


# from typing import Protocol, runtime_checkable
# import pytest
#
# # 1. Единый интерфейс для любого окружения
# @runtime_checkable
# class InfraProvider(Protocol):
#     def start_service(self, name: str, image_or_template: str, **kwargs) -> dict:
#         """Возвращает connection info: {'host': ..., 'port': ...}"""
#         ...
#
#     def stop_all(self) -> None:
#         ...
#
# # 2. Реализации под разные среды
# class IContainerProvider(Protocol):
#     def __init__(self):
#         self._containers = []
#
#     def start_service(self, name: str, image_or_template: str, **kwargs) -> dict:
#         from testcontainers.core.container import DockerContainer
#         container = DockerContainer(image_or_template).with_exposed_ports(kwargs["port"])
#         container.start()
#         self._containers.append(container)
#         return {
#             "host": container.get_container_host_ip(),
#             "port": container.get_exposed_port(kwargs["port"]),
#         }
#
#     def stop_all(self) -> None:
#         for c in reversed(self._containers):
#             c.stop()
#
# class ProxmoxProvider:
#     def __init__(self, api_kwargs: dict):
#         from proxmoxer import ProxmoxAPI
#         self.proxmox = ProxmoxAPI(**api_kwargs)
#         self._created_vmids = []
#
#     def start_service(self, name: str, image_or_template: str, **kwargs) -> dict:
#         # Клонирование шаблона LXC/VM, старт, ожидание IP
#         ...
#
#     def stop_all(self) -> None:
#         # Остановка и удаление созданных VMID
#         ...
#
# # 3. Реестр сервисов для тестов
# class ServiceRegistry:
#     def __init__(self, provider: InfraProvider):
#         self.provider = provider
#         self._services: dict[str, dict] = {}
#
#     def get_or_create(self, name: str, spec: str, **kwargs) -> dict:
#         if name not in self._services:
#             self._services[name] = self.provider.start_service(name, spec, **kwargs)
#         return self._services[name]
