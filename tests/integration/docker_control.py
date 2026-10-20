import asyncio
import json
import os
import shutil
import subprocess
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import aio_pika
import pytest
from aio_pika.exceptions import AMQPException
from sqlalchemy.engine import make_url
from yarl import URL

from .console import wait_for_enter


class DockerStackControl(SimpleNamespace):
    def __repr__(self):
        return (
            "DockerStackControl()"  # Do not print connection credentials in failures.
        )


@asynccontextmanager
async def docker_stack_control(request, managed_services):
    assert shutil.which("docker"), (
        "Run container tests on the Docker host, with Docker CLI"
    )
    for name in ("DATABASE_URL", "RABBITMQ_URL", "API_KEY"):
        if not os.getenv(name):
            pytest.fail(f"{name} is required for live container tests")

    async def docker(*args):
        result = await asyncio.to_thread(
            subprocess.run,
            ["docker", *args],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            timeout=45,
        )
        assert result.returncode == 0, result.stderr
        return result.stdout.strip()

    services = ("consumer", "rabbitmq", "api", "postgres", "outbox_publisher")
    ids = (await docker("compose", "ps", "-a", "-q", *services)).splitlines()
    assert len(ids) == len(services), "All five payment stack containers must exist"
    details = {
        item["Config"]["Labels"]["com.docker.compose.service"]: item
        for item in json.loads(await docker("inspect", *ids))
    }
    assert set(details) == set(services)
    container_ids = {service: item["Id"] for service, item in details.items()}
    initial_states = {service: item["State"] for service, item in details.items()}
    changed = set()

    async def state(service="consumer"):
        (item,) = json.loads(await docker("inspect", container_ids[service]))
        return item["State"]

    async def assert_running(*names, unchanged=False):
        for service in names:
            current = await state(service)
            assert current["Running"], f"{service} must be running"
            if unchanged:
                assert current["StartedAt"] == initial_states[service]["StartedAt"], (
                    f"{service} must recover without a container restart"
                )

    async def stop(service="consumer"):
        assert service in managed_services
        changed.add(service)
        await docker("stop", "--time", "10", container_ids[service])
        assert not (await state(service))["Running"]

    async def start(service="consumer"):
        assert service in managed_services
        changed.add(service)
        await docker("start", container_ids[service])
        await wait_until_ready(service)

    async def wait_until_ready(service):
        await assert_running(service)
        if service == "rabbitmq":
            async with asyncio.timeout(60):
                while True:
                    try:
                        connection = await aio_pika.connect(rabbitmq_url, timeout=2)
                    except (OSError, AMQPException, TimeoutError):
                        await asyncio.sleep(0.25)
                    else:
                        await connection.close()
                        break
        elif service == "consumer":
            await wait_for_consumer()

    async def wait_for_manual_start(service, *details):
        assert service in managed_services
        command = f"docker compose start {service}"
        while True:
            await wait_for_enter(
                request,
                "После запуска контейнера нажмите Enter для проверки восстановления... ",
                *details,
                f"В другом терминале выполните: {command}",
            )
            if (await state(service))["Running"]:
                break
            details = (f"Контейнер {service} ещё остановлен.",)
        await wait_until_ready(service)

    async def queue_counts():
        queues = json.loads(
            await docker(
                "compose",
                "exec",
                "-T",
                "rabbitmq",
                "rabbitmqctl",
                "list_queues",
                "name",
                "consumers",
                "messages_ready",
                "messages_unacknowledged",
                "--formatter=json",
            )
        )
        return {queue["name"]: queue for queue in queues}

    async def wait_for_consumer():
        async with asyncio.timeout(30):
            while True:
                queues = await queue_counts()
                if queues.get("payments.new", {}).get("consumers", 0) > 0:
                    return queues
                await asyncio.sleep(0.25)

    async def published_address(service, port):
        address = (await docker("compose", "port", service, str(port))).splitlines()[0]
        host, port = address.rsplit(":", 1)
        host = host.strip("[]")
        if host in ("0.0.0.0", "::"):
            host = "127.0.0.1"
        return host, int(port)

    db_host, db_port = await published_address("postgres", 5432)
    rabbit_host, rabbit_port = await published_address("rabbitmq", 5672)
    api_host, api_port = await published_address("api", 8000)
    webhook_host = request.config.getoption("--stack-webhook-host")
    if not webhook_host:
        network_name = next(iter(details["consumer"]["NetworkSettings"]["Networks"]))
        (network,) = json.loads(await docker("network", "inspect", network_name))
        webhook_host = next(
            item["Gateway"]
            for item in network["IPAM"]["Config"]
            if item.get("Gateway") and ":" not in item["Gateway"]
        )
    database_url = make_url(os.environ["DATABASE_URL"]).set(host=db_host, port=db_port)
    rabbitmq_url = str(
        URL(os.environ["RABBITMQ_URL"]).with_host(rabbit_host).with_port(rabbit_port)
    )
    try:
        yield DockerStackControl(
            start=start,
            stop=stop,
            state=state,
            assert_running=assert_running,
            queue_counts=queue_counts,
            wait_for_consumer=wait_for_consumer,
            wait_for_manual_start=wait_for_manual_start,
            database_url=database_url,
            rabbitmq_url=rabbitmq_url,
            api_url=f"http://{api_host}:{api_port}",
            webhook_host=webhook_host,
        )
    finally:
        # Restore the broker before its consumer; do not touch unmodified services.
        async with AsyncExitStack() as restore:
            for service in ("consumer", "rabbitmq"):
                if service in changed:
                    action = start if initial_states[service]["Running"] else stop
                    restore.push_async_callback(action, service)
