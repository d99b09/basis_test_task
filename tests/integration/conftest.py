import os
from contextlib import AsyncExitStack
from functools import partial
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import aio_pika
import pytest
import pytest_asyncio
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from .cleanup import pause_before_cleanup as wait_before_cleanup
from .docker_control import docker_stack_control


def pytest_addoption(parser):
    parser.getgroup("integration inspection").addoption(
        "--pause-before-cleanup",
        action="store_true",
        help="Wait for Enter before cleanup",
    )
    load = parser.getgroup("payment load")
    load.addoption(
        "--payment-load", action="store_true", help="Run the live payment load test"
    )
    load.addoption("--load-payment-count", type=int, default=10_000)
    load.addoption("--load-concurrency", type=int, default=20)
    load.addoption("--load-timeout", type=float, default=None)
    load.addoption("--load-poll-seconds", type=float, default=5)
    load.addoption("--load-report", default="test-results/payment-load.json")
    group = parser.getgroup("live payment stack")
    group.addoption(
        "--rabbitmq-unavailable",
        action="store_true",
        help="Run the real RabbitMQ outage scenario from the Docker host",
    )
    group.addoption(
        "--rabbitmq-recovery",
        action="store_true",
        help="Run the real RabbitMQ recovery scenario from the Docker host",
    )
    group.addoption(
        "--consumer-recovery",
        action="store_true",
        help="Run consumer container recovery from the Docker host",
    )
    group.addoption(
        "--stack-webhook-host",
        default=None,
        help="Host address reachable by the consumer; defaults to Docker bridge gateway",
    )
    group.addoption(
        "--consumer-unavailable",
        action="store_true",
        help="Run the live-stack test after stopping the consumer container",
    )
    group.addoption(
        "--stack-api-url",
        default="http://localhost:8000",
        help="URL of the running API used by live-stack tests",
    )


@pytest_asyncio.fixture
async def consumer_control(request):
    if not request.config.getoption("--consumer-recovery"):
        pytest.skip("Run on the Docker host with --consumer-recovery")
    async with docker_stack_control(request, {"consumer"}) as control:
        yield control


@pytest_asyncio.fixture
async def rabbitmq_control(request):
    if not (
        request.config.getoption("--rabbitmq-unavailable")
        or request.config.getoption("--rabbitmq-recovery")
    ):
        pytest.skip("Run on the Docker host with a RabbitMQ scenario flag")
    async with docker_stack_control(request, {"rabbitmq", "consumer"}) as control:
        yield control


@pytest_asyncio.fixture
async def payment_load_control(request):
    if not request.config.getoption("--payment-load"):
        pytest.skip("Run the load test explicitly with --payment-load")
    async with docker_stack_control(request, {"consumer"}) as control:
        yield control


@pytest_asyncio.fixture
async def payment_pipeline(pause_before_cleanup):
    if not (os.getenv("DATABASE_URL") and os.getenv("RABBITMQ_URL")):
        pytest.skip("Set DATABASE_URL and RABBITMQ_URL for the integration test")

    from app.core.config import CONFIG
    from app.core.db_config import get_session
    from app.infrastructure import rabbitmq
    from app.main import app
    from app.models import Base
    from app.workers import consumer, outbox_publisher

    async with AsyncExitStack() as stack:
        schema = "payment_test"
        admin_engine = create_async_engine(os.environ["DATABASE_URL"])
        stack.push_async_callback(admin_engine.dispose)

        async def drop_schema():
            async with admin_engine.begin() as connection:
                await connection.execute(
                    text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
                )

        async with admin_engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        stack.push_async_callback(drop_schema)
        engine = create_async_engine(
            os.environ["DATABASE_URL"],
            connect_args={"server_settings": {"search_path": schema}},
        )
        stack.push_async_callback(engine.dispose)
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(engine, expire_on_commit=False)

        async def get_test_session():
            async with sessions() as session:
                yield session

        stack.enter_context(
            patch.dict(
                app.dependency_overrides,
                {
                    get_session: get_test_session,
                },
            )
        )
        stack.enter_context(patch.object(consumer, "SessionFactory", sessions))
        stack.enter_context(patch.object(outbox_publisher, "SessionFactory", sessions))

        prefix = f"payments.test.{uuid4().hex}"
        dlx = RabbitExchange(f"{prefix}.dlx")
        dlq = RabbitQueue(f"{prefix}.dlq", durable=True)
        queue = RabbitQueue(
            f"{prefix}.new",
            durable=True,
            arguments={
                "x-dead-letter-exchange": dlx.name,
                "x-dead-letter-routing-key": dlq.name,
            },
        )
        broker = RabbitBroker(
            os.environ["RABBITMQ_URL"],
            default_channel=rabbitmq.payments_channel,
        )
        stack.push_async_callback(broker.stop)
        stack.enter_context(
            patch.multiple(
                rabbitmq,
                broker=broker,
                payments_queue=queue,
                dead_letter_exchange=dlx,
                dead_letter_queue=dlq,
            )
        )
        stack.enter_context(
            patch.multiple(
                consumer,
                broker=broker,
                payments_queue=queue,
            )
        )
        await rabbitmq.setup_rabbitmq()

        inspection = await aio_pika.connect(os.environ["RABBITMQ_URL"])
        stack.push_async_callback(inspection.close)
        channel = await inspection.channel()
        exchange = await channel.get_exchange(dlx.name)
        stack.push_async_callback(exchange.delete)
        for name in (dlq.name, queue.name):
            declared = await channel.get_queue(name)
            stack.push_async_callback(declared.delete, if_empty=False)

        webhook_client = await stack.enter_async_context(
            AsyncClient(timeout=5, trust_env=False)
        )
        client = await stack.enter_async_context(
            AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                headers={"X-API-Key": CONFIG.API_KEY},
            )
        )
        stack.push_async_callback(broker.stop)
        try:
            yield SimpleNamespace(
                sessions=sessions,
                broker=broker,
                channel=channel,
                queue=queue,
                dlq=dlq,
                client=client,
                webhook_client=webhook_client,
            )
        finally:
            await pause_before_cleanup(
                f"Схема БД: {schema}",
                f'SQL для просмотра платежей: SELECT * FROM "{schema}".payments;',
                f'SQL для просмотра Outbox: SELECT * FROM "{schema}".outbox;',
                f"Очереди: {queue.name}, {dlq.name}",
            )


@pytest.fixture
def pause_before_cleanup(request):
    return partial(wait_before_cleanup, request)
