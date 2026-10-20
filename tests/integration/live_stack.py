import asyncio
import json
import socket
from contextlib import AsyncExitStack, asynccontextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Queue
from threading import Thread
from types import SimpleNamespace
from uuid import UUID, uuid4

import aio_pika
from httpx import AsyncClient
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@asynccontextmanager
async def live_webhook_receiver(advertised_host=None):
    requests = Queue()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            requests.put((self.path, body))
            self.respond()

        def do_GET(self):
            self.respond()

        def respond(self):
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
    server.daemon_threads = True
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host = advertised_host or socket.gethostbyname(socket.gethostname())
        yield f"http://{host}:{server.server_port}/webhook", requests
    finally:
        await asyncio.to_thread(server.shutdown)
        server.server_close()
        await asyncio.to_thread(thread.join)


@asynccontextmanager
async def live_payment_stack(
    *,
    database_url,
    rabbitmq_url,
    api_url,
    api_key,
    webhook_host=None,
    require_stopped_consumer=True,
    pause_before_cleanup=None,
):
    from app.models import OutboxEvent, Payment
    from app.models.enums import PaymentStatus

    payment_ids = set()
    run_id = uuid4().hex
    async with AsyncExitStack() as stack:
        engine = create_async_engine(database_url)
        stack.push_async_callback(engine.dispose)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        connection = None
        channel = None

        async def disconnect_broker():
            nonlocal connection, channel
            if connection is not None:
                await connection.close()
            connection = channel = None

        async def reconnect_broker():
            nonlocal connection, channel
            await disconnect_broker()
            connection = await aio_pika.connect(rabbitmq_url, timeout=5)
            channel = await connection.channel()

        stack.push_async_callback(disconnect_broker)
        await reconnect_broker()
        queue = await channel.declare_queue("payments.new", passive=True)
        dlq = await channel.declare_queue("payments.dlq", passive=True)
        if require_stopped_consumer:
            assert queue.declaration_result.consumer_count == 0, (
                "The real payments.new consumer is running: stop the consumer container"
            )
        assert queue.declaration_result.message_count == 0, (
            "payments.new must be empty before the test; existing messages are preserved"
        )
        assert dlq.declaration_result.message_count == 0, (
            "payments.dlq must be empty before the test; existing messages are preserved"
        )
        assert dlq.declaration_result.consumer_count == 0
        async with sessions() as session:
            payments_before = await session.scalar(
                select(func.count()).select_from(Payment)
            )
            outbox_before = await session.scalar(
                select(func.count()).select_from(OutboxEvent)
            )
            unpublished = await session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
            )
            assert unpublished == 0, "The live Outbox must have no pending events"

        async def cleanup_test_data():
            # Finish any concurrent publication before draining this run's messages.
            async with sessions() as session, session.begin():
                # Include rows committed by a request whose HTTP response was lost.
                stored_ids = await session.scalars(
                    select(Payment.id).where(
                        Payment.metadata_["test_run_id"].astext == run_id
                    )
                )
                payment_ids.update(stored_ids)
                if not payment_ids:
                    return
                await session.execute(
                    delete(OutboxEvent).where(
                        OutboxEvent.payload["payment_id"].astext.in_(
                            [str(payment_id) for payment_id in payment_ids]
                        )
                    )
                )
                await session.execute(
                    delete(Payment).where(Payment.id.in_(payment_ids))
                )
            for name in ("payments.new", "payments.dlq"):
                current = await channel.declare_queue(name, passive=True)
                held = []
                try:
                    for _ in range(current.declaration_result.message_count):
                        message = await current.get(fail=False, timeout=5)
                        if message is None:
                            break
                        held.append(message)
                    for message in held:
                        try:
                            payment_id = UUID(json.loads(message.body)["payment_id"])
                        except (ValueError, TypeError, KeyError):
                            continue
                        if payment_id in payment_ids:
                            await message.ack()
                finally:
                    for message in held:
                        if not message.processed:
                            await message.nack(requeue=True)

        client = await stack.enter_async_context(
            AsyncClient(
                base_url=api_url,
                headers={"X-API-Key": api_key},
                timeout=10,
                trust_env=False,
            )
        )
        webhook_url, webhooks = await stack.enter_async_context(
            live_webhook_receiver(webhook_host)
        )
        stack.push_async_callback(cleanup_test_data)
        health = await client.get(webhook_url)
        assert health.status_code == 200

        async def create_payments(count, source, *, concurrency=1):
            assert count > 0 and concurrency > 0
            indices = iter(range(count))
            created = [None] * count
            failed = asyncio.Event()

            async def create_one(index):
                response = await client.post(
                    "/api/v1/payments",
                    headers={"Idempotency-Key": f"{source}-{uuid4().hex}"},
                    json={
                        "amount": "125.50",
                        "currency": "RUB",
                        "description": f"{source} #{index}",
                        "metadata": {
                            "source": source,
                            "index": index,
                            "test_run_id": run_id,
                        },
                        "webhook_url": webhook_url,
                    },
                )
                assert response.status_code == 202, response.text
                payment_id = UUID(response.json()["payment_id"])
                assert payment_id not in payment_ids
                payment_ids.add(payment_id)
                created[index] = payment_id
                assert response.json()["status"] == "pending"

            async def worker():
                for index in indices:
                    if failed.is_set():
                        return
                    succeeded = False
                    try:
                        await create_one(index)
                        succeeded = True
                    finally:
                        if not succeeded:
                            failed.set()

            # Let all in-flight requests finish before cleanup; do not hide errors
            # with retries or create thousands of simultaneous HTTP tasks.
            results = await asyncio.gather(
                *(worker() for _ in range(min(concurrency, count))),
                return_exceptions=True,
            )
            errors = [result for result in results if isinstance(result, BaseException)]
            if errors:
                raise BaseExceptionGroup("Payment creation failed", errors)
            assert all(payment_id is not None for payment_id in created)
            return created

        async def database_snapshot():
            async with sessions() as session:
                payments = (
                    await session.scalars(
                        select(Payment).where(Payment.id.in_(payment_ids))
                    )
                ).all()
                events = (
                    await session.scalars(
                        select(OutboxEvent).where(
                            OutboxEvent.payload["payment_id"].astext.in_(
                                [str(payment_id) for payment_id in payment_ids]
                            )
                        )
                    )
                ).all()
                payments_total = await session.scalar(
                    select(func.count()).select_from(Payment)
                )
                outbox_total = await session.scalar(
                    select(func.count()).select_from(OutboxEvent)
                )
            assert {payment.id for payment in payments} == payment_ids
            assert {
                UUID(event.payload["payment_id"]) for event in events
            } == payment_ids
            assert payments_total - payments_before == len(payment_ids)
            assert outbox_total - outbox_before == len(payment_ids)
            return SimpleNamespace(
                payments=payments,
                events=events,
                metrics={
                    "created_payments": len(payments),
                    "pending_payments": sum(
                        payment.status == PaymentStatus.PENDING for payment in payments
                    ),
                    "published_outbox_events": sum(
                        event.published_at is not None for event in events
                    ),
                    "received_webhooks": webhooks.qsize(),
                },
            )

        async def get_queues():
            assert channel is not None, "Reconnect inspection after broker recovery"
            current = await channel.declare_queue("payments.new", passive=True)
            dead_letters = await channel.declare_queue("payments.dlq", passive=True)
            return current, dead_letters

        async def snapshot():
            current, dead_letters = await get_queues()
            state = await database_snapshot()
            assert dead_letters.declaration_result.consumer_count == 0
            state.consumers = current.declaration_result.consumer_count
            state.metrics.update(
                ready_messages=current.declaration_result.message_count,
                dlq_messages=dead_letters.declaration_result.message_count,
            )
            return state

        async def wait_for_publication(count):
            async with asyncio.timeout(30):
                while True:
                    state = await snapshot()
                    assert state.consumers == 0
                    if state.metrics["published_outbox_events"] == count:
                        return state
                    await asyncio.sleep(0.1)

        try:
            yield SimpleNamespace(
                exit_stack=stack,
                sessions=sessions,
                run_id=run_id,
                get_queues=get_queues,
                payment_ids=payment_ids,
                webhooks=webhooks,
                create_payments=create_payments,
                snapshot=snapshot,
                database_snapshot=database_snapshot,
                disconnect_broker=disconnect_broker,
                reconnect_broker=reconnect_broker,
                wait_for_publication=wait_for_publication,
            )
        finally:
            if pause_before_cleanup is not None:
                await pause_before_cleanup(
                    f"test_run_id: {run_id}",
                    "Очереди: payments.new, payments.dlq",
                )
