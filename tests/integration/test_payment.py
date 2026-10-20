import asyncio
import json
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Queue
from threading import Thread
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from faststream import AckPolicy
from faststream.message import AckStatus
from faststream.rabbit import RabbitMessage
from sqlalchemy import select


@asynccontextmanager
async def webhook_receiver():
    requests = Queue()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            requests.put((self.path, json.loads(body)))
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/webhook", requests
    finally:
        await asyncio.to_thread(server.shutdown)
        server.server_close()
        await asyncio.to_thread(thread.join)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payment_count", [1, 10, 100], ids=["1-payment", "10-payments", "100-payments"]
)
async def test_payments_are_processed_and_acknowledged(
    payment_count, payment_pipeline, monkeypatch
):
    from app.models import OutboxEvent, Payment
    from app.models.enums import PaymentStatus
    from app.workers import consumer, outbox_publisher

    monkeypatch.setattr(
        consumer.PaymentProcessor,
        "process",
        AsyncMock(return_value=PaymentStatus.SUCCEEDED),
    )
    sessions = payment_pipeline.sessions
    broker = payment_pipeline.broker
    channel = payment_pipeline.channel
    queue = payment_pipeline.queue
    dlq = payment_pipeline.dlq
    client = payment_pipeline.client
    webhook_client = payment_pipeline.webhook_client

    async with AsyncExitStack() as stack:
        webhook_url, webhooks = await stack.enter_async_context(webhook_receiver())
        completed = asyncio.get_running_loop().create_future()
        deliveries = []
        payment_ids = set()
        acknowledged_ids = set()

        @broker.subscriber(
            queue,
            channel=consumer.payments_channel,
            ack_policy=AckPolicy.MANUAL,
            decoder=lambda message: message.body,
        )
        async def handle(data: bytes, message: RabbitMessage):
            # The real handler sends the HTTP request and acknowledges RabbitMQ.
            deliveries.append(message)
            try:
                await consumer.consume(data, message, webhook_client)
                assert message.committed == AckStatus.ACKED
                assert message.raw_message.processed
                payment_id = UUID(message.message_id)
                assert payment_id in payment_ids
                assert payment_id not in acknowledged_ids, "Duplicate delivery"
                acknowledged_ids.add(payment_id)
            except Exception as exc:
                if not completed.done():
                    completed.set_exception(exc)
                raise
            else:
                if len(acknowledged_ids) == payment_count and not completed.done():
                    completed.set_result(None)

        # Stop deliveries before closing HTTP clients or removing the queues.
        stack.push_async_callback(broker.stop)
        for index in range(payment_count):
            response = await client.post(
                "/api/v1/payments",
                headers={
                    "Idempotency-Key": f"integration-{uuid4().hex}",
                },
                json={
                    "amount": "125.50",
                    "currency": "RUB",
                    "description": f"Payment integration test #{index}",
                    "metadata": {"source": "integration-test", "index": index},
                    "webhook_url": webhook_url,
                },
            )
            assert response.status_code == 202, response.text
            payment_id = UUID(response.json()["payment_id"])
            assert payment_id not in payment_ids
            payment_ids.add(payment_id)
            assert response.json()["status"] == "pending"

        async with sessions() as session:
            payments = (await session.scalars(select(Payment))).all()
            events = (await session.scalars(select(OutboxEvent))).all()
            assert len(payments) == payment_count
            assert {payment.id for payment in payments} == payment_ids
            for payment in payments:
                assert payment.status == PaymentStatus.PENDING
                assert payment.processed_at is None
            assert len(events) == payment_count
            assert {
                UUID(event.payload["payment_id"]) for event in events
            } == payment_ids
            for event in events:
                assert set(event.payload) == {"payment_id"}
                assert event.published_at is None
            event_payloads = {event.id: event.payload for event in events}

        for _ in range(payment_count):
            assert await outbox_publisher.publish_pending()
        assert not (await outbox_publisher.publish_pending())
        await broker.start()
        # This success scenario excludes random failures; retry cases have their own test.
        await asyncio.wait_for(completed, timeout=payment_count * 7 + 30)

        await broker.stop()
        assert len(deliveries) == payment_count
        assert acknowledged_ids == payment_ids
        assert webhooks.qsize() == payment_count
        notifications = {}
        for _ in range(payment_count):
            path, payload = webhooks.get_nowait()
            assert path == "/webhook"
            payment_id = UUID(payload["payment_id"])
            assert payment_id in payment_ids
            assert payment_id not in notifications, "Duplicate webhook"
            notifications[payment_id] = payload
        assert notifications.keys() == payment_ids

        async with sessions() as session:
            payments = (await session.scalars(select(Payment))).all()
            events = (await session.scalars(select(OutboxEvent))).all()
            assert len(payments) == payment_count
            assert {payment.id for payment in payments} == payment_ids
            for payment in payments:
                assert payment.status in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
                assert payment.processed_at is not None
                payload = notifications[payment.id]
                assert payload["status"] == payment.status.value
                assert (
                    datetime.fromisoformat(payload["processed_at"])
                    == payment.processed_at
                )
            assert len(events) == payment_count
            assert {event.id: event.payload for event in events} == event_payloads
            assert all(event.published_at is not None for event in events)

        for name in (queue.name, dlq.name):
            state = await channel.declare_queue(name, passive=True)
            assert state.declaration_result.message_count == 0, name
            assert state.declaration_result.consumer_count == 0, name


@pytest.mark.asyncio
async def test_consumer_prefetch_limits_inflight_payments(
    payment_pipeline, monkeypatch
):
    from app.models import OutboxEvent, Payment
    from app.models.enums import PaymentStatus
    from app.workers import consumer, outbox_publisher

    payment_count = 25
    prefetch = 10
    release = asyncio.Event()
    at_capacity = asyncio.Event()
    completed = asyncio.get_running_loop().create_future()
    active = 0
    max_active = 0
    payment_ids = set()
    acknowledged_ids = set()

    async def held_processing(_self):
        await release.wait()
        return PaymentStatus.SUCCEEDED

    monkeypatch.setattr(consumer.PaymentProcessor, "process", held_processing)

    async with AsyncExitStack() as stack:
        webhook_url, webhooks = await stack.enter_async_context(webhook_receiver())
        stack.push_async_callback(payment_pipeline.broker.stop)
        stack.callback(release.set)

        @payment_pipeline.broker.subscriber(
            payment_pipeline.queue,
            channel=consumer.payments_channel,
            ack_policy=AckPolicy.MANUAL,
            decoder=lambda message: message.body,
        )
        async def handle(data: bytes, message: RabbitMessage):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            if active >= prefetch:
                at_capacity.set()
            try:
                await consumer.consume(data, message, payment_pipeline.webhook_client)
                assert message.committed == AckStatus.ACKED
                payment_id = UUID(message.message_id)
                assert payment_id in payment_ids
                assert payment_id not in acknowledged_ids
                acknowledged_ids.add(payment_id)
                if len(acknowledged_ids) == payment_count and not completed.done():
                    completed.set_result(None)
            except Exception as exc:
                if not completed.done():
                    completed.set_exception(exc)
                raise
            finally:
                active -= 1

        for _ in range(payment_count):
            response = await payment_pipeline.client.post(
                "/api/v1/payments",
                headers={"Idempotency-Key": f"prefetch-{uuid4().hex}"},
                json={
                    "amount": "125.50",
                    "currency": "RUB",
                    "description": "Consumer prefetch regression test",
                    "webhook_url": webhook_url,
                },
            )
            assert response.status_code == 202, response.text
            payment_ids.add(UUID(response.json()["payment_id"]))
        assert len(payment_ids) == payment_count
        for _ in range(payment_count):
            assert await outbox_publisher.publish_pending()
        assert not (await outbox_publisher.publish_pending())

        await payment_pipeline.broker.start()
        await asyncio.wait_for(at_capacity.wait(), timeout=15)
        queued = await payment_pipeline.channel.declare_queue(
            payment_pipeline.queue.name, passive=True
        )
        assert active == max_active == prefetch
        assert queued.declaration_result.message_count == payment_count - prefetch
        assert webhooks.empty()

        release.set()
        await asyncio.wait_for(completed, timeout=30)
        await payment_pipeline.broker.stop()
        assert max_active == prefetch
        assert acknowledged_ids == payment_ids
        assert webhooks.qsize() == payment_count
        webhook_ids = {
            UUID(webhooks.get_nowait()[1]["payment_id"]) for _ in range(payment_count)
        }
        assert webhook_ids == payment_ids
        async with payment_pipeline.sessions() as session:
            payments = (await session.scalars(select(Payment))).all()
            events = (await session.scalars(select(OutboxEvent))).all()
            assert len(payments) == len(events) == payment_count
            assert all(
                payment.status == PaymentStatus.SUCCEEDED for payment in payments
            )
            assert all(payment.processed_at is not None for payment in payments)
            assert all(event.published_at is not None for event in events)
        for name in (payment_pipeline.queue.name, payment_pipeline.dlq.name):
            state = await payment_pipeline.channel.declare_queue(name, passive=True)
            assert state.declaration_result.message_count == 0
