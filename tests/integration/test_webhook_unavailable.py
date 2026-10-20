import asyncio
import json
import logging
import socket
from collections import defaultdict
from contextlib import AsyncExitStack
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from faststream import AckPolicy
from faststream.message import AckStatus
from faststream.rabbit import RabbitMessage
from sqlalchemy import select


@pytest.mark.asyncio
async def test_ten_payments_with_unavailable_webhook_go_to_dlq(
    payment_pipeline,
    monkeypatch,
    caplog,
):
    from app.models import OutboxEvent, Payment
    from app.models.enums import PaymentStatus
    from app.workers import consumer, outbox_publisher

    monkeypatch.setattr(
        consumer.PaymentProcessor,
        "process",
        AsyncMock(return_value=PaymentStatus.SUCCEEDED),
    )
    payment_count = 10
    attempts_per_payment = consumer.PROCESSING_ATTEMPTS
    sessions = payment_pipeline.sessions
    broker = payment_pipeline.broker
    channel = payment_pipeline.channel
    queue = payment_pipeline.queue
    dlq = payment_pipeline.dlq
    client = payment_pipeline.client
    completed = asyncio.get_running_loop().create_future()
    payment_ids = set()
    rejected_ids = set()
    deliveries = defaultdict(list)
    webhook_results = defaultdict(list)
    send_webhook = consumer.send_webhook
    caplog.set_level(logging.WARNING, logger=consumer.__name__)

    async def record_webhook_attempt(payment, http_client):
        delivered = await send_webhook(payment, http_client)
        webhook_results[payment.id].append(
            (delivered, payment.status, payment.processed_at)
        )
        return delivered

    # Observe the real HTTP result, retries and acknowledgements.
    monkeypatch.setattr(consumer, "send_webhook", record_webhook_attempt)

    @broker.subscriber(
        queue,
        channel=consumer.payments_channel,
        ack_policy=AckPolicy.MANUAL,
        decoder=lambda message: message.body,
    )
    async def handle(data: bytes, message: RabbitMessage):
        try:
            await consumer.consume(data, message, payment_pipeline.webhook_client)
            payment_id = UUID(message.message_id)
            assert payment_id in payment_ids
            failures = message.headers.get(consumer.PROCESSING_FAILURES_HEADER, 0)
            deliveries[payment_id].append(failures)
            assert message.raw_message.processed
            if failures == attempts_per_payment - 1:
                assert message.committed == AckStatus.REJECTED
                assert payment_id not in rejected_ids
                rejected_ids.add(payment_id)
            else:
                assert message.committed == AckStatus.ACKED
        except Exception as exc:
            if not completed.done():
                completed.set_exception(exc)
            raise
        else:
            if len(rejected_ids) == payment_count and not completed.done():
                completed.set_result(None)

    async with AsyncExitStack() as stack:
        # Reserve a port without listening: HTTP connections really get refused,
        # and another process cannot start a listener on this port during the test.
        endpoint = stack.enter_context(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        )
        endpoint.bind(("127.0.0.1", 0))
        webhook_url = f"http://127.0.0.1:{endpoint.getsockname()[1]}/webhook"
        stack.push_async_callback(broker.stop)

        for index in range(payment_count):
            response = await client.post(
                "/api/v1/payments",
                headers={"Idempotency-Key": f"webhook-unavailable-{uuid4().hex}"},
                json={
                    "amount": "125.50",
                    "currency": "RUB",
                    "description": f"Unavailable webhook #{index}",
                    "metadata": {"source": "webhook-unavailable", "index": index},
                    "webhook_url": webhook_url,
                },
            )
            assert response.status_code == 202, response.text
            assert response.json()["status"] == "pending"
            payment_id = UUID(response.json()["payment_id"])
            assert payment_id not in payment_ids
            payment_ids.add(payment_id)

        async with sessions() as session:
            payments = (await session.scalars(select(Payment))).all()
            events = (await session.scalars(select(OutboxEvent))).all()
            assert len(payments) == payment_count
            assert {payment.id for payment in payments} == payment_ids
            assert all(payment.status == PaymentStatus.PENDING for payment in payments)
            assert all(payment.processed_at is None for payment in payments)
            assert len(events) == payment_count
            assert {
                UUID(event.payload["payment_id"]) for event in events
            } == payment_ids
            assert all(event.published_at is None for event in events)
            event_payloads = {event.id: event.payload for event in events}

        for _ in range(payment_count):
            assert await outbox_publisher.publish_pending()
        assert not (await outbox_publisher.publish_pending())
        await broker.start()
        # Includes the one- and two-second retry delays.
        await asyncio.wait_for(completed, timeout=payment_count * 12 + 30)
        await broker.stop()

        assert rejected_ids == payment_ids
        assert webhook_results.keys() == payment_ids
        assert deliveries.keys() == payment_ids
        for payment_id in payment_ids:
            assert deliveries[payment_id] == list(range(attempts_per_payment))
            assert len(webhook_results[payment_id]) == attempts_per_payment
            assert all(not result[0] for result in webhook_results[payment_id])

        errors = [
            record.getMessage()
            for record in caplog.records
            if record.name == consumer.__name__
            and "Ошибка доставки webhook" in record.getMessage()
        ]
        assert len(errors) == payment_count * attempts_per_payment
        assert all("ConnectError" in error for error in errors)

        # Dead lettering is asynchronous; wait for all rejected messages to arrive.
        async with asyncio.timeout(10):
            while True:
                dead_letters = await channel.declare_queue(dlq.name, passive=True)
                dlq_count = dead_letters.declaration_result.message_count
                assert dlq_count <= payment_count
                if dlq_count == payment_count:
                    break
                await asyncio.sleep(0.05)
        assert dead_letters.declaration_result.consumer_count == 0
        main_queue = await channel.declare_queue(queue.name, passive=True)
        assert main_queue.declaration_result.message_count == 0
        assert main_queue.declaration_result.consumer_count == 0

        async with sessions() as session:
            payments = (await session.scalars(select(Payment))).all()
            events = (await session.scalars(select(OutboxEvent))).all()
            assert {payment.id for payment in payments} == payment_ids
            assert len(events) == payment_count
            assert {event.id: event.payload for event in events} == event_payloads
            assert all(event.published_at is not None for event in events)
            for payment in payments:
                assert payment.status in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
                assert payment.processed_at is not None
                # Webhook retries must preserve the initial processing result and time.
                assert all(
                    (status, processed_at) == (payment.status, payment.processed_at)
                    for _, status, processed_at in webhook_results[payment.id]
                )

            metrics = {
                "created_payments": len(payments),
                "processed_payments": sum(
                    payment.processed_at is not None for payment in payments
                ),
                "pending_payments": sum(
                    payment.status == PaymentStatus.PENDING for payment in payments
                ),
                "failed_webhook_deliveries": len(webhook_results),
                "failed_webhook_attempts": sum(map(len, webhook_results.values())),
                "dlq_messages": dlq_count,
            }
            assert metrics == {
                "created_payments": 10,
                "processed_payments": 10,
                "pending_payments": 0,
                "failed_webhook_deliveries": 10,
                "failed_webhook_attempts": 30,
                "dlq_messages": 10,
            }

        dlq_payment_ids = set()
        held = []
        try:
            for _ in range(payment_count):
                message = await dead_letters.get(no_ack=False, timeout=5)
                held.append(message)
                payment_id = UUID(json.loads(message.body)["payment_id"])
                assert payment_id in payment_ids
                assert payment_id not in dlq_payment_ids
                assert message.message_id == str(payment_id)
                assert message.headers[consumer.PROCESSING_FAILURES_HEADER] == 2
                assert any(
                    death["reason"] == "rejected"
                    and death["queue"] == queue.name
                    and death["count"] == 1
                    for death in message.headers["x-death"]
                )
                dlq_payment_ids.add(payment_id)
        finally:
            # Keep the inspected messages available until fixture cleanup.
            for message in held:
                if not message.processed:
                    await message.nack(requeue=True)
        assert dlq_payment_ids == payment_ids
