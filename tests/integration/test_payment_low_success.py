import asyncio
import json
from collections import defaultdict
from contextlib import AsyncExitStack
from contextvars import ContextVar
from datetime import datetime
from random import Random
from uuid import UUID, uuid4

import pytest
from faststream import AckPolicy
from faststream.message import AckStatus
from faststream.rabbit import RabbitMessage
from sqlalchemy import select

from .test_payment import webhook_receiver


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payment_count", [1, 10, 100], ids=["1-payment", "10-payments", "100-payments"]
)
async def test_payments_with_ten_percent_success(
    payment_count, payment_pipeline, monkeypatch
):
    from app.models import OutboxEvent, Payment
    from app.models.enums import PaymentStatus
    from app.workers import consumer, outbox_publisher

    current_payment = ContextVar("low_success_payment_id")
    randomizers = {}
    outcomes = defaultdict(list)
    deliveries = defaultdict(list)
    completed_ids = set()
    completed = asyncio.get_running_loop().create_future()

    async def low_success_processing(_self):
        payment_id = current_payment.get()
        status = (
            PaymentStatus.SUCCEEDED
            if randomizers[payment_id].random() < 0.1
            else PaymentStatus.FAILED
        )
        outcomes[payment_id].append(status)
        return status

    # Only the simulated processing result/delay changes. Retries, transactions,
    # RabbitMQ acknowledgements, dead lettering and HTTP webhooks remain real.
    monkeypatch.setattr(consumer.PaymentProcessor, "process", low_success_processing)

    async with AsyncExitStack() as stack:
        webhook_url, webhooks = await stack.enter_async_context(webhook_receiver())
        stack.push_async_callback(payment_pipeline.broker.stop)

        @payment_pipeline.broker.subscriber(
            payment_pipeline.queue,
            channel=consumer.payments_channel,
            ack_policy=AckPolicy.MANUAL,
            decoder=lambda message: message.body,
        )
        async def handle(data: bytes, message: RabbitMessage):
            payment_id = UUID(message.message_id)
            token = current_payment.set(payment_id)
            try:
                assert payment_id in randomizers
                assert payment_id not in completed_ids, "Delivery after final status"
                attempt = message.headers.get(consumer.PROCESSING_FAILURES_HEADER, 0)
                assert attempt == len(deliveries[payment_id])
                deliveries[payment_id].append(attempt)
                await consumer.consume(data, message, payment_pipeline.webhook_client)
                assert message.raw_message.processed
                assert len(outcomes[payment_id]) == attempt + 1
                succeeded = outcomes[payment_id][-1] == PaymentStatus.SUCCEEDED
                exhausted = attempt + 1 == consumer.PROCESSING_ATTEMPTS
                assert message.committed == (
                    AckStatus.REJECTED
                    if exhausted and not succeeded
                    else AckStatus.ACKED
                )
                if succeeded or exhausted:
                    completed_ids.add(payment_id)
                if len(completed_ids) == payment_count and not completed.done():
                    completed.set_result(None)
            except Exception as exc:
                if not completed.done():
                    completed.set_exception(exc)
                raise
            finally:
                current_payment.reset(token)

        for index in range(payment_count):
            response = await payment_pipeline.client.post(
                "/api/v1/payments",
                headers={"Idempotency-Key": f"low-success-{uuid4().hex}"},
                json={
                    "amount": "125.50",
                    "currency": "RUB",
                    "description": f"10% success integration test #{index}",
                    "webhook_url": webhook_url,
                },
            )
            assert response.status_code == 202, response.text
            assert response.json()["status"] == "pending"
            payment_id = UUID(response.json()["payment_id"])
            assert payment_id not in randomizers
            # A separate seed per payment keeps results independent of delivery order.
            randomizers[payment_id] = Random(index)

        for _ in range(payment_count):
            assert await outbox_publisher.publish_pending()
        assert not await outbox_publisher.publish_pending()
        await payment_pipeline.broker.start()
        await asyncio.wait_for(completed, timeout=payment_count * 3 + 30)
        await payment_pipeline.broker.stop()

        payment_ids = set(randomizers)
        assert completed_ids == payment_ids
        assert set(outcomes) == set(deliveries) == payment_ids
        failed_ids = set()
        for payment_id, results in outcomes.items():
            assert 1 <= len(results) <= consumer.PROCESSING_ATTEMPTS
            assert deliveries[payment_id] == list(range(len(results)))
            assert all(status == PaymentStatus.FAILED for status in results[:-1])
            if results[-1] == PaymentStatus.FAILED:
                assert len(results) == consumer.PROCESSING_ATTEMPTS
                failed_ids.add(payment_id)
        # Seed 0 exhausts all attempts even in the single-payment case. Larger
        # batches also cover success; no statistical tolerance can make this flaky.
        assert failed_ids
        if payment_count > 1:
            assert failed_ids != payment_ids

        assert webhooks.qsize() == payment_count
        notifications = {}
        for _ in range(payment_count):
            path, payload = webhooks.get_nowait()
            assert path == "/webhook"
            payment_id = UUID(payload["payment_id"])
            assert payment_id in payment_ids
            assert payment_id not in notifications, "Duplicate webhook"
            notifications[payment_id] = payload
        assert set(notifications) == payment_ids

        async with payment_pipeline.sessions() as session:
            payments = (await session.scalars(select(Payment))).all()
            events = (await session.scalars(select(OutboxEvent))).all()
            assert len(payments) == len(events) == payment_count
            assert {payment.id for payment in payments} == payment_ids
            assert {
                UUID(event.payload["payment_id"]) for event in events
            } == payment_ids
            assert all(event.published_at is not None for event in events)
            for payment in payments:
                assert payment.status == outcomes[payment.id][-1]
                assert payment.processed_at is not None
                assert payment.processed_at >= payment.created_at
                payload = notifications[payment.id]
                assert payload["status"] == payment.status.value
                assert datetime.fromisoformat(payload["processed_at"]) == (
                    payment.processed_at
                )

        main = await payment_pipeline.channel.declare_queue(
            payment_pipeline.queue.name, passive=True
        )
        assert main.declaration_result.message_count == 0
        assert main.declaration_result.consumer_count == 0
        async with asyncio.timeout(5):
            while True:
                dlq = await payment_pipeline.channel.declare_queue(
                    payment_pipeline.dlq.name, passive=True
                )
                if dlq.declaration_result.message_count == len(failed_ids):
                    break
                await asyncio.sleep(0.05)
        assert dlq.declaration_result.consumer_count == 0
        dead_letters = await payment_pipeline.channel.get_queue(
            payment_pipeline.dlq.name
        )
        dlq_ids = set()
        for _ in failed_ids:
            message = await dead_letters.get(timeout=5)
            async with message.process():
                payment_id = UUID(json.loads(message.body)["payment_id"])
                assert payment_id in failed_ids
                assert payment_id not in dlq_ids, "Duplicate dead letter"
                assert message.message_id == str(payment_id)
                assert message.headers[consumer.PROCESSING_FAILURES_HEADER] == 2
                assert any(
                    death["reason"] == "rejected"
                    and death["queue"] == payment_pipeline.queue.name
                    for death in message.headers["x-death"]
                )
                dlq_ids.add(payment_id)
        assert dlq_ids == failed_ids
