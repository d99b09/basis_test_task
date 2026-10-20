import asyncio
import json
from contextlib import AsyncExitStack
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from faststream import AckPolicy
from faststream.message import AckStatus
from faststream.rabbit import RabbitMessage

from .test_payment import webhook_receiver


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcomes",
    [
        ["succeeded"],
        ["failed", "succeeded"],
        [TimeoutError("processor unavailable"), "succeeded"],
        ["failed", TimeoutError("processor unavailable"), "succeeded"],
        ["failed", "failed", "failed"],
        [TimeoutError("processor unavailable") for _ in range(3)],
        ["failed", TimeoutError("processor unavailable"), "failed"],
    ],
    ids=[
        "success-first",
        "processing-error-then-success",
        "technical-error-then-success",
        "mixed-errors-then-success",
        "processing-errors-exhausted",
        "technical-errors-exhausted",
        "mixed-errors-exhausted",
    ],
)
async def test_payment_retry_with_real_database_and_dlq(
    payment_pipeline, monkeypatch, outcomes
):
    from app.models import Payment
    from app.models.enums import PaymentStatus
    from app.workers import consumer, outbox_publisher

    processor = AsyncMock(
        side_effect=[
            PaymentStatus(outcome) if isinstance(outcome, str) else outcome
            for outcome in outcomes
        ]
    )
    monkeypatch.setattr(consumer.PaymentProcessor, "process", processor)
    completed = asyncio.get_running_loop().create_future()
    deliveries = []
    snapshots = []
    succeeded = outcomes[-1] == "succeeded"
    expected = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED

    async with AsyncExitStack() as stack:
        webhook_url, webhooks = await stack.enter_async_context(webhook_receiver())
        response = await payment_pipeline.client.post(
            "/api/v1/payments",
            headers={"Idempotency-Key": f"retry-{uuid4().hex}"},
            json={
                "amount": "125.50",
                "currency": "RUB",
                "description": "Payment retry integration test",
                "webhook_url": webhook_url,
            },
        )
        assert response.status_code == 202, response.text
        payment_id = UUID(response.json()["payment_id"])

        @payment_pipeline.broker.subscriber(
            payment_pipeline.queue,
            channel=consumer.payments_channel,
            ack_policy=AckPolicy.MANUAL,
            decoder=lambda message: message.body,
        )
        async def handle(data: bytes, message: RabbitMessage):
            try:
                await consumer.consume(data, message, payment_pipeline.webhook_client)
                deliveries.append(message)
                async with payment_pipeline.sessions() as session:
                    payment = await session.get(Payment, payment_id)
                    snapshots.append((payment.status, payment.processed_at))
                if len(deliveries) < len(outcomes):
                    assert snapshots[-1] == (PaymentStatus.PENDING, None)
                    assert message.committed == AckStatus.ACKED
                    assert webhooks.empty()
                else:
                    assert snapshots[-1][0] == expected
                    assert snapshots[-1][1] is not None
                    assert message.committed == (
                        AckStatus.ACKED if succeeded else AckStatus.REJECTED
                    )
                    completed.set_result(None)
            except Exception as exc:
                if not completed.done():
                    completed.set_exception(exc)
                raise

        stack.push_async_callback(payment_pipeline.broker.stop)
        assert await outbox_publisher.publish_pending()
        await payment_pipeline.broker.start()
        await asyncio.wait_for(completed, timeout=20)
        await payment_pipeline.broker.stop()

        assert processor.await_count == len(outcomes)
        assert len(deliveries) == len(outcomes)
        assert [
            message.headers.get(consumer.PROCESSING_FAILURES_HEADER, 0)
            for message in deliveries
        ] == list(range(len(outcomes)))
        assert webhooks.qsize() == 1
        path, payload = webhooks.get_nowait()
        assert path == "/webhook"
        assert payload == {
            "payment_id": str(payment_id),
            "status": expected.value,
            "processed_at": snapshots[-1][1].isoformat(),
        }

        main = await payment_pipeline.channel.declare_queue(
            payment_pipeline.queue.name, passive=True
        )
        assert main.declaration_result.message_count == 0
        dlq = await payment_pipeline.channel.get_queue(payment_pipeline.dlq.name)
        if succeeded:
            state = await payment_pipeline.channel.declare_queue(dlq.name, passive=True)
            assert state.declaration_result.message_count == 0
        else:
            async with asyncio.timeout(5):
                while True:
                    state = await payment_pipeline.channel.declare_queue(
                        dlq.name, passive=True
                    )
                    if state.declaration_result.message_count == 1:
                        break
                    await asyncio.sleep(0.05)
            message = await dlq.get(timeout=5)
            try:
                assert json.loads(message.body) == {"payment_id": str(payment_id)}
                assert message.message_id == str(payment_id)
                assert message.headers[consumer.PROCESSING_FAILURES_HEADER] == 2
                assert any(
                    death["reason"] == "rejected"
                    and death["queue"] == payment_pipeline.queue.name
                    for death in message.headers["x-death"]
                )
            finally:
                await message.nack(requeue=True)
