import asyncio
import json
from datetime import datetime
from uuid import UUID

import pytest

from .rabbitmq_stack import observe_rabbitmq_outage, rabbitmq_outage


@pytest.mark.asyncio
async def test_outbox_and_payments_are_processed_after_rabbitmq_recovery(
    rabbitmq_control,
    pause_before_cleanup,
):
    from app.models.enums import PaymentStatus

    async with rabbitmq_outage(rabbitmq_control, pause_before_cleanup) as stack:
        before = await observe_rabbitmq_outage(stack, rabbitmq_control)
        outbox_before = {event.id: event.payload for event in before.events}

        # The user resumes the broker; publisher and consumer must reconnect.
        await rabbitmq_control.wait_for_manual_start(
            "rabbitmq",
            "\nОтправлено 10 платежей. RabbitMQ остановлен; события ждут в Outbox.",
            f"test_run_id: {stack.run_id}",
        )
        await stack.reconnect_broker()
        seen_consumer = False
        async with asyncio.timeout(160):
            while True:
                after = await stack.snapshot()
                seen_consumer |= after.consumers > 0
                assert after.metrics["dlq_messages"] == 0
                assert after.metrics["received_webhooks"] <= 10
                if (
                    after.metrics["pending_payments"] == 0
                    and after.metrics["published_outbox_events"] == 10
                    and after.metrics["ready_messages"] == 0
                    and after.metrics["received_webhooks"] == 10
                ):
                    break
                await asyncio.sleep(0.1)
        assert seen_consumer
        await rabbitmq_control.assert_running(
            "api",
            "postgres",
            "consumer",
            "outbox_publisher",
            unchanged=True,
        )

        # Any unacknowledged delivery would return to the queue after this stop.
        await rabbitmq_control.stop("consumer")
        after = await stack.snapshot()
        assert after.consumers == 0
        assert after.metrics == {
            "created_payments": 10,
            "pending_payments": 0,
            "published_outbox_events": 10,
            "ready_messages": 0,
            "received_webhooks": 10,
            "dlq_messages": 0,
        }
        assert len(after.events) == 10
        assert {event.id: event.payload for event in after.events} == outbox_before
        assert all(event.published_at is not None for event in after.events)
        assert all(event.published_at >= event.created_at for event in after.events)

        notifications = {}
        for _ in range(10):
            path, body = stack.webhooks.get_nowait()
            assert path == "/webhook"
            payload = json.loads(body)
            payment_id = UUID(payload["payment_id"])
            assert payment_id in stack.payment_ids
            assert payment_id not in notifications, (
                "Duplicate webhook after broker recovery"
            )
            notifications[payment_id] = payload
        assert notifications.keys() == stack.payment_ids
        for payment in after.payments:
            assert payment.status in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
            assert payment.processed_at is not None
            payload = notifications[payment.id]
            assert payload["status"] == payment.status.value
            assert (
                datetime.fromisoformat(payload["processed_at"]) == payment.processed_at
            )
