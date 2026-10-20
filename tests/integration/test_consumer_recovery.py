import asyncio
import json
import os
from datetime import datetime
from uuid import UUID

import pytest

from .live_stack import live_payment_stack


@pytest.mark.asyncio
async def test_ten_pending_payments_are_processed_after_consumer_recovery(
    consumer_control,
    pause_before_cleanup,
):
    from app.models.enums import PaymentStatus

    payment_count = 10
    async with live_payment_stack(
        database_url=consumer_control.database_url,
        rabbitmq_url=consumer_control.rabbitmq_url,
        api_url=consumer_control.api_url,
        api_key=os.environ["API_KEY"],
        webhook_host=consumer_control.webhook_host,
        require_stopped_consumer=False,
        pause_before_cleanup=pause_before_cleanup,
    ) as stack:
        queues = await consumer_control.queue_counts()
        assert queues["payments.new"]["messages_unacknowledged"] == 0, (
            "The consumer must be idle before the recovery test"
        )
        # Stop before deleting test data, including on failure. The fixture then
        # restores the original container state after receiver and DB cleanup.
        stack.exit_stack.push_async_callback(consumer_control.stop)
        await consumer_control.stop()
        await stack.create_payments(payment_count, "consumer-recovery")
        await stack.wait_for_publication(payment_count)
        await asyncio.sleep(1)
        before = await stack.snapshot()
        assert before.consumers == 0
        assert before.metrics == {
            "created_payments": 10,
            "pending_payments": 10,
            "published_outbox_events": 10,
            "ready_messages": 10,
            "received_webhooks": 0,
            "dlq_messages": 0,
        }
        assert all(payment.processed_at is None for payment in before.payments)
        outbox_before = {
            event.id: (event.payload, event.published_at) for event in before.events
        }

        # The user resumes the container while API and publisher remain running.
        await consumer_control.wait_for_manual_start(
            "consumer",
            "\nОтправлено 10 платежей. Consumer остановлен; сообщения ждут в очереди.",
            f"test_run_id: {stack.run_id}",
        )
        seen_consumer = False
        async with asyncio.timeout(payment_count * 7 + 30):
            while True:
                after = await stack.snapshot()
                seen_consumer |= after.consumers > 0
                assert after.metrics["dlq_messages"] == 0
                assert after.metrics["received_webhooks"] <= payment_count
                if (
                    after.metrics["pending_payments"] == 0
                    and after.metrics["received_webhooks"] == payment_count
                    and after.metrics["ready_messages"] == 0
                ):
                    break
                await asyncio.sleep(0.1)
        assert seen_consumer

        # Closing the consumer channel returns any unacknowledged messages to
        # payments.new. An empty queue afterward proves the backlog was acknowledged.
        await consumer_control.stop()
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
        assert {
            event.id: (event.payload, event.published_at) for event in after.events
        } == outbox_before

        notifications = {}
        for _ in range(payment_count):
            path, body = stack.webhooks.get_nowait()
            assert path == "/webhook"
            payload = json.loads(body)
            payment_id = UUID(payload["payment_id"])
            assert payment_id in stack.payment_ids
            assert payment_id not in notifications, "Duplicate webhook after recovery"
            notifications[payment_id] = payload
        assert notifications.keys() == stack.payment_ids
        for payment in after.payments:
            assert payment.status in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
            assert payment.processed_at is not None
            assert payment.processed_at >= payment.created_at
            payload = notifications[payment.id]
            assert payload["status"] == payment.status.value
            assert (
                datetime.fromisoformat(payload["processed_at"]) == payment.processed_at
            )
