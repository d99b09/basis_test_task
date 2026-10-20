import asyncio
import os

import pytest

from .live_stack import live_payment_stack


@pytest.mark.asyncio
async def test_ten_payments_stay_pending_when_consumer_unavailable(
    request, pause_before_cleanup
):
    if not request.config.getoption("--consumer-unavailable"):
        pytest.skip("Stop the consumer container and pass --consumer-unavailable")
    for name in ("DATABASE_URL", "RABBITMQ_URL", "API_KEY"):
        if not os.getenv(name):
            pytest.fail(f"{name} is required for the live-stack test")

    async with live_payment_stack(
        database_url=os.environ["DATABASE_URL"],
        rabbitmq_url=os.environ["RABBITMQ_URL"],
        api_url=request.config.getoption("--stack-api-url"),
        api_key=os.environ["API_KEY"],
        pause_before_cleanup=pause_before_cleanup,
    ) as stack:
        await stack.create_payments(10, "consumer-unavailable")
        await stack.wait_for_publication(10)
        for _ in range(3):
            await asyncio.sleep(1)
            state = await stack.snapshot()
            assert state.consumers == 0
            assert all(payment.processed_at is None for payment in state.payments)
            assert state.metrics == {
                "created_payments": 10,
                "pending_payments": 10,
                "published_outbox_events": 10,
                "ready_messages": 10,
                "received_webhooks": 0,
                "dlq_messages": 0,
            }
