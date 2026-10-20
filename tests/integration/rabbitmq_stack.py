import asyncio
import os
from contextlib import asynccontextmanager

from .live_stack import live_payment_stack


@asynccontextmanager
async def rabbitmq_outage(control, pause_before_cleanup):
    await control.assert_running(
        "api", "postgres", "rabbitmq", "consumer", "outbox_publisher"
    )
    async with live_payment_stack(
        database_url=control.database_url,
        rabbitmq_url=control.rabbitmq_url,
        api_url=control.api_url,
        api_key=os.environ["API_KEY"],
        webhook_host=control.webhook_host,
        require_stopped_consumer=False,
        pause_before_cleanup=pause_before_cleanup,
    ) as stack:
        queues = await control.wait_for_consumer()
        assert queues["payments.new"]["messages_unacknowledged"] == 0
        assert queues["payments.new"]["consumers"] > 0

        async def prepare_cleanup():
            # Keep this run's events from reaching a closed webhook receiver.
            # Resume RabbitMQ before DB cleanup so publisher locks can be released.
            await control.stop("consumer")
            await control.start("rabbitmq")
            await stack.reconnect_broker()

        stack.exit_stack.push_async_callback(prepare_cleanup)
        await stack.disconnect_broker()
        await control.stop("rabbitmq")
        await control.assert_running(
            "api", "postgres", "consumer", "outbox_publisher", unchanged=True
        )
        await stack.create_payments(10, "rabbitmq-outage")
        yield stack


async def assert_pending_outbox(stack, control):
    # Only HTTP and PostgreSQL are used while the real broker is down.
    assert not (await control.state("rabbitmq"))["Running"]
    state = await stack.database_snapshot()
    assert state.metrics == {
        "created_payments": 10,
        "pending_payments": 10,
        "published_outbox_events": 0,
        "received_webhooks": 0,
    }
    assert len(state.events) == 10
    assert all(event.published_at is None for event in state.events)
    assert all(payment.processed_at is None for payment in state.payments)
    return state


async def observe_rabbitmq_outage(stack, control):
    for _ in range(3):
        await asyncio.sleep(1)
        state = await assert_pending_outbox(stack, control)
    await control.assert_running(
        "api", "postgres", "consumer", "outbox_publisher", unchanged=True
    )
    return state
