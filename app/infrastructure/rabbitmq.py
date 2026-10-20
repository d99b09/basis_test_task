from uuid import UUID

from faststream.rabbit import Channel, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import CONFIG

payments_channel = Channel(prefetch_count=10, on_return_raises=True)
broker = RabbitBroker(
    CONFIG.RABBITMQ_URL,
    timeout=10,
    default_channel=payments_channel,
)
dead_letter_exchange = RabbitExchange("payments.dlx")
dead_letter_queue = RabbitQueue("payments.dlq", durable=True)
payments_queue = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-dead-letter-exchange": dead_letter_exchange.name,
        "x-dead-letter-routing-key": dead_letter_queue.name,
    },
)


async def setup_rabbitmq() -> None:
    await broker.connect()
    dlx = await broker.declare_exchange(dead_letter_exchange)
    dlq = await broker.declare_queue(dead_letter_queue)
    await dlq.bind(dlx, routing_key=dead_letter_queue.name)
    await broker.declare_queue(payments_queue)


async def publish_payment(payment_id: UUID) -> None:
    await broker.publish(
        {"payment_id": str(payment_id)},
        queue=payments_queue,
        message_id=str(payment_id),
        persist=True,
        timeout=10,
    )
