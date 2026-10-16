import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from faststream import AckPolicy, Context, FastStream
from faststream.rabbit import Channel, RabbitMessage
from httpx import AsyncClient
from pydantic import ValidationError

from app.core.db_config import SessionFactory, engine
from app.exceptions.payment import PaymentNotFoundError
from app.infrastructure.rabbitmq import broker, payments_queue, setup_rabbitmq
from app.models.enums import PaymentStatus
from app.models.payment import Payment
from app.repos.payment import PaymentRepository
from app.services.processor import PaymentProcessor
from app.shemas.events import PaymentCreatedEvent

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan() -> AsyncIterator[None]:
    async with AsyncExitStack() as stack:
        stack.push_async_callback(engine.dispose)
        client = await stack.enter_async_context(
            AsyncClient(timeout=10, follow_redirects=False)
        )
        app.context.set_global("http_client", client)
        stack.push_async_callback(broker.stop)
        await setup_rabbitmq()
        yield


app = FastStream(broker, lifespan=lifespan)


async def process_payment(payment_id: UUID) -> Payment:
    async with SessionFactory() as session, session.begin():
        repository = PaymentRepository(session)
        payment = await repository.get_by_id_for_update(payment_id)
        if payment is None:
            raise PaymentNotFoundError("Платёж не найден.")
        if payment.status == PaymentStatus.PENDING:
            status = await PaymentProcessor().process()
            await repository.update_status(payment, status, datetime.now(UTC))
    logger.info("Платёж %s: сохранённый статус %s", payment.id, payment.status.value)
    return payment


@broker.subscriber(
    payments_queue,
    channel=Channel(prefetch_count=1),
    ack_policy=AckPolicy.MANUAL,
    decoder=lambda message: message.body,
)
async def consume(
    data: bytes,
    message: RabbitMessage,
    http_client: Annotated[AsyncClient, Context()],
) -> None:
    try:
        event = PaymentCreatedEvent.model_validate_json(data)
    except ValidationError:
        logger.exception("Некорректное событие платежа")
        await message.reject(requeue=False)
        return

    for attempt in range(3):
        try:
            payment = await process_payment(event.payment_id)
            response = await http_client.post(
                payment.webhook_url,
                json={
                    "payment_id": str(payment.id),
                    "status": payment.status.value,
                    "processed_at": (
                        payment.processed_at.isoformat()
                        if payment.processed_at is not None
                        else None
                    ),
                },
            )
            response.raise_for_status()
        except PaymentNotFoundError:
            logger.warning("Платёж %s не найден", event.payment_id)
            await message.reject(requeue=False)
            return
        except Exception:
            logger.exception(
                "Ошибка обработки платежа %s, попытка %s из 3",
                event.payment_id,
                attempt + 1,
            )
            if attempt < 2:
                await asyncio.sleep(2**attempt)
        else:
            await message.ack()
            return

    await message.reject(requeue=False)


if __name__ == "__main__":
    asyncio.run(app.run())
