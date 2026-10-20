import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from faststream import AckPolicy, Context, FastStream
from faststream.rabbit import RabbitMessage
from httpx import AsyncClient, HTTPError
from pydantic import ValidationError

from app.core.db_config import SessionFactory, engine
from app.exceptions.payment import PaymentProcessingError
from app.infrastructure.rabbitmq import (
    broker,
    payments_channel,
    payments_queue,
    setup_rabbitmq,
)
from app.models.enums import PaymentStatus
from app.models.payment import Payment
from app.repos.payment import PaymentRepository
from app.schemas.events import PaymentCreatedEvent
from app.services.processor import PaymentProcessor

logger = logging.getLogger(__name__)
PROCESSING_ATTEMPTS = 3
PROCESSING_FAILURES_HEADER = "x-processing-failures"


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


async def process_payment(payment_id: UUID) -> Payment | None:
    async with SessionFactory() as session, session.begin():
        repository = PaymentRepository(session)
        payment = await repository.get_by_id_for_update(payment_id)
        if payment is None:
            logger.warning("Платёж %s не найден", payment_id)
            return None
        if payment.status == PaymentStatus.PENDING:
            status = await PaymentProcessor().process()
            if status != PaymentStatus.SUCCEEDED:
                raise PaymentProcessingError(f"Ошибка обработки платежа {payment_id}")
            await repository.update_status(payment, status, datetime.now(UTC))
    logger.info("Платёж %s: сохранённый статус %s", payment.id, payment.status.value)
    return payment


async def fail_payment(payment_id: UUID) -> Payment | None:
    async with SessionFactory() as session, session.begin():
        repository = PaymentRepository(session)
        payment = await repository.get_by_id_for_update(payment_id)
        if payment is not None and payment.status == PaymentStatus.PENDING:
            await repository.update_status(
                payment, PaymentStatus.FAILED, datetime.now(UTC)
            )
    return payment


async def send_webhook(payment: Payment, client: AsyncClient) -> bool:
    try:
        payload = {
            "payment_id": str(payment.id),
            "status": payment.status.value,
            "processed_at": (
                payment.processed_at.isoformat()
                if payment.processed_at is not None
                else None
            ),
        }
        response = await client.post(payment.webhook_url, json=payload)
        response.raise_for_status()
    except HTTPError as exc:
        logger.warning(
            "Ошибка доставки webhook %s, платёж %s: %s: %s",
            payment.webhook_url,
            payment.id,
            type(exc).__name__,
            exc,
        )
        return False
    except Exception:
        logger.exception("Ошибка отправки webhook для платежа %s", payment.id)
        return False
    return True


def processing_failures(message: RabbitMessage) -> int:
    failures = message.headers.get(PROCESSING_FAILURES_HEADER, 0)
    if type(failures) is not int or failures < 0:
        logger.warning("Некорректный счётчик ошибок сообщения %s", message.message_id)
        failures = 0
    return failures


async def retry_or_reject(data: bytes, message: RabbitMessage, failures: int) -> None:
    failures += 1
    logger.warning(
        "Ошибка обработки сообщения %s, попытка %s из %s",
        message.message_id,
        failures,
        PROCESSING_ATTEMPTS,
    )
    if failures >= PROCESSING_ATTEMPTS:
        await message.reject(requeue=False)
        return

    await republish_retry(data, message, failures)


async def republish_retry(data: bytes, message: RabbitMessage, failures: int) -> None:
    await asyncio.sleep(2 ** min(failures - 1, PROCESSING_ATTEMPTS - 2))
    try:
        await broker.publish(
            data,
            queue=payments_queue,
            headers={**message.headers, PROCESSING_FAILURES_HEADER: failures},
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            content_type=message.raw_message.content_type,
            content_encoding=message.raw_message.content_encoding,
            persist=True,
            mandatory=True,
            timeout=10,
        )
    except Exception:
        logger.exception("Ошибка публикации повтора сообщения %s", message.message_id)
        await message.nack(requeue=True)
        return
    await message.ack()


@broker.subscriber(
    payments_queue,
    channel=payments_channel,
    ack_policy=AckPolicy.MANUAL,
    decoder=lambda message: message.body,
)
async def consume(
    data: bytes,
    message: RabbitMessage,
    http_client: Annotated[AsyncClient, Context()],
) -> None:
    failures = processing_failures(message)
    try:
        event = PaymentCreatedEvent.model_validate_json(data)
    except ValidationError:
        logger.warning("Некорректное событие платежа")
        await retry_or_reject(data, message, failures)
        return

    payment = None
    if failures < PROCESSING_ATTEMPTS:
        try:
            payment = await process_payment(event.payment_id)
        except Exception:
            logger.exception("Ошибка обработки сообщения %s", message.message_id)
            if failures + 1 < PROCESSING_ATTEMPTS:
                await retry_or_reject(data, message, failures)
                return
        else:
            if payment is None:
                await retry_or_reject(data, message, failures)
                return

    if payment is None:
        try:
            payment = await fail_payment(event.payment_id)
        except Exception:
            logger.exception(
                "Не удалось сохранить failed для платежа %s", event.payment_id
            )
            await republish_retry(data, message, PROCESSING_ATTEMPTS)
            return

    delivered = payment is not None and await send_webhook(payment, http_client)
    if delivered:
        if payment.status == PaymentStatus.FAILED:
            await message.reject(requeue=False)
        else:
            await message.ack()
        return
    await retry_or_reject(data, message, failures)


if __name__ == "__main__":
    asyncio.run(app.run())
