import logging
from uuid import UUID

from asyncpg import PostgresError
from pydantic import HttpUrl
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions.payment import (
    IdempotencyConflictError,
    PaymentNotFoundError,
    PaymentStorageError,
)
from app.models.payment import Payment
from app.repos.outbox import OutboxRepository
from app.repos.payment import PaymentRepository
from app.shemas.events import PaymentCreatedEvent
from app.shemas.payment import PaymentCreate

logger = logging.getLogger(__name__)


class PaymentService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.repository = PaymentRepository(session)
        self.outbox_repository = OutboxRepository(session)

    async def create_payment(
        self, data: PaymentCreate, idempotency_key: str
    ) -> Payment:
        try:
            async with self.session.begin():
                payment = await self.repository.create(data, idempotency_key)
                if payment is not None:
                    event = PaymentCreatedEvent(payment_id=payment.id)
                    await self.outbox_repository.created(event.model_dump(mode="json"))
                    return payment
                payment = await self.repository.get_by_idempotency_key(idempotency_key)
                if payment is None:
                    raise PaymentStorageError("Не удалось получить сохранённый платёж.")
                if (
                    payment.amount != data.amount
                    or payment.currency != data.currency
                    or payment.description != data.description
                    or payment.metadata_ != data.metadata
                    or str(HttpUrl(payment.webhook_url)) != str(data.webhook_url)
                ):
                    raise IdempotencyConflictError(
                        "Ключ идемпотентности уже использован с другими данными."
                    )
                return payment
        except (SQLAlchemyError, PostgresError, OSError, TimeoutError) as exc:
            logger.exception("Ошибка сохранения платежа")
            raise PaymentStorageError("Не удалось сохранить платёж.") from exc

    async def get_payment(self, payment_id: UUID) -> Payment:
        try:
            async with self.session.begin():
                payment = await self.repository.get_by_id(payment_id)
                if payment is None:
                    raise PaymentNotFoundError("Платёж не найден.")
                return payment
        except (SQLAlchemyError, PostgresError, OSError, TimeoutError) as exc:
            logger.exception("Ошибка получения платежа")
            raise PaymentStorageError("Не удалось получить платёж.") from exc
