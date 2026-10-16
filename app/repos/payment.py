from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import PaymentStatus
from app.models.payment import Payment
from app.shemas.payment import PaymentCreate


class PaymentRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, data: PaymentCreate, idempotency_key: str) -> Payment | None:
        statement = (
            insert(Payment)
            .values(
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                metadata_=data.metadata,
                webhook_url=str(data.webhook_url),
                idempotency_key=idempotency_key,
                status=PaymentStatus.PENDING,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return (await self.session.execute(statement)).scalar_one_or_none()

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        return await self.session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        return await self.session.scalar(
            select(Payment).where(Payment.idempotency_key == key)
        )
