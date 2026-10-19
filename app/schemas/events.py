from uuid import UUID

from pydantic import BaseModel


class PaymentCreatedEvent(BaseModel):
    """Событие создания платежа для очереди payments.new."""

    payment_id: UUID
