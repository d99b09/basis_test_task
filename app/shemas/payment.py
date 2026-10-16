from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, HttpUrl

from app.models.enums import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    """Данные для создания платежа."""

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency = Currency.RUB
    description: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentCreated(BaseModel):
    """Ответ о принятии платежа в обработку."""

    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias=AliasChoices("payment_id", "id"))
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(PaymentCreated):
    """Полная информация о платеже."""

    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(
        validation_alias=AliasChoices("metadata_", "metadata")
    )
    idempotency_key: str
    webhook_url: HttpUrl
    processed_at: datetime | None = None
