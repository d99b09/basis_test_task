from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db_config import get_session
from app.exceptions.payment import (
    IdempotencyConflictError,
    PaymentNotFoundError,
    PaymentStorageError,
)
from app.schemas.payment import PaymentCreate, PaymentCreated, PaymentResponse
from app.services.payment import PaymentService

router = APIRouter(
    prefix="/v1/payments",
    tags=["Платежи"],
    responses={503: {"description": "Ошибка доступа к базе данных."}},
)


@router.post(
    "",
    status_code=202,
    summary="Создать платёж",
    description="Сохраняет платёж. Повтор с тем же ключом и данными возвращает его снова.",
    response_description="Платёж принят в обработку.",
    responses={409: {"description": "Ключ уже использован с другими данными."}},
)
async def create_payment(
    data: PaymentCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=255,
            description="Уникальный ключ для защиты от повторного создания платежа.",
        ),
    ],
) -> PaymentCreated:
    try:
        payment = await PaymentService(session).create_payment(data, idempotency_key)
    except IdempotencyConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except PaymentStorageError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return PaymentCreated.model_validate(payment)


@router.get(
    "/{payment_id}",
    summary="Получить платёж",
    response_description="Полная информация о платеже.",
    responses={404: {"description": "Платёж не найден."}},
)
async def get_payment(
    payment_id: UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentResponse:
    try:
        payment = await PaymentService(session).get_payment(payment_id)
    except PaymentNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PaymentStorageError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return PaymentResponse.model_validate(payment)
