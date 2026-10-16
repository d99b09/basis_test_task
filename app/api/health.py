from asyncio import timeout
from typing import Annotated

from asyncpg import PostgresError
from fastapi import APIRouter, Depends, Response
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db_config import get_session

router = APIRouter()


@router.get(
    "/health",
    tags=["Состояние сервиса"],
    summary="Проверка состояния API и PostgreSQL",
    response_description="API и PostgreSQL доступны.",
    responses={503: {"description": "PostgreSQL недоступен."}},
)
async def health(
    response: Response,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, str]:
    try:
        async with timeout(3):
            await session.execute(text("SELECT 1"))
    except (SQLAlchemyError, PostgresError, OSError, TimeoutError):
        response.status_code = 503
        return {"status": "degraded", "api": "ok", "postgres": "unavailable"}
    return {"status": "ok", "api": "ok", "postgres": "ok"}
