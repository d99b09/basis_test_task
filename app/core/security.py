from secrets import compare_digest
from typing import Annotated

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from app.core.config import CONFIG

api_key_header = APIKeyHeader(
    name="X-API-Key",
    scheme_name="APIKey",
    description="Статический API-ключ из переменной окружения API_KEY.",
    auto_error=False,
)


async def verify_api_key(
    api_key: Annotated[str | None, Security(api_key_header)],
) -> None:
    if api_key is None or not compare_digest(
        api_key.encode("utf-8"), CONFIG.API_KEY.encode("utf-8")
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный или отсутствующий API-ключ.",
            headers={"WWW-Authenticate": "APIKey"},
        )
