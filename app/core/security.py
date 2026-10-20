from secrets import compare_digest

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from starlette.types import ASGIApp, Receive, Scope, Send

from app.core.config import CONFIG

api_key_header = APIKeyHeader(
    name="X-API-Key",
    scheme_name="APIKey",
    description="Статический API-ключ из переменной окружения API_KEY.",
    auto_error=False,
)


async def verify_api_key(api_key: str | None) -> None:
    if api_key is None or not compare_digest(
        api_key.encode("utf-8"), CONFIG.API_KEY.encode("utf-8")
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный или отсутствующий API-ключ.",
            headers={"WWW-Authenticate": "APIKey"},
        )


class APIKeyMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            request = Request(scope)
            try:
                await verify_api_key(request.headers.get(api_key_header.model.name))
            except HTTPException as exc:
                response = JSONResponse(
                    status_code=exc.status_code,
                    content={"detail": exc.detail},
                    headers=exc.headers,
                )
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)
