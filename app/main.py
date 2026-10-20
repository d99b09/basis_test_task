from fastapi import FastAPI, Security

from app.api.payments import router as payments_router
from app.core.lifespan import lifespan
from app.core.security import APIKeyMiddleware, api_key_header

app = FastAPI(
    title="Тестовое задание «ООО МКК Базис»",
    lifespan=lifespan,
    dependencies=[Security(api_key_header)],
    responses={401: {"description": "API-ключ отсутствует или неверен."}},
)

app.add_middleware(APIKeyMiddleware)

app.include_router(payments_router, prefix="/api")
