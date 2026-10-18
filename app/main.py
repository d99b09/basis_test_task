from fastapi import Depends, FastAPI

from app.api.payments import router as payments_router
from app.core.lifespan import lifespan
from app.core.security import APIKeyMiddleware, verify_api_key

app = FastAPI(
    title="Тестовое задание «ООО МКК Базис»",
    lifespan=lifespan,
    dependencies=[Depends(verify_api_key)],
    responses={401: {"description": "API-ключ отсутствует или неверен."}},
)

app.add_middleware(APIKeyMiddleware)

app.include_router(payments_router, prefix="/api")
