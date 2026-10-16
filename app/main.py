from fastapi import FastAPI

from app.api.health import router as health_router
from app.api.payments import router as payments_router
from app.core.lifespan import lifespan

app = FastAPI(title="Тестовое задание «ООО МКК Базис»", lifespan=lifespan)
api = FastAPI(title=app.title)

api.include_router(health_router)
api.include_router(payments_router)

app.mount("/api", api)
