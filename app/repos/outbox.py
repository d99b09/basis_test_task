from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def created(self, payload: dict[str, Any]) -> OutboxEvent:
        event = OutboxEvent(payload=payload)
        self.session.add(event)
        await self.session.flush()
        return event

    async def get_unpublished(self, limit: int = 100) -> list[OutboxEvent]:
        statement = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None), OutboxEvent.error.is_(None))
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(await self.session.scalars(statement))
