import asyncio
import logging
from datetime import UTC, datetime

from pydantic import ValidationError

from app.core.db_config import SessionFactory, engine
from app.infrastructure.rabbitmq import broker, publish_payment, setup_rabbitmq
from app.repos.outbox import OutboxRepository
from app.schemas.events import PaymentCreatedEvent

logger = logging.getLogger(__name__)


async def publish_pending() -> bool:
    async with SessionFactory() as session, session.begin():
        events = await OutboxRepository(session).get_unpublished(limit=1)
        for event in events:
            try:
                data = PaymentCreatedEvent.model_validate(event.payload)
            except ValidationError as exc:
                event.error = str(exc)
                logger.warning("Некорректное событие Outbox %s: %s", event.id, exc)
                continue
            await publish_payment(data.payment_id)
            event.published_at = datetime.now(UTC)
        return bool(events)


async def main() -> None:
    try:
        await setup_rabbitmq()
        while True:
            try:
                if await publish_pending():
                    continue
            except Exception:
                logger.exception("Ошибка публикации события Outbox")
            await asyncio.sleep(1)
    finally:
        try:
            await broker.stop()
        finally:
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
