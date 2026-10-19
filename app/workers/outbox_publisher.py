import asyncio
import logging
from datetime import UTC, datetime

from app.core.db_config import SessionFactory, engine
from app.infrastructure.rabbitmq import broker, publish_payment, setup_rabbitmq
from app.repos.outbox import OutboxRepository
from app.schemas.events import PaymentCreatedEvent

logger = logging.getLogger(__name__)


async def publish_pending() -> bool:
    async with SessionFactory() as session, session.begin():
        events = await OutboxRepository(session).get_unpublished(limit=1)
        for event in events:
            data = PaymentCreatedEvent.model_validate(event.payload)
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
