import asyncio
import random

from app.models.enums import PaymentStatus


class PaymentProcessor:
    async def process(self) -> PaymentStatus:
        delay = random.uniform(2, 5)
        await asyncio.sleep(delay)

        return (
            PaymentStatus.SUCCEEDED
            if random.random() < 0.9
            else PaymentStatus.FAILED
        )
