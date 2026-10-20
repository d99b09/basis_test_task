import pytest

from .rabbitmq_stack import observe_rabbitmq_outage, rabbitmq_outage


@pytest.mark.asyncio
async def test_b3_api_accepts_ten_payments_when_rabbitmq_unavailable(
    rabbitmq_control, pause_before_cleanup
):
    async with rabbitmq_outage(rabbitmq_control, pause_before_cleanup) as stack:
        await observe_rabbitmq_outage(stack, rabbitmq_control)
