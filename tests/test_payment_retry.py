import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call
from uuid import uuid4

import pytest
from httpx import Request, Response
from sqlalchemy.exc import SQLAlchemyError


@pytest.fixture
def pipeline(monkeypatch):
    defaults = {
        "DATABASE_URL": "postgresql+asyncpg://test:test@localhost/test",
        "RABBITMQ_URL": "amqp://guest:guest@localhost/",
        "API_KEY": "test",
    }
    for name, value in defaults.items():
        if not os.getenv(name):
            monkeypatch.setenv(name, value)

    from app.models.enums import PaymentStatus
    from app.models.payment import Payment
    from app.schemas.events import PaymentCreatedEvent
    from app.workers import consumer

    payment = Payment(
        id=uuid4(),
        status=PaymentStatus.PENDING,
        webhook_url="http://webhook.test/payment",
    )
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    session.begin.return_value = AsyncMock()
    monkeypatch.setattr(consumer, "SessionFactory", MagicMock(return_value=session))

    async def update_status(payment, status, processed_at):
        payment.status = status
        payment.processed_at = processed_at

    repository = SimpleNamespace(
        get_by_id_for_update=AsyncMock(return_value=payment),
        update_status=AsyncMock(side_effect=update_status),
    )
    monkeypatch.setattr(consumer, "PaymentRepository", lambda session: repository)
    processor = AsyncMock(return_value=PaymentStatus.SUCCEEDED)
    monkeypatch.setattr(consumer.PaymentProcessor, "process", processor)
    publish = AsyncMock()
    monkeypatch.setattr(consumer.broker, "publish", publish)
    sleep = AsyncMock()
    monkeypatch.setattr(consumer.asyncio, "sleep", sleep)
    client = SimpleNamespace(
        post=AsyncMock(
            return_value=Response(200, request=Request("POST", payment.webhook_url))
        )
    )
    event = PaymentCreatedEvent(payment_id=payment.id)
    return SimpleNamespace(
        consumer=consumer,
        payment=payment,
        repository=repository,
        processor=processor,
        publish=publish,
        sleep=sleep,
        client=client,
        data=event.model_dump_json().encode(),
    )


def make_message(pipeline, headers=None):
    return SimpleNamespace(
        headers=headers or {},
        message_id=str(pipeline.payment.id),
        correlation_id="payment-correlation",
        raw_message=SimpleNamespace(
            content_type="application/json", content_encoding="utf-8"
        ),
        ack=AsyncMock(),
        nack=AsyncMock(),
        reject=AsyncMock(),
    )


async def deliver(pipeline, headers=None):
    message = make_message(pipeline, headers)
    await pipeline.consumer.consume(pipeline.data, message, pipeline.client)
    return message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcomes",
    [
        ["succeeded"],
        ["failed", "succeeded"],
        [TimeoutError("processor unavailable"), "succeeded"],
        ["failed", TimeoutError("processor unavailable"), "succeeded"],
        ["failed", "failed", "failed"],
        [TimeoutError("processor unavailable") for _ in range(3)],
        ["failed", TimeoutError("processor unavailable"), "failed"],
    ],
    ids=[
        "success-first",
        "processing-error-then-success",
        "technical-error-then-success",
        "mixed-errors-then-success",
        "processing-errors-exhausted",
        "technical-errors-exhausted",
        "mixed-errors-exhausted",
    ],
)
async def test_payment_retries_and_final_status(pipeline, outcomes):
    from app.models.enums import PaymentStatus

    pipeline.processor.side_effect = [
        PaymentStatus(outcome) if isinstance(outcome, str) else outcome
        for outcome in outcomes
    ]
    headers = {"custom-header": "preserved"}
    for attempt in range(len(outcomes)):
        assert pipeline.payment.status == PaymentStatus.PENDING
        assert pipeline.payment.processed_at is None
        message = await deliver(pipeline, headers)
        message.nack.assert_not_awaited()
        if attempt < len(outcomes) - 1:
            assert pipeline.payment.status == PaymentStatus.PENDING
            assert pipeline.payment.processed_at is None
            pipeline.client.post.assert_not_awaited()
            message.ack.assert_awaited_once()
            message.reject.assert_not_awaited()
            args, kwargs = pipeline.publish.await_args
            assert args == (pipeline.data,)
            assert kwargs["persist"] is True
            assert kwargs["mandatory"] is True
            assert kwargs["message_id"] == message.message_id
            assert kwargs["correlation_id"] == message.correlation_id
            assert kwargs["content_type"] == "application/json"
            assert kwargs["content_encoding"] == "utf-8"
            headers = kwargs["headers"]
            assert headers == {
                "custom-header": "preserved",
                pipeline.consumer.PROCESSING_FAILURES_HEADER: attempt + 1,
            }

    succeeded = outcomes[-1] == "succeeded"
    expected = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
    assert pipeline.payment.status == expected
    assert pipeline.payment.processed_at is not None
    pipeline.repository.update_status.assert_awaited_once()
    assert pipeline.processor.await_count == len(outcomes)
    assert pipeline.publish.await_count == len(outcomes) - 1
    assert pipeline.sleep.await_args_list == [
        call(2**i) for i in range(len(outcomes) - 1)
    ]
    if succeeded:
        message.ack.assert_awaited_once()
        message.reject.assert_not_awaited()
    else:
        message.ack.assert_not_awaited()
        message.reject.assert_awaited_once_with(requeue=False)
    pipeline.client.post.assert_awaited_once_with(
        pipeline.payment.webhook_url,
        json={
            "payment_id": str(pipeline.payment.id),
            "status": expected.value,
            "processed_at": pipeline.payment.processed_at.isoformat(),
        },
    )


@pytest.mark.asyncio
async def test_database_errors_exhaust_attempts_and_persist_failed(pipeline):
    from app.models.enums import PaymentStatus

    pipeline.repository.get_by_id_for_update.side_effect = [
        SQLAlchemyError("database unavailable"),
        SQLAlchemyError("database unavailable"),
        SQLAlchemyError("database unavailable"),
        pipeline.payment,
    ]
    headers = {}
    for _ in range(3):
        message = await deliver(pipeline, headers)
        if pipeline.publish.await_args is not None:
            headers = pipeline.publish.await_args.kwargs["headers"]
    pipeline.processor.assert_not_awaited()
    assert pipeline.payment.status == PaymentStatus.FAILED
    assert pipeline.payment.processed_at is not None
    assert pipeline.publish.await_count == 2
    message.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_failed_status_is_committed_before_dlq(pipeline):
    from app.models.enums import PaymentStatus

    pipeline.processor.side_effect = TimeoutError("processor unavailable")
    # The third attempt fails; writing its final status fails as well.
    pipeline.repository.get_by_id_for_update.side_effect = [
        pipeline.payment,
        SQLAlchemyError("database unavailable"),
        pipeline.payment,
    ]
    header = pipeline.consumer.PROCESSING_FAILURES_HEADER
    message = await deliver(pipeline, {header: 2})
    assert pipeline.payment.status == PaymentStatus.PENDING
    message.reject.assert_not_awaited()
    message.ack.assert_awaited_once()
    pipeline.client.post.assert_not_awaited()
    headers = pipeline.publish.await_args.kwargs["headers"]
    assert headers[header] == 3

    # Redelivery only retries saving failed, without a fourth processing attempt.
    message = await deliver(pipeline, headers)
    pipeline.processor.assert_awaited_once()
    assert pipeline.payment.status == PaymentStatus.FAILED
    message.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["succeeded", "failed"])
async def test_terminal_payment_is_not_processed_again(pipeline, status):
    from datetime import UTC, datetime

    from app.models.enums import PaymentStatus

    pipeline.payment.status = PaymentStatus(status)
    pipeline.payment.processed_at = datetime.now(UTC)
    processed_at = pipeline.payment.processed_at
    message = await deliver(pipeline)
    pipeline.processor.assert_not_awaited()
    pipeline.repository.update_status.assert_not_awaited()
    assert pipeline.payment.processed_at == processed_at
    if status == "failed":
        message.reject.assert_awaited_once_with(requeue=False)
    else:
        message.ack.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcomes",
    [
        ["succeeded"],
        ["failed", "succeeded"],
        ["failed", "failed", "succeeded"],
        ["failed", "failed", "failed"],
    ],
)
@pytest.mark.parametrize("success_attempt", [1, 2, 3, None])
async def test_webhook_retries_inside_consumer_without_republishing(
    pipeline, outcomes, success_attempt
):
    from httpx import ConnectError

    from app.models.enums import PaymentStatus

    pipeline.processor.side_effect = [PaymentStatus(outcome) for outcome in outcomes]
    headers = {"custom-header": "preserved"}
    for _ in outcomes[:-1]:
        message = await deliver(pipeline, headers)
        message.ack.assert_awaited_once()
        pipeline.client.post.assert_not_awaited()
        headers = pipeline.publish.await_args.kwargs["headers"]

    attempts = success_attempt or pipeline.consumer.WEBHOOK_ATTEMPTS
    http_results = [ConnectError("webhook unavailable") for _ in range(attempts)]
    if success_attempt is not None:
        http_results[-1] = Response(
            200, request=Request("POST", pipeline.payment.webhook_url)
        )
    pipeline.client.post.side_effect = http_results
    pipeline.publish.reset_mock()
    pipeline.sleep.reset_mock()
    message = make_message(pipeline, headers)

    async def wait_for_retry(delay):
        # The original message stays unacknowledged throughout HTTP retries.
        message.ack.assert_not_awaited()
        message.reject.assert_not_awaited()
        message.nack.assert_not_awaited()
        pipeline.publish.assert_not_awaited()

    pipeline.sleep.side_effect = wait_for_retry
    await pipeline.consumer.consume(pipeline.data, message, pipeline.client)

    assert pipeline.processor.await_count == len(outcomes)
    pipeline.repository.update_status.assert_awaited_once()
    assert pipeline.payment.status == PaymentStatus(outcomes[-1])
    assert pipeline.payment.processed_at is not None
    expected_webhook = call(
        pipeline.payment.webhook_url,
        json={
            "payment_id": str(pipeline.payment.id),
            "status": pipeline.payment.status.value,
            "processed_at": pipeline.payment.processed_at.isoformat(),
        },
    )
    assert pipeline.client.post.await_args_list == [expected_webhook] * attempts
    assert pipeline.sleep.await_args_list == [call(2**i) for i in range(attempts - 1)]
    pipeline.publish.assert_not_awaited()
    message.nack.assert_not_awaited()
    if success_attempt is not None and outcomes[-1] == "succeeded":
        message.ack.assert_awaited_once()
        message.reject.assert_not_awaited()
    else:
        message.ack.assert_not_awaited()
        message.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_webhook_retry_cancellation_leaves_message_unacknowledged(pipeline):
    import asyncio

    from httpx import ConnectError

    pipeline.client.post.side_effect = ConnectError("webhook unavailable")
    pipeline.sleep.side_effect = asyncio.CancelledError
    message = make_message(pipeline)
    with pytest.raises(asyncio.CancelledError):
        await pipeline.consumer.consume(pipeline.data, message, pipeline.client)
    pipeline.client.post.assert_awaited_once()
    pipeline.publish.assert_not_awaited()
    message.ack.assert_not_awaited()
    message.reject.assert_not_awaited()
    message.nack.assert_not_awaited()


@pytest.mark.asyncio
async def test_retry_publish_error_requeues_original_message(pipeline):
    pipeline.processor.side_effect = TimeoutError("processor unavailable")
    pipeline.publish.side_effect = ConnectionError("broker unavailable")
    message = await deliver(pipeline)
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()
    message.reject.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("header", [True, -1, "2", None])
async def test_invalid_attempt_header_starts_from_first_attempt(pipeline, header):
    pipeline.processor.side_effect = TimeoutError("processor unavailable")
    message = await deliver(
        pipeline, {pipeline.consumer.PROCESSING_FAILURES_HEADER: header}
    )
    message.ack.assert_awaited_once()
    message.reject.assert_not_awaited()
    assert (
        pipeline.publish.await_args.kwargs["headers"][
            pipeline.consumer.PROCESSING_FAILURES_HEADER
        ]
        == 1
    )
