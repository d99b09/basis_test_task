import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from queue import Empty
from uuid import UUID

import pytest
from sqlalchemy import String, cast, distinct, func, select

from .live_stack import live_payment_stack


async def database_metrics(stack):
    from app.models import OutboxEvent, Payment
    from app.models.enums import PaymentStatus

    scope = Payment.metadata_["test_run_id"].astext == stack.run_id
    async with stack.sessions() as session:
        payments = (
            (
                await session.execute(
                    select(
                        func.count().label("created_payments"),
                        func.count()
                        .filter(Payment.status == PaymentStatus.PENDING)
                        .label("pending_payments"),
                        func.count()
                        .filter(Payment.status == PaymentStatus.SUCCEEDED)
                        .label("succeeded_payments"),
                        func.count()
                        .filter(Payment.status == PaymentStatus.FAILED)
                        .label("failed_payments"),
                        func.count()
                        .filter(Payment.processed_at.is_not(None))
                        .label("processed_payments"),
                    )
                    .select_from(Payment)
                    .where(scope)
                )
            )
            .mappings()
            .one()
        )
        events = (
            (
                await session.execute(
                    select(
                        func.count().label("outbox_events"),
                        func.count(distinct(OutboxEvent.id)).label(
                            "unique_outbox_events"
                        ),
                        func.count(
                            distinct(OutboxEvent.payload["payment_id"].astext)
                        ).label("unique_outbox_payment_ids"),
                        func.count()
                        .filter(OutboxEvent.published_at.is_not(None))
                        .label("published_outbox_events"),
                    )
                    .select_from(OutboxEvent)
                    .join(
                        Payment,
                        OutboxEvent.payload["payment_id"].astext
                        == cast(Payment.id, String),
                    )
                    .where(scope)
                )
            )
            .mappings()
            .one()
        )
    return dict(payments) | dict(events)


def receive_webhooks(stack, notifications):
    attempts = 0
    while True:
        try:
            path, body = stack.webhooks.get_nowait()
        except Empty:
            return attempts
        assert path == "/webhook"
        payload = json.loads(body)
        payment_id = UUID(payload["payment_id"])
        if payment_id in notifications:
            assert notifications[payment_id] == payload, (
                "Conflicting webhook deliveries"
            )
        notifications[payment_id] = payload
        attempts += 1


async def inspect_dead_letters(stack, expected_ids):
    _, dlq = await stack.get_queues()
    held = []
    ids = set()
    try:
        for _ in range(dlq.declaration_result.message_count):
            message = await dlq.get(no_ack=False, timeout=5)
            held.append(message)
            payment_id = UUID(json.loads(message.body)["payment_id"])
            assert payment_id in expected_ids, "Foreign message in payments.dlq"
            assert message.message_id == str(payment_id)
            assert message.headers["x-processing-failures"] == 2
            assert any(
                death["reason"] == "rejected" and death["queue"] == "payments.new"
                for death in message.headers["x-death"]
            )
            ids.add(payment_id)
        # Acknowledge only this test's messages, after validating the whole batch.
        for message in held:
            await message.ack()
    finally:
        for message in held:
            if not message.processed:
                await message.nack(requeue=True)
    return ids


@pytest.mark.asyncio
async def test_payment_load_without_lost_payments(
    payment_load_control, request, pause_before_cleanup
):
    from app.models.enums import PaymentStatus

    count = request.config.getoption("--load-payment-count")
    concurrency = request.config.getoption("--load-concurrency")
    timeout = request.config.getoption("--load-timeout")
    poll = request.config.getoption("--load-poll-seconds")
    report_path = Path(request.config.getoption("--load-report"))
    assert 1 <= count <= 10_000
    assert 1 <= concurrency <= 100
    if timeout is None:
        timeout = count * 7 + 120
    assert timeout > 0 and poll > 0
    control = payment_load_control
    clock = asyncio.get_running_loop().time
    started = clock()
    report = {
        "status": "running",
        "started_at": datetime.now(UTC).isoformat(),
        "requested_payments": count,
        "concurrency": concurrency,
        "timeout_seconds": timeout,
        "metrics": {},
    }

    def save_report():
        report["elapsed_seconds"] = round(clock() - started, 2)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2) + "\n")

    notifications = {}
    webhook_attempts = 0
    creator = None
    try:
        await control.assert_running(
            "api", "postgres", "rabbitmq", "outbox_publisher", "consumer"
        )
        queues = await control.wait_for_consumer()
        assert queues["payments.new"]["messages_unacknowledged"] == 0, (
            "Consumer must be idle"
        )
        async with live_payment_stack(
            database_url=control.database_url,
            rabbitmq_url=control.rabbitmq_url,
            api_url=control.api_url,
            api_key=os.environ["API_KEY"],
            webhook_host=control.webhook_host,
            require_stopped_consumer=False,
            pause_before_cleanup=pause_before_cleanup,
        ) as stack:
            # Quiesce before deleting this run's rows/messages, even after failure.
            # The control fixture restores the original container state afterward.
            stack.exit_stack.push_async_callback(control.stop)
            report["run_id"] = stack.run_id
            save_report()
            creator = asyncio.create_task(
                stack.create_payments(count, "payment-load", concurrency=concurrency)
            )
            try:
                next_progress = started
                idle_since = None
                async with asyncio.timeout(timeout):
                    while True:
                        if creator.done():
                            await creator  # Surface any non-202 response immediately.
                        metrics = await database_metrics(stack)
                        main, dlq = await stack.get_queues()
                        assert dlq.declaration_result.consumer_count == 0
                        webhook_attempts += receive_webhooks(stack, notifications)
                        metrics.update(
                            accepted_payments=len(stack.payment_ids),
                            ready_messages=main.declaration_result.message_count,
                            consumers=main.declaration_result.consumer_count,
                            dlq_messages=dlq.declaration_result.message_count,
                            unique_webhook_deliveries=len(notifications),
                            webhook_attempts=webhook_attempts,
                            duplicate_webhook_deliveries=webhook_attempts
                            - len(notifications),
                        )
                        report["metrics"] = metrics
                        save_report()
                        if clock() >= next_progress:
                            print(
                                json.dumps(
                                    {
                                        "elapsed_seconds": report["elapsed_seconds"],
                                        **metrics,
                                    }
                                ),
                                flush=True,
                            )
                            next_progress = clock() + 30
                        if (
                            creator.done()
                            and metrics["published_outbox_events"] == count
                            and metrics["ready_messages"] == 0
                        ):
                            queues = await control.queue_counts()
                            main_counts = queues["payments.new"]
                            metrics["unacknowledged_messages"] = main_counts[
                                "messages_unacknowledged"
                            ]
                            if (
                                main_counts["messages_ready"]
                                == main_counts["messages_unacknowledged"]
                                == 0
                            ):
                                if (
                                    len(notifications) + metrics["dlq_messages"]
                                    >= count
                                ):
                                    break
                                # Allow asynchronous dead lettering/HTTP recording.
                                if idle_since is None:
                                    idle_since = clock()
                                assert clock() - idle_since < 30, (
                                    "Pipeline is idle but some payments have no webhook or DLQ message"
                                )
                            else:
                                idle_since = None
                        await asyncio.sleep(poll)

                await control.stop()
                final = await stack.database_snapshot()
                queues = await control.queue_counts()
                webhook_attempts += receive_webhooks(stack, notifications)
                expected_ids = set(await creator)
                dlq_ids = await inspect_dead_letters(stack, expected_ids)
                recorded_ids = set(notifications) | dlq_ids
                lost_ids = expected_ids - recorded_ids
                report["metrics"] = await database_metrics(stack) | {
                    "accepted_payments": len(expected_ids),
                    "unique_webhook_deliveries": len(notifications),
                    "webhook_attempts": webhook_attempts,
                    "duplicate_webhook_deliveries": webhook_attempts
                    - len(notifications),
                    "dlq_messages": queues["payments.dlq"]["messages_ready"],
                    "unique_dlq_payment_ids": len(dlq_ids),
                    "lost_payments": len(lost_ids),
                    "ready_messages": queues["payments.new"]["messages_ready"],
                    "unacknowledged_messages": queues["payments.new"][
                        "messages_unacknowledged"
                    ],
                }
                report["lost_payment_id_sample"] = sorted(map(str, lost_ids))[:20]
                save_report()
                assert len(expected_ids) == count
                assert len(final.payments) == count
                assert {payment.id for payment in final.payments} == expected_ids
                assert len(final.events) == count
                assert len({event.id for event in final.events}) == count
                assert {
                    UUID(event.payload["payment_id"]) for event in final.events
                } == expected_ids
                assert all(event.published_at is not None for event in final.events)
                assert not lost_ids, report
                assert not recorded_ids - expected_ids, (
                    "Unexpected webhook or DLQ payment_id"
                )
                assert queues["payments.new"]["messages_ready"] == 0
                assert queues["payments.new"]["messages_unacknowledged"] == 0
                for payment in final.payments:
                    assert payment.status in (
                        PaymentStatus.SUCCEEDED,
                        PaymentStatus.FAILED,
                    )
                    assert payment.processed_at is not None
                    assert payment.processed_at >= payment.created_at
                    if payment.id in notifications:
                        payload = notifications[payment.id]
                        assert payload["status"] == payment.status.value
                        assert (
                            datetime.fromisoformat(payload["processed_at"])
                            == payment.processed_at
                        )
            finally:
                if not creator.done():
                    creator.cancel()
                await asyncio.gather(creator, return_exceptions=True)
        report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"[:2000]
        raise
    finally:
        save_report()
        print(json.dumps(report, indent=2), flush=True)
