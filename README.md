# Тестовое задание ООО МКК Базис

### Описание проекта

Сервис принимает платежи через HTTP API, имитирует их обработку и позволяет получить текущий статус.
Обработка платежей выполняется асинхронно: платёж и событие Outbox сохраняются в одной транзакции, после чего отдельный воркер публикует событие в RabbitMQ.
Обработчик (consumer) получает событие из очереди, сохраняет результат и отправляет его на указанный webhook.
При технических ошибках и неуспешном результате обработки выполняется до 3 попыток всего с задержками 1 и 2 секунды. До исчерпания попыток платёж остаётся в `pending`. При успехе сохраняется `succeeded`; после третьей ошибки сохраняется `failed`, отправляется webhook и сообщение попадает в DLQ.

### Технологический стек

Python 3.13, FastAPI, Pydantic, SQLAlchemy (asyncio), asyncpg, Alembic, PostgreSQL 18, RabbitMQ 4.2, FastStream, HTTPX, Docker Compose.

### Быстрый запуск

```bash
# Основной сервис и инфраструктура
docker compose up -d --build

# Тестовый webhook-сервис
docker compose -f docker-compose.yml -f docker-compose.webhook.yml up -d --build webhook-receiver
```

### Архитектура

```text
API -> PostgreSQL (Payment + Outbox) -> Publisher -> RabbitMQ -> Consumer -> Webhook

Ошибка обработки -> Retry (до 3 попыток всего) -> failed + DLQ
```

Payment и Outbox сохраняются в одной транзакции.
Publisher обеспечивает доставку событий в RabbitMQ с гарантией at-least-once.
Consumer обрабатывает до 10 платежей одновременно и отправляет webhook; остальные сообщения ждут в RabbitMQ.
Retry и DLQ используются при технических ошибках, неуспешном результате обработки или ошибках доставки webhook. Повторная доставка webhook сохраняет итоговый статус платежа.

### API — примеры использования

Подставьте `API_KEY` из `.env` и `payment_id` из ответа на создание платежа.

Создание платежа:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: <API_KEY>' \
  -H 'Idempotency-Key: payment-001' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount": "100.00",
    "currency": "RUB",
    "description": "Оплата заказа №1",
    "webhook_url": "http://webhook-receiver:8080/webhook"
  }'
```

Получение статуса платежа:

```bash
curl 'http://localhost:8000/api/v1/payments/<PAYMENT_ID>' \
  -H 'X-API-Key: <API_KEY>'
```

### Обработка ошибок и гарантии

| Ситуация | Поведение и гарантия |
| --- | --- |
| Повторный `Idempotency-Key` | Те же данные возвращают существующий платёж; другие данные — HTTP 409. |
| Ошибка доступа к PostgreSQL | API возвращает HTTP 503; Payment и Outbox сохраняются атомарно. |
| RabbitMQ недоступен | Событие остаётся в Outbox до успешной публикации; доставка at-least-once допускает дубликаты. |
| Повторная доставка события | Платёж с итоговым статусом не обрабатывается заново; webhook может прийти повторно. |
| Техническая ошибка или неуспешный результат обработки | До 3 попыток всего с задержками 1 и 2 секунды. После третьей ошибки сохраняется `failed`, отправляется webhook и сообщение попадает в `payments.dlq`. |
| Ошибка сохранения итогового `failed` | Сообщение возвращается в очередь: отправка в DLQ выполняется только после сохранения статуса. |
| Ошибка доставки webhook | До 3 доставок сообщения всего; затем оно попадает в `payments.dlq`. Итоговый статус платежа не меняется, обработка платежа не повторяется. |
| Платёж получил статус `failed` | Это итоговый результат после исчерпания попыток: повторная обработка не выполняется, отправляется webhook и сообщение попадает в DLQ. |

### Ограничения реализации

Обработка платежей имитируется случайной задержкой и выбором результата, без интеграции с платёжным провайдером.
Webhook может доставляться повторно, поэтому получатель должен учитывать дубликаты по `payment_id`.
Автоматический возврат сообщений из DLQ не реализован: после устранения причины ошибки их нужно отправить на повторную обработку вручную.

### Запуск интеграционных тестов через uv

Подготовка: Python 3.13+, uv, Docker Compose. В `.env.test` укажите `DATABASE_URL` и `RABBITMQ_URL` с хостом `localhost`, `API_KEY` — из `.env`. Команды выполняются на Docker-хосте, из корня проекта.

```bash
uv sync --dev # зависимости
docker compose up -d --build --wait # сервисы и инфраструктура
set -a; source .env.test; set +a # переменные тестового окружения
```

```bash
uv run task integration # все обычные интеграционные тесты
uv run task integration-payment # платежи, webhook и лимит параллельной обработки
uv run task integration-payment-low-success # 10% успеха, повторы и DLQ
uv run task integration-webhook-unavailable # недоступный webhook и DLQ
docker compose stop consumer # подготовка сценария без consumer
uv run task integration-consumer-unavailable # платежи остаются pending
docker compose start consumer # восстановление consumer
uv run task integration-consumer-recovery # восстановление consumer вручную по подсказке
uv run task integration-rabbitmq-unavailable # платежи сохраняются при отказе RabbitMQ
uv run task integration-rabbitmq-recovery # восстановление RabbitMQ вручную по подсказке
uv run task integration-load # нагрузка: 10 000 платежей
uv run task integration-payment-pause # пауза перед очисткой; продолжить — Enter
```
