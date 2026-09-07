# Order Fulfillment Saga

This sample adds an event-driven order fulfillment workflow using .NET 8, MassTransit Kafka Rider, SQL Server, and an EF Core transactional outbox.

## Flow

```text
POST /orders
  -> SQL transaction: Order + SagaState + OutboxMessage
  -> SagaOutboxDispatcher -> orders.submitted (OrderId partition key)
  -> payment-service-group
       amount <= 1000 -> payments.events / PaymentCompletedEvent
       amount > 1000  -> payments.events / PaymentFailedEvent -> Cancelled
  -> inventory-service-group
       stock available -> inventory.events / InventoryReservedEvent -> Completed
       stock unavailable -> inventory.events / InventoryFailedEvent -> Cancelled + PaymentRefundedEvent
```

Kafka Rider endpoints use manual acknowledgement semantics: MassTransit acknowledges and commits the Kafka offset only after the consumer completes successfully. Each topic endpoint has exponential retry with three attempts; failed messages are handled by the Kafka endpoint error/dead-letter mechanism.

`orders.submitted`, `payments.events`, and `inventory.events` are initialized with three partitions. Every event producer sets the Kafka partition key to `OrderId`, preserving per-order ordering while allowing unrelated orders to process concurrently.

## Run

```bash
docker compose up -d --build order-fulfillment-saga
curl -X POST http://localhost:8090/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"11111111-1111-1111-1111-111111111111","amount":250.00}'
```

Amounts above `1000` exercise payment compensation. The sample inventory decision is deterministic and reserves stock for non-empty order IDs; replace `InventoryConsumer` with the real inventory reservation transaction in production.
