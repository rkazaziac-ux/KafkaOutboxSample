using MassTransit;
using Microsoft.EntityFrameworkCore;
using OrderFulfillmentSaga.Contracts;

namespace OrderFulfillmentSaga.Worker;

public sealed class PaymentConsumer(ITopicProducer<string, PaymentEvent> producer, ILogger<PaymentConsumer> logger) : IConsumer<OrderSubmittedEvent>
{
    public async Task Consume(ConsumeContext<OrderSubmittedEvent> context)
    {
        var message = context.Message;
        var payment = message.Amount <= 1000m
            ? new PaymentEvent(message.CorrelationId, message.OrderId, "PaymentCompleted", null, DateTime.UtcNow)
            : new PaymentEvent(message.CorrelationId, message.OrderId, "PaymentFailed", "Amount exceeds payment limit", DateTime.UtcNow);
        await producer.Produce(message.OrderId.ToString(), payment, context.CancellationToken);
        logger.LogInformation("PaymentDecision {OrderId} {Type}", message.OrderId, payment.Type);
    }
}

public sealed class InventoryConsumer(ITopicProducer<string, InventoryEvent> producer, ILogger<InventoryConsumer> logger) : IConsumer<PaymentEvent>
{
    public async Task Consume(ConsumeContext<PaymentEvent> context)
    {
        var message = context.Message;
        if (message.Type != "PaymentCompleted") return;
        var inventory = message.OrderId != Guid.Empty
            ? new InventoryEvent(message.CorrelationId, message.OrderId, "InventoryReserved", null, DateTime.UtcNow)
            : new InventoryEvent(message.CorrelationId, message.OrderId, "InventoryFailed", "Stock unavailable", DateTime.UtcNow);
        await producer.Produce(message.OrderId.ToString(), inventory, context.CancellationToken);
        logger.LogInformation("InventoryDecision {OrderId} {Type}", message.OrderId, inventory.Type);
    }
}

public sealed class PaymentSagaConsumer(SagaDbContext db, ILogger<PaymentSagaConsumer> logger) : IConsumer<PaymentEvent>
{
    public async Task Consume(ConsumeContext<PaymentEvent> context)
    {
        if (context.Message.Type != "PaymentFailed") return;
        await UpdateOrderAsync(context.Message.CorrelationId, context.Message.OrderId, "Cancelled", context.Message.Reason ?? "Payment failed");
        logger.LogWarning("OrderCancelled {OrderId} {Reason}", context.Message.OrderId, context.Message.Reason);
    }

    private async Task UpdateOrderAsync(Guid correlationId, Guid orderId, string status, string reason)
    {
        var state = await db.SagaStates.SingleOrDefaultAsync(x => x.CorrelationId == correlationId) ?? new OrderSagaState { CorrelationId = correlationId, OrderId = orderId };
        state.Status = status; state.Reason = reason; state.UpdatedAt = DateTime.UtcNow; db.SagaStates.Update(state);
        var order = await db.Orders.FindAsync(orderId);
        if (order is not null) { order.Status = status; order.Reason = reason; }
        await db.SaveChangesAsync();
    }
}

public sealed class InventorySagaConsumer(SagaDbContext db, ITopicProducer<string, PaymentEvent> paymentProducer, ILogger<InventorySagaConsumer> logger) : IConsumer<InventoryEvent>
{
    public async Task Consume(ConsumeContext<InventoryEvent> context)
    {
        var message = context.Message;
        var status = message.Type == "InventoryReserved" ? "Completed" : "Cancelled";
        var state = await db.SagaStates.SingleOrDefaultAsync(x => x.CorrelationId == message.CorrelationId) ?? new OrderSagaState { CorrelationId = message.CorrelationId, OrderId = message.OrderId };
        state.Status = status; state.Reason = message.Reason; state.UpdatedAt = DateTime.UtcNow; db.SagaStates.Update(state);
        var order = await db.Orders.FindAsync(message.OrderId);
        if (order is not null) { order.Status = status; order.Reason = message.Reason; }
        await db.SaveChangesAsync();
        if (message.Type == "InventoryFailed")
            await paymentProducer.Produce(message.OrderId.ToString(), new PaymentEvent(message.CorrelationId, message.OrderId, "PaymentRefunded", null, DateTime.UtcNow), context.CancellationToken);
        logger.LogInformation("OrderSagaUpdated {OrderId} {Status}", message.OrderId, status);
    }
}

