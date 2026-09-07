namespace OrderFulfillmentSaga.Contracts;

public sealed record OrderSubmittedEvent(
    Guid CorrelationId,
    Guid OrderId,
    Guid CustomerId,
    decimal Amount,
    DateTime CreatedAt);

public sealed record PaymentCompletedEvent(Guid CorrelationId, Guid OrderId, DateTime CompletedAt);
public sealed record PaymentFailedEvent(Guid CorrelationId, Guid OrderId, string Reason, DateTime FailedAt);
public sealed record InventoryReservedEvent(Guid CorrelationId, Guid OrderId, DateTime ReservedAt);
public sealed record InventoryFailedEvent(Guid CorrelationId, Guid OrderId, string Reason, DateTime FailedAt);
public sealed record PaymentRefundedEvent(Guid CorrelationId, Guid OrderId, DateTime RefundedAt);
public sealed record OrderStatusChangedEvent(Guid CorrelationId, Guid OrderId, string Status, string Reason, DateTime ChangedAt);
public sealed record PaymentEvent(Guid CorrelationId, Guid OrderId, string Type, string? Reason, DateTime OccurredAt);
public sealed record InventoryEvent(Guid CorrelationId, Guid OrderId, string Type, string? Reason, DateTime OccurredAt);
