namespace KafkaOutboxSample.Contracts;

/// <summary>Base contract for integration events.</summary>
public interface IIntegrationEvent
{
    Guid EventId { get; }
    DateTime OccurredOnUtc { get; }
}

/// <summary>Raised when an order is created.</summary>
public sealed record OrderCreatedIntegrationEvent(
    Guid EventId,
    Guid OrderId,
    Guid CustomerId,
    decimal Amount,
    DateTime OccurredOnUtc) : IIntegrationEvent;

/// <summary>Payload sent to the dead-letter topic.</summary>
public sealed record DeadLetterMessage(
    string OriginalEvent,
    string OriginalTopic,
    Guid EventId,
    string ErrorMessage,
    DateTime FailedAtUtc,
    int RetryCount);
