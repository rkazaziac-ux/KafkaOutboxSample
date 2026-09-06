using KafkaOutboxSample.Contracts;
using Microsoft.Extensions.Logging;

namespace NotificationService.Application;

public interface INotificationHandler
{
    Task HandleAsync(OrderCreatedIntegrationEvent message, CancellationToken cancellationToken);
}

public sealed class NotificationHandler(ILogger<NotificationHandler> logger) : INotificationHandler
{
    public Task HandleAsync(OrderCreatedIntegrationEvent message, CancellationToken cancellationToken)
    {
        logger.LogInformation("MessageProcessed {EventId} {OrderId}", message.EventId, message.OrderId);
        return Task.CompletedTask;
    }
}
