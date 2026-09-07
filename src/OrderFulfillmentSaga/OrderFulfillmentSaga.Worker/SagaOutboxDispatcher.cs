using MassTransit;
using Microsoft.EntityFrameworkCore;
using OrderFulfillmentSaga.Contracts;
using System.Text.Json;

namespace OrderFulfillmentSaga.Worker;

public sealed class SagaOutboxDispatcher(
    IServiceScopeFactory scopeFactory,
    ILogger<SagaOutboxDispatcher> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<SagaDbContext>();
                var producer = scope.ServiceProvider.GetRequiredService<ITopicProducer<string, OrderSubmittedEvent>>();
                var message = await db.OutboxMessages
                    .Where(x => x.Status == "Pending")
                    .OrderBy(x => x.CreatedAt)
                    .FirstOrDefaultAsync(stoppingToken);

                if (message is not null)
                {
                    var submitted = JsonSerializer.Deserialize<OrderSubmittedEvent>(message.Payload)
                        ?? throw new InvalidOperationException("Invalid outbox payload");
                    await producer.Produce(message.OrderId.ToString(), submitted, stoppingToken);
                    message.Status = "Published";
                    message.PublishedAt = DateTime.UtcNow;
                    await db.SaveChangesAsync(stoppingToken);
                    logger.LogInformation("SagaOutboxPublished {OrderId} {MessageId}", message.OrderId, message.Id);
                }
                else
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "SagaOutboxDispatchFailed");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }
    }
}