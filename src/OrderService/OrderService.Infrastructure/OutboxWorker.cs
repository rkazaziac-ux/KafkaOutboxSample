using KafkaOutboxSample.Common;
using KafkaOutboxSample.Messaging;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace OrderService.Infrastructure;

public sealed class OutboxWorker(IServiceScopeFactory scopeFactory, IOptions<OutboxOptions> options, IOptions<KafkaOptions> kafka, ILogger<OutboxWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try { await ProcessBatchAsync(stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception exception) { logger.LogError(exception, "Outbox batch failed"); }
            await Task.Delay(TimeSpan.FromSeconds(options.Value.PollingIntervalSeconds), stoppingToken);
        }
    }

    private async Task ProcessBatchAsync(CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
        var producer = scope.ServiceProvider.GetRequiredService<IKafkaProducer>();
        var cutoff = DateTime.UtcNow.AddMinutes(-options.Value.ProcessingTimeoutMinutes);
        await db.OutboxMessages.Where(x => x.Status == OutboxStatus.Processing && x.ProcessedOnUtc == null && x.OccurredOnUtc < cutoff).ExecuteUpdateAsync(x => x.SetProperty(m => m.Status, OutboxStatus.Pending), cancellationToken);
        var messages = await db.OutboxMessages.FromSqlInterpolated($"SELECT TOP ({options.Value.BatchSize}) * FROM OutboxMessages WITH (UPDLOCK, READPAST, ROWLOCK) WHERE Status IN ('Pending','Failed') AND RetryCount < {options.Value.MaxRetryCount} ORDER BY OccurredOnUtc").ToListAsync(cancellationToken);
        foreach (var message in messages)
        {
            message.Claim(); await db.SaveChangesAsync(cancellationToken); logger.LogInformation("OutboxMessageClaimed {EventId}", message.Id);
            try { await producer.ProduceAsync(kafka.Value.Topics.OrderCreated, message.Id, message.Payload, cancellationToken); message.Published(); logger.LogInformation("OutboxMessagePublished {EventId}", message.Id); }
            catch (Exception exception) { message.Failed(exception.Message); logger.LogError(exception, "KafkaPublishFailed {EventId}", message.Id); }
            await db.SaveChangesAsync(cancellationToken);
        }
    }
}
