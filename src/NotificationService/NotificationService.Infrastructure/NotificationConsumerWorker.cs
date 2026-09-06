using Confluent.Kafka;
using KafkaOutboxSample.Common;
using KafkaOutboxSample.Contracts;
using KafkaOutboxSample.Messaging;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using NotificationService.Application;
using System.Text;
using System.Text.Json;

namespace NotificationService.Infrastructure;

public sealed class NotificationConsumerWorker(IServiceScopeFactory scopeFactory, IOptions<KafkaOptions> kafka, IOptions<OutboxOptions> retryOptions, ILogger<NotificationConsumerWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = kafka.Value.Notification;
        var config = new ConsumerConfig { BootstrapServers = kafka.Value.BootstrapServers, GroupId = options.GroupId, AutoOffsetReset = ParseAutoOffsetReset(options.AutoOffsetReset), EnableAutoCommit = options.EnableAutoCommit };
        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = kafka.Value.BootstrapServers }).Build();
        consumer.Subscribe(kafka.Value.Topics.OrderCreated);
        await Task.Yield();
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                await ProcessWithRetryAsync(result.Message.Value, result.Topic, producer, stoppingToken);
                consumer.Commit(result);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception exception) { logger.LogError(exception, "ConsumerMessageFailed"); }
        }
        consumer.Close();
    }

    private static AutoOffsetReset ParseAutoOffsetReset(string value) => Enum.TryParse<AutoOffsetReset>(value, true, out var result) ? result : AutoOffsetReset.Earliest;

    private async Task ProcessWithRetryAsync(string payload, string topic, IProducer<string, string> producer, CancellationToken cancellationToken)
    {
        var message = JsonSerializer.Deserialize<OrderCreatedIntegrationEvent>(payload, JsonDefaults.Options) ?? throw new InvalidOperationException("Invalid event payload");
        var delays = new[] { 5, 15, 30, 60 };
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                using var scope = scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<InboxDbContext>();
                var handler = scope.ServiceProvider.GetRequiredService<INotificationHandler>();
                await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
                if (await db.InboxMessages.AnyAsync(x => x.EventId == message.EventId, cancellationToken)) { logger.LogInformation("InboxDuplicateDetected {EventId}", message.EventId); return; }
                var inbox = new InboxMessage(message.EventId, nameof(OrderCreatedIntegrationEvent));
                db.InboxMessages.Add(inbox);
                await handler.HandleAsync(message, cancellationToken);
                inbox.MarkProcessed();
                await db.SaveChangesAsync(cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return;
            }
            catch (Exception exception) when (attempt < Math.Min(delays.Length, retryOptions.Value.MaxRetryCount))
            {
                logger.LogWarning(exception, "ConsumerRetry {EventId} {Attempt}", message.EventId, attempt + 1);
                await Task.Delay(TimeSpan.FromSeconds(delays[attempt]), cancellationToken);
            }
            catch (Exception exception)
            {
                var dlq = new DeadLetterMessage(payload, topic, message.EventId, exception.Message, DateTime.UtcNow, attempt + 1);
                await producer.ProduceAsync(kafka.Value.Topics.OrderCreatedDlq, new Message<string, string>
                {
                    Key = message.EventId.ToString(),
                    Value = JsonSerializer.Serialize(dlq, JsonDefaults.Options)
                }, cancellationToken);
                logger.LogError(exception, "MessageSentToDLQ {EventId}", message.EventId);
                return;
            }
        }
    }
}
