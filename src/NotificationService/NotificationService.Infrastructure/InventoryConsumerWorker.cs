using Confluent.Kafka;
using KafkaOutboxSample.Common;
using KafkaOutboxSample.Contracts;
using KafkaOutboxSample.Messaging;
using Microsoft.Extensions.Hosting;
using System.Text.Json;

namespace NotificationService.Infrastructure;

public sealed class InventoryConsumerWorker(
    IOptions<KafkaOptions> kafka,
    ILogger<InventoryConsumerWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = kafka.Value.Inventory;
        var config = new ConsumerConfig
        {
            BootstrapServers = kafka.Value.BootstrapServers,
            GroupId = options.GroupId,
            AutoOffsetReset = ParseAutoOffsetReset(options.AutoOffsetReset),
            EnableAutoCommit = options.EnableAutoCommit
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(kafka.Value.Topics.OrderCreated);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);
                    var message = JsonSerializer.Deserialize<OrderCreatedIntegrationEvent>(result.Message.Value, JsonDefaults.Options)
                        ?? throw new InvalidOperationException("Invalid event payload");

                    await DeductStockAsync(message, stoppingToken);
                    consumer.Commit(result);
                    logger.LogInformation("InventoryProcessed {EventId} {OrderId}", message.EventId, message.OrderId);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    logger.LogError(exception, "InventoryMessageFailed");
                }
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private static Task DeductStockAsync(OrderCreatedIntegrationEvent message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    private static AutoOffsetReset ParseAutoOffsetReset(string value) =>
        Enum.TryParse<AutoOffsetReset>(value, true, out var result) ? result : AutoOffsetReset.Earliest;
}
