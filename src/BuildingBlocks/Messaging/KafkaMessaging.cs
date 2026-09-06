using Confluent.Kafka;
using KafkaOutboxSample.Common;
using System.Text.Json;

namespace KafkaOutboxSample.Messaging;

public interface IKafkaProducer
{
    Task ProduceAsync(string topic, Guid eventId, string payload, CancellationToken cancellationToken);
}

public sealed class KafkaProducer(IOptions<KafkaOptions> options, ILogger<KafkaProducer> logger) : IKafkaProducer, IDisposable
{
    private readonly IProducer<string, string> producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = options.Value.BootstrapServers, EnableIdempotence = false }).Build();
    public async Task ProduceAsync(string topic, Guid eventId, string payload, CancellationToken cancellationToken)
    {
        logger.LogInformation("KafkaPublishStarted {EventId} {Topic}", eventId, topic);
        var result = await producer.ProduceAsync(topic, new Message<string, string> { Key = eventId.ToString(), Value = payload, Headers = new Headers { { Correlation.EventIdHeader, System.Text.Encoding.UTF8.GetBytes(eventId.ToString()) } } }, cancellationToken);
        logger.LogInformation("KafkaPublishSucceeded {EventId} {Partition} {Offset}", eventId, result.Partition, result.Offset);
    }
    public void Dispose() => producer.Dispose();
}

public static class JsonDefaults
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
