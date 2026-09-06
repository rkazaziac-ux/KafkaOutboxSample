namespace KafkaOutboxSample.Common;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:29092";
    public KafkaTopics Topics { get; set; } = new();
    public KafkaConsumerOptions Notification { get; set; } = new() { GroupId = "notification-service" };
    public KafkaConsumerOptions Inventory { get; set; } = new() { GroupId = "inventory-service" };
}

public sealed class KafkaConsumerOptions
{
    public string GroupId { get; set; } = string.Empty;
    public string AutoOffsetReset { get; set; } = "Earliest";
    public bool EnableAutoCommit { get; set; }
}

public sealed class KafkaTopics
{
    public string OrderCreated { get; set; } = "orders.created";
    public string OrderCreatedRetry { get; set; } = "orders.created.retry";
    public string OrderCreatedDlq { get; set; } = "orders.created.dlq";
}

public sealed class OutboxOptions
{
    public int BatchSize { get; set; } = 100;
    public int PollingIntervalSeconds { get; set; } = 5;
    public int ProcessingTimeoutMinutes { get; set; } = 5;
    public int MaxRetryCount { get; set; } = 5;
}

public static class Correlation
{
    public const string Header = "x-correlation-id";
    public const string EventIdHeader = "x-event-id";
}
