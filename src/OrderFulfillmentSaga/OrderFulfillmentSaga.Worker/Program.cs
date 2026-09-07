using Confluent.Kafka;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using OrderFulfillmentSaga.Contracts;
using OrderFulfillmentSaga.Worker;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
var kafka = builder.Configuration.GetSection("Kafka");
var topics = kafka.GetSection("Topics");
var bootstrapServers = kafka["BootstrapServers"] ?? "localhost:29092";
var connectionString = builder.Configuration.GetConnectionString("Saga") ?? throw new InvalidOperationException("ConnectionStrings:Saga is required");

builder.Services.AddDbContext<SagaDbContext>(options => options.UseSqlServer(connectionString));
builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<PaymentConsumer>();
    x.AddConsumer<InventoryConsumer>();
    x.AddConsumer<PaymentSagaConsumer>();
    x.AddConsumer<InventorySagaConsumer>();
    x.AddEntityFrameworkOutbox<SagaDbContext>(o =>
    {
        o.UseSqlServer();
        o.UseBusOutbox();
    });
    x.UsingInMemory((context, cfg) => cfg.ConfigureEndpoints(context));
    x.AddRider(rider =>
    {
        rider.AddProducer<string, OrderSubmittedEvent>(topics["Submitted"] ?? "orders.submitted");
        rider.AddProducer<string, PaymentEvent>(topics["Payments"] ?? "payments.events");
        rider.AddProducer<string, InventoryEvent>(topics["Inventory"] ?? "inventory.events");
        rider.AddConsumer<PaymentConsumer>();
        rider.AddConsumer<InventoryConsumer>();
        rider.AddConsumer<PaymentSagaConsumer>();
        rider.AddConsumer<InventorySagaConsumer>();
        rider.UsingKafka((context, cfg) =>
        {
            cfg.Host(bootstrapServers);
            var groups = kafka.GetSection("Groups");
            ConfigureEndpoint<OrderSubmittedEvent, PaymentConsumer>(cfg, context, topics["Submitted"] ?? "orders.submitted", groups["Payment"] ?? "payment-service-group");
            ConfigureEndpoint<PaymentEvent, InventoryConsumer>(cfg, context, topics["Payments"] ?? "payments.events", groups["Inventory"] ?? "inventory-service-group");
            ConfigureEndpoint<PaymentEvent, PaymentSagaConsumer>(cfg, context, topics["Payments"] ?? "payments.events", groups["Saga"] ?? "order-saga-group");
            ConfigureEndpoint<InventoryEvent, InventorySagaConsumer>(cfg, context, topics["Inventory"] ?? "inventory.events", groups["Saga"] ?? "order-saga-group");
        });
    });
});
builder.Services.AddHostedService<SagaOutboxDispatcher>();

var app = builder.Build();
await using (var scope = app.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<SagaDbContext>();
    await db.Database.EnsureCreatedAsync();
}

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));
app.MapPost("/orders", async (CreateOrderRequest request, SagaDbContext db, CancellationToken cancellationToken) =>
{
    var orderId = Guid.NewGuid();
    var correlationId = Guid.NewGuid();
    var createdAt = DateTime.UtcNow;
    db.Orders.Add(new OrderRecord { OrderId = orderId, CustomerId = request.CustomerId, Amount = request.Amount, CreatedAt = createdAt });
    db.SagaStates.Add(new OrderSagaState { CorrelationId = correlationId, OrderId = orderId, UpdatedAt = createdAt });
    db.OutboxMessages.Add(new SagaOutboxMessage
    {
        Id = Guid.NewGuid(),
        OrderId = orderId,
        EventType = nameof(OrderSubmittedEvent),
        Payload = JsonSerializer.Serialize(new OrderSubmittedEvent(correlationId, orderId, request.CustomerId, request.Amount, createdAt)),
        CreatedAt = createdAt
    });
    await db.SaveChangesAsync(cancellationToken);
    return Results.Accepted($"/orders/{orderId}", new { orderId, correlationId, status = "Pending" });
});
app.MapGet("/orders/{orderId:guid}", async (Guid orderId, SagaDbContext db, CancellationToken cancellationToken) =>
    await db.Orders.AsNoTracking().SingleOrDefaultAsync(x => x.OrderId == orderId, cancellationToken) is { } order ? Results.Ok(order) : Results.NotFound());

app.Run();

static void ConfigureEndpoint<TMessage, TConsumer>(IKafkaFactoryConfigurator cfg, IRiderRegistrationContext context, string topic, string group)
    where TMessage : class
    where TConsumer : class, IConsumer<TMessage>
{
    cfg.TopicEndpoint<TMessage>(topic, group, endpoint =>
    {
        endpoint.AutoOffsetReset = AutoOffsetReset.Earliest;
        endpoint.UseMessageRetry(retry => retry.Exponential(3, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2)));
        endpoint.ConfigureConsumer<TConsumer>(context);
    });
}

public sealed record CreateOrderRequest(Guid CustomerId, decimal Amount);
