using KafkaOutboxSample.Contracts;
using Microsoft.EntityFrameworkCore;
using OrderService.Application;
using OrderService.Domain;
using System.Text.Json;

namespace OrderService.Infrastructure;

public enum OutboxStatus { Pending, Processing, Published, Failed }
public sealed class OutboxMessage
{
    private OutboxMessage() { }
    public OutboxMessage(Guid id, string eventType, string payload, DateTime occurredOnUtc) { Id = id; EventType = eventType; Payload = payload; OccurredOnUtc = occurredOnUtc; Status = OutboxStatus.Pending; }
    public Guid Id { get; private set; }
    public string EventType { get; private set; } = null!;
    public string Payload { get; private set; } = null!;
    public DateTime OccurredOnUtc { get; private set; }
    public DateTime? ProcessedOnUtc { get; private set; }
    public int RetryCount { get; private set; }
    public string? Error { get; private set; }
    public OutboxStatus Status { get; private set; }
    public void Claim() => Status = OutboxStatus.Processing;
    public void Published() { Status = OutboxStatus.Published; ProcessedOnUtc = DateTime.UtcNow; Error = null; }
    public void Failed(string error) { RetryCount++; Error = error[..Math.Min(error.Length, 2000)]; Status = OutboxStatus.Failed; }
    public void Requeue() => Status = OutboxStatus.Pending;
}

public sealed class OrderDbContext(DbContextOptions<OrderDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity => { entity.ToTable("Orders"); entity.HasKey(x => x.Id); entity.Property(x => x.Amount).HasPrecision(18, 2); entity.Property(x => x.Status).HasConversion<string>(); });
        modelBuilder.Entity<OutboxMessage>(entity => { entity.ToTable("OutboxMessages"); entity.HasKey(x => x.Id); entity.HasIndex(x => new { x.Status, x.OccurredOnUtc }); entity.Property(x => x.Status).HasConversion<string>(); entity.Property(x => x.Payload).HasColumnType("nvarchar(max)"); });
    }
}

public sealed class OrderRepository(OrderDbContext db) : IOrderRepository
{
    public Task<Order?> GetAsync(Guid id, CancellationToken cancellationToken) => db.Orders.AsNoTracking().SingleOrDefaultAsync(x => x.Id == id, cancellationToken);
    public async Task AddAsync(Order order, CancellationToken cancellationToken) => await db.Orders.AddAsync(order, cancellationToken);
    public async Task AddOutboxAsync(OrderCreatedIntegrationEvent integrationEvent, CancellationToken cancellationToken) => await db.OutboxMessages.AddAsync(new OutboxMessage(integrationEvent.EventId, nameof(OrderCreatedIntegrationEvent), JsonSerializer.Serialize(integrationEvent), integrationEvent.OccurredOnUtc), cancellationToken);
    public async Task SaveChangesAsync(CancellationToken cancellationToken) => await db.SaveChangesAsync(cancellationToken);
}
