using Microsoft.EntityFrameworkCore;
using MassTransit;

namespace OrderFulfillmentSaga.Worker;

public sealed class SagaDbContext(DbContextOptions<SagaDbContext> options) : DbContext(options)
{
    public DbSet<OrderRecord> Orders => Set<OrderRecord>();
    public DbSet<OrderSagaState> SagaStates => Set<OrderSagaState>();
    public DbSet<SagaOutboxMessage> OutboxMessages => Set<SagaOutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.AddInboxStateEntity();
        modelBuilder.AddOutboxMessageEntity();
        modelBuilder.AddOutboxStateEntity();
        modelBuilder.Entity<OrderRecord>(entity =>
        {
            entity.HasKey(x => x.OrderId);
            entity.Property(x => x.Amount).HasPrecision(18, 2);
            entity.Property(x => x.Status).HasMaxLength(32);
            entity.Property(x => x.Reason).HasMaxLength(500);
        });
        modelBuilder.Entity<OrderSagaState>(entity =>
        {
            entity.HasKey(x => x.CorrelationId);
            entity.HasIndex(x => x.OrderId).IsUnique();
            entity.Property(x => x.Status).HasMaxLength(32);
            entity.Property(x => x.Reason).HasMaxLength(500);
        });
        modelBuilder.Entity<SagaOutboxMessage>(entity =>
        {
            entity.HasKey(x => x.Id);
            entity.HasIndex(x => new { x.Status, x.CreatedAt });
            entity.Property(x => x.Payload).HasColumnType("nvarchar(max)");
            entity.Property(x => x.Status).HasMaxLength(16);
        });
    }
}

public sealed class SagaOutboxMessage
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public string EventType { get; set; } = null!;
    public string Payload { get; set; } = null!;
    public string Status { get; set; } = "Pending";
    public DateTime CreatedAt { get; set; }
    public DateTime? PublishedAt { get; set; }
}

public sealed class OrderRecord
{
    public Guid OrderId { get; set; }
    public Guid CustomerId { get; set; }
    public decimal Amount { get; set; }
    public string Status { get; set; } = "Pending";
    public string? Reason { get; set; }
    public DateTime CreatedAt { get; set; }
}

public sealed class OrderSagaState
{
    public Guid CorrelationId { get; set; }
    public Guid OrderId { get; set; }
    public string Status { get; set; } = "Pending";
    public string? Reason { get; set; }
    public DateTime UpdatedAt { get; set; }
}
