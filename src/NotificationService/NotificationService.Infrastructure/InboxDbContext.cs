using Microsoft.EntityFrameworkCore;

namespace NotificationService.Infrastructure;

public sealed class InboxMessage
{
    private InboxMessage() { }
    public InboxMessage(Guid eventId, string eventType) { Id = Guid.NewGuid(); EventId = eventId; EventType = eventType; ReceivedAtUtc = DateTime.UtcNow; }
    public Guid Id { get; private set; }
    public Guid EventId { get; private set; }
    public string EventType { get; private set; } = null!;
    public DateTime ReceivedAtUtc { get; private set; }
    public DateTime? ProcessedAtUtc { get; private set; }
    public void MarkProcessed() => ProcessedAtUtc = DateTime.UtcNow;
}

public sealed class InboxDbContext(DbContextOptions<InboxDbContext> options) : DbContext(options)
{
    public DbSet<InboxMessage> InboxMessages => Set<InboxMessage>();
    protected override void OnModelCreating(ModelBuilder modelBuilder) { modelBuilder.Entity<InboxMessage>(e => { e.ToTable("InboxMessages"); e.HasKey(x => x.Id); e.HasIndex(x => x.EventId).IsUnique(); }); }
}
