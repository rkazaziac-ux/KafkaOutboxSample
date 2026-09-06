namespace OrderService.Domain;

public enum OrderStatus { Created, Paid, Cancelled }

/// <summary>Order aggregate.</summary>
public sealed class Order
{
    private Order() { }
    public Order(Guid customerId, decimal amount)
    {
        if (customerId == Guid.Empty) throw new ArgumentException("CustomerId is required.");
        if (amount <= 0) throw new ArgumentOutOfRangeException(nameof(amount));
        Id = Guid.NewGuid(); CustomerId = customerId; Amount = amount; Status = OrderStatus.Created; CreatedAtUtc = DateTime.UtcNow;
    }
    public Guid Id { get; private set; }
    public Guid CustomerId { get; private set; }
    public decimal Amount { get; private set; }
    public OrderStatus Status { get; private set; }
    public DateTime CreatedAtUtc { get; private set; }
}
