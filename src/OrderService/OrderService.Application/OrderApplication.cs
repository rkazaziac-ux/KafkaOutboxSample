using KafkaOutboxSample.Contracts;
using Microsoft.Extensions.Logging;
using OrderService.Domain;

namespace OrderService.Application;

public sealed record CreateOrderCommand(Guid CustomerId, decimal Amount);
public sealed record OrderDto(Guid Id, Guid CustomerId, decimal Amount, string Status, DateTime CreatedAtUtc);

public interface IOrderService
{
    Task<OrderDto> CreateAsync(CreateOrderCommand command, CancellationToken cancellationToken);
    Task<OrderDto?> GetAsync(Guid id, CancellationToken cancellationToken);
}

public interface IOrderRepository
{
    Task<Order?> GetAsync(Guid id, CancellationToken cancellationToken);
    Task AddAsync(Order order, CancellationToken cancellationToken);
    Task AddOutboxAsync(OrderCreatedIntegrationEvent integrationEvent, CancellationToken cancellationToken);
    Task SaveChangesAsync(CancellationToken cancellationToken);
}

public sealed class OrderService(IOrderRepository repository, ILogger<OrderService> logger) : IOrderService
{
    public async Task<OrderDto> CreateAsync(CreateOrderCommand command, CancellationToken cancellationToken)
    {
        var order = new Order(command.CustomerId, command.Amount);
        await repository.AddAsync(order, cancellationToken);
        var integrationEvent = new OrderCreatedIntegrationEvent(Guid.NewGuid(), order.Id, order.CustomerId, order.Amount, DateTime.UtcNow);
        await repository.AddOutboxAsync(integrationEvent, cancellationToken);
        await repository.SaveChangesAsync(cancellationToken);
        logger.LogInformation("OrderCreated {OrderId} {EventId}", order.Id, integrationEvent.EventId);
        return Map(order);
    }

    public async Task<OrderDto?> GetAsync(Guid id, CancellationToken cancellationToken) =>
        (await repository.GetAsync(id, cancellationToken)) is { } order ? Map(order) : null;

    private static OrderDto Map(Order order) => new(order.Id, order.CustomerId, order.Amount, order.Status.ToString(), order.CreatedAtUtc);
}
