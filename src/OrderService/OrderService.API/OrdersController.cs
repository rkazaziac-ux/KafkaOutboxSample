using Microsoft.AspNetCore.Mvc;
using OrderService.Application;

namespace OrderService.API;

[ApiController, Route("api/orders")]
public sealed class OrdersController(IOrderService service) : ControllerBase
{
    [HttpPost]
    public async Task<ActionResult<OrderDto>> Create(CreateOrderRequest request, CancellationToken cancellationToken)
    {
        var result = await service.CreateAsync(new CreateOrderCommand(request.CustomerId, request.Amount), cancellationToken);
        return CreatedAtAction(nameof(Get), new { id = result.Id }, result);
    }
    [HttpGet("{id:guid}")]
    public async Task<ActionResult<OrderDto>> Get(Guid id, CancellationToken cancellationToken) => await service.GetAsync(id, cancellationToken) is { } result ? Ok(result) : NotFound();
}
public sealed record CreateOrderRequest(Guid CustomerId, decimal Amount);
