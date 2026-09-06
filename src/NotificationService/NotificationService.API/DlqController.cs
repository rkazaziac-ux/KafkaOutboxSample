using Confluent.Kafka;
using KafkaOutboxSample.Common;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace NotificationService.API;

[ApiController, Route("api/dlq")]
public sealed class DlqController(IOptions<KafkaOptions> options) : ControllerBase
{
    [HttpGet("health")]
    public IActionResult Get() => Ok(new { topic = options.Value.Topics.OrderCreatedDlq, status = "available" });
}
