using KafkaOutboxSample.Common;
using KafkaOutboxSample.Messaging;
using Microsoft.EntityFrameworkCore;
using OrderService.Application;
using OrderService.Infrastructure;
using Serilog;

var builder = WebApplication.CreateBuilder(args);
builder.Host.UseSerilog((context, configuration) => configuration.ReadFrom.Configuration(context.Configuration).Enrich.FromLogContext().WriteTo.Console());
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.Configure<OutboxOptions>(builder.Configuration.GetSection("Outbox"));
builder.Services.AddDbContext<OrderDbContext>(options => options.UseSqlServer(builder.Configuration.GetConnectionString("SqlServer")));
builder.Services.AddScoped<IOrderRepository, OrderRepository>();
builder.Services.AddScoped<IOrderService, OrderService.Application.OrderService>();
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddHostedService<OutboxWorker>();

// Add services to the container.

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddHealthChecks().AddDbContextCheck<OrderDbContext>();

var app = builder.Build();

await using (var scope = app.Services.CreateAsyncScope())
{
	var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
	await db.Database.MigrateAsync();
}

// Configure the HTTP request pipeline.

app.UseSerilogRequestLogging();
app.UseSwagger(); app.UseSwaggerUI();

app.UseAuthorization();

app.MapControllers();
app.MapHealthChecks("/health");

app.Run();
