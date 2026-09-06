using KafkaOutboxSample.Common;
using Microsoft.EntityFrameworkCore;
using NotificationService.Application;
using NotificationService.Infrastructure;
using Serilog;

var builder = WebApplication.CreateBuilder(args);
builder.Host.UseSerilog((context, configuration) => configuration.ReadFrom.Configuration(context.Configuration).Enrich.FromLogContext().WriteTo.Console());
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.Configure<OutboxOptions>(builder.Configuration.GetSection("Outbox"));
builder.Services.AddDbContext<InboxDbContext>(options => options.UseSqlServer(builder.Configuration.GetConnectionString("SqlServer")));
builder.Services.AddScoped<INotificationHandler, NotificationHandler>();
builder.Services.AddHostedService<NotificationConsumerWorker>();
builder.Services.AddHostedService<InventoryConsumerWorker>();

// Add services to the container.

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddHealthChecks().AddDbContextCheck<InboxDbContext>();

var app = builder.Build();

await using (var scope = app.Services.CreateAsyncScope())
{
	var db = scope.ServiceProvider.GetRequiredService<InboxDbContext>();
	await db.Database.MigrateAsync();
}

app.UseSerilogRequestLogging();
app.UseSwagger(); app.UseSwaggerUI();

app.MapControllers();
app.MapHealthChecks("/health");

app.Run();
