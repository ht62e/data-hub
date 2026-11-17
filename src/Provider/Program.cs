using System.Security.Cryptography;
using System.Text;
using DataHub.Contracts;
using DataHub.Platform;

var builder = WebApplication.CreateBuilder(args);
var apiKey = builder.Configuration["ApiKey"];
if (string.IsNullOrWhiteSpace(apiKey)) {
  throw new InvalidOperationException("ApiKey is required");
}
var sourceId = builder.Configuration["SourceId"];
var displayName = builder.Configuration["DisplayName"];
if (!DataProtocol.IsIdentifier(sourceId) || string.IsNullOrWhiteSpace(displayName) ||
    displayName.Length > 200) {
  throw new InvalidOperationException("Valid SourceId and DisplayName (max 200) are required");
}
builder.Services.AddGrpc(options => {
  options.MaxReceiveMessageSize = DataProtocol.MaxGrpcMessageBytes;
  options.MaxSendMessageSize = DataProtocol.MaxGrpcMessageBytes;
});
var dataLimits = builder.Configuration.GetSection("DataLimits").Get<ReadPayloadLimits>() ?? new();
dataLimits.Validate();
builder.Services.AddSingleton(dataLimits);
var externalNodeId = builder.Configuration["ExternalNodeId"];
ProviderAddress identity;
if (string.IsNullOrEmpty(externalNodeId)) {
  var platform = builder.Configuration.GetSection("Platform").Get<PlatformOptions>() ?? new();
  identity = new ProviderAddress.Platform(new(platform.Id, platform.ServiceId));
  var hub = new ServiceAddress(
      builder.Configuration["HubPlatformId"] ?? platform.Id,
      builder.Configuration["HubServiceId"] ?? "data-hub");
  builder.Services.AddSingleton<IPlatformGrpcClientFactory>(
      _ => new StaticPlatformGrpcClientFactory(platform));
  builder.Services.AddSingleton(sp => new Registry.RegistryClient(
      sp.GetRequiredService<IPlatformGrpcClientFactory>().Create(hub)));
} else {
  identity = new ProviderAddress.External(externalNodeId);
  var endpoint = builder.Configuration["HubEndpoint"] ?? "";
  var hubKey = builder.Configuration["HubApiKey"];
  if (string.IsNullOrWhiteSpace(hubKey)) {
    throw new InvalidOperationException("HubApiKey is required for external provider registration");
  }
  builder.Services.AddSingleton<GrpcConnections>();
  builder.Services.AddSingleton(sp => new Registry.RegistryClient(
      sp.GetRequiredService<GrpcConnections>().Create(endpoint, hubKey)));
}
if (!identity.IsValid) {
  throw new InvalidOperationException("Valid provider identity is required");
}
builder.Services.AddSingleton(new ProviderRegistration(sourceId!, displayName, identity));
builder.Services.AddHttpClient<ILoadMonitor, HttpLoadMonitor>(
    client => client.Timeout = TimeSpan.FromSeconds(2));
builder.Services.AddSingleton<ReadLimiter>();
builder.Services.AddSingleton<ILogSource>(_ =>
    builder.Configuration["SourceType"]?.ToLowerInvariant() switch {
      "file" => new FileLogSource(builder.Configuration, builder.Environment),
      "postgres" => new PostgresLogSource(builder.Configuration, dataLimits),
      _ => throw new InvalidOperationException("SourceType must be File or Postgres")
    });
builder.Services.AddSingleton<LogReader>();
builder.Services.AddHostedService<RegistrationWorker>();
var app = builder.Build();
app.Use(async (context, next) => {
  var supplied = context.Request.Headers["x-api-key"].ToString();
  if (!CryptographicOperations.FixedTimeEquals(
      Encoding.UTF8.GetBytes(apiKey), Encoding.UTF8.GetBytes(supplied))) {
    context.Response.StatusCode = 401;
    return;
  }
  await next();
});
app.MapGrpcService<ProviderService>();
app.Run();
