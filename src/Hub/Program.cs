using System.Security.Cryptography;
using System.Text;
using DataHub.Contracts;
using DataHub.Platform;

var builder = WebApplication.CreateBuilder(args);
var apiKey = builder.Configuration["ApiKey"];
if (string.IsNullOrWhiteSpace(apiKey)) {
  throw new InvalidOperationException("ApiKey is required");
}
var federation = builder.Configuration.GetSection("Federation").Get<FederationOptions>() ?? new();
federation.Validate(apiKey);
builder.Services.AddSingleton(federation);
var reads = builder.Configuration.GetSection("Read").Get<ReadOptions>() ?? new();
reads.Validate();
builder.Services.AddSingleton(reads);
builder.Services.AddGrpc(options => {
  options.Interceptors.Add<RpcErrorInterceptor>();
  options.MaxReceiveMessageSize = DataProtocol.MaxGrpcMessageBytes;
  options.MaxSendMessageSize = DataProtocol.MaxGrpcMessageBytes;
});
builder.Services.AddControllers();
var platform = builder.Configuration.GetSection("Platform").Get<PlatformOptions>() ?? new();
if (!DataProtocol.IsIdentifier(platform.Id) || !DataProtocol.IsIdentifier(platform.ServiceId)) {
  throw new InvalidOperationException("Hub platform identity is required");
}

builder.Services.AddSingleton(platform);
builder.Services.AddSingleton<IPlatformGrpcClientFactory>(
    _ => new StaticPlatformGrpcClientFactory(platform));
builder.Services.AddSingleton(
    _ => new ExternalNodeClients(
        builder.Configuration.GetSection("ExternalNodes").Get<ExternalNode[]>() ?? []));
builder.Services.AddSingleton<IReadRequestPolicy, DevelopmentReadRequestPolicy>();
builder.Services.AddSingleton<IReadAuditSink, LoggingReadAuditSink>();
builder.Services.AddSingleton<IProviderReader, GrpcProviderReader>();
builder.Services.AddSingleton<HubState>();
builder.Services.AddSingleton<FederationNetwork>();
builder.Services.AddSingleton<HubCoordinator>();
builder.Services.AddHostedService<FederationNegotiator>();
var app = builder.Build();
app.Use(async (context, next) => {
  // A federation credential is only valid for FederationApi, never for registration/client APIs.
  bool isFederation = context.Request.Path.StartsWithSegments("/datahub.FederationApi");
  var expected = isFederation
      ? federation.TrustedHubs.SingleOrDefault(
          p => p.HubId == context.Request.Headers["x-federation-peer"].ToString())?.ApiKey
      : apiKey;
  var headerName = isFederation ? "x-federation-key" : "x-api-key";
  var supplied = context.Request.Headers[headerName].ToString();
  if (string.IsNullOrEmpty(expected) ||
      !CryptographicOperations.FixedTimeEquals(
          Encoding.UTF8.GetBytes(expected), Encoding.UTF8.GetBytes(supplied))) {
    context.Response.StatusCode = 401;
    return;
  }
  await next();
});
app.MapControllers();
app.MapGrpcService<RegistryService>();
app.MapGrpcService<HubService>();
app.MapGrpcService<HubRelayService>();
app.MapGrpcService<FederationService>();
app.Run();
